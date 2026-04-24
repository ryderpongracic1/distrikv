package lsm

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// testLSM creates a fresh LSMTree in a temp dir and registers cleanup.
func testLSM(t *testing.T) *LSMTree {
	t.Helper()
	dir := t.TempDir()
	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	return tree
}

// ---------------------------------------------------------------------------
// Memtable
// ---------------------------------------------------------------------------

func TestMemtable_PutGetDelete(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal-0001.log")
	w, err := storewal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	var seq atomic.Uint64
	mem := NewMemtable(w, walPath, &seq, 4<<20)

	for _, kv := range []struct{ k, v string }{{"b", "B"}, {"a", "A"}, {"c", "C"}} {
		if err := mem.Put(kv.k, []byte(kv.v)); err != nil {
			t.Fatalf("Put %s: %v", kv.k, err)
		}
	}

	e, ok := mem.Get("a")
	if !ok || string(e.Value) != "A" {
		t.Fatalf("Get a: got %v ok=%v", e, ok)
	}

	if err := mem.Delete("b"); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	e, ok = mem.Get("b")
	if !ok || !e.Tombstone {
		t.Fatalf("expected tombstone for b, got %v ok=%v", e, ok)
	}

	_, ok = mem.Get("missing")
	if ok {
		t.Fatal("Get missing key should return false")
	}
}

func TestMemtable_IsFull(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal-0001.log")
	w, _ := storewal.Open(walPath)
	defer w.Close()

	var seq atomic.Uint64
	const maxSize = 100
	mem := NewMemtable(w, walPath, &seq, maxSize)

	if mem.IsFull() {
		t.Fatal("should not be full initially")
	}
	for i := 0; i < 20; i++ {
		_ = mem.Put(fmt.Sprintf("key-%02d", i), []byte("value"))
	}
	if !mem.IsFull() {
		t.Fatal("should be full after exceeding maxSize")
	}
}

// ---------------------------------------------------------------------------
// Bloom filter
// ---------------------------------------------------------------------------

func TestBloom_NoFalseNegatives(t *testing.T) {
	const n = 1000
	bf := NewBloomFilter(n)
	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}
	for i := 0; i < n; i++ {
		if !bf.MightContain(fmt.Sprintf("key-%d", i)) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

func TestBloom_FalsePositiveRate(t *testing.T) {
	const inserted = 10_000
	const tested = 10_000
	bf := NewBloomFilter(inserted)
	for i := 0; i < inserted; i++ {
		bf.Add(fmt.Sprintf("inserted-%d", i))
	}
	fps := 0
	for i := 0; i < tested; i++ {
		if bf.MightContain(fmt.Sprintf("absent-%d", i)) {
			fps++
		}
	}
	fpr := float64(fps) / float64(tested)
	if fpr > 0.05 {
		t.Errorf("false positive rate %.3f exceeds 5%% threshold", fpr)
	}
}

func TestBloom_MarshalRoundTrip(t *testing.T) {
	bf := NewBloomFilter(100)
	for i := 0; i < 50; i++ {
		bf.Add(fmt.Sprintf("k%d", i))
	}
	raw := bf.Marshal()
	bf2, err := UnmarshalBloom(raw)
	if err != nil {
		t.Fatalf("UnmarshalBloom: %v", err)
	}
	for i := 0; i < 50; i++ {
		if !bf2.MightContain(fmt.Sprintf("k%d", i)) {
			t.Fatalf("false negative after round-trip for k%d", i)
		}
	}
}

// ---------------------------------------------------------------------------
// SSTable write / read
// ---------------------------------------------------------------------------

func TestSSTable_WriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sst-00000001.sst")

	const n = 1000
	w, err := NewSSTableWriter(path, n)
	if err != nil {
		t.Fatalf("NewSSTableWriter: %v", err)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := []byte(fmt.Sprintf("val-%d", i))
		if err := w.Write(Entry{Key: key, Value: val, SeqNum: uint64(i + 1)}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := OpenSSTableReader(path, 1)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		e, found, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Get %s: not found", key)
		}
		want := fmt.Sprintf("val-%d", i)
		if string(e.Value) != want {
			t.Fatalf("Get %s: got %q want %q", key, e.Value, want)
		}
	}
}

// ---------------------------------------------------------------------------
// LSM: flush and recovery
// ---------------------------------------------------------------------------

func TestLSM_FlushAndRecover(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: write keys and close.
	func() {
		tree, err := NewLSMTree(dir, nil)
		if err != nil {
			t.Fatalf("NewLSMTree (phase1): %v", err)
		}
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("k-%04d", i)
			if err := tree.Put(ctx, k, []byte(fmt.Sprintf("v-%d", i))); err != nil {
				t.Fatalf("Put %s: %v", k, err)
			}
		}
		if err := tree.Close(); err != nil {
			t.Fatalf("Close (phase1): %v", err)
		}
	}()

	// Phase 2: reopen and read all keys back.
	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree (phase2): %v", err)
	}
	defer tree.Close()

	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("k-%04d", i)
		val, err := tree.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %s after recover: %v", k, err)
		}
		want := fmt.Sprintf("v-%d", i)
		if string(val) != want {
			t.Fatalf("Get %s: got %q want %q", k, val, want)
		}
	}
}

// ---------------------------------------------------------------------------
// LSM: concurrent readers + rotating writer (race detector test)
// ---------------------------------------------------------------------------

func TestLSM_ImmRace(t *testing.T) {
	tree := testLSM(t)
	ctx := context.Background()

	const writers = 3
	const readers = 5
	const ops = 200

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := fmt.Sprintf("w%d-k%d", id, i)
				_ = tree.Put(ctx, k, []byte("value"))
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := fmt.Sprintf("w%d-k%d", id%writers, i)
				_, _ = tree.Get(ctx, k)
			}
		}(r)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// LSM: compaction merges correctly (latest value wins)
// ---------------------------------------------------------------------------

func TestLSM_CompactionMerges(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}

	const rounds = 5
	const keysPerRound = 20
	for round := 0; round < rounds; round++ {
		for i := 0; i < keysPerRound; i++ {
			k := fmt.Sprintf("key-%03d", i)
			v := []byte(fmt.Sprintf("round-%d-val-%d", round, i))
			if err := tree.Put(ctx, k, v); err != nil {
				t.Fatalf("Put round %d key %d: %v", round, i, err)
			}
		}
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tree2, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree (reopen): %v", err)
	}
	defer tree2.Close()

	for i := 0; i < keysPerRound; i++ {
		k := fmt.Sprintf("key-%03d", i)
		val, err := tree2.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		want := fmt.Sprintf("round-%d-val-%d", rounds-1, i)
		if string(val) != want {
			t.Fatalf("Get %s: got %q want %q", k, val, want)
		}
	}
}

// ---------------------------------------------------------------------------
// LSM: Snapshot / Restore
// ---------------------------------------------------------------------------

func TestLSM_SnapshotRestore(t *testing.T) {
	tree := testLSM(t)
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("snap-key-%d", i)
		if err := tree.Put(ctx, k, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	snap, err := tree.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 50 {
		t.Fatalf("Snapshot len: got %d want 50", len(snap))
	}

	dir2 := t.TempDir()
	tree2, err := NewLSMTree(dir2, nil)
	if err != nil {
		t.Fatalf("NewLSMTree restore: %v", err)
	}
	defer tree2.Close()

	if err := tree2.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("snap-key-%d", i)
		val, err := tree2.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %s after restore: %v", k, err)
		}
		want := fmt.Sprintf("v%d", i)
		if string(val) != want {
			t.Fatalf("Get %s: got %q want %q", k, val, want)
		}
	}
}
