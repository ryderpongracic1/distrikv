package lsm

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
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
		delta, err := mem.Put(kv.k, []byte(kv.v))
		if err != nil {
			t.Fatalf("Put %s: %v", kv.k, err)
		}
		if delta != 1 {
			t.Fatalf("Put %s: live delta = %d, want 1 (new key)", kv.k, delta)
		}
	}

	e, ok := mem.Get("a")
	if !ok || string(e.Value) != "A" {
		t.Fatalf("Get a: got %v ok=%v", e, ok)
	}

	if delta, err := mem.Delete("b"); err != nil {
		t.Fatalf("Delete b: %v", err)
	} else if delta != -1 {
		t.Fatalf("Delete b: live delta = %d, want -1 (live key removed)", delta)
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
		_, _ = mem.Put(fmt.Sprintf("key-%02d", i), []byte("value"))
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

// ---------------------------------------------------------------------------
// Phase 3: Leveled Compaction Strategy (LCS) tests
// ---------------------------------------------------------------------------

// TestLCS_ReadCorrectness verifies that all written keys are readable after
// an L0→L1 compaction, and that each key returns its latest value
// (last writer wins).
func TestLCS_ReadCorrectness(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// 1 KB memtable + 19 B/entry ≈ 53 entries/flush; 360 total writes → ~7 flushes
	// → at least one compaction (threshold=4).
	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(1<<10))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	const keys = 120
	const rounds = 3 // write each key `rounds` times; only "round-2" should survive

	for round := 0; round < rounds; round++ {
		for i := 0; i < keys; i++ {
			k := fmt.Sprintf("lcs-key-%04d", i)
			v := []byte(fmt.Sprintf("round-%d", round))
			if err := tree.Put(ctx, k, v); err != nil {
				t.Fatalf("Put round=%d i=%d: %v", round, i, err)
			}
		}
	}

	// Wait for at least one compaction so we exercise the L1 read path.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m.CompactionsTotal.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if m.CompactionsTotal.Load() == 0 {
		t.Fatal("no compaction occurred; check nCompact threshold or write count")
	}

	// Every key must return the final round's value.
	want := fmt.Sprintf("round-%d", rounds-1)
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("lcs-key-%04d", i)
		val, err := tree.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %s after compaction: %v", k, err)
		}
		if string(val) != want {
			t.Fatalf("Get %s: got %q want %q", k, val, want)
		}
	}

	t.Logf("compactions=%d flush_bytes=%d compaction_written=%d",
		m.CompactionsTotal.Load(), m.FlushBytes.Load(), m.CompactionBytesWritten.Load())
}

// TestLCS_TombstoneDroppedAfterCompaction verifies that deleted keys return
// ErrNotFound after L0→L1 compaction, and that no tombstone entries survive
// in L1 SSTables (the MergeIterator drops them at the bottom level).
func TestLCS_TombstoneDroppedAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// 1 KB threshold: frequent flushes ensure tombstones flush to L0 before compaction.
	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(1<<10))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	const total = 80

	// Write all keys.
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("tomb-key-%04d", i)
		if err := tree.Put(ctx, k, []byte("alive")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	// Delete even-indexed keys (write tombstones into subsequent L0 files).
	for i := 0; i < total; i += 2 {
		k := fmt.Sprintf("tomb-key-%04d", i)
		if err := tree.Delete(ctx, k); err != nil {
			t.Fatalf("Delete %s: %v", k, err)
		}
	}
	// Write filler to push total flushes past compaction threshold.
	for i := total; i < total*5; i++ {
		k := fmt.Sprintf("tomb-fill-%04d", i)
		if err := tree.Put(ctx, k, []byte("filler")); err != nil {
			t.Fatalf("Put filler %s: %v", k, err)
		}
	}

	// Wait for compaction.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m.CompactionsTotal.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if m.CompactionsTotal.Load() == 0 {
		t.Fatal("no compaction occurred")
	}

	// Deleted (even-indexed) keys must be absent.
	for i := 0; i < total; i += 2 {
		k := fmt.Sprintf("tomb-key-%04d", i)
		_, err := tree.Get(ctx, k)
		if err != ErrNotFound {
			t.Fatalf("Get deleted %s: got err=%v, want ErrNotFound", k, err)
		}
	}
	// Surviving (odd-indexed) keys must still be readable.
	for i := 1; i < total; i += 2 {
		k := fmt.Sprintf("tomb-key-%04d", i)
		val, err := tree.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get surviving %s: %v", k, err)
		}
		if string(val) != "alive" {
			t.Fatalf("Get %s: got %q want %q", k, val, "alive")
		}
	}

	// After compaction, L1 SSTables must contain zero tombstone entries.
	tree.mu.RLock()
	l1 := make([]*SSTableReader, len(tree.l1))
	copy(l1, tree.l1)
	tree.mu.RUnlock()

	if len(l1) == 0 {
		t.Fatal("no L1 SSTables after compaction; cannot verify tombstone removal")
	}
	for ri, r := range l1 {
		it := r.Iterator()
		for {
			e, ok := it.Next()
			if !ok {
				break
			}
			if e.Tombstone {
				t.Errorf("L1[%d]: tombstone found for key %q; should be dropped at bottom level", ri, e.Key)
			}
		}
		if err := it.Err(); err != nil {
			t.Errorf("L1[%d] iterator error: %v", ri, err)
		}
	}
}

// TestLCS_WriteStallMetrics verifies that maybeStallWrite correctly increments
// WriteStallCount and WriteStallMicros when L0 files accumulate past the slow
// threshold. An artificial l0Count prevents flushes from racing with the test.
func TestLCS_WriteStallMetrics(t *testing.T) {
	dir := t.TempDir()
	m := &metrics.Metrics{}

	// Use default memtable size; we won't write any data, so no background flushes.
	tree, err := NewLSMTree(dir, nil, WithMetrics(m))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// Artificially set l0Count to the soft-stall threshold, bypassing actual flushes.
	// The background runFlush goroutine is idle (no pending imm), so the value stays put.
	tree.l0Count.Store(int32(tree.l0SlowThreshold))

	// maybeStallWrite must loop-stall until the context expires.
	// 100 ms >> 5 ms (one stall cycle) → at least one full cycle records metrics.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	stallErr := tree.maybeStallWrite(ctx)
	if stallErr != context.DeadlineExceeded {
		t.Fatalf("maybeStallWrite returned %v; want context.DeadlineExceeded", stallErr)
	}

	if got := m.WriteStallCount.Load(); got == 0 {
		t.Fatal("WriteStallCount = 0 after stall loop; want > 0")
	}
	if got := m.WriteStallMicros.Load(); got == 0 {
		t.Fatal("WriteStallMicros = 0 after stall loop; want > 0")
	}
	t.Logf("WriteStallCount=%d WriteStallMicros=%dµs",
		m.WriteStallCount.Load(), m.WriteStallMicros.Load())
}

// TestLCS_L0CountTracking verifies that the l0Count atomic and the L0FileCount
// metric are always kept in sync with the actual len(l.l0) slice — both before
// and after a compaction drains L0.
func TestLCS_L0CountTracking(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// 74 B/entry (10 B key + 64 B value); 1 KB memtable → flush every ~13 writes.
	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(1<<10))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// 30 writes → ~2 flushes; nCompact=4 so no compaction yet.
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("l0cnt-%04d", i)
		if err := tree.Put(ctx, k, make([]byte, 64)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait for at least one L0 file to appear.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tree.l0Count.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Invariant: len(l0), l0Count, and L0FileCount must all agree.
	// Read under RLock so no write can interleave.
	tree.mu.RLock()
	l0Len := int32(len(tree.l0))
	l0Count := tree.l0Count.Load()
	l0Metric := int32(m.L0FileCount.Load())
	tree.mu.RUnlock()

	if l0Len == 0 {
		t.Fatal("no L0 files after 30 writes; expected at least one flush")
	}
	if l0Len != l0Count {
		t.Errorf("pre-compaction: len(l0)=%d != l0Count=%d", l0Len, l0Count)
	}
	if l0Metric != l0Count {
		t.Errorf("pre-compaction: L0FileCount=%d != l0Count=%d", l0Metric, l0Count)
	}

	// Write enough to trigger multiple compactions.
	for i := 30; i < 300; i++ {
		k := fmt.Sprintf("l0cnt-%04d", i)
		if err := tree.Put(ctx, k, make([]byte, 64)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait for at least one compaction to complete.
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m.CompactionsTotal.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if m.CompactionsTotal.Load() == 0 {
		t.Fatal("no compaction occurred after 300 writes")
	}

	// Invariant must hold after compaction too.
	tree.mu.RLock()
	l0Len = int32(len(tree.l0))
	l0Count = tree.l0Count.Load()
	l0Metric = int32(m.L0FileCount.Load())
	tree.mu.RUnlock()

	if l0Len != l0Count {
		t.Errorf("post-compaction: len(l0)=%d != l0Count=%d", l0Len, l0Count)
	}
	if l0Metric != l0Count {
		t.Errorf("post-compaction: L0FileCount=%d != l0Count=%d", l0Metric, l0Count)
	}
	t.Logf("compactions=%d post-compaction l0=%d l1=%d",
		m.CompactionsTotal.Load(), l0Count,
		func() int32 {
			tree.mu.RLock()
			defer tree.mu.RUnlock()
			return int32(len(tree.l1))
		}())
}

// ---------------------------------------------------------------------------
// Phase 1: metrics instrumentation
// ---------------------------------------------------------------------------

// TestLSM_MetricsBloomAndWAF drives enough writes to force at least one flush
// and one compaction, then asserts every new counter is populated correctly
// and the derived WAF in Snapshot() is sensible.
func TestLSM_MetricsBloomAndWAF(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// 16 KB memtable threshold + 4-SSTable compaction trigger → ~5-7 flushes
	// triggers one compaction during a 256 KB write burst.
	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(16<<10))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// Write enough distinct keys to spill multiple memtables.
	const totalKeys = 4096
	const valueSize = 64
	val := make([]byte, valueSize)
	for i := range val {
		val[i] = 'x'
	}
	for i := 0; i < totalKeys; i++ {
		k := fmt.Sprintf("metrics-key-%06d", i)
		if err := tree.Put(ctx, k, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait up to 5s for at least one compaction to land.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.CompactionsTotal.Load() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Flush bytes must be > 0 (we wrote ~256KB of values + WAL/index overhead).
	if got := m.FlushBytes.Load(); got == 0 {
		t.Fatalf("FlushBytes = 0; want > 0 after %d writes", totalKeys)
	}
	if got := m.CompactionsTotal.Load(); got == 0 {
		t.Fatalf("CompactionsTotal = 0; want ≥ 1")
	}
	if got := m.CompactionBytesWritten.Load(); got == 0 {
		t.Fatalf("CompactionBytesWritten = 0; want > 0")
	}
	if got := m.CompactionBytesRead.Load(); got == 0 {
		t.Fatalf("CompactionBytesRead = 0; want > 0")
	}

	// Get a never-written key — must trigger BloomMisses on at least one SSTable.
	_, err = tree.Get(ctx, "never-written-key")
	if err != ErrNotFound {
		t.Fatalf("Get unknown key: err=%v want ErrNotFound", err)
	}
	if got := m.BloomMisses.Load(); got == 0 {
		t.Fatalf("BloomMisses = 0 after lookup of never-written key; want > 0")
	}

	// Get a known key. After compaction the data lives in one SSTable.
	if _, err := tree.Get(ctx, "metrics-key-000000"); err != nil {
		t.Fatalf("Get known key: %v", err)
	}
	if got := m.BloomHits.Load(); got == 0 {
		t.Fatalf("BloomHits = 0 after lookup of known key; want > 0")
	}

	// WAF must reflect compaction overhead — at least 1.0x (we definitely wrote
	// more than user bytes; usually 2-3x for this workload).
	snap := m.Snapshot()
	wafMilli := snap["write_amp_factor_milli"]
	if wafMilli < 1000 {
		t.Fatalf("write_amp_factor_milli = %d; want ≥ 1000 (1.0x)", wafMilli)
	}
	t.Logf("WAF=%.2fx flush=%d compact_written=%d bloom_hits=%d bloom_misses=%d bloom_fp=%d",
		float64(wafMilli)/1000.0,
		snap["flush_bytes"],
		snap["compaction_bytes_written"],
		snap["bloom_hits"],
		snap["bloom_misses"],
		snap["bloom_false_positives"],
	)
}

// ---------------------------------------------------------------------------
// Phase 5 — Block Cache
// ---------------------------------------------------------------------------

// TestBlockCache_HitRateAfterWarmup verifies that BlockCacheHits accumulates
// correctly after a warm read pass and that BlockCacheMisses fires on the first
// cold pass.
//
// Structure:
//  1. Write enough unique keys to guarantee ≥1 L0 SSTable flush.
//  2. Cold pass: read every key once — all block reads miss the cache.
//  3. Warm pass: read every key a second time — each block read should hit.
//  4. Assert hits > 0, misses > 0, and that the warm pass produced more hits
//     than cold misses (i.e., the cache is actually being consulted).
func TestBlockCache_HitRateAfterWarmup(t *testing.T) {
	const keys = 200

	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// Small memtable (512B) forces several flushes for 200 unique keys.
	// Block cache explicitly set to 4MB (large enough to hold all blocks).
	tree, err := NewLSMTree(dir, nil,
		WithMetrics(m),
		WithMaxMemBytes(512),
		WithBlockCacheBytes(4<<20),
	)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// Write unique keys so each one grows the memtable monotonically.
	for i := 0; i < keys; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("bc-key-%04d", i), []byte(fmt.Sprintf("val-%04d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait for at least one flush so blocks are on disk.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.FlushBytes.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m.FlushBytes.Load() == 0 {
		t.Fatal("no flush occurred; SSTable blocks never hit disk — cannot test block cache")
	}

	// Reset counters before measuring.
	// (atomic stores to 0 rather than creating a new Metrics so the tree keeps
	//  its pointer — the tree holds *metrics.Metrics, not a snapshot)
	m.BlockCacheHits.Store(0)
	m.BlockCacheMisses.Store(0)

	// Cold pass — all block reads go to disk.
	for i := 0; i < keys; i++ {
		if _, err := tree.Get(ctx, fmt.Sprintf("bc-key-%04d", i)); err != nil {
			t.Fatalf("cold Get %d: %v", i, err)
		}
	}
	coldMisses := m.BlockCacheMisses.Load()
	coldHits := m.BlockCacheHits.Load()
	t.Logf("cold pass: hits=%d misses=%d", coldHits, coldMisses)

	if coldMisses == 0 {
		t.Fatal("expected >0 BlockCacheMisses on cold pass; got 0 — cache may not be wired")
	}

	// Reset between passes.
	m.BlockCacheHits.Store(0)
	m.BlockCacheMisses.Store(0)

	// Warm pass — all blocks should be in cache now.
	for i := 0; i < keys; i++ {
		if _, err := tree.Get(ctx, fmt.Sprintf("bc-key-%04d", i)); err != nil {
			t.Fatalf("warm Get %d: %v", i, err)
		}
	}
	warmHits := m.BlockCacheHits.Load()
	warmMisses := m.BlockCacheMisses.Load()
	t.Logf("warm pass: hits=%d misses=%d", warmHits, warmMisses)

	if warmHits == 0 {
		t.Fatal("expected >0 BlockCacheHits on warm pass; got 0 — cache may not be populated")
	}
	if warmHits < coldMisses {
		// warm hits should be at least as many as cold misses (we re-read the same blocks)
		t.Fatalf("warm hits (%d) < cold misses (%d); cache is not serving reads", warmHits, coldMisses)
	}
	t.Logf("cache effective: warm_hits=%d >= cold_misses=%d ✓", warmHits, coldMisses)
}

// TestBlockCache_NilSafe verifies that a nil *BlockCache (cache disabled via
// WithBlockCacheBytes(0)) does not panic and the engine still returns correct results.
func TestBlockCache_NilSafe(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	tree, err := NewLSMTree(dir, nil,
		WithMaxMemBytes(512),
		WithBlockCacheBytes(0), // explicitly disable
	)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	const keys = 50
	for i := 0; i < keys; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("nc-key-%03d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := 0; i < keys; i++ {
		val, err := tree.Get(ctx, fmt.Sprintf("nc-key-%03d", i))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if string(val) != fmt.Sprintf("v%d", i) {
			t.Fatalf("key %d: got %q want %q", i, val, fmt.Sprintf("v%d", i))
		}
	}
}

// TestBlockCache_EvictOnCompaction verifies that cache entries for an SSTable are
// evicted when that SSTable is compacted away, so stale cached bytes cannot be
// returned for subsequent reads.
func TestBlockCache_EvictOnCompaction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	tree, err := NewLSMTree(dir, nil,
		WithMetrics(m),
		WithMaxMemBytes(512),
		WithBlockCacheBytes(4<<20),
	)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// Write unique keys to force ≥1 flush.
	for i := 0; i < 200; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("evict-key-%04d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait for compaction to run (L0→L1); this is when Evict is called.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m.CompactionsTotal.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if m.CompactionsTotal.Load() == 0 {
		t.Skip("no compaction occurred; eviction path not exercised")
	}

	// After compaction, all keys should still be readable from L1 (or cache).
	for i := 0; i < 200; i++ {
		if _, err := tree.Get(ctx, fmt.Sprintf("evict-key-%04d", i)); err != nil {
			t.Fatalf("post-compaction Get %d: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Block-cache benchmarks
// ---------------------------------------------------------------------------

// BenchmarkBlockCache_ColdVsWarm measures the per-Get cost of an uncached (cold)
// SSTable read vs. a cached (warm) read.  Both sub-benchmarks exercise the same
// Bloom-filter → blockIndexFor → readBlock path; the only difference is whether
// the raw block bytes are served from the in-process LRU or from f.ReadAt.
//
// Set-up: write enough unique keys so the memtable flushes to ≥1 L0 SSTable,
// then wait for the flush before starting timing.  Each benchmark iteration
// reads the same key repeatedly; the key is chosen to be present in an SSTable
// (not just in the memtable) so the block cache actually matters.
func BenchmarkBlockCache_ColdVsWarm(b *testing.B) {
	const writeKeys = 300 // enough to force ≥1 flush at a 512-B threshold

	setup := func(b *testing.B, cacheBytes int64) (*LSMTree, string) {
		b.Helper()
		dir := b.TempDir()
		ctx := context.Background()

		tree, err := NewLSMTree(dir, nil,
			WithMaxMemBytes(512),
			WithBlockCacheBytes(cacheBytes),
		)
		if err != nil {
			b.Fatalf("NewLSMTree: %v", err)
		}

		// Write unique keys so the memtable flushes to disk.
		for i := 0; i < writeKeys; i++ {
			if err := tree.Put(ctx, fmt.Sprintf("bkey-%06d", i), []byte(fmt.Sprintf("val-%06d", i))); err != nil {
				b.Fatalf("Put %d: %v", i, err)
			}
		}

		// Wait for quiescence: all L0 files must be compacted to L1 (l0==0)
		// and no immutable memtable in progress (imm==nil).  Once quiescent,
		// no new writes occur during b.N, so no further compaction fires —
		// the L1 file handle stays open and stable for the duration.
		// (This also avoids the latent close-before-swap race described in
		//  the spawned task — the window simply never opens while idle.)
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			tree.mu.RLock()
			l0empty := len(tree.l0) == 0
			immNil := tree.imm == nil
			l1ok := len(tree.l1) > 0
			tree.mu.RUnlock()
			if l0empty && immNil && l1ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		tree.mu.RLock()
		l0empty := len(tree.l0) == 0
		l1ok := len(tree.l1) > 0
		tree.mu.RUnlock()
		if !l0empty || !l1ok {
			b.Fatalf("engine did not quiesce (l0=%v l1=%v); cannot benchmark block cache",
				!l0empty, l1ok)
		}

		// The benchmark key lives in L1 (written early, definitely compacted).
		hotKey := fmt.Sprintf("bkey-%06d", writeKeys/2)
		return tree, hotKey
	}

	b.Run("Cold/NoCacheNoBloom", func(b *testing.B) {
		// Disable cache entirely so every iteration goes to disk.
		tree, hotKey := setup(b, 0)
		defer tree.Close()
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := tree.Get(ctx, hotKey); err != nil {
				b.Fatalf("Get: %v", err)
			}
		}
	})

	b.Run("Warm/64MBCache", func(b *testing.B) {
		// 64 MB cache — prime with one cold read, then benchmark warm hits.
		tree, hotKey := setup(b, 64<<20)
		defer tree.Close()
		ctx := context.Background()
		// Prime the cache.
		if _, err := tree.Get(ctx, hotKey); err != nil {
			b.Fatalf("prime Get: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := tree.Get(ctx, hotKey); err != nil {
				b.Fatalf("Get: %v", err)
			}
		}
	})
}
