package lsm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Sequence numbers must be monotonic across a restart, because compaction
// resolves two versions of a key by keeping the higher one. A counter that
// restarts at zero makes "newer" mean "written earlier in a longer-lived
// process", which is how an acknowledged write is silently replaced by the value
// it overwrote.
//
// These tests build the condition through the public API only — write, close,
// reopen, write, compact — because that is what a node restart does.

// openForEpoch opens the store the way a restarted node does, with a compaction
// threshold low enough that the L0 files two epochs leave behind are merged.
func openForEpoch(t *testing.T, dir string) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(dir, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		WithCompactThreshold(2))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return l
}

// waitForCompaction blocks until the L0 backlog has been merged into a single L1
// file, which is what the compactor produces from "all of L0 plus all of L1".
func waitForCompaction(t *testing.T, l *LSMTree) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		l.mu.RLock()
		l0, l1 := len(l.l0), len(l.l1)
		l.mu.RUnlock()
		if l0 == 0 && l1 == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	l.mu.RLock()
	l0, l1 := len(l.l0), len(l.l1)
	l.mu.RUnlock()
	t.Fatalf("compaction did not finish: l0=%d l1=%d", l0, l1)
}

// TestCompactionKeepsTheNewerWriteAcrossARestart is the repro, and the reason
// the sequence counter is now seeded from disk.
//
// Epoch 1 writes filler so that its last write to "hot" carries a high sequence
// number. Epoch 2 restarts the store and overwrites "hot" — with an unseeded
// counter that write gets sequence 1. Both epochs flush an L0 SSTable on close,
// and the compaction that merges them keeps the entry with the higher sequence
// number: epoch 1's. The acknowledged epoch-2 value is then gone from disk, not
// merely shadowed.
//
// Revert check: with the seeding removed from NewLSMTree, the final Get returns
// "epoch1" — an acknowledged write replaced by the value it overwrote.
func TestCompactionKeepsTheNewerWriteAcrossARestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Epoch 1: filler writes push the sequence counter up, then the value that
	// must lose.
	e1 := openForEpoch(t, dir)
	for i := 0; i < 32; i++ {
		if _, err := e1.Put(ctx, fmt.Sprintf("filler-%03d", i), []byte("x")); err != nil {
			t.Fatalf("epoch 1 filler put: %v", err)
		}
	}
	if _, err := e1.Put(ctx, "hot", []byte("epoch1")); err != nil {
		t.Fatalf("epoch 1 put: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("epoch 1 close: %v", err)
	}

	// Epoch 2: one acknowledged write to the same key, then a clean close. This
	// is the write a client was told succeeded.
	e2 := openForEpoch(t, dir)
	if _, err := e2.Put(ctx, "hot", []byte("epoch2")); err != nil {
		t.Fatalf("epoch 2 put: %v", err)
	}
	if v, err := e2.Get(ctx, "hot"); err != nil || string(v) != "epoch2" {
		t.Fatalf("epoch 2 read back = (%q, %v), want epoch2", v, err)
	}
	if err := e2.Close(); err != nil {
		t.Fatalf("epoch 2 close: %v", err)
	}

	// Epoch 3: two L0 files at a threshold of 2 arms compaction at open.
	e3 := openForEpoch(t, dir)
	defer e3.Close()
	waitForCompaction(t, e3)

	v, err := e3.Get(ctx, "hot")
	if err != nil {
		t.Fatalf("read after compaction: %v", err)
	}
	if string(v) != "epoch2" {
		t.Errorf("compaction resurrected a stale value: got %q, want %q — the "+
			"acknowledged epoch-2 write was replaced by the value it overwrote",
			v, "epoch2")
	}
}

// TestCompactionKeepsATombstoneAcrossARestart is the same defect on the delete
// path, and the more dangerous half: compaction drops tombstones because L1 is
// the bottom level, so a tombstone that loses to a stale value does not merely
// lose — the key comes back.
func TestCompactionKeepsATombstoneAcrossARestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	e1 := openForEpoch(t, dir)
	for i := 0; i < 32; i++ {
		if _, err := e1.Put(ctx, fmt.Sprintf("filler-%03d", i), []byte("x")); err != nil {
			t.Fatalf("epoch 1 filler put: %v", err)
		}
	}
	if _, err := e1.Put(ctx, "doomed", []byte("epoch1")); err != nil {
		t.Fatalf("epoch 1 put: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("epoch 1 close: %v", err)
	}

	e2 := openForEpoch(t, dir)
	if _, err := e2.Delete(ctx, "doomed"); err != nil {
		t.Fatalf("epoch 2 delete: %v", err)
	}
	if _, err := e2.Get(ctx, "doomed"); err != ErrNotFound {
		t.Fatalf("epoch 2 read after delete = %v, want ErrNotFound", err)
	}
	if err := e2.Close(); err != nil {
		t.Fatalf("epoch 2 close: %v", err)
	}

	e3 := openForEpoch(t, dir)
	defer e3.Close()
	waitForCompaction(t, e3)

	if v, err := e3.Get(ctx, "doomed"); err != ErrNotFound {
		t.Errorf("compaction resurrected a deleted key: got (%q, %v), want ErrNotFound", v, err)
	}
}

// stripManifestSeqNums rewrites the manifest as a pre-upgrade version would have
// written it: every field intact except the per-SSTable sequence number. This is
// the state every existing data directory is in on its first open after the fix,
// so it needs a test rather than an assumption.
func stripManifestSeqNums(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "manifest.log")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var out bytes.Buffer
	dec := json.NewDecoder(bytes.NewReader(body))
	enc := json.NewEncoder(&out)
	stripped := 0
	for dec.More() {
		var ev map[string]any
		if err := dec.Decode(&ev); err != nil {
			t.Fatalf("decode manifest event: %v", err)
		}
		if _, ok := ev["max_seq_num"]; ok {
			delete(ev, "max_seq_num")
			stripped++
		}
		if err := enc.Encode(ev); err != nil {
			t.Fatalf("encode manifest event: %v", err)
		}
	}
	if stripped == 0 {
		t.Fatal("no sequence numbers to strip — the fixture is not exercising the fallback")
	}
	if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

// TestLegacyManifestRecoversTheWriteOrderByScanning covers the upgrade path: a
// data directory written before the manifest recorded sequence numbers must still
// reopen with a counter above everything on disk, because the alternative is the
// same silent resurrection on exactly the volumes that have the most history.
func TestLegacyManifestRecoversTheWriteOrderByScanning(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	e1 := openForEpoch(t, dir)
	for i := 0; i < 32; i++ {
		if _, err := e1.Put(ctx, fmt.Sprintf("filler-%03d", i), []byte("x")); err != nil {
			t.Fatalf("epoch 1 filler put: %v", err)
		}
	}
	if _, err := e1.Put(ctx, "hot", []byte("epoch1")); err != nil {
		t.Fatalf("epoch 1 put: %v", err)
	}
	wrote := e1.seqNum.Load()
	if err := e1.Close(); err != nil {
		t.Fatalf("epoch 1 close: %v", err)
	}

	stripManifestSeqNums(t, dir)

	// The manifest must now report itself incomplete, which is what sends the
	// engine to the scan.
	m, err := OpenManifest(filepath.Join(dir, "manifest.log"))
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	if _, complete := m.MaxSeqNum(); complete {
		t.Fatal("a manifest with no recorded sequence numbers reports itself complete")
	}

	e2 := openForEpoch(t, dir)
	if got := e2.seqNum.Load(); got < wrote {
		t.Errorf("counter seeded at %d after scanning, below the %d on disk", got, wrote)
	}
	if _, err := e2.Put(ctx, "hot", []byte("epoch2")); err != nil {
		t.Fatalf("epoch 2 put: %v", err)
	}
	if err := e2.Close(); err != nil {
		t.Fatalf("epoch 2 close: %v", err)
	}

	e3 := openForEpoch(t, dir)
	defer e3.Close()
	waitForCompaction(t, e3)
	v, err := e3.Get(ctx, "hot")
	if err != nil {
		t.Fatalf("read after compaction: %v", err)
	}
	if string(v) != "epoch2" {
		t.Errorf("legacy data dir resurrected a stale value: got %q, want %q", v, "epoch2")
	}
}

// TestSequenceNumbersAreMonotonicAcrossARestart states the invariant directly,
// so a future change that reintroduces a zeroed counter fails here with a
// readable reason rather than only through a compaction test.
func TestSequenceNumbersAreMonotonicAcrossARestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	e1 := openForEpoch(t, dir)
	for i := 0; i < 10; i++ {
		if _, err := e1.Put(ctx, fmt.Sprintf("k-%02d", i), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	before := e1.seqNum.Load()
	if before == 0 {
		t.Fatal("epoch 1 wrote nothing")
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2 := openForEpoch(t, dir)
	defer e2.Close()
	if got := e2.seqNum.Load(); got < before {
		t.Errorf("sequence counter reopened at %d, below the %d already on disk: "+
			"every write in this epoch will look older than the data it replaces",
			got, before)
	}
}
