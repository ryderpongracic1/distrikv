package lsm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Live-key count (served as /status key_count)
// ---------------------------------------------------------------------------

// TestLiveKeys_MemtableDeltas pins the delta rules while all writes are still
// resident in the active memtable, which is the case the approximation gets
// exactly right.
func TestLiveKeys_MemtableDeltas(t *testing.T) {
	tree := testLSM(t)
	ctx := context.Background()

	if got := tree.LiveKeys(); got != 0 {
		t.Fatalf("empty tree: LiveKeys() = %d, want 0", got)
	}

	for i := 0; i < 5; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("Put k%d: %v", i, err)
		}
	}
	if got := tree.LiveKeys(); got != 5 {
		t.Fatalf("after 5 distinct puts: LiveKeys() = %d, want 5", got)
	}

	// Overwrites of resident keys are neutral.
	for i := 0; i < 5; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("k%d", i), []byte("v2")); err != nil {
			t.Fatalf("overwrite k%d: %v", i, err)
		}
	}
	if got := tree.LiveKeys(); got != 5 {
		t.Fatalf("after overwrites: LiveKeys() = %d, want 5", got)
	}

	// Delete of a live key removes it; repeating is neutral.
	if err := tree.Delete(ctx, "k0"); err != nil {
		t.Fatalf("Delete k0: %v", err)
	}
	if err := tree.Delete(ctx, "k0"); err != nil {
		t.Fatalf("Delete k0 again: %v", err)
	}
	if got := tree.LiveKeys(); got != 4 {
		t.Fatalf("after deleting k0 twice: LiveKeys() = %d, want 4", got)
	}

	// Re-creating a tombstoned key counts again.
	if err := tree.Put(ctx, "k0", []byte("back")); err != nil {
		t.Fatalf("re-Put k0: %v", err)
	}
	if got := tree.LiveKeys(); got != 5 {
		t.Fatalf("after re-creating k0: LiveKeys() = %d, want 5", got)
	}
}

// TestLiveKeys_ClampedAtZero pins that blind deletes of absent keys drift the
// approximation down (the documented inaccuracy) but the published value never
// goes negative.
func TestLiveKeys_ClampedAtZero(t *testing.T) {
	tree := testLSM(t)
	ctx := context.Background()

	if err := tree.Put(ctx, "real", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := tree.Delete(ctx, fmt.Sprintf("ghost-%d", i)); err != nil {
			t.Fatalf("Delete ghost-%d: %v", i, err)
		}
	}
	if got := tree.LiveKeys(); got != 0 {
		t.Fatalf("LiveKeys() = %d, want 0 (clamped)", got)
	}

	// The clamp must not reset the internal counter: adding a key still reads as
	// zero because the ghost deletes over-decremented past it. (Only the clamped
	// output is observable here, which is the point — nothing outside the engine
	// can see the counter go negative.)
	if err := tree.Put(ctx, "another", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := tree.LiveKeys(); got != 0 {
		t.Fatalf("LiveKeys() = %d, want 0 — clamp must not reset the counter", got)
	}
}

// TestLiveKeys_SurvivesFlushAndReopen is the durability path: with a tiny
// memtable threshold the writes are spread across several flushed SSTables, so
// the reopened count must come from the manifest record plus replay of the one
// WAL that had not been flushed.
func TestLiveKeys_SurvivesFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	const keys = 200

	func() {
		tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(512))
		if err != nil {
			t.Fatalf("NewLSMTree (phase 1): %v", err)
		}
		for i := 0; i < keys; i++ {
			if err := tree.Put(ctx, fmt.Sprintf("k-%04d", i), []byte("value")); err != nil {
				t.Fatalf("Put k-%04d: %v", i, err)
			}
		}
		// Every key is distinct, so the approximation is exact here.
		if got := tree.LiveKeys(); got != keys {
			t.Fatalf("before close: LiveKeys() = %d, want %d", got, keys)
		}
		if err := tree.Close(); err != nil {
			t.Fatalf("Close (phase 1): %v", err)
		}
	}()

	// At least one flush must have happened for this test to be meaningful.
	assertSSTablesExist(t, dir)

	tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(512))
	if err != nil {
		t.Fatalf("NewLSMTree (phase 2): %v", err)
	}
	if got := tree.LiveKeys(); got != keys {
		t.Fatalf("after reopen: LiveKeys() = %d, want %d", got, keys)
	}

	// Cross-check against the authoritative live key set.
	snap, err := tree.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != keys {
		t.Fatalf("Snapshot has %d keys, want %d", len(snap), keys)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("Close (phase 2): %v", err)
	}
}

// TestLiveKeys_SeededFromManifestNotWALOnly proves the manifest record is what
// carries the count across a restart: after a flush, the flushed memtable's WAL
// is deleted, so a WAL-replay-only recovery would report far fewer keys.
func TestLiveKeys_SeededFromManifestNotWALOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(512))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	const keys = 100
	for i := 0; i < keys; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("k-%04d", i), []byte("value")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Wait for the background flush to land its manifest record.
	waitForManifestLiveKeys(t, tree)

	recorded, ok := tree.manifest.LastLiveKeys()
	if !ok {
		t.Fatal("no live-key count recorded in the manifest after flushes")
	}
	if recorded <= 0 || recorded > keys {
		t.Fatalf("recorded live keys = %d, want 0 < n <= %d", recorded, keys)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The count on reopen is the recorded value plus the deltas of the WALs that
	// were still on disk — together they must reconstruct the whole store.
	reopened, err := NewLSMTree(dir, nil, WithMaxMemBytes(512))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.LiveKeys(); got != keys {
		t.Fatalf("after reopen: LiveKeys() = %d, want %d", got, keys)
	}
}

// TestLiveKeys_LegacyManifestWithoutCount pins backward compatibility: a data
// directory whose manifest predates the live_keys field still opens, and falls
// back to whatever the surviving WALs imply.
func TestLiveKeys_LegacyManifestWithoutCount(t *testing.T) {
	dir := t.TempDir()

	// Hand-write a manifest in the pre-live_keys format.
	legacy := `{"type":"add","path":"sst-00000001.sst","sst_seq":1,"level":0}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "manifest.log"), []byte(legacy), 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	m, err := OpenManifest(filepath.Join(dir, "manifest.log"))
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	if _, ok := m.LastLiveKeys(); ok {
		t.Fatal("legacy manifest must report no recorded live-key count")
	}
	if len(m.LiveFiles()) != 1 {
		t.Fatalf("legacy manifest live files = %d, want 1", len(m.LiveFiles()))
	}
}

// TestManifest_LiveKeysRoundTrip pins that the recorded count survives the
// manifest's write-then-reopen cycle and that only the latest record wins.
func TestManifest_LiveKeysRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.log")

	m, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	if err := m.AddWithLiveKeys("sst-00000001.sst", 1, 0, 7); err != nil {
		t.Fatalf("AddWithLiveKeys: %v", err)
	}
	// A compaction event must not clobber the recorded count.
	if err := m.Add("sst-00000002.sst", 2, 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := m.AddWithLiveKeys("sst-00000003.sst", 3, 0, 11); err != nil {
		t.Fatalf("AddWithLiveKeys: %v", err)
	}

	reopened, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("reopen manifest: %v", err)
	}
	got, ok := reopened.LastLiveKeys()
	if !ok {
		t.Fatal("reopened manifest lost the live-key record")
	}
	if got != 11 {
		t.Fatalf("LastLiveKeys() = %d, want 11 (latest record)", got)
	}

	// The plain Add event must be serialised without a live_keys field so old
	// readers and the "no count recorded" case stay distinguishable.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	var withCount, withoutCount int
	for dec.More() {
		var ev ManifestEvent
		if err := dec.Decode(&ev); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		if ev.LiveKeys == nil {
			withoutCount++
		} else {
			withCount++
		}
	}
	if withCount != 2 || withoutCount != 1 {
		t.Fatalf("events with/without live_keys = %d/%d, want 2/1", withCount, withoutCount)
	}
}

// ---------------------------------------------------------------------------
// WAL append counter (served as /metrics wal_writes)
// ---------------------------------------------------------------------------

// TestWALAppends_CountsWritesOnly pins that the counter tracks fsync'd WAL
// appends: one per Put and per Delete, none for reads, and none for the replay
// that happens during recovery.
func TestWALAppends_CountsWritesOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	if got := tree.WALAppends(); got != 0 {
		t.Fatalf("fresh tree: WALAppends() = %d, want 0", got)
	}

	for i := 0; i < 10; i++ {
		if err := tree.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := tree.Delete(ctx, "k0"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := tree.Get(ctx, "k1"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := tree.Get(ctx, "missing"); err != ErrNotFound {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
	if got := tree.WALAppends(); got != 11 {
		t.Fatalf("WALAppends() = %d, want 11 (10 puts + 1 delete)", got)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The counter is per-process: recovery replay is not an append.
	reopened, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.WALAppends(); got != 0 {
		t.Fatalf("after reopen: WALAppends() = %d, want 0 (replay is not an append)", got)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func assertSSTablesExist(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, de := range entries {
		if filepath.Ext(de.Name()) == ".sst" {
			return
		}
	}
	t.Fatal("expected at least one flushed SSTable; test would not exercise the manifest path")
}

// waitForManifestLiveKeys blocks until a background flush has recorded a
// live-key count in the manifest.
func waitForManifestLiveKeys(t *testing.T, tree *LSMTree) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := tree.manifest.LastLiveKeys(); ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a flush to record a live-key count")
}
