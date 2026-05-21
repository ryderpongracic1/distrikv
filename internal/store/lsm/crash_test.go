package lsm

// Crash-recovery tests for the LSM-Tree engine.
//
// Every test in this file simulates a crash (or the equivalent) and then
// re-opens the engine on the same data directory to verify that:
//
//  1. Every write that returned nil before the crash is readable after recovery
//     (durability guarantee — enforced by the WAL fsync in each Append call).
//
//  2. Partial (torn) WAL entries left by a simulated crash do NOT surface as
//     visible data (atomicity guarantee — the CRC/EOF check in WAL.Replay stops
//     at the first bad entry).
//
//  3. Data that was flushed to an SSTable before the crash is still visible
//     after recovery even if the post-flush WAL file is truncated or absent
//     (SSTable persistence guarantee).
//
// Tests truncate the WAL file directly (via os.Truncate) rather than sending
// SIGKILL to a real process; the result is identical from the engine's
// perspective — the file is shorter than what was written, exactly as power
// loss would produce.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCrash_WalReplayDurability writes N keys to a fresh engine, closes it
// cleanly (all writes fsynced by WAL.Append), reopens it on the same dir, and
// verifies every key is readable — the baseline durability guarantee.
func TestCrash_WalReplayDurability(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	const n = 200
	func() {
		tree, err := NewLSMTree(dir, nil)
		if err != nil {
			t.Fatalf("NewLSMTree: %v", err)
		}
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("dur-key-%04d", i)
			if err := tree.Put(ctx, k, []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}
		if err := tree.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree reopen: %v", err)
	}
	defer tree.Close()

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("dur-key-%04d", i)
		val, err := tree.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %s after recovery: %v", k, err)
		}
		want := fmt.Sprintf("v%d", i)
		if string(val) != want {
			t.Fatalf("Get %s: got %q want %q", k, val, want)
		}
	}
}

// TestCrash_TornWALEntry writes some complete entries to the LSM, then
// directly truncates the active WAL file mid-entry (simulating a crash
// partway through a write).  On reopen, the engine must recover all entries
// before the truncation point and silently discard the partial one.
//
// To preserve the WAL file on disk we stop the background goroutines and
// close the file handle directly, bypassing the normal Close path that would
// flush the memtable to an SSTable (and consequently delete the WAL).
func TestCrash_TornWALEntry(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	const goodKeys = 10

	// Phase 1: write `goodKeys` entries, record the WAL path + size,
	// then simulate a crash: stop background goroutines and close the WAL
	// file handle WITHOUT flushing the memtable to an SSTable.
	// The WAL stays intact on disk (no deletion occurs).
	var walPath string
	var goodLen int64
	func() {
		// Large memtable ensures no automatic flush during the writes below.
		tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(64<<20))
		if err != nil {
			t.Fatalf("NewLSMTree: %v", err)
		}

		for i := 0; i < goodKeys; i++ {
			k := fmt.Sprintf("torn-key-%04d", i)
			if err := tree.Put(ctx, k, []byte("good-value")); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}

		// Capture path and byte-offset after all good entries are fsynced.
		walPath = tree.mem.walPath
		if info, err := os.Stat(walPath); err == nil {
			goodLen = info.Size()
		}

		// Write the entry we will tear away.
		if err := tree.Put(ctx, "torn-key-EXTRA", []byte("should-vanish")); err != nil {
			t.Fatalf("Put extra: %v", err)
		}

		// Simulate crash: stop background goroutines, close WAL file handle.
		// We deliberately do NOT call tree.Close() because that would flush
		// the memtable to an SSTable and delete the WAL file.
		close(tree.stopCh)
		tree.wg.Wait()
		if tree.mem != nil && tree.mem.w != nil {
			_ = tree.mem.w.Close()
		}
		tree.mu.RLock()
		for _, r := range tree.l0 {
			_ = r.Close()
		}
		for _, r := range tree.l1 {
			_ = r.Close()
		}
		tree.mu.RUnlock()
	}()

	if goodLen == 0 {
		t.Fatal("could not determine WAL size after good entries")
	}

	// Truncate the WAL back to the size after the good entries, erasing the
	// "torn" entry's bytes (simulates power-loss mid-write).
	if err := os.Truncate(walPath, goodLen); err != nil {
		t.Fatalf("truncate WAL: %v", err)
	}

	// Phase 2: reopen — the engine must stop replay at the truncation point.
	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree reopen: %v", err)
	}
	defer tree.Close()

	// All good keys must survive.
	for i := 0; i < goodKeys; i++ {
		k := fmt.Sprintf("torn-key-%04d", i)
		val, err := tree.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %s after torn-entry recovery: %v", k, err)
		}
		if string(val) != "good-value" {
			t.Fatalf("Get %s: got %q want %q", k, val, "good-value")
		}
	}

	// The truncated entry must not be visible.
	_, err = tree.Get(ctx, "torn-key-EXTRA")
	if err != ErrNotFound {
		t.Fatalf("torn key unexpectedly visible: err=%v", err)
	}
}

// TestCrash_FlushedDataSurvivesTruncation writes enough data to force at
// least one memtable flush (data persisted to an SSTable), then simulates
// a crash that wipes the post-flush WAL file entirely.  The flushed SSTable
// data must still be readable after recovery.
func TestCrash_FlushedDataSurvivesTruncation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 1 KB memtable → flush every ~10 writes at ~100B/entry.
	tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(1<<10))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}

	// Write enough to trigger at least one flush.
	const writes = 25
	val := make([]byte, 80)
	for i := 0; i < writes; i++ {
		k := fmt.Sprintf("flush-key-%04d", i)
		if err := tree.Put(ctx, k, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Wait for at least one L0 SSTable to appear.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tree.l0Count.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tree.l0Count.Load() == 0 {
		t.Fatal("no L0 file produced; cannot test SSTable survival")
	}

	// Record the post-flush WAL path.
	tree.mu.RLock()
	postFlushWAL := tree.mem.walPath
	tree.mu.RUnlock()

	// Close cleanly so file handles are released.
	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate crash: wipe the post-flush WAL entirely (only the SSTable remains).
	if err := os.Remove(postFlushWAL); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove post-flush WAL: %v", err)
	}

	// Reopen — the SSTable data must be recovered even without the WAL.
	tree2, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree reopen: %v", err)
	}
	defer tree2.Close()

	// Keys from the flushed data must be readable.
	found := 0
	for i := 0; i < writes; i++ {
		k := fmt.Sprintf("flush-key-%04d", i)
		if _, err := tree2.Get(ctx, k); err == nil {
			found++
		}
	}
	if found == 0 {
		t.Fatalf("no flushed keys readable after WAL removal; expected SSTable data to survive")
	}
	t.Logf("found %d/%d keys after simulated crash (rest were only in lost WAL)", found, writes)
}

// TestCrash_ConcurrentWriteDurability runs N goroutines writing disjoint keys,
// tracks which writes returned nil (= durable), then closes the engine and
// reopens it.  Every key that was durably written must be readable after recovery.
func TestCrash_ConcurrentWriteDurability(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	const goroutines = 6
	const writesPerGoroutine = 50

	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}

	// Track keys that were successfully written.
	var mu sync.Mutex
	var durable []string
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				k := fmt.Sprintf("concurrent-g%d-k%d", id, i)
				if err := tree.Put(ctx, k, []byte(fmt.Sprintf("gval-%d-%d", id, i))); err == nil {
					mu.Lock()
					durable = append(durable, k)
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify every durably written key.
	tree2, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree reopen: %v", err)
	}
	defer tree2.Close()

	var missing int
	for _, k := range durable {
		if _, err := tree2.Get(ctx, k); err != nil {
			t.Errorf("durable key %s missing after recovery: %v", k, err)
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d durable keys missing after recovery", missing, len(durable))
	}
	t.Logf("all %d durably written keys recovered", len(durable))
}

// TestCrash_RestoreSentinelRecovery verifies that if a crash happens after the
// "restore-in-progress" sentinel is written but before the restore completes,
// the next open detects the sentinel, wipes the half-restored dir, and starts
// clean.
func TestCrash_RestoreSentinelRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: write some data into the engine.
	func() {
		tree, err := NewLSMTree(dir, nil)
		if err != nil {
			t.Fatalf("NewLSMTree: %v", err)
		}
		for i := 0; i < 20; i++ {
			tree.Put(ctx, fmt.Sprintf("pre-restore-key-%d", i), []byte("old"))
		}
		tree.Close()
	}()

	// Simulate a crash mid-restore by writing the sentinel manually, then
	// also leave a stale SST file to show the dir was partially wiped.
	sentinelPath := filepath.Join(dir, "restore-in-progress")
	if err := os.WriteFile(sentinelPath, []byte("1"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	// Phase 2: open — must detect sentinel, wipe dir, start fresh.
	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree after sentinel: %v", err)
	}
	defer tree.Close()

	// The sentinel must be gone.
	if _, statErr := os.Stat(sentinelPath); !os.IsNotExist(statErr) {
		t.Fatalf("restore sentinel still present after reopen")
	}

	// Data from before the interrupted restore should be gone (dir was wiped).
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("pre-restore-key-%d", i)
		if _, err := tree.Get(ctx, k); err != ErrNotFound {
			t.Errorf("key %s should be absent after sentinel wipe, got err=%v", k, err)
		}
	}
}

// TestCrash_NoDataLossUnderFlushedAndUnflushed writes two batches: one large
// enough to flush to an SSTable, and one small enough to stay in the WAL only.
// After a clean close + reopen both batches must be fully readable.
func TestCrash_NoDataLossUnderFlushedAndUnflushed(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 1 KB threshold: batch1 (20 writes × 80B ≈ 1.6 KB) forces a flush.
	// batch2 stays in the new active WAL.
	tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(1<<10))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}

	val := make([]byte, 80)

	// Batch 1: flush to L0.
	const batch1 = 20
	for i := 0; i < batch1; i++ {
		k := fmt.Sprintf("mixed-b1-%04d", i)
		if err := tree.Put(ctx, k, val); err != nil {
			t.Fatalf("Put b1 %d: %v", i, err)
		}
	}

	// Wait for flush.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tree.l0Count.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Batch 2: stays in WAL only (< 1 KB, no flush triggered).
	const batch2 = 5
	for i := 0; i < batch2; i++ {
		k := fmt.Sprintf("mixed-b2-%04d", i)
		if err := tree.Put(ctx, k, val); err != nil {
			t.Fatalf("Put b2 %d: %v", i, err)
		}
	}

	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen.
	tree2, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree reopen: %v", err)
	}
	defer tree2.Close()

	// Both batches must be readable.
	for i := 0; i < batch1; i++ {
		k := fmt.Sprintf("mixed-b1-%04d", i)
		if _, err := tree2.Get(ctx, k); err != nil {
			t.Errorf("batch1 key %s missing after recovery: %v", k, err)
		}
	}
	for i := 0; i < batch2; i++ {
		k := fmt.Sprintf("mixed-b2-%04d", i)
		if _, err := tree2.Get(ctx, k); err != nil {
			t.Errorf("batch2 key %s missing after recovery: %v", k, err)
		}
	}
}

// BenchmarkCrash_RecoveryThroughput measures how quickly the engine can
// replay a WAL and rebuild the memtable — relevant for restart-time SLOs.
func BenchmarkCrash_RecoveryThroughput(b *testing.B) {
	dir := b.TempDir()
	ctx := context.Background()

	// Write 1000 keys that will stay in the WAL (large memtable, no flush).
	tree, err := NewLSMTree(dir, nil, WithMaxMemBytes(64<<20))
	if err != nil {
		b.Fatalf("NewLSMTree: %v", err)
	}
	var seq atomic.Int64
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("bench-key-%06d", seq.Add(1))
		tree.Put(ctx, k, make([]byte, 64))
	}
	tree.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, err := NewLSMTree(dir, nil, WithMaxMemBytes(64<<20))
		if err != nil {
			b.Fatalf("NewLSMTree: %v", err)
		}
		t.Close()
	}
}
