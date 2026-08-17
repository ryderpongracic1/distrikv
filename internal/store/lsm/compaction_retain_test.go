package lsm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// ---------------------------------------------------------------------------
// Regression: an L0 SSTable flushed while a compaction is in flight must not be
// dropped when the compaction publishes its result.
//
// runCompact snapshots its input set, merges without holding l.mu (the merge is
// disk work), then swaps the result in. Before the fix, the swap set l.l0 = nil
// unconditionally, so any SSTable that runFlush prepended during the merge was
// erased from the live set — its entries were never in the merge output, so
// every key whose newest version lived there returned ErrNotFound for the rest
// of the process lifetime.
//
// Whether the window is hit depends on the ratio of merge duration to memtable
// refill duration, which is why this reproduced deterministically on darwin
// (F_FULLFSYNC makes the merge's manifest rewrites far more expensive) and never
// on ext4 with the shipped test parameters. Both tests below force the ratio
// rather than depending on the platform: a value large enough to fill the
// memtable in a couple of writes makes flushes cheap and frequent relative to a
// four-input merge.
// ---------------------------------------------------------------------------

// TestCompaction_RetainsL0FlushedDuringMerge drives the engine through its public
// API only. On the unfixed swap it loses whole L0 tables' worth of keys.
func TestCompaction_RetainsL0FlushedDuringMerge(t *testing.T) {
	const keys = 400

	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// 512B memtable with ~411B entries → ~2 keys per L0 table, so flushes are
	// frequent and cheap while each 4-input merge is comparatively slow.
	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(512))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	value := make([]byte, 400)
	for i := range value {
		value[i] = 'v'
	}

	for i := 0; i < keys; i++ {
		if _, err := tree.Put(ctx, fmt.Sprintf("rk-%04d", i), value); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Let the last flush and compaction settle so the assertion is about the
	// live set, not about work still in flight.
	waitForIdleCompaction(t, tree)

	var missing []int
	for i := 0; i < keys; i++ {
		if _, err := tree.Get(ctx, fmt.Sprintf("rk-%04d", i)); err != nil {
			missing = append(missing, i)
		}
	}
	if len(missing) > 0 {
		show := missing
		if len(show) > 12 {
			show = show[:12]
		}
		t.Fatalf("%d of %d acknowledged keys are unreadable (first: %v); "+
			"an L0 SSTable flushed during a compaction was dropped from the live set",
			len(missing), keys, show)
	}
	if m.CompactionsTotal.Load() == 0 {
		t.Fatal("no compaction ran; the test never exercised the swap it is guarding")
	}
}

// TestInstallCompactionResult_KeepsUnmergedL0 pins the invariant directly, with
// no dependence on timing: it hands installCompactionResult the same stale input
// snapshot runCompact would hold, with one extra table published in between.
func TestInstallCompactionResult_KeepsUnmergedL0(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(512))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// Park the background compactor so this test owns the merge and the swap.
	// Set before the first write, so the compactCh send at the end of the first
	// flush orders this write ahead of any read of the field.
	tree.compact.threshold = 1 << 20

	value := make([]byte, 400)
	for i := range value {
		value[i] = 'v'
	}

	// Build up L0 without letting a real compaction interfere.
	for i := 0; i < 6; i++ {
		if _, err := tree.Put(ctx, fmt.Sprintf("merged-%d", i), value); err != nil {
			t.Fatalf("Put merged-%d: %v", i, err)
		}
	}
	l0Inputs := waitForL0(t, tree, 2)

	// A flush landing mid-merge: publish one more L0 table that is NOT among
	// l0Inputs, exactly as runFlush would while the merge was running.
	for i := 0; i < 2; i++ {
		if _, err := tree.Put(ctx, fmt.Sprintf("late-%d", i), value); err != nil {
			t.Fatalf("Put late-%d: %v", i, err)
		}
	}
	waitForL0(t, tree, len(l0Inputs)+1)

	// Merge only the snapshotted inputs, then publish the result.
	tree.mu.RLock()
	l1Inputs := tree.l1
	tree.mu.RUnlock()
	out, err := tree.compact.Compact(ctx, compactInputOrder(l0Inputs, l1Inputs))
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if out != nil {
		out.Level = 1
		out.metrics = tree.metrics
		out.cache = tree.cache
	}
	tree.installCompactionResult(l0Inputs, l1Inputs, out)
	// Mirror runCompact's post-swap cleanup of the merged inputs.
	for _, r := range l0Inputs {
		tree.cache.Evict(r.path)
		r.Release()
	}
	for _, r := range l1Inputs {
		tree.cache.Evict(r.path)
		r.Release()
	}

	// The late table was never merged, so it must still be in L0.
	tree.mu.RLock()
	gotL0 := len(tree.l0)
	tree.mu.RUnlock()
	if gotL0 == 0 {
		t.Fatal("all L0 tables were dropped; the one flushed after the input " +
			"snapshot was never merged and its keys are now unreadable")
	}
	if int32(gotL0) != tree.l0Count.Load() {
		t.Errorf("l0Count = %d, want %d (stall backpressure reads this counter)",
			tree.l0Count.Load(), gotL0)
	}
	if m.L0FileCount.Load() != int64(gotL0) {
		t.Errorf("metrics.L0FileCount = %d, want %d", m.L0FileCount.Load(), gotL0)
	}

	// Both the merged and the retained keys must be readable.
	for i := 0; i < 6; i++ {
		if _, err := tree.Get(ctx, fmt.Sprintf("merged-%d", i)); err != nil {
			t.Errorf("merged-%d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := tree.Get(ctx, fmt.Sprintf("late-%d", i)); err != nil {
			t.Errorf("late-%d (flushed after the merge snapshotted its inputs): %v", i, err)
		}
	}
}

// waitForL0 waits until at least n L0 tables are live and returns a snapshot of
// them, mirroring what runCompact captures before merging.
func waitForL0(t *testing.T, tree *LSMTree, n int) []*SSTableReader {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tree.mu.RLock()
		snap := tree.l0
		tree.mu.RUnlock()
		if len(snap) >= n {
			return snap
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d L0 SSTables", n)
	return nil
}

// waitForIdleCompaction waits for background flush/compaction activity to settle.
func waitForIdleCompaction(t *testing.T, tree *LSMTree) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastL0 int32 = -1
	stable := 0
	for time.Now().Before(deadline) {
		tree.mu.RLock()
		imm := tree.imm
		tree.mu.RUnlock()
		n := tree.l0Count.Load()
		if imm == nil && n == lastL0 {
			if stable++; stable >= 5 {
				return
			}
		} else {
			stable = 0
		}
		lastL0 = n
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for flush/compaction to settle")
}
