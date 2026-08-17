package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// Claim integrity in the anti-entropy engine.
//
// The engine's whole value is the sentence "replica caught up", so the thing it
// must never do is say it without evidence. Two paths did. Both start from the
// same place — the primary's log no longer reaches back far enough to cover what
// the replica missed — and both ended in a pass that shipped nothing, which the
// engine read as convergence:
//
//   - A replica behind since before its first cursor was ever persisted has no
//     recorded cursor, so it contributed nothing to the WAL retention floor and
//     advanceQuietCursors would not give it one (it only adopts a tip for a
//     replica that is *not* behind). The healthy replicas' cursors moved the floor
//     forward, and the engine deleted exactly the segments the down replica was
//     owed. On its return the pass started at the oldest survivor and reported
//     success, with no error and no metric, because NewReader returns on
//     from.IsZero() before it ever looks for the cursor's segment.
//   - A cursor that *is* recorded but points into a released segment does raise
//     wal.ErrCursorStale, which runPass handled correctly — and then returned
//     (0, nil) when nothing in the survivors was owed, so repair read it as
//     convergence anyway.
//
// These tests are written to fail on the pre-fix code: each one asserts the
// absence of the claim, so reverting the fix reinstates the claim and breaks them.

// forceWALSegmentsReleased writes enough to overflow the 4 MiB memtable and waits
// until the engine has actually released its first segment, returning the oldest
// segment number that survives. It reproduces the defect's precondition through
// the real mechanism rather than staging files: retention is off while no replica
// is behind, so the flush deletes the segment outright.
func forceWALSegmentsReleased(t *testing.T, n *Node) uint64 {
	t.Helper()

	value := make([]byte, 1<<20) // 1 MiB, so a handful of writes overflows 4 MiB
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for i := 0; i < 8; i++ {
		if err := n.store.Put(context.Background(), fmt.Sprintf("pad-%d", i), value); err != nil {
			t.Fatalf("padding write %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		segs, err := n.store.WALSegments()
		if err != nil {
			t.Fatalf("WALSegments: %v", err)
		}
		if len(segs) > 0 && segs[0].Seq > 1 {
			return segs[0].Seq
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("the WAL still starts at segment 1 after 8 MiB of writes (%v); "+
				"this test needs the engine to have released its first segment", segs)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRetentionPinsTheWALForABehindReplicaWithNoCursor is the prevention half of
// the fix: the retention floor must account for a replica that is known to be
// behind and has no cursor, because "no cursor" says nothing about how far back it
// needs — so the only honest answer is to keep everything still on disk.
//
// On the pre-fix code the floor was the minimum over *recorded* cursors alone, so
// this replica contributed nothing and the healthy replica's cursor carried the
// floor forward past the segments the behind replica was owed.
func TestRetentionPinsTheWALForABehindReplicaWithNoCursor(t *testing.T) {
	n, _ := testNode(t, 2, "node2", "node3")
	ae, _ := withAntiEntropyLogging(t, n, false, "node2", "node3")

	// A healthy cluster that has recorded nothing yet is owed nothing, so
	// retention must stay off. Without this, the fix could pass by pinning
	// segment 1 unconditionally, which would defeat retention's own bound.
	ae.publishRetentionFloor()
	if got := n.store.WALRetentionFloor(); got != 0 {
		t.Errorf("retention floor = %d with no replica behind and no cursors recorded, "+
			"want 0 (retention off): pinning here would keep every segment forever", got)
	}

	// node3 keeps up, so the quiet-cursor path adopts a tip well into the log.
	// This is what makes the cursor-derived floor march forward.
	n.cursors.Advance("node3", storewal.Position{Segment: 5})

	// node2 falls behind before a cursor was ever recorded for it — the fresh
	// cluster / early fault window.
	ae.NoteReplicationFailure("node2")
	if !n.cursors.Get("node2").IsZero() {
		t.Fatal("precondition: node2 has a recorded cursor, so this is not the no-cursor case")
	}

	if got := n.store.WALRetentionFloor(); got != 1 {
		t.Errorf("retention floor = %d immediately after a replica with no cursor fell "+
			"behind, want 1: the engine is free to delete the segments that replica is "+
			"owed, and waiting for the flush ticker leaves that window open at exactly "+
			"the moment a fault starts", got)
	}

	// The same must hold on the periodic path, not only on the failure transition.
	ae.publishRetentionFloor()
	if got := n.store.WALRetentionFloor(); got != 1 {
		t.Errorf("retention floor = %d after republishing, want 1: the healthy replica's "+
			"cursor at segment 5 has carried the floor past what node2 needs", got)
	}

	// Once the replica has a cursor of its own the pin must lift, or retention
	// would keep every segment for the rest of the process's life.
	n.cursors.Advance("node2", storewal.Position{Segment: 3})
	ae.publishRetentionFloor()
	if got := n.store.WALRetentionFloor(); got != 3 {
		t.Errorf("retention floor = %d once the behind replica has a cursor at segment 3, "+
			"want 3: the pin must lift as soon as there is evidence to derive a floor from", got)
	}
}

// TestAntiEntropyWithholdsClaimWhenAZeroCursorGapCannotBeCovered is defect 1's
// honesty half, driven through the real engine: the segments are genuinely gone
// before the fault is noticed, so the gap cannot be closed and the claim cannot be
// made. Pre-fix this logged "replica caught up" with no warning and no metric.
func TestAntiEntropyWithholdsClaimWhenAZeroCursorGapCannotBeCovered(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae, logs := withAntiEntropyLogging(t, n, false, "node2")

	oldest := forceWALSegmentsReleased(t, n)

	// The fault is noticed only now, after the segments have been released. The
	// cursor is still zero, so nothing records how far back node2 needs.
	ae.NoteReplicationFailure("node2")
	if !n.cursors.Get("node2").IsZero() {
		t.Fatal("precondition: node2 has a recorded cursor, so this is not the no-cursor case")
	}

	ae.repair(context.Background(), "node2")

	out := logs.String()
	if strings.Contains(out, "replica caught up") {
		t.Errorf("the node claimed \"replica caught up\" for a replica with no cursor "+
			"whose WAL now starts at segment %d: the writes it missed before that "+
			"segment were never shipped and cannot be.\nlogs:\n%s", oldest, out)
	}
	if !strings.Contains(out, "NOT known to agree") {
		t.Errorf("the empty pass over a truncated log was silent; the operator has no "+
			"way to tell it apart from convergence.\nlogs:\n%s", out)
	}
	if got := n.metrics.AntiEntropyStaleCursors.Load(); got == 0 {
		t.Error("anti_entropy_stale = 0: a gap the log cannot cover was not counted")
	}
	if got := n.metrics.AntiEntropyFullSyncRequired.Load(); got != 1 {
		t.Errorf("anti_entropy_full_sync_required = %d, want 1: the standing inability "+
			"to converge this replica from the log is not surfaced as a gauge", got)
	}

	// The segments are gone permanently, so a restart must not forget.
	reopened, err := store.OpenCursorStore(n.cfg.DataDir)
	if err != nil {
		t.Fatalf("reopen cursor store: %v", err)
	}
	if required, reason := reopened.FullSyncRequired(); !required {
		t.Errorf("the full-sync-required latch did not survive a restart (reason %q); "+
			"the next process would claim convergence over the same hole", reason)
	}
	// And it must not have thrown away the cursors while doing so — they are the
	// only record of how far the replica did get.
	if reopened.Get("node2").IsZero() {
		t.Error("latching full-sync-required discarded node2's cursor, so every later " +
			"pass restarts from the oldest surviving segment")
	}
}

// TestAntiEntropyWithholdsClaimWhenTheCursorIsOlderThanTheRetainedWAL is defect 2:
// the ErrCursorStale path was already detected, counted and logged, and then
// returned (0, nil) into repair, which read it as convergence. The fullSync branch
// ten lines below already did the right thing for the same semantic condition.
func TestAntiEntropyWithholdsClaimWhenTheCursorIsOlderThanTheRetainedWAL(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae, logs := withAntiEntropyLogging(t, n, false, "node2")

	oldest := forceWALSegmentsReleased(t, n)

	// A recorded cursor pointing into a released segment: non-zero, so this is the
	// ErrCursorStale path and not the no-cursor one above.
	stale := storewal.Position{Segment: oldest - 1, Offset: 4}
	if !n.cursors.Advance("node2", stale) {
		t.Fatalf("could not plant a stale cursor at %s", stale)
	}
	ae.NoteReplicationFailure("node2")

	ae.repair(context.Background(), "node2")

	out := logs.String()
	if !strings.Contains(out, "older than the retained WAL") {
		t.Errorf("the stale cursor was not reported at all.\nlogs:\n%s", out)
	}
	if strings.Contains(out, "replica caught up") {
		t.Errorf("the node claimed \"replica caught up\" after reporting that the "+
			"cursor %s points into a released segment: the keys whose only write fell "+
			"in the lost range were never shipped.\nlogs:\n%s", stale, out)
	}
	if !strings.Contains(out, "NOT known to agree") {
		t.Errorf("the pass over the truncated log did not withhold the claim.\nlogs:\n%s", out)
	}
	if got := n.metrics.AntiEntropyStaleCursors.Load(); got == 0 {
		t.Error("anti_entropy_stale = 0: the stale cursor was not counted")
	}
	if got := n.metrics.AntiEntropyFullSyncRequired.Load(); got != 1 {
		t.Errorf("anti_entropy_full_sync_required = %d, want 1", got)
	}
}

// TestReplicateWriteMarksAReplicaWithNoClientBehind closes the one hole in "any
// replication failure marks the replica behind". Every other failure path in
// ReplicateWrite called noteReplicationFailure; the no-client branch bumped the
// error counter and moved on, so nothing would ever schedule a catch-up.
func TestReplicateWriteMarksAReplicaWithNoClientBehind(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae, _ := withAntiEntropyLogging(t, n, false, "node2")
	key := keysOwedTo(t, ae, "node2", 1)[0]

	// A ring member with no client entry — the shape this branch exists for.
	delete(n.peerClients, "node2")

	if err := n.store.Put(context.Background(), key, []byte("kept-locally")); err != nil {
		t.Fatalf("local put: %v", err)
	}
	if err := n.ReplicateWrite(context.Background(), server.OpPut, key, []byte("kept-locally")); err == nil {
		t.Fatal("replication to a replica with no client returned nil; the client's " +
			"write would be acknowledged despite the replica never seeing it")
	}

	if !ae.isBehind("node2") {
		t.Error("a replica the primary holds no client for was not marked behind: the " +
			"write is refused to the client but kept locally, and nothing will ever " +
			"schedule a catch-up to converge it")
	}
	if got := n.metrics.ReplicationErrors.Load(); got != 1 {
		t.Errorf("replication_errors = %d, want 1", got)
	}
	// Marking the replica behind is also what arms WAL retention for it, so the
	// two halves of the fix are connected: without the flag, the segments holding
	// this write are released on the next flush.
	if got := n.store.WALRetentionFloor(); got != 1 {
		t.Errorf("retention floor = %d after the failure, want 1: the write this "+
			"replica missed is free to be deleted before it can be shipped", got)
	}
}
