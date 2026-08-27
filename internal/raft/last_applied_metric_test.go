package raft

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// raft_last_applied_index is Raft's to publish, not the state machine's.
//
// The gauge is the denominator health_transitions_committed is read against, and
// that reading only holds if every advance of lastApplied is reported. A state
// machine can only report the advances it is handed an entry for, so the one that
// skips the log entirely — a snapshot moving the frontier straight to its
// boundary — went unreported when the state machine owned this. That is the case
// the second half of this test pins.

// recordingMetrics captures the last index published, plus how many times.
type recordingMetrics struct {
	lastApplied atomic.Uint64
	calls       atomic.Uint64
}

func (m *recordingMetrics) IncRaftTerms()       {}
func (m *recordingMetrics) IncLeaderElections() {}
func (m *recordingMetrics) SetLastAppliedIndex(index uint64) {
	m.lastApplied.Store(index)
	m.calls.Add(1)
}

// TestRaft_LastAppliedIndexIsPublishedOnEveryAdvance covers both ways lastApplied
// moves: entry by entry through the apply loop, and in one jump when a snapshot
// is installed.
//
// Revert-check: drop the setLastAppliedLocked call from applyCommitted and the
// first assertion fails; drop it from HandleInstallSnapshot and the second does.
func TestRaft_LastAppliedIndexIsPublishedOnEveryAdvance(t *testing.T) {
	ctx := context.Background()
	m := &recordingMetrics{}
	sm := newTestSM()

	node, err := New(Config{
		NodeID:             "n1",
		DataDir:            t.TempDir(),
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
	}, nil, sm, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	// Three committed entries applied one at a time.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2, 2),
		LeaderCommit: 3,
	}).Success)
	require.Len(t, sm.appliedEntries(), 3)
	require.Equal(t, uint64(3), m.lastApplied.Load(),
		"the gauge must follow the apply loop entry by entry")

	// A snapshot past the log's end: lastApplied jumps to the boundary without a
	// single entry reaching the state machine.
	before := m.calls.Load()
	resp, err := node.HandleInstallSnapshot(ctx, &kvpb.InstallSnapshotRequest{
		Term: 2, LeaderId: "leader-1",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              []byte("applied=10"),
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	require.Len(t, sm.appliedEntries(), 3, "a snapshot install applies no entries")
	require.Greater(t, m.calls.Load(), before,
		"installing a snapshot advances lastApplied, so it must publish")
	require.Equal(t, uint64(10), m.lastApplied.Load(),
		"the gauge must follow a snapshot-driven jump too — the advance the state "+
			"machine cannot see")
}

// TestRaft_SnapshotRequestDuringCaptureIsServedNotDropped pins the single-flight
// slot's obligation.
//
// Declining a concurrent capture is the point of the slot — they serialise on
// SnapshotStore.mu for all but one to be discarded, each having paid an fsync.
// But the request behind it must not be lost with it: the running capture fixed
// its cut point before the request existed, so it compacts less far, and nothing
// re-triggers compaction on an idle leader. The log would sit over threshold for
// good.
//
// Revert-check: make takeSnapshot `if !CAS { return }` with no pending flag, and
// the call count drops to 1 and the boundary stops at 3.
func TestRaft_SnapshotRequestDuringCaptureIsServedNotDropped(t *testing.T) {
	ctx := context.Background()
	sm := newTestSM()
	sm.snapshotGate = make(chan struct{})

	// Threshold left at its 1000 default so the apply path never triggers a
	// capture of its own: this test drives the coordinator directly.
	node := openNodeInDir(t, t.TempDir(), sm)
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2, 2),
		LeaderCommit: 3,
	}).Success)

	// A capture starts and parks inside SnapshotState, holding the slot.
	done := make(chan struct{})
	go func() {
		defer close(done)
		node.takeSnapshot(ctx)
	}()
	require.Eventually(t, func() bool { return sm.snapshotCallCount() == 1 },
		2*time.Second, time.Millisecond, "the first capture never reached the state machine")

	// Two more entries commit, so a capture starting now would cut at 5 rather
	// than the 3 the parked one is holding.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 3, PrevLogTerm: 2,
		Entries:      wireEntries(4, 2, 2),
		LeaderCommit: 5,
	}).Success)

	// This one finds the slot held. It must return without capturing — and
	// without being forgotten.
	node.takeSnapshot(ctx)
	require.Equal(t, 1, sm.snapshotCallCount(),
		"a request that finds the slot held must not start a second concurrent capture")

	close(sm.snapshotGate)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the capture goroutine never returned")
	}

	require.Equal(t, 2, sm.snapshotCallCount(),
		"the deferred request must be served before the slot is released")

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(5), node.snapLastIndex,
		"the second pass must compact to the newer cut point; stopping at 3 means "+
			"the request was dropped and the log stays over threshold")
}
