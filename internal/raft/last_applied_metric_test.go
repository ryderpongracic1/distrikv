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
