package raft

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// This file covers what survives a crash, and how the log and the snapshot
// interact. A "crash" here is the honest version of one: the process state is
// dropped entirely and a fresh node is opened over the same data directory, so
// anything the tests then see came off the disk.

// openNodeInDir opens a node over a data directory the caller owns. It is both
// the first-open and the restart path — the same call either way, which is the
// point: a restart in these tests is nothing but calling this again on the same
// directory.
func openNodeInDir(t *testing.T, dir string, sm StateMachine) *RaftNode {
	t.Helper()
	cfg := Config{
		NodeID:             "restarted",
		DataDir:            dir,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
	}
	node, err := New(cfg, nil, sm, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	return node
}

// ---------------------------------------------------------------------------
// Crash-restart
// ---------------------------------------------------------------------------

// TestPersistence_AcknowledgedEntrySurvivesRestart is the durability pin: an
// entry the follower acknowledged is still there after the process dies, and the
// leader's inevitable retransmission is deduped rather than appended twice.
//
// This is the scenario the acknowledgement means something about — a follower
// that persisted an entry, crashed before its reply arrived, and came back to a
// leader that never heard the answer.
func TestPersistence_AcknowledgedEntrySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	node := openNodeInDir(t, dir, newTestSM())

	node.mu.Lock()
	node.currentTerm = 4
	node.mu.Unlock()

	req := &kvpb.AppendEntriesRequest{
		Term: 4, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 4, 4, 4),
		LeaderCommit: 3,
	}
	require.True(t, appendTo(t, node, req).Success)
	before := snapshotLog(node)
	require.Len(t, before, 3)

	// Crash: everything in memory is gone.
	restarted := openNodeInDir(t, dir, newTestSM())

	require.Equal(t, before, snapshotLog(restarted), "acknowledged entries must come back off the disk")
	restarted.mu.Lock()
	require.Equal(t, uint64(4), restarted.currentTerm, "term survives too")
	require.Equal(t, uint64(3), restarted.lastLogIndex())
	restarted.mu.Unlock()

	// The leader, never having heard the reply, sends the same batch again.
	require.True(t, appendTo(t, restarted, req).Success)
	require.Equal(t, before, snapshotLog(restarted),
		"the retransmission must dedup against what was recovered, not append a second copy")
}

// TestPersistence_TruncationSurvivesRestart verifies that removing entries is as
// durable as adding them. A truncation that lived only in memory would resurrect
// a conflicting entry on restart, and the follower would then answer for a
// history the leader has already overwritten.
func TestPersistence_TruncationSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	node := openNodeInDir(t, dir, newTestSM())

	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	// Three entries from an old term.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 5, LeaderId: "old-leader",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: wireEntries(1, 2, 2, 2),
	}).Success)

	// A new leader overwrites from index 2.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 5, LeaderId: "new-leader",
		PrevLogIndex: 1, PrevLogTerm: 2,
		Entries: wireEntries(2, 5),
	}).Success)
	require.Equal(t, []uint64{2, 5}, termsOf(snapshotLog(node)))

	restarted := openNodeInDir(t, dir, newTestSM())
	require.Equal(t, []uint64{2, 5}, termsOf(snapshotLog(restarted)),
		"the truncated entries must not come back")
}

// TestPersistence_ProposedEntrySurvivesRestart verifies the leader's half: an
// index Propose returned is durable, which is what licenses counting the
// leader's own copy toward the majority that commits it.
func TestPersistence_ProposedEntrySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	leader := openNodeInDir(t, dir, newTestSM())

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 3
	leader.peers = []PeerClient{{NodeID: "p1"}, {NodeID: "p2"}}
	leader.nextIndex = map[string]uint64{"p1": 1, "p2": 1}
	leader.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
	leader.mu.Unlock()

	idx, err := leader.Propose(context.Background(), "health-down", "n2", []byte("down"))
	require.NoError(t, err)

	restarted := openNodeInDir(t, dir, newTestSM())
	log := snapshotLog(restarted)
	require.Len(t, log, 1)
	require.Equal(t, idx, log[0].Index)
	require.Equal(t, uint64(3), log[0].Term)
	require.Equal(t, []byte("down"), log[0].Value)
}

// TestPersistence_CommitFrontierStartsAtSnapshotBoundary pins the consequence of
// keeping commitIndex and lastApplied volatile: after a restart they sit at the
// snapshot boundary, so entries above it are applied again once the leader
// re-announces its commit index. That is why StateMachine.Apply must be
// idempotent, and this test is what makes the requirement observable rather than
// merely documented.
func TestPersistence_CommitFrontierStartsAtSnapshotBoundary(t *testing.T) {
	dir := t.TempDir()
	node := openNodeInDir(t, dir, newTestSM())

	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2),
		LeaderCommit: 2,
	}).Success)

	sm2 := newTestSM()
	restarted := openNodeInDir(t, dir, sm2)

	restarted.mu.Lock()
	require.Zero(t, restarted.commitIndex, "commitIndex is volatile and starts at the snapshot boundary (0 here)")
	require.Zero(t, restarted.lastApplied)
	restarted.mu.Unlock()
	require.Empty(t, sm2.appliedEntries(), "nothing is applied until the leader says what is committed")

	// The leader re-announces, and the entries are applied a second time in this
	// process's life — exactly the replay Apply must tolerate.
	require.True(t, appendTo(t, restarted, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 2, PrevLogTerm: 2,
		LeaderCommit: 2,
	}).Success)
	require.Equal(t, []string{"k1", "k2"}, sm2.appliedKeys())
}

// TestPersistence_LogGapIsTruncatedOnLoad verifies the recovery rule for a
// corrupt state file: keep the longest valid prefix. A hole in the index
// sequence makes every entry after it unlocatable, and refusing to start would
// brick a node over a log the leader can refill in one round.
func TestPersistence_LogGapIsTruncatedOnLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft-state")

	corrupt := persistedState{
		CurrentTerm: 3,
		VotedFor:    "n1",
		Log: []LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 4, Term: 3}, // gap: index 3 is missing
			{Index: 5, Term: 3},
		},
	}
	raw, err := json.Marshal(corrupt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	node := openNodeInDir(t, dir, newTestSM())

	require.Equal(t, []uint64{1, 1}, termsOf(snapshotLog(node)),
		"only the contiguous prefix may be kept")
	require.Equal(t, uint64(2), node.LastLogIndex())
}

// TestPersistence_SnapshotAheadOfStateFileWins covers the crash window between
// the two files. takeSnapshot writes the snapshot first and records its bounds
// second, so a crash in between leaves a snapshot ahead of the state file. The
// snapshot is the more advanced record of applied state, so its bounds are
// adopted and the entries it covers are dropped.
func TestPersistence_SnapshotAheadOfStateFileWins(t *testing.T) {
	dir := t.TempDir()

	// State file: no snapshot recorded, log 1–7.
	ps := newPersistentState(filepath.Join(dir, "raft-state"))
	require.NoError(t, ps.Save(persistedState{
		CurrentTerm: 4,
		Log:         entriesOf(1, 1, 2, 2, 3, 3, 4),
	}))

	// Snapshot file: written, but its bounds never made it to the state file.
	ss := NewSnapshotStore(dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, ss.Save(Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  3,
		Data:              []byte("state-at-index-5"),
	}))

	sm := newTestSM()
	node := openNodeInDir(t, dir, sm)

	require.Equal(t, 1, sm.restoreCount(), "the snapshot on disk must be restored at startup")
	require.Equal(t, []byte("state-at-index-5"), sm.lastRestore())

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(5), node.snapLastIndex, "the snapshot's bounds win")
	require.Equal(t, uint64(3), node.snapLastTerm)
	require.Equal(t, []uint64{3, 4}, termsOf(node.log), "entries 6–7 survive; 1–5 are in the snapshot")
	require.Equal(t, uint64(5), node.commitIndex)
	require.Equal(t, uint64(5), node.lastApplied)
	require.Equal(t, uint64(7), node.lastLogIndex())
}

// ---------------------------------------------------------------------------
// Snapshot ↔ log interplay
// ---------------------------------------------------------------------------

// TestInstallSnapshot_ThenAppendEntriesFromBoundary is the far-behind-follower
// path end to end: the follower is too far behind for the log to repair it, the
// leader installs a snapshot, and the very next AppendEntries — starting at
// snapLastIndex+1 — must be accepted.
//
// Before the log machinery existed the middle step was unreachable, so this
// sequence had never run.
func TestInstallSnapshot_ThenAppendEntriesFromBoundary(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 6
	node.log = entriesOf(1) // hopelessly behind
	node.mu.Unlock()

	resp, err := node.HandleInstallSnapshot(context.Background(), &kvpb.InstallSnapshotRequest{
		Term: 6, LeaderId: "leader-1",
		LastIncludedIndex: 20, LastIncludedTerm: 5,
		Data: []byte("snapshot-at-20"),
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	node.mu.Lock()
	require.Equal(t, uint64(20), node.snapLastIndex)
	require.Equal(t, uint64(20), node.commitIndex)
	require.Equal(t, uint64(20), node.lastApplied)
	require.Nil(t, node.log)
	node.mu.Unlock()

	// The follow-on AppendEntries starts at the boundary.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 6, LeaderId: "leader-1",
		PrevLogIndex: 20, PrevLogTerm: 5,
		Entries:      wireEntries(21, 6, 6),
		LeaderCommit: 22,
	}).Success)

	node.mu.Lock()
	require.Equal(t, uint64(22), node.lastLogIndex())
	require.Equal(t, uint64(22), node.commitIndex)
	node.mu.Unlock()

	require.Equal(t, []string{"k21", "k22"}, sm.appliedKeys(),
		"only the entries after the snapshot are applied — the snapshot already carried the rest")
}

// TestInstallSnapshot_RetainsMatchingTail pins §7's retention rule: if the
// follower holds the snapshot's last included entry with the same term, the
// entries after it are still valid history and are kept rather than thrown away
// and re-fetched.
func TestInstallSnapshot_RetainsMatchingTail(t *testing.T) {
	node, _ := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 4
	node.log = entriesOf(1, 2, 3, 4, 4) // index 3 is term 3, 4–5 are term 4
	node.mu.Unlock()

	resp, err := node.HandleInstallSnapshot(context.Background(), &kvpb.InstallSnapshotRequest{
		Term: 4, LeaderId: "leader-1",
		LastIncludedIndex: 3, LastIncludedTerm: 3, // matches our index 3
		Data: []byte("snapshot-at-3"),
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, []uint64{4, 4}, termsOf(node.log), "indices 4–5 are still valid and kept")
	require.Equal(t, uint64(5), node.lastLogIndex())
	require.Equal(t, uint64(3), node.commitIndex, "the snapshot's boundary is committed; 4–5 are not")
}

// TestInstallSnapshot_DiscardsDivergentLog is the other branch: a term mismatch
// at the snapshot's last included index means our log describes a history the
// leader has superseded, so all of it goes.
func TestInstallSnapshot_DiscardsDivergentLog(t *testing.T) {
	node, _ := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 4
	node.log = entriesOf(1, 2, 2, 2) // index 3 is term 2, not 3
	node.mu.Unlock()

	resp, err := node.HandleInstallSnapshot(context.Background(), &kvpb.InstallSnapshotRequest{
		Term: 4, LeaderId: "leader-1",
		LastIncludedIndex: 3, LastIncludedTerm: 3,
		Data: []byte("snapshot-at-3"),
	})
	require.NoError(t, err)
	require.True(t, resp.Success)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Nil(t, node.log, "a divergent log is discarded entirely")
	require.Equal(t, uint64(3), node.lastLogIndex())
}

// TestInstallSnapshot_StaleSnapshotIsIgnored verifies a snapshot no newer than
// our own is not installed. Installing it would drive the state machine
// backwards to a state our log says is already in the past.
func TestInstallSnapshot_StaleSnapshotIsIgnored(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 5
	node.snapLastIndex = 10
	node.snapLastTerm = 4
	node.commitIndex = 10
	node.lastApplied = 10
	node.log = entriesOf() // compacted away
	node.mu.Unlock()

	resp, err := node.HandleInstallSnapshot(context.Background(), &kvpb.InstallSnapshotRequest{
		Term: 5, LeaderId: "leader-1",
		LastIncludedIndex: 8, LastIncludedTerm: 4,
		Data: []byte("older-snapshot"),
	})
	require.NoError(t, err)
	require.True(t, resp.Success, "nothing is wrong with the request — there is just nothing to do")

	require.Zero(t, sm.restoreCount(), "the state machine must not be rolled back")
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(10), node.snapLastIndex)
	require.Equal(t, uint64(10), node.lastApplied)
}

// TestTakeSnapshot_CutsAtLastAppliedNotLastLogIndex pins the snapshot's cut
// point. The payload reflects only what the state machine has consumed, so a
// snapshot labelled with the last *appended* index would promise a follower
// entries it does not contain — and would compact those entries away locally, so
// nothing could ever supply them again.
func TestTakeSnapshot_CutsAtLastAppliedNotLastLogIndex(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 3
	node.mu.Unlock()

	// Four entries stored, only the first two committed and applied.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 3, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 3, 3, 3, 3),
		LeaderCommit: 2,
	}).Success)
	require.Len(t, sm.appliedEntries(), 2)

	node.takeSnapshot(context.Background())

	snap, ok, err := node.snapshotStore.Load()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2), snap.LastIncludedIndex,
		"the snapshot must be labelled with the last applied index, not the last appended one")
	require.Equal(t, uint64(3), snap.LastIncludedTerm)
	require.Equal(t, []byte("applied=2"), snap.Data, "and carry the state machine's own bytes")

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(2), node.snapLastIndex)
	require.Equal(t, []uint64{3, 3}, termsOf(node.log), "indices 3–4 are not yet applied and must survive")
	require.Equal(t, uint64(4), node.lastLogIndex())
}

// TestTakeSnapshot_NoopWhenNothingApplied verifies a snapshot is not taken when
// there is nothing new in it: an empty snapshot would discard nothing and cost a
// full state-machine serialisation.
func TestTakeSnapshot_NoopWhenNothingApplied(t *testing.T) {
	node, _ := newTestNodeWithSM(t)
	node.mu.Lock()
	node.snapLastIndex = 4
	node.snapLastTerm = 2
	node.lastApplied = 4
	node.mu.Unlock()

	node.takeSnapshot(context.Background())

	_, ok, err := node.snapshotStore.Load()
	require.NoError(t, err)
	require.False(t, ok, "no snapshot should have been written")
}
