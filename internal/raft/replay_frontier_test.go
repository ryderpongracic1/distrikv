package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// The replay frontier: which applies Raft calls a replay, and which it calls
// live.
//
// lastApplied is volatile (see persistedState), so a restarting node re-applies
// every committed entry above the snapshot boundary. Rebuilding the state that
// way is correct and required. Re-emitting the *effects* of those entries is
// not, and it is what defect 14 records: the health state machine announced
// every historical down→up pair again on every restart. Raft is the only layer
// that can tell the two cases apart, because only Raft knows which entries
// predate the process — so it draws the line here and routes accordingly.

// TestReplayFrontier_PersistedEntriesReplayLiveEntriesDoNot is the frontier pin.
//
// It is an honest restart: entries are applied live in one incarnation, the node
// is dropped, and a fresh node with a fresh state machine is opened over the same
// directory. Everything that came off the disk must arrive as a replay, and an
// entry appended afterwards must arrive as live.
//
// Revert-check: make applyCommitted always call r.sm.Apply and the replay
// assertion fails on the reopened node; make it always call ReplayApply and the
// live assertion at the end fails.
func TestReplayFrontier_PersistedEntriesReplayLiveEntriesDoNot(t *testing.T) {
	dir := t.TempDir()

	// --- First incarnation: three entries arrive committed and are applied live.
	first := newTestSM()
	node := openNodeInDir(t, dir, first)
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2, 2),
		LeaderCommit: 3,
	}).Success)

	require.Equal(t, []string{"k1", "k2", "k3"}, first.appliedKeys())
	require.Equal(t, []bool{false, false, false}, first.replayFlags(),
		"an entry appended during this process's lifetime is live, not a replay")

	// --- Restart: the process state is gone, the directory is not.
	second := newTestSM()
	reopened := openNodeInDir(t, dir, second)

	reopened.mu.Lock()
	frontier := reopened.replayFrontier
	lastApplied := reopened.lastApplied
	reopened.mu.Unlock()
	require.Equal(t, uint64(3), frontier,
		"the frontier is the highest index that was already on disk at open")
	require.Zero(t, lastApplied,
		"lastApplied is volatile, so the reopened node has applied nothing yet — "+
			"which is exactly why the entries below the frontier are about to be applied again")

	// The leader says the same three entries are committed. They are applied a
	// second time, and every one of them is a replay.
	require.True(t, appendTo(t, reopened, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 3, PrevLogTerm: 2,
		LeaderCommit: 3,
	}).Success)

	require.Equal(t, []string{"k1", "k2", "k3"}, second.appliedKeys(),
		"the state must still be rebuilt from the log; only the effects are suppressed")
	require.Equal(t, []bool{true, true, true}, second.replayFlags(),
		"every entry that was on disk before this process started must arrive as a replay")

	// --- A genuinely new entry, above the frontier, is live again.
	require.True(t, appendTo(t, reopened, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 3, PrevLogTerm: 2,
		Entries:      wireEntries(4, 2),
		LeaderCommit: 4,
	}).Success)

	require.Equal(t, []string{"k1", "k2", "k3", "k4"}, second.appliedKeys())
	require.Equal(t, []bool{true, true, true, false}, second.replayFlags(),
		"crossing the frontier must restore live semantics: an entry appended in "+
			"this incarnation has never been applied before, so its effects are news")
}

// TestReplayFrontier_FreshNodeHasNoReplayWindow covers the common case. A node
// with nothing on disk cannot be replaying anything, so every entry it ever
// applies is live — the frontier must not swallow the first entries of a
// brand-new cluster.
func TestReplayFrontier_FreshNodeHasNoReplayWindow(t *testing.T) {
	node, sm := newTestNodeWithSM(t)

	node.mu.Lock()
	frontier := node.replayFrontier
	node.currentTerm = 1
	node.mu.Unlock()
	require.Zero(t, frontier, "an empty log leaves no window for a replay")

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 1, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 1, 1),
		LeaderCommit: 2,
	}).Success)

	require.Equal(t, []bool{false, false}, sm.replayFlags(),
		"the first entries a new node applies are live")
}

// TestReplayFrontier_SnapshotBoundaryLeavesNoReplayWindow covers a restart where
// compaction has already absorbed the whole log. The state machine is restored
// from the snapshot instead of replaying entries, so there is nothing below the
// frontier and nothing to suppress.
func TestReplayFrontier_SnapshotBoundaryLeavesNoReplayWindow(t *testing.T) {
	dir := t.TempDir()

	first := newTestSM()
	node := openNodeInDir(t, dir, first)
	node.mu.Lock()
	node.currentTerm = 3
	node.mu.Unlock()

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 3, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 3, 3),
		LeaderCommit: 2,
	}).Success)
	node.takeSnapshot(context.Background())

	second := newTestSM()
	reopened := openNodeInDir(t, dir, second)

	reopened.mu.Lock()
	frontier := reopened.replayFrontier
	snapLast := reopened.snapLastIndex
	reopened.mu.Unlock()

	require.Equal(t, uint64(2), snapLast)
	require.Equal(t, snapLast, frontier,
		"with the log fully compacted the frontier collapses onto the snapshot "+
			"boundary: the view came from RestoreFromSnapshot, so no entry is replayed")
	require.Equal(t, 1, second.restoreCount(), "the view is restored, not replayed")
	require.Empty(t, second.appliedKeys())

	// Everything from here is above the boundary and therefore live.
	require.True(t, appendTo(t, reopened, &kvpb.AppendEntriesRequest{
		Term: 3, LeaderId: "leader-1",
		PrevLogIndex: 2, PrevLogTerm: 3,
		Entries:      wireEntries(3, 3),
		LeaderCommit: 3,
	}).Success)
	require.Equal(t, []bool{false}, second.replayFlags())
}
