package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// This file covers the follower half of §5.3: the consistency check, conflict
// truncation, idempotent append, durability before acknowledgement, the bound on
// commitIndex, and apply-on-commit.

// wireEntries builds AppendEntries payload entries starting at index start, with
// terms taken from terms.
func wireEntries(start uint64, terms ...uint64) []*kvpb.LogEntry {
	out := make([]*kvpb.LogEntry, 0, len(terms))
	for i, term := range terms {
		idx := start + uint64(i)
		out = append(out, &kvpb.LogEntry{
			Index: idx,
			Term:  term,
			Op:    "health-down",
			Key:   fmt.Sprintf("k%d", idx),
		})
	}
	return out
}

// appendTo drives one AppendEntries into a node and returns the response.
func appendTo(t *testing.T, node *RaftNode, req *kvpb.AppendEntriesRequest) *kvpb.AppendEntriesResponse {
	t.Helper()
	resp, err := node.HandleAppendEntries(context.Background(), req)
	require.NoError(t, err)
	return resp
}

// ---------------------------------------------------------------------------
// The consistency check (§5.3)
// ---------------------------------------------------------------------------

// TestAppendEntries_RejectsMissingPrevIndex verifies a follower refuses a
// request whose PrevLogIndex it simply does not have. Accepting it would leave a
// hole in the log at the indices in between.
func TestAppendEntries_RejectsMissingPrevIndex(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 3
	node.log = entriesOf(1, 1) // indices 1–2
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 3, LeaderId: "leader-1",
		PrevLogIndex: 7, PrevLogTerm: 3,
		Entries: wireEntries(8, 3),
	})

	require.False(t, resp.Success, "no entry at index 7 means the check must fail")
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, []uint64{1, 1}, termsOf(node.log), "a rejected request must not touch the log")
}

// TestAppendEntries_RejectsPrevTermMismatch verifies the term half of the check:
// the follower has the index, but it was created by a different leader.
func TestAppendEntries_RejectsPrevTermMismatch(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 5
	node.log = entriesOf(1, 2, 2) // index 3 is term 2
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 5, LeaderId: "leader-1",
		PrevLogIndex: 3, PrevLogTerm: 4, // leader thinks index 3 is term 4
		Entries: wireEntries(4, 5),
	})

	require.False(t, resp.Success)
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, []uint64{1, 2, 2}, termsOf(node.log))
}

// TestAppendEntries_AcceptsAtSnapshotBoundary verifies the boundary case that
// once broke this cluster: a follower whose log was compacted still knows the
// term of the snapshot's last included index, so a PrevLogIndex pointing at it
// must be answerable without the snapshot being resent.
func TestAppendEntries_AcceptsAtSnapshotBoundary(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 4
	node.snapLastIndex = 5
	node.snapLastTerm = 3
	node.commitIndex = 5
	node.lastApplied = 5
	node.log = nil
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 4, LeaderId: "leader-1",
		PrevLogIndex: 5, PrevLogTerm: 3,
		Entries: wireEntries(6, 4),
	})

	require.True(t, resp.Success, "the snapshot boundary is a matchable position")
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(6), node.lastLogIndex())
}

// TestAppendEntries_RejectsBelowSnapshotBoundary verifies that a PrevLogIndex
// buried inside the snapshot is rejected rather than guessed at. The rejection
// is what makes the leader fall back to InstallSnapshot, so answering "yes" here
// would strand the follower.
func TestAppendEntries_RejectsBelowSnapshotBoundary(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 4
	node.snapLastIndex = 5
	node.snapLastTerm = 3
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 4, LeaderId: "leader-1",
		PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: wireEntries(4, 4),
	})

	require.False(t, resp.Success, "an index inside the snapshot cannot be vouched for")
}

// TestAppendEntries_ResetsElectionTimerEvenWhenRejected pins the ordering that
// keeps a diverging follower from electing against the leader repairing it.
// Recognition of the leader happens before the log is inspected, and survives a
// failed check.
func TestAppendEntries_ResetsElectionTimerEvenWhenRejected(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 6
	node.log = entriesOf(6)
	node.drainResetTimer()
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 6, LeaderId: "leader-7",
		PrevLogIndex: 99, PrevLogTerm: 6, // guaranteed rejection
	})
	require.False(t, resp.Success)

	node.mu.Lock()
	leaderID := node.leaderID
	heard := node.lastHeardFromLeader
	node.mu.Unlock()

	require.Equal(t, "leader-7", leaderID, "a rejected request still identifies the leader")
	require.False(t, heard.IsZero(), "and still counts as hearing from it")
	require.Len(t, node.resetTimerCh, 1, "and still resets the election timer")
}

// ---------------------------------------------------------------------------
// Truncation and idempotence
// ---------------------------------------------------------------------------

// TestAppendEntries_TruncatesConflictingSuffix verifies §5.3's third rule: an
// existing entry with the same index but a different term is deleted along with
// everything after it.
func TestAppendEntries_TruncatesConflictingSuffix(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 5
	node.log = entriesOf(1, 2, 2, 2) // indices 1–4
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 5, LeaderId: "leader-1",
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: wireEntries(2, 5), // index 2 is term 5, not 2
	})

	require.True(t, resp.Success)
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, []uint64{1, 5}, termsOf(node.log),
		"the conflicting entry and every entry after it must be gone")
}

// TestAppendEntries_DuplicateRequestIsIdempotent verifies that a retransmission —
// which happens whenever a reply is lost — neither duplicates entries nor
// rewrites the log file. Retransmission is normal, so it must be free.
func TestAppendEntries_DuplicateRequestIsIdempotent(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	req := &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: wireEntries(1, 2, 2, 2),
	}

	require.True(t, appendTo(t, node, req).Success)
	afterFirst := node.persist.saveCount()
	first := snapshotLog(node)
	require.Len(t, first, 3)

	// Same request again, three times over.
	for i := 0; i < 3; i++ {
		require.True(t, appendTo(t, node, req).Success)
	}

	require.Equal(t, first, snapshotLog(node), "duplicates must not change the log")
	require.Equal(t, afterFirst, node.persist.saveCount(),
		"a duplicate request already satisfied by the log must not write to disk")
}

// TestAppendEntries_PureHeartbeatDoesNotPersist pins the hot path. A heartbeat
// carries no entries and changes nothing, so it must not touch the disk: putting
// an fsync on the interval every follower depends on is the shape of the defect
// that once starved this cluster of heartbeats entirely.
func TestAppendEntries_PureHeartbeatDoesNotPersist(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 4
	node.log = entriesOf(4, 4)
	node.mu.Unlock()

	before := node.persist.saveCount()
	for i := 0; i < 20; i++ {
		require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
			Term: 4, LeaderId: "leader-1",
			PrevLogIndex: 2, PrevLogTerm: 4,
			LeaderCommit: 2,
		}).Success)
	}
	require.Equal(t, before, node.persist.saveCount(), "heartbeats must not hit disk")
}

// TestAppendEntries_RefusesToTruncateCommittedEntry pins the safety guard. A
// committed entry cannot conflict with the current leader's log — the Leader
// Completeness Property forbids it — so reaching that state means an invariant
// is already broken. Refusing to truncate keeps a detectable bug from becoming
// silent data loss.
func TestAppendEntries_RefusesToTruncateCommittedEntry(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 7
	node.log = entriesOf(3, 3, 3)
	node.commitIndex = 2 // indices 1–2 are committed
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 7, LeaderId: "leader-1",
		PrevLogIndex: 1, PrevLogTerm: 3,
		Entries: wireEntries(2, 9), // conflicts with committed index 2
	})

	require.False(t, resp.Success, "the request must be refused, not obeyed")
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, []uint64{3, 3, 3}, termsOf(node.log), "committed entries must survive")
}

// ---------------------------------------------------------------------------
// Durability before acknowledgement
// ---------------------------------------------------------------------------

// TestAppendEntries_PersistFailureIsNotAcknowledged is the structural pin for
// "a node never acknowledges an entry it has not stored".
//
// The persistence target is made unwritable, so the write cannot succeed. The
// follower must reply false *and* leave its in-memory log untouched — the second
// half matters as much as the first: if the entry lived in memory only, the
// leader's retry would find it already present, skip the write as a duplicate,
// and be acknowledged for an entry no disk ever saw.
func TestAppendEntries_PersistFailureIsNotAcknowledged(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 2
	node.log = entriesOf(2)
	// A path whose parent directory does not exist: os.Create fails.
	node.persist = newPersistentState("/nonexistent-directory-for-raft-test/raft-state")
	node.mu.Unlock()

	req := &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 1, PrevLogTerm: 2,
		Entries: wireEntries(2, 2),
	}

	resp := appendTo(t, node, req)
	require.False(t, resp.Success, "an entry that could not be stored must not be acknowledged")

	node.mu.Lock()
	require.Equal(t, []uint64{2}, termsOf(node.log),
		"the in-memory log must match what durable storage has, so the retry re-derives the append")
	node.mu.Unlock()

	// Retry against a working disk: the same entry is stored and only then
	// acknowledged.
	node.mu.Lock()
	node.persist = newPersistentState(t.TempDir() + "/raft-state")
	node.mu.Unlock()

	require.True(t, appendTo(t, node, req).Success)
	st, err := node.persist.Load()
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 2}, termsOf(st.Log), "the retry is what makes it durable")
}

// ---------------------------------------------------------------------------
// commitIndex
// ---------------------------------------------------------------------------

// TestAppendEntries_CommitIndexBoundedByRequest verifies the min() in §5.3's
// fifth rule. A leader's commitIndex can be far ahead of what this particular
// request delivered, and commitIndex must not run past the entries the request
// actually vouched for.
func TestAppendEntries_CommitIndexBoundedByRequest(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 3
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 3, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 3, 3),
		LeaderCommit: 99, // the leader is way ahead
	})
	require.True(t, resp.Success)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(2), node.commitIndex,
		"commitIndex is bounded by the last entry this request delivered, not by leaderCommit")
}

// TestAppendEntries_CommitIndexIgnoresUnvouchedTail pins the subtler half of the
// same bound: when the merge leaves entries beyond the request's range in place,
// those entries came from some earlier leader and this request says nothing
// about them. Bounding on lastLogIndex instead of the request's range would
// commit them on no authority at all.
func TestAppendEntries_CommitIndexIgnoresUnvouchedTail(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 6
	node.log = entriesOf(6, 6, 6, 6, 6) // indices 1–5, the tail unvouched-for
	node.mu.Unlock()

	resp := appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 6, LeaderId: "leader-1",
		PrevLogIndex: 1, PrevLogTerm: 6,
		Entries:      wireEntries(2, 6), // re-sends index 2 only
		LeaderCommit: 5,
	})
	require.True(t, resp.Success)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(2), node.commitIndex,
		"only indices this request covered may be committed")
}

// ---------------------------------------------------------------------------
// Apply-on-commit
// ---------------------------------------------------------------------------

// TestApply_OnlyAfterCommitAndInOrder is the apply-on-commit pin. Entries are
// invisible to the state machine while they are merely stored, and arrive in log
// order once committed.
//
// The old code applied on receipt, which inverted the paper's ordering: an entry
// that a later leader truncated had already changed the state machine.
func TestApply_OnlyAfterCommitAndInOrder(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	// Stored but not committed.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2, 2),
		LeaderCommit: 0,
	}).Success)

	require.Empty(t, sm.appliedEntries(), "an uncommitted entry must not reach the state machine")

	// A later heartbeat commits them.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 3, PrevLogTerm: 2,
		LeaderCommit: 3,
	}).Success)

	require.Equal(t, []string{"k1", "k2", "k3"}, sm.appliedKeys(),
		"committed entries are applied in log order")

	node.mu.Lock()
	require.Equal(t, uint64(3), node.lastApplied)
	node.mu.Unlock()
}

// TestApply_ExactlyOncePerEntry verifies that repeated commit notifications do
// not re-apply. Heartbeats repeat the same leaderCommit every interval, so an
// apply loop keyed on anything but lastApplied would replay the whole log
// continuously.
func TestApply_ExactlyOncePerEntry(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2),
		LeaderCommit: 2,
	}).Success)
	require.Len(t, sm.appliedEntries(), 2)

	for i := 0; i < 10; i++ {
		require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
			Term: 2, LeaderId: "leader-1",
			PrevLogIndex: 2, PrevLogTerm: 2,
			LeaderCommit: 2,
		}).Success)
	}

	require.Equal(t, []string{"k1", "k2"}, sm.appliedKeys(),
		"ten more commit notifications must not re-apply anything")
}

// TestApply_ErrorLeavesEntryPendingAndRetries verifies the retry contract: a
// failing Apply does not advance lastApplied, so the entry is offered again
// rather than skipped. Skipping would let the state machine silently miss a
// committed change.
func TestApply_ErrorLeavesEntryPendingAndRetries(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	sm.setApplyErr(errors.New("state machine unavailable"))

	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      wireEntries(1, 2, 2),
		LeaderCommit: 2,
	}).Success)

	require.Empty(t, sm.appliedEntries())
	node.mu.Lock()
	require.Zero(t, node.lastApplied, "a failed apply must not advance the frontier")
	node.mu.Unlock()

	sm.setApplyErr(nil)

	// The next heartbeat retries from where it stopped.
	require.True(t, appendTo(t, node, &kvpb.AppendEntriesRequest{
		Term: 2, LeaderId: "leader-1",
		PrevLogIndex: 2, PrevLogTerm: 2,
		LeaderCommit: 2,
	}).Success)

	require.Equal(t, []string{"k1", "k2"}, sm.appliedKeys(), "nothing was skipped")
}
