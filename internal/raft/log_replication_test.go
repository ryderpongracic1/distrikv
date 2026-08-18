package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// ---------------------------------------------------------------------------
// Deterministic replication harness
// ---------------------------------------------------------------------------
//
// These tests drive replication one round at a time, synchronously, through the
// same two functions a heartbeat tick uses: appendArgsForLocked to build the
// payload and sendAppendEntries to send it and fold in the reply. That keeps the
// production path under test while making the number of rounds a convergence
// takes an exact, assertable quantity instead of a race against a timer.

// directPeer dispatches straight into another RaftNode's AppendEntries handler.
type directPeer struct {
	kvpb.KVServiceClient
	target *RaftNode
	calls  int
}

func (p *directPeer) AppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	p.calls++
	return p.target.HandleAppendEntries(ctx, req)
}

// entriesOf builds a log from a compact "term per index" description: terms[i]
// is the term of the entry at index i+1. This is how the Raft paper's Figure 7
// draws logs, so the fixtures below read the same way the figure does.
func entriesOf(terms ...uint64) []LogEntry {
	out := make([]LogEntry, 0, len(terms))
	for i, term := range terms {
		idx := uint64(i + 1)
		out = append(out, LogEntry{
			Index: idx,
			Term:  term,
			Op:    "health-down",
			Key:   fmt.Sprintf("n%d-t%d", idx, term),
		})
	}
	return out
}

// termsOf is the inverse of entriesOf, for comparing two logs by shape.
func termsOf(entries []LogEntry) []uint64 {
	out := make([]uint64, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Term)
	}
	return out
}

// newLeaderWithLog returns a leader holding the given log at the given term,
// wired to one follower, plus the follower and the peer handle.
func newLeaderWithLog(t *testing.T, term uint64, leaderTerms []uint64, followerTerms []uint64) (leader, follower *RaftNode, peer *directPeer) {
	t.Helper()

	leader = newTestNode(t)
	follower = newTestNode(t)

	leader.mu.Lock()
	leader.currentTerm = term
	leader.role = Leader
	leader.leaderID = leader.nodeID
	leader.log = entriesOf(leaderTerms...)
	leader.mu.Unlock()

	follower.mu.Lock()
	follower.currentTerm = term
	follower.log = entriesOf(followerTerms...)
	follower.mu.Unlock()

	peer = &directPeer{target: follower}
	leader.peers = []PeerClient{{NodeID: follower.nodeID, Client: peer}}
	leader.mu.Lock()
	leader.nextIndex = map[string]uint64{follower.nodeID: leader.lastLogIndex() + 1}
	leader.matchIndex = map[string]uint64{follower.nodeID: 0}
	leader.mu.Unlock()

	return leader, follower, peer
}

// replicationRound runs exactly one replication round to one peer.
func replicationRound(leader *RaftNode, peer PeerClient) {
	leader.mu.Lock()
	a, _ := leader.appendArgsForLocked(peer.NodeID)
	leader.mu.Unlock()
	leader.sendAppendEntries(context.Background(), peer, a)
}

// snapshotLog returns a copy of a node's log.
func snapshotLog(n *RaftNode) []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]LogEntry, len(n.log))
	copy(out, n.log)
	return out
}

// ---------------------------------------------------------------------------
// Figure 7: the leader repairs every follower shape in the paper
// ---------------------------------------------------------------------------

// TestFigure7_LeaderConvergesEveryFollowerShape reproduces the six follower logs
// of Figure 7 in the Raft paper against the figure's leader log, and proves each
// one converges to the leader's log.
//
// The leader's log (indices 1–10) has terms 1,1,1,4,4,5,5,6,6,6. The follower
// shapes are the paper's: (a) and (b) are missing a suffix, (c) and (d) have
// extra entries the leader does not have, and (e) and (f) diverge in term as
// well as length — the case that requires truncation, not just appending.
//
// # Two mechanisms, and why (c) and (d) need the second one
//
// Phase 1 is decrement-and-retry: the leader walks nextIndex back one index per
// rejection until the consistency check passes, then ships everything from there.
// That converges (a), (b), (e) and (f) exactly.
//
// It does not converge (c) and (d), and that is the algorithm working as
// specified rather than a gap. Their divergence is *past* the leader's last
// index, so the consistency check at PrevLogIndex=10 succeeds on the first try
// and the leader has no entries to send for indices 11 and 12. §5.3's truncation
// rule fires on a *conflicting entry received*, so with nothing received there
// is nothing to truncate. The extra entries are removed the moment the leader
// has something to put at that index — which the second phase below does.
//
// This is safe in the interval, and the test pins the reason: the follower's
// commitIndex is bounded by what the request vouches for, so an entry past the
// leader's last index can never be committed while it survives. It is dead
// weight, not a divergence anyone can observe.
func TestFigure7_LeaderConvergesEveryFollowerShape(t *testing.T) {
	leaderTerms := []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}

	cases := []struct {
		name      string
		follower  []uint64
		maxRounds int // rejections needed, plus the accepted round
	}{
		{"a: short by one", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6}, 2},
		{"b: short by six", []uint64{1, 1, 1, 4}, 7},
		{"c: one extra entry", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}, 1},
		{"d: two extra entries", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}, 1},
		{"e: divergent term-4 tail", []uint64{1, 1, 1, 4, 4, 4, 4}, 6},
		{"f: divergent term-2/3 history", []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}, 8},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			leader, follower, peer := newLeaderWithLog(t, 8, leaderTerms, tc.follower)

			// Phase 1: the leader replicates its whole log into place.
			rounds := 0
			for round := 1; round <= tc.maxRounds; round++ {
				replicationRound(leader, leader.peers[0])
				rounds = round
				leader.mu.Lock()
				matched := leader.matchIndex[follower.nodeID]
				leader.mu.Unlock()
				if matched == uint64(len(leaderTerms)) {
					break
				}
			}

			// Every index the leader holds now agrees, entry for entry.
			leaderLog := snapshotLog(leader)
			followerLog := snapshotLog(follower)
			require.GreaterOrEqual(t, len(followerLog), len(leaderLog),
				"follower is still short of the leader after %d rounds: %v", rounds, termsOf(followerLog))
			require.Equal(t, leaderLog, followerLog[:len(leaderLog)],
				"the leader's indices must match exactly after %d rounds", rounds)

			leader.mu.Lock()
			require.Equal(t, uint64(len(leaderTerms)), leader.matchIndex[follower.nodeID])
			require.Equal(t, uint64(len(leaderTerms)+1), leader.nextIndex[follower.nodeID])
			leader.mu.Unlock()

			// Any entry beyond the leader's last index is uncommitted and stays
			// that way: the follower's commitIndex is bounded by the leader's.
			follower.mu.Lock()
			followerCommit := follower.commitIndex
			follower.mu.Unlock()
			require.LessOrEqual(t, followerCommit, uint64(len(leaderTerms)),
				"a follower must never commit past what the leader vouched for")

			// Phase 2: the leader appends an entry of its own term. For (c) and
			// (d) this is the conflicting entry that truncates the extra tail;
			// for the rest it is an ordinary append. Either way the logs end up
			// identical.
			_, err := leader.Propose(context.Background(), "health-up", "n9", nil)
			require.NoError(t, err)
			replicationRound(leader, leader.peers[0])

			require.Equal(t, snapshotLog(leader), snapshotLog(follower),
				"logs must be identical once the leader has an entry at every divergent index")
			t.Logf("%s: converged in %d round(s) + 1 append, %d RPCs", tc.name, rounds, peer.calls)
		})
	}
}

// TestFigure7_BackoffIsOnePerRound pins the backoff step itself: a rejection
// moves nextIndex down by exactly one, and only one, per round. A step of zero
// would loop forever; a step of more than one could skip past the first matching
// index and re-send entries the follower already had.
func TestFigure7_BackoffIsOnePerRound(t *testing.T) {
	leaderTerms := []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	leader, follower, _ := newLeaderWithLog(t, 8, leaderTerms, []uint64{1, 1, 1, 4})

	want := uint64(len(leaderTerms) + 1)
	for round := 1; round <= 6; round++ {
		leader.mu.Lock()
		got := leader.nextIndex[follower.nodeID]
		leader.mu.Unlock()
		require.Equal(t, want, got, "nextIndex after %d rejection(s)", round-1)

		replicationRound(leader, leader.peers[0])
		want--
	}
}

// ---------------------------------------------------------------------------
// §5.4.2 / Figure 8: an old-term entry never commits on its own majority
// ---------------------------------------------------------------------------

// TestCommitRule_OldTermEntryNeedsCurrentTermEntry is the Figure-8 pin.
//
// The leader holds an entry from an earlier term that is replicated to a
// majority. Committing it on that majority alone is the lost-commit bug: a
// future leader that never received it can still win an election and overwrite
// it, so an acknowledged commit would be undone. §5.4.2 forbids counting the
// majority unless the entry is from the leader's own term.
//
// The test therefore asserts two things in sequence: nothing commits while the
// only majority-replicated entry is old, and the moment an entry of the current
// term commits, the older entry commits with it — indirectly, which is how the
// paper says it must happen.
func TestCommitRule_OldTermEntryNeedsCurrentTermEntry(t *testing.T) {
	leader := newTestNode(t)
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 4
	// Index 1 was created in term 2 — carried over from a previous leader.
	leader.log = entriesOf(2)
	leader.nextIndex = map[string]uint64{"p1": 2, "p2": 2}
	leader.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
	leader.peers = []PeerClient{{NodeID: "p1"}, {NodeID: "p2"}}
	leader.mu.Unlock()

	// Both followers now hold index 1: a full majority, in fact the whole
	// cluster. It must still not commit.
	leader.mu.Lock()
	leader.matchIndex["p1"] = 1
	leader.matchIndex["p2"] = 1
	leader.advanceCommitIndexLocked()
	commit := leader.commitIndex
	leader.mu.Unlock()

	require.Equal(t, uint64(0), commit,
		"an entry from term 2 must not commit on a majority while the leader is in term 4 (§5.4.2)")

	// The leader appends an entry of its own term and gets it onto a majority.
	idx, err := leader.Propose(context.Background(), "health-up", "n2", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), idx)

	leader.mu.Lock()
	leader.matchIndex["p1"] = 2
	leader.advanceCommitIndexLocked() // leader + p1 = majority of 3
	commit = leader.commitIndex
	leader.mu.Unlock()

	require.Equal(t, uint64(2), commit,
		"the current-term entry commits, and carries the older entry with it")
}

// TestCommitRule_RequiresMajorityNotJustOne pins the other half of the rule: a
// current-term entry on one follower out of two is not a majority.
func TestCommitRule_RequiresMajorityNotJustOne(t *testing.T) {
	leader := newTestNode(t)
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 3
	leader.log = entriesOf(3, 3)
	leader.peers = []PeerClient{{NodeID: "p1"}, {NodeID: "p2"}, {NodeID: "p3"}, {NodeID: "p4"}}
	leader.nextIndex = map[string]uint64{}
	leader.matchIndex = map[string]uint64{"p1": 2, "p2": 0, "p3": 0, "p4": 0}
	leader.advanceCommitIndexLocked()
	commit := leader.commitIndex
	leader.mu.Unlock()

	require.Equal(t, uint64(0), commit, "leader + 1 of 4 peers is not a majority of 5")

	leader.mu.Lock()
	leader.matchIndex["p2"] = 2
	leader.advanceCommitIndexLocked()
	commit = leader.commitIndex
	leader.mu.Unlock()

	require.Equal(t, uint64(2), commit, "leader + 2 of 4 peers is a majority of 5")
}

// TestCommitRule_LeaderCountsItsOwnPersistedEntry pins that the leader's own
// copy counts toward the majority. On a three-node cluster one follower is
// enough precisely because the leader has already made the entry durable before
// Propose returned.
func TestCommitRule_LeaderCountsItsOwnPersistedEntry(t *testing.T) {
	leader := newTestNode(t)
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 2
	leader.peers = []PeerClient{{NodeID: "p1"}, {NodeID: "p2"}}
	leader.nextIndex = map[string]uint64{"p1": 1, "p2": 1}
	leader.matchIndex = map[string]uint64{"p1": 0, "p2": 0}
	leader.mu.Unlock()

	idx, err := leader.Propose(context.Background(), "health-down", "n3", nil)
	require.NoError(t, err)

	// Durable before Propose returned — that is what licenses counting it.
	st, err := leader.persist.Load()
	require.NoError(t, err)
	require.Len(t, st.Log, 1)
	require.Equal(t, idx, st.Log[0].Index)

	leader.mu.Lock()
	leader.matchIndex["p1"] = idx
	leader.advanceCommitIndexLocked()
	commit := leader.commitIndex
	leader.mu.Unlock()

	require.Equal(t, idx, commit, "leader + 1 follower is a majority of 3")
}

// TestPropose_RejectedOnFollower pins the leader-only contract.
func TestPropose_RejectedOnFollower(t *testing.T) {
	node := newTestNode(t)

	_, err := node.Propose(context.Background(), "health-down", "n2", nil)
	require.ErrorIs(t, err, ErrNotLeader)

	node.mu.Lock()
	logLen := len(node.log)
	node.mu.Unlock()
	require.Zero(t, logLen, "a rejected proposal must not touch the log")
}

// TestPropose_AssignsContiguousIndicesInCurrentTerm pins the index/term the
// leader stamps on a proposal.
func TestPropose_AssignsContiguousIndicesInCurrentTerm(t *testing.T) {
	leader := newTestNode(t)
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 9
	leader.mu.Unlock()

	for want := uint64(1); want <= 3; want++ {
		got, err := leader.Propose(context.Background(), "health-up", fmt.Sprintf("n%d", want), []byte("x"))
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	leader.mu.Lock()
	defer leader.mu.Unlock()
	require.Equal(t, []uint64{9, 9, 9}, termsOf(leader.log))
}

// TestPropose_CopiesValue pins that the log does not alias a caller's buffer.
func TestPropose_CopiesValue(t *testing.T) {
	leader := newTestNode(t)
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.mu.Unlock()

	value := []byte("original")
	_, err := leader.Propose(context.Background(), "health-up", "n2", value)
	require.NoError(t, err)

	value[0] = 'X' // caller reuses its buffer

	leader.mu.Lock()
	defer leader.mu.Unlock()
	require.Equal(t, []byte("original"), leader.log[0].Value)
}

// ---------------------------------------------------------------------------
// Health signal
// ---------------------------------------------------------------------------

// healthSink records what the leader reported about each peer.
type healthSink struct {
	observations []bool
}

func (h *healthSink) ObserveHeartbeat(_ string, ok bool) {
	h.observations = append(h.observations, ok)
}

// rejectingPeer answers every AppendEntries with Success=false — a live follower
// whose log disagrees.
type rejectingPeer struct {
	kvpb.KVServiceClient
	term uint64
}

func (p *rejectingPeer) AppendEntries(_ context.Context, _ *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	return &kvpb.AppendEntriesResponse{Term: p.term, Success: false}, nil
}

// unreachablePeer fails every AppendEntries at the transport.
type unreachablePeer struct {
	kvpb.KVServiceClient
}

func (p *unreachablePeer) AppendEntries(_ context.Context, _ *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	return nil, errors.New("connection refused")
}

// TestHealthSignal_RejectionIsALivePeer is the pin for the subtlety this whole
// feature exists to serve: a follower that answers "no" is a follower that
// answered.
//
// A log mismatch means a healthy peer disagrees about history — it is reachable,
// it is responsive, and the leader is about to repair it. Reporting that as
// unreachable would mark a node down at the exact moment it is proving itself
// alive, and node health is the only thing this Raft log carries. Only a
// transport failure is ok=false.
func TestHealthSignal_RejectionIsALivePeer(t *testing.T) {
	leader := newTestNode(t)
	sink := &healthSink{}
	leader.SetPeerHealthObserver(sink)

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 5
	leader.log = entriesOf(5, 5)
	leader.peers = []PeerClient{{NodeID: "rejecter", Client: &rejectingPeer{term: 5}}}
	leader.nextIndex = map[string]uint64{"rejecter": 3}
	leader.matchIndex = map[string]uint64{"rejecter": 0}
	leader.mu.Unlock()

	replicationRound(leader, leader.peers[0])

	require.Equal(t, []bool{true}, sink.observations,
		"a peer that replied Success=false is up, and must be reported ok=true")

	// And the rejection still drove the repair machinery.
	leader.mu.Lock()
	defer leader.mu.Unlock()
	require.Equal(t, uint64(2), leader.nextIndex["rejecter"], "rejection must back nextIndex off")
	require.Zero(t, leader.matchIndex["rejecter"], "a rejection tells us nothing is matched")
}

// TestHealthSignal_TransportFailureIsDown is the other side of the same pin: an
// RPC that never got an answer is the one case that means down.
func TestHealthSignal_TransportFailureIsDown(t *testing.T) {
	leader := newTestNode(t)
	sink := &healthSink{}
	leader.SetPeerHealthObserver(sink)

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 5
	leader.peers = []PeerClient{{NodeID: "gone", Client: &unreachablePeer{}}}
	leader.nextIndex = map[string]uint64{"gone": 1}
	leader.matchIndex = map[string]uint64{"gone": 0}
	leader.mu.Unlock()

	replicationRound(leader, leader.peers[0])

	require.Equal(t, []bool{false}, sink.observations,
		"a transport failure is the only outcome that means the peer is down")
}

// TestReplication_StaleTermReplyIsIgnored pins that a reply belonging to a term
// the leader has left cannot move its bookkeeping. Up to a few AppendEntries can
// be in flight to one peer at once, so a reply arriving after a step-down and a
// re-election is a real occurrence, not a hypothetical.
//
// The stale payload deliberately claims a *higher* match than the leader
// currently credits the peer with. A reply that would change nothing anyway
// would pin nothing: it has to be one the guard is the only thing stopping.
func TestReplication_StaleTermReplyIsIgnored(t *testing.T) {
	leader := newTestNode(t)
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 3
	leader.log = entriesOf(3, 3)
	leader.peers = []PeerClient{{NodeID: "p1"}}
	leader.nextIndex = map[string]uint64{"p1": 3}
	leader.matchIndex = map[string]uint64{"p1": 1}
	leader.mu.Unlock()

	// A success reply for term 2 — a term this leader has long since left —
	// covering indices 6–8, well past anything it currently credits p1 with.
	stale := appendArgs{
		term:          2,
		prevLogIndex:  5,
		entries:       wireEntries(6, 2, 2, 2),
		nextIndexSent: 6,
	}
	leader.sendAppendEntries(context.Background(), PeerClient{
		NodeID: "p1",
		Client: &acceptingPeer{term: 2},
	}, stale)

	leader.mu.Lock()
	defer leader.mu.Unlock()
	require.Equal(t, uint64(1), leader.matchIndex["p1"],
		"a reply from a term the leader has left must not advance matchIndex")
	require.Equal(t, uint64(3), leader.nextIndex["p1"], "nor nextIndex")
	require.Zero(t, leader.commitIndex, "and must not be allowed to commit anything")
}

// acceptingPeer answers every AppendEntries with Success=true.
type acceptingPeer struct {
	kvpb.KVServiceClient
	term uint64
}

func (p *acceptingPeer) AppendEntries(_ context.Context, _ *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	return &kvpb.AppendEntriesResponse{Term: p.term, Success: true}, nil
}
