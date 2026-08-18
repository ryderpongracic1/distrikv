package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// §5.4.1's election restriction — a candidate must hold a log at least as
// up-to-date as the voter's — has been implemented in this package all along,
// but until the log could hold entries it was decided on two zeroes on both
// sides. Every comparison was between empty logs, so the rule was vacuously
// true and untested. These tests give it real logs to judge.

// TestElectionRestriction_LogComparison walks the decision table with divergent
// logs. The rule is lexicographic on (lastTerm, lastIndex): a higher last term
// always wins regardless of length, and length decides only within one term.
//
// The asymmetry in the middle rows is the point of the rule. A long log full of
// old-term entries loses to a short log with a newer one, because those old
// entries may never have been committed anywhere — while a shorter log in the
// newest term cannot be missing anything that was.
func TestElectionRestriction_LogComparison(t *testing.T) {
	cases := []struct {
		name          string
		voterLog      []uint64
		candidateIdx  uint64
		candidateTerm uint64
		wantUpToDate  bool
	}{
		{"identical logs", []uint64{1, 2, 3}, 3, 3, true},
		{"candidate one entry ahead in same term", []uint64{1, 2, 3}, 4, 3, true},
		{"candidate one entry behind in same term", []uint64{1, 2, 3}, 2, 3, false},
		{"candidate higher term, shorter log", []uint64{1, 1, 1, 1, 1}, 2, 4, true},
		{"candidate lower term, longer log", []uint64{1, 2, 5}, 9, 4, false},
		{"candidate lower term, same length", []uint64{1, 2, 5}, 3, 4, false},
		{"empty candidate against populated voter", []uint64{1, 2}, 0, 0, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			node := newTestNode(t)
			node.mu.Lock()
			node.log = entriesOf(tc.voterLog...)
			got := node.candidateLogUpToDateLocked(tc.candidateIdx, tc.candidateTerm)
			node.mu.Unlock()

			require.Equal(t, tc.wantUpToDate, got,
				"voter log %v vs candidate (index=%d, term=%d)", tc.voterLog, tc.candidateIdx, tc.candidateTerm)
		})
	}
}

// TestElectionRestriction_VoteDeniedToDivergentCandidate takes the same rule
// through the real RPC: a candidate with a stale log is refused even though its
// term is higher, which is exactly the case that would otherwise lose committed
// entries when that candidate won and overwrote the voter's log.
func TestElectionRestriction_VoteDeniedToDivergentCandidate(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 5
	// Our log ends at index 6, term 5 — a full, current history.
	node.log = entriesOf(1, 2, 2, 4, 5, 5)
	node.mu.Unlock()

	resp, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:         6, // a higher term …
		CandidateId:  "stale-candidate",
		LastLogIndex: 9, // … and a longer log …
		LastLogTerm:  4, // … but its newest entry is older than ours
	})
	require.NoError(t, err)
	require.False(t, resp.VoteGranted, "length must not beat term")

	// The term still advances: the candidate's term is legitimately higher, and
	// refusing the vote is not refusing the term (§5.1).
	require.Equal(t, uint64(6), node.CurrentTerm())
}

// TestElectionRestriction_VoteGrantedToUpToDateCandidate is the positive control
// for the test above: same voter, same divergence in length, but the candidate's
// newest entry is from a term at least as new, so the vote is granted.
func TestElectionRestriction_VoteGrantedToUpToDateCandidate(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 5
	node.log = entriesOf(1, 2, 2, 4, 5, 5)
	node.mu.Unlock()

	resp, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:         6,
		CandidateId:  "good-candidate",
		LastLogIndex: 6,
		LastLogTerm:  5,
	})
	require.NoError(t, err)
	require.True(t, resp.VoteGranted)
}

// TestElectionRestriction_CompactedVoterUsesSnapshotBoundary verifies the rule
// still works on a voter whose log has been compacted away entirely: its last
// index and term come from the snapshot boundary, not from an empty slice.
//
// Getting this wrong would be quiet and serious — a node that had just compacted
// would report an empty log and grant its vote to anybody.
func TestElectionRestriction_CompactedVoterUsesSnapshotBoundary(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 7
	node.snapLastIndex = 40
	node.snapLastTerm = 6
	node.log = nil
	node.mu.Unlock()

	denied, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term: 8, CandidateId: "behind-the-snapshot",
		LastLogIndex: 30, LastLogTerm: 6,
	})
	require.NoError(t, err)
	require.False(t, denied.VoteGranted, "a candidate behind our snapshot is behind us")

	node2 := newTestNode(t)
	node2.mu.Lock()
	node2.currentTerm = 7
	node2.snapLastIndex = 40
	node2.snapLastTerm = 6
	node2.mu.Unlock()

	granted, err := node2.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term: 8, CandidateId: "at-the-snapshot",
		LastLogIndex: 40, LastLogTerm: 6,
	})
	require.NoError(t, err)
	require.True(t, granted.VoteGranted, "a candidate level with our snapshot is level with us")
}

// TestElectionRestriction_PreVoteAppliesTheSameRule pins that the pre-vote round
// screens on the log too. If it did not, a node with a stale log would pass
// pre-vote, increment its term for a real election it cannot win, and disturb a
// healthy leader — which is the entire thing pre-vote exists to prevent.
func TestElectionRestriction_PreVoteAppliesTheSameRule(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 4
	node.log = entriesOf(1, 3, 4, 4)
	node.mu.Unlock()

	resp, err := node.HandlePreVote(context.Background(), &kvpb.PreVoteRequest{
		NextTerm: 5, CandidateId: "stale",
		LastLogIndex: 10, LastLogTerm: 3, // longer, but older
	})
	require.NoError(t, err)
	require.False(t, resp.VoteGranted)

	// And the dry run really was dry.
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, uint64(4), node.currentTerm)
	require.Equal(t, "", node.votedFor)
}
