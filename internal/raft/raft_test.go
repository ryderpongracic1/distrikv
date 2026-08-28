package raft

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func newTestNode(t *testing.T) *RaftNode {
	t.Helper()
	node, _ := newTestNodeWithSM(t)
	return node
}

// newTestNodeWithSM returns a node and the state machine Raft applies to, for
// tests that need to see what was applied.
func newTestNodeWithSM(t *testing.T) (*RaftNode, *testSM) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		NodeID:             "test-node",
		DataDir:            dir,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sm := newTestSM()
	node, err := New(cfg, nil, sm, nil, logger)
	require.NoError(t, err)
	return node, sm
}

// noopMetrics satisfies metricsInterface without recording anything.
type noopMetrics struct{}

func (n *noopMetrics) IncRaftTerms()                {}
func (n *noopMetrics) IncLeaderElections()          {}
func (n *noopMetrics) SetLastAppliedIndex(_ uint64) {}

// stubKVClient is a minimal kvpb.KVServiceClient for injecting vote responses.
type stubKVClient struct {
	kvpb.KVServiceClient
	voteResp *kvpb.RequestVoteResponse
	voteErr  error
}

func (s *stubKVClient) RequestVote(_ context.Context, _ *kvpb.RequestVoteRequest, _ ...grpc.CallOption) (*kvpb.RequestVoteResponse, error) {
	return s.voteResp, s.voteErr
}

func (s *stubKVClient) AppendEntries(_ context.Context, _ *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	return &kvpb.AppendEntriesResponse{Term: 0, Success: true}, nil
}

// ---------------------------------------------------------------------------
// RequestVote tests
// ---------------------------------------------------------------------------

// TestRequestVote_GrantsVote verifies a fresh node grants a vote to a
// candidate with a higher term and an up-to-date log.
func TestRequestVote_GrantsVote(t *testing.T) {
	node := newTestNode(t)

	resp, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(1), resp.Term)

	// votedFor should now be persisted.
	node.mu.Lock()
	assert.Equal(t, "candidate-1", node.votedFor)
	node.mu.Unlock()
}

// TestRequestVote_RejectsStaleTerm verifies that a RequestVote with a term
// lower than the node's current term is rejected.
func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	node := newTestNode(t)
	node.currentTerm = 5

	resp, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:        3,
		CandidateId: "stale-candidate",
	})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)
	assert.Equal(t, uint64(5), resp.Term)
}

// TestRequestVote_RejectsDuplicateVote verifies that after voting for
// candidateA in term 1, a vote request from candidateB in the same term
// is denied.
func TestRequestVote_RejectsDuplicateVote(t *testing.T) {
	node := newTestNode(t)

	// First vote — should be granted.
	resp1, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:        1,
		CandidateId: "candidate-A",
	})
	require.NoError(t, err)
	assert.True(t, resp1.VoteGranted)

	// Second vote in same term from different candidate — must be denied.
	resp2, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:        1,
		CandidateId: "candidate-B",
	})
	require.NoError(t, err)
	assert.False(t, resp2.VoteGranted)
}

// TestRequestVote_SameCandidateIdempotent verifies that voting for the same
// candidate twice in the same term is idempotent.
func TestRequestVote_SameCandidateIdempotent(t *testing.T) {
	node := newTestNode(t)

	for i := 0; i < 2; i++ {
		resp, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
			Term:        1,
			CandidateId: "candidate-A",
		})
		require.NoError(t, err)
		assert.True(t, resp.VoteGranted, "vote %d should be granted", i+1)
	}
}

// TestRequestVote_RejectsOutdatedLog verifies that a candidate whose log
// is older than ours (lower last term) is rejected even if the term is higher.
func TestRequestVote_RejectsOutdatedLog(t *testing.T) {
	node := newTestNode(t)
	// Give our node a log entry at term 3.
	node.log = []LogEntry{{Index: 1, Term: 3, Op: "put", Key: "k"}}

	resp, err := node.HandleRequestVote(context.Background(), &kvpb.RequestVoteRequest{
		Term:         4,
		CandidateId:  "stale-log-candidate",
		LastLogIndex: 1,
		LastLogTerm:  2, // older term than ours (3)
	})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)
}

// ---------------------------------------------------------------------------
// AppendEntries tests
// ---------------------------------------------------------------------------

// TestAppendEntries_ResetsToFollower verifies that a Candidate or Follower
// transitions back to Follower on receiving a valid AppendEntries.
func TestAppendEntries_ResetsToFollower(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.role = Candidate
	node.currentTerm = 1
	node.mu.Unlock()

	resp, err := node.HandleAppendEntries(context.Background(), &kvpb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "leader-1",
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	node.mu.Lock()
	assert.Equal(t, Follower, node.role)
	assert.Equal(t, "leader-1", node.leaderID)
	node.mu.Unlock()
}

// TestAppendEntries_RejectsStaleTerm verifies that a heartbeat with a lower
// term is rejected.
func TestAppendEntries_RejectsStaleTerm(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	resp, err := node.HandleAppendEntries(context.Background(), &kvpb.AppendEntriesRequest{
		Term:     3,
		LeaderId: "old-leader",
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(5), resp.Term)
}

// ---------------------------------------------------------------------------
// PersistentState tests
// ---------------------------------------------------------------------------

// TestPersistentState_RoundTrip verifies that Save + Load round-trips state
// correctly — including snapshot metadata and, now that the log is persistent
// state, the entries themselves.
func TestPersistentState_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state")
	ps := newPersistentState(path)

	want := persistedState{
		CurrentTerm:   5,
		VotedFor:      "node2",
		SnapLastIndex: 10,
		SnapLastTerm:  2,
		Log: []LogEntry{
			{Index: 11, Term: 2, Op: "health-down", Key: "node3"},
			{Index: 12, Term: 3, Op: "health-up", Key: "node3", Value: []byte{0x00, 0xFF}},
		},
	}
	require.NoError(t, ps.Save(want))

	got, err := ps.Load()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestPersistentState_FreshNode verifies that Load on a non-existent file
// returns zero values without error.
func TestPersistentState_FreshNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist")
	ps := newPersistentState(path)

	got, err := ps.Load()
	require.NoError(t, err)
	assert.Equal(t, persistedState{}, got)
}

// TestPersistentState_Overwrite verifies that multiple Save calls correctly
// overwrite prior state — including shrinking the log, which is what compaction
// and conflict truncation both do.
func TestPersistentState_Overwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state")
	ps := newPersistentState(path)

	require.NoError(t, ps.Save(persistedState{
		CurrentTerm: 1, VotedFor: "node1",
		Log: []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	}))
	want := persistedState{
		CurrentTerm: 7, VotedFor: "node3", SnapLastIndex: 50, SnapLastTerm: 5,
		Log: []LogEntry{{Index: 51, Term: 5}},
	}
	require.NoError(t, ps.Save(want))

	got, err := ps.Load()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestElectionBecomesLeader verifies that a node with no peers (quorum = 1)
// elects itself leader after the election timeout fires.
func TestElectionBecomesLeader(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:             "solo",
		DataDir:            dir,
		ElectionTimeoutMin: 10 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	node, err := New(cfg, nil, newTestSM(), &noopMetrics{}, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go node.Run(ctx)

	// Poll until leader or context deadline.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			return // success
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("node did not become leader within deadline")
}

// ---------------------------------------------------------------------------
// PreVote tests
// ---------------------------------------------------------------------------

// TestPreVote_GrantedWhenLeaderSilent verifies a node grants a pre-vote when
// it hasn't heard from a leader recently and the candidate log is up-to-date.
func TestPreVote_GrantedWhenLeaderSilent(t *testing.T) {
	node := newTestNode(t)
	// lastHeardFromLeader is zero value — far in the past.

	resp, err := node.HandlePreVote(context.Background(), &kvpb.PreVoteRequest{
		NextTerm:     1,
		CandidateId:  "candidate-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted)
}

// TestPreVote_DeniedWhenLeaderActive verifies that pre-vote is denied when
// the node heard from a valid leader very recently.
func TestPreVote_DeniedWhenLeaderActive(t *testing.T) {
	node := newTestNode(t)

	node.mu.Lock()
	node.lastHeardFromLeader = time.Now() // just heard from leader
	node.mu.Unlock()

	resp, err := node.HandlePreVote(context.Background(), &kvpb.PreVoteRequest{
		NextTerm:     1,
		CandidateId:  "candidate-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)
}

// TestPreVote_DoesNotIncrementTerm verifies that handling a PreVoteRequest
// never modifies the receiving node's currentTerm or votedFor.
func TestPreVote_DoesNotIncrementTerm(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 3
	node.mu.Unlock()

	_, err := node.HandlePreVote(context.Background(), &kvpb.PreVoteRequest{
		NextTerm:     5,
		CandidateId:  "ambitious-candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	require.NoError(t, err)

	node.mu.Lock()
	term := node.currentTerm
	voted := node.votedFor
	node.mu.Unlock()

	assert.Equal(t, uint64(3), term, "currentTerm must not change during pre-vote")
	assert.Equal(t, "", voted, "votedFor must not change during pre-vote")
}

// TestPreVote_DeniedWhenLogStale verifies that even with a silent leader,
// pre-vote is denied if our log is newer than the candidate's.
func TestPreVote_DeniedWhenLogStale(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.log = []LogEntry{{Index: 1, Term: 3, Op: "put", Key: "k"}}
	node.mu.Unlock()

	resp, err := node.HandlePreVote(context.Background(), &kvpb.PreVoteRequest{
		NextTerm:     4,
		CandidateId:  "stale-candidate",
		LastLogIndex: 1,
		LastLogTerm:  2, // older than our term 3
	})
	require.NoError(t, err)
	assert.False(t, resp.VoteGranted)
}

// ---------------------------------------------------------------------------
// Virtual log indexing tests
// ---------------------------------------------------------------------------

func TestLogSliceIndex(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.snapLastIndex = 100
	node.mu.Unlock()

	// Entry at absolute index 101 is at slice position 0.
	idx := node.logSliceIndex(101)
	assert.Equal(t, 0, idx)

	// Entry at absolute index 150 is at slice position 49.
	idx = node.logSliceIndex(150)
	assert.Equal(t, 49, idx)
}

func TestLastLogIndex_EmptyLog(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.snapLastIndex = 42
	node.log = nil
	node.mu.Unlock()

	assert.Equal(t, uint64(42), node.lastLogIndex())
	assert.Equal(t, uint64(0), node.lastLogTerm()) // snapLastTerm=0 default
}

func TestLastLogIndex_WithEntries(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.snapLastIndex = 10
	node.log = []LogEntry{
		{Index: 11, Term: 2},
		{Index: 12, Term: 3},
	}
	node.mu.Unlock()

	assert.Equal(t, uint64(12), node.lastLogIndex())
	assert.Equal(t, uint64(3), node.lastLogTerm())
}

// ---------------------------------------------------------------------------
// InstallSnapshot test
// ---------------------------------------------------------------------------

// TestInstallSnapshot_ResetsFollower verifies a follower adopts the leader's
// snapshot: its log is discarded, its snapshot bounds move, and its term
// follows the leader's.
//
// The payload used to be a gob-encoded KV map, because the snapshot carried the
// key/value store. It is now whatever the state machine produced, and Raft's
// only obligation is to hand those exact bytes to RestoreFromSnapshot — which
// this test now also asserts, since the pass-through is the new contract.
func TestInstallSnapshot_ResetsFollower(t *testing.T) {
	node, sm := newTestNodeWithSM(t)
	node.mu.Lock()
	node.currentTerm = 2
	node.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	}
	node.mu.Unlock()

	payload := []byte("opaque-state-machine-bytes")

	resp, err := node.HandleInstallSnapshot(context.Background(), &kvpb.InstallSnapshotRequest{
		Term:              3,
		LeaderId:          "leader-1",
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
		Data:              payload,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(3), resp.Term)

	assert.Equal(t, payload, sm.lastRestore(), "the state machine must receive the payload verbatim")

	node.mu.Lock()
	defer node.mu.Unlock()
	assert.Nil(t, node.log, "log must be nil after snapshot install")
	assert.Equal(t, uint64(50), node.snapLastIndex)
	assert.Equal(t, uint64(3), node.snapLastTerm)
	assert.Equal(t, uint64(3), node.currentTerm)
	assert.Equal(t, uint64(50), node.commitIndex, "a snapshot's contents are committed by definition")
	assert.Equal(t, uint64(50), node.lastApplied, "and applied by definition")
}

func TestInstallSnapshot_RejectsStaleTerm(t *testing.T) {
	node := newTestNode(t)
	node.mu.Lock()
	node.currentTerm = 10
	node.mu.Unlock()

	resp, err := node.HandleInstallSnapshot(context.Background(), &kvpb.InstallSnapshotRequest{
		Term:     5,
		LeaderId: "old-leader",
	})
	require.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, uint64(10), resp.Term)
}
