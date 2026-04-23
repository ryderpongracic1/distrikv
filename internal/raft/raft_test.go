package raft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"log/slog"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func newTestNode(t *testing.T) *RaftNode {
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
	node, err := New(cfg, nil, &noopStore{}, nil, logger)
	require.NoError(t, err)
	return node
}

// noopStore satisfies StoreInterface without doing anything.
type noopStore struct{}

func (n *noopStore) Put(_ context.Context, _ string, _ []byte) error { return nil }
func (n *noopStore) Delete(_ context.Context, _ string) error        { return nil }

// noopMetrics satisfies metricsInterface without recording anything.
type noopMetrics struct{}

func (n *noopMetrics) IncRaftTerms()       {}
func (n *noopMetrics) IncLeaderElections() {}

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
// correctly.
func TestPersistentState_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state")
	ps := newPersistentState(path)

	require.NoError(t, ps.Save(5, "node2"))

	term, votedFor, err := ps.Load()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), term)
	assert.Equal(t, "node2", votedFor)
}

// TestPersistentState_FreshNode verifies that Load on a non-existent file
// returns zero values without error.
func TestPersistentState_FreshNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist")
	ps := newPersistentState(path)

	term, votedFor, err := ps.Load()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), term)
	assert.Equal(t, "", votedFor)
}

// TestPersistentState_Overwrite verifies that multiple Save calls correctly
// overwrite prior state.
func TestPersistentState_Overwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state")
	ps := newPersistentState(path)

	require.NoError(t, ps.Save(1, "node1"))
	require.NoError(t, ps.Save(7, "node3"))

	term, votedFor, err := ps.Load()
	require.NoError(t, err)
	assert.Equal(t, uint64(7), term)
	assert.Equal(t, "node3", votedFor)
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
	node, err := New(cfg, nil, &noopStore{}, &noopMetrics{}, logger)
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
