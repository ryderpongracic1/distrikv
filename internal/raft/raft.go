// Package raft implements a simplified Raft leader election and heartbeat
// protocol for distrikv.
//
// # Scope
//
// This implementation covers:
//   - Randomised election timeouts (150–300 ms default)
//   - RequestVote RPC with log up-to-date check
//   - Majority-vote leader election
//   - Periodic AppendEntries heartbeats (75 ms default)
//   - Term-based split-brain prevention
//   - Persistent storage of currentTerm and votedFor
//
// # Deviations from the Raft paper
//
// 1. Data writes are NOT replicated through the Raft log. They flow through
// the consistent-hash ring's ReplicationManager. Raft is purely an election
// and failure-detection mechanism.
//
// 2. No log truncation on leader change. The in-memory log is only used for
// the last-log-index/term comparison in RequestVote.
//
// 3. No pre-vote phase (unlike etcd). A partitioned rejoining node may briefly
// disrupt the cluster by presenting a higher term; stepDown handles it.
//
// 4. No membership change protocol. Cluster members are static (env-var config).
//
// 5. No snapshot/compaction. The log grows unboundedly (entries are minimal).
package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// Role represents the current Raft role of a node.
type Role uint8

const (
	// Follower is the default role. Followers grant votes and reset their
	// election timer on each valid heartbeat.
	Follower Role = iota
	// Candidate is a node that has started an election and is soliciting votes.
	Candidate
	// Leader sends periodic heartbeats and coordinates replication.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// PeerClient wraps the gRPC client for a single peer node.
type PeerClient struct {
	NodeID string
	Client kvpb.KVServiceClient
}

// StoreInterface is the subset of store.Store that RaftNode uses when applying
// committed log entries to the state machine. The interface breaks the import
// cycle raft → store.
type StoreInterface interface {
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// Config carries RaftNode configuration. All durations must be positive.
type Config struct {
	NodeID             string
	DataDir            string
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// RaftNode implements the simplified Raft state machine. All state is
// protected by mu; no field may be read or written outside of mu without
// explicit documentation of why it is safe to do so.
//
// Goroutine model:
//   - Run() starts exactly one goroutine: runElectionTimer.
//   - runElectionTimer is the sole owner of all role transitions.
//   - When this node becomes Leader, runElectionTimer starts runLeader in a
//     new goroutine and passes a leaderStop channel. stepDown closes that
//     channel to stop the leader goroutine.
//   - Replication fan-out goroutines (started in broadcastHeartbeat) are
//     scoped to a single call and exit before the next heartbeat tick.
type RaftNode struct {
	mu sync.Mutex

	// --- Persistent state (must be saved before responding to RPCs) ---
	currentTerm uint64
	votedFor    string // "" means not yet voted in this term

	// --- Volatile state ---
	role     Role
	leaderID string // ID of the node we believe to be the current leader
	log      []LogEntry

	// --- Infrastructure ---
	nodeID  string
	peers   []PeerClient
	store   StoreInterface
	persist *PersistentState
	metrics metricsInterface
	logger  *slog.Logger

	// --- Timing ---
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	// resetTimerCh is sent to by HandleAppendEntries and a successful vote
	// grant to tell runElectionTimer to reset its timeout.
	resetTimerCh chan struct{}

	// leaderStop is created fresh each time this node becomes leader. When
	// stepDown is called the channel is closed, terminating the leader loop.
	leaderStop chan struct{}
}

// metricsInterface is the subset of metrics.Metrics that RaftNode increments.
type metricsInterface interface {
	IncRaftTerms()
	IncLeaderElections()
}

// New creates a RaftNode, loads any persisted state from disk, and prepares
// the node to run. Call Run to start the election timer goroutine.
func New(cfg Config, peers []PeerClient, store StoreInterface, m metricsInterface, logger *slog.Logger) (*RaftNode, error) {
	persistPath := cfg.DataDir + "/raft-state"
	ps := newPersistentState(persistPath)

	term, votedFor, err := ps.Load()
	if err != nil {
		return nil, fmt.Errorf("raft.New: load persistent state: %w", err)
	}

	r := &RaftNode{
		currentTerm:        term,
		votedFor:           votedFor,
		role:               Follower,
		nodeID:             cfg.NodeID,
		peers:              peers,
		store:              store,
		persist:            ps,
		metrics:            m,
		logger:             logger.With("component", "raft", "node_id", cfg.NodeID),
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		resetTimerCh:       make(chan struct{}, 1),
	}

	return r, nil
}

// Run starts the Raft election timer goroutine and blocks until ctx is
// cancelled. It is safe to call Run exactly once.
func (r *RaftNode) Run(ctx context.Context) {
	r.logger.Info("raft node starting", "term", r.currentTerm, "voted_for", r.votedFor)
	r.runElectionTimer(ctx)
	r.logger.Info("raft node stopped")
}

// runElectionTimer is the core Raft loop. It fires a new election if no
// heartbeat or valid RPC arrives within a randomised timeout window.
func (r *RaftNode) runElectionTimer(ctx context.Context) {
	for {
		timeout := r.randomElectionTimeout()
		select {
		case <-ctx.Done():
			return
		case <-r.resetTimerCh:
			// Received a valid heartbeat or RPC — reset without electing.
		case <-time.After(timeout):
			r.mu.Lock()
			role := r.role
			r.mu.Unlock()

			if role != Leader {
				r.startElection(ctx)
			}
		}
	}
}

// startElection transitions this node to Candidate, increments the term,
// and sends RequestVote RPCs to all peers. If a majority responds positively
// this node becomes Leader.
func (r *RaftNode) startElection(ctx context.Context) {
	r.mu.Lock()
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.nodeID
	term := r.currentTerm
	lastIdx, lastTerm := r.lastLogIndexAndTermLocked()

	if err := r.persist.Save(r.currentTerm, r.votedFor); err != nil {
		r.logger.Error("failed to persist state before election", "error", err)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	if r.metrics != nil {
		r.metrics.IncRaftTerms()
	}
	r.logger.Info("starting election", "term", term)

	votes := 1 // vote for self
	majority := (len(r.peers)+1)/2 + 1

	// Single-node cluster: self-vote is already a majority.
	if votes >= majority {
		r.mu.Lock()
		if r.role == Candidate && r.currentTerm == term {
			r.becomeLeaderLocked(ctx)
		}
		r.mu.Unlock()
		return
	}

	type voteResult struct{ granted bool }
	results := make(chan voteResult, len(r.peers))

	for _, peer := range r.peers {
		peer := peer
		go func() {
			resp, err := peer.Client.RequestVote(ctx, &kvpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  r.nodeID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				r.logger.Warn("RequestVote RPC failed", "peer", peer.NodeID, "error", err)
				results <- voteResult{granted: false}
				return
			}

			r.mu.Lock()
			if resp.Term > r.currentTerm {
				r.stepDownLocked(resp.Term)
			}
			r.mu.Unlock()

			results <- voteResult{granted: resp.VoteGranted}
		}()
	}

	for range r.peers {
		res := <-results

		r.mu.Lock()
		if r.role != Candidate || r.currentTerm != term {
			r.mu.Unlock()
			return // another election superseded us
		}
		if res.granted {
			votes++
			if votes >= majority {
				r.becomeLeaderLocked(ctx)
				r.mu.Unlock()
				return
			}
		}
		r.mu.Unlock()
	}
}

// becomeLeaderLocked transitions this node to Leader and starts the heartbeat
// goroutine. Caller must hold r.mu.
func (r *RaftNode) becomeLeaderLocked(ctx context.Context) {
	r.role = Leader
	r.leaderID = r.nodeID
	stop := make(chan struct{})
	r.leaderStop = stop

	if r.metrics != nil {
		r.metrics.IncLeaderElections()
	}
	r.logger.Info("became leader", "term", r.currentTerm)

	go r.runLeader(ctx, stop)
}

// runLeader sends periodic AppendEntries heartbeats until the context is
// cancelled or the leaderStop channel is closed (triggered by stepDown).
func (r *RaftNode) runLeader(ctx context.Context, leaderStop <-chan struct{}) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-leaderStop:
			return
		case <-ticker.C:
			r.broadcastHeartbeat(ctx)
		}
	}
}

// broadcastHeartbeat sends an empty AppendEntries RPC to every peer. Failures
// are logged but do not abort the broadcast — the cluster tolerates slow peers.
func (r *RaftNode) broadcastHeartbeat(ctx context.Context) {
	r.mu.Lock()
	term := r.currentTerm
	lastIdx, lastTerm := r.lastLogIndexAndTermLocked()
	r.mu.Unlock()

	for _, peer := range r.peers {
		peer := peer
		go func() {
			hbCtx, cancel := context.WithTimeout(ctx, r.heartbeatInterval)
			defer cancel()

			resp, err := peer.Client.AppendEntries(hbCtx, &kvpb.AppendEntriesRequest{
				Term:        term,
				LeaderId:    r.nodeID,
				PrevLogIndex: lastIdx,
				PrevLogTerm:  lastTerm,
			})
			if err != nil {
				r.logger.Warn("heartbeat failed", "peer", peer.NodeID, "error", err)
				return
			}

			r.mu.Lock()
			if resp.Term > r.currentTerm {
				r.stepDownLocked(resp.Term)
			}
			r.mu.Unlock()
		}()
	}
}

// stepDownLocked reverts this node to Follower with the given higher term.
// Caller must hold r.mu. If this node was Leader, the leaderStop channel is
// closed which terminates the runLeader goroutine.
func (r *RaftNode) stepDownLocked(newTerm uint64) {
	wasLeader := r.role == Leader
	r.currentTerm = newTerm
	r.role = Follower
	r.votedFor = ""

	if err := r.persist.Save(r.currentTerm, r.votedFor); err != nil {
		r.logger.Error("failed to persist state on step-down", "error", err)
	}

	if wasLeader && r.leaderStop != nil {
		close(r.leaderStop)
		r.leaderStop = nil
	}

	r.logger.Info("stepped down to follower", "new_term", newTerm)
	r.drainResetTimer()
}

// drainResetTimer empties the resetTimerCh buffer so the election timer loop
// does not immediately fire again after a step-down.
func (r *RaftNode) drainResetTimer() {
	select {
	case <-r.resetTimerCh:
	default:
	}
}

// sendResetTimer non-blockingly signals the election timer to reset. We use a
// buffered channel of size 1: if a signal is already pending, this is a no-op.
func (r *RaftNode) sendResetTimer() {
	select {
	case r.resetTimerCh <- struct{}{}:
	default:
	}
}

// HandleRequestVote processes an incoming RequestVote RPC. It implements the
// vote-granting rules from §5.2 and §5.4.1 of the Raft paper.
func (r *RaftNode) HandleRequestVote(ctx context.Context, req *kvpb.RequestVoteRequest) (*kvpb.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &kvpb.RequestVoteResponse{Term: r.currentTerm}

	// Rule 1 — reject stale candidates.
	if req.Term < r.currentTerm {
		return resp, nil
	}

	// Rule 2 — update our term if we see a higher one (and step down if needed).
	if req.Term > r.currentTerm {
		r.stepDownLocked(req.Term)
		resp.Term = r.currentTerm
	}

	// Rule 3 — grant vote only if we haven't voted this term AND the
	// candidate's log is at least as up-to-date as ours.
	alreadyVoted := r.votedFor != "" && r.votedFor != req.CandidateId
	logOK := r.candidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)

	if !alreadyVoted && logOK {
		r.votedFor = req.CandidateId
		if err := r.persist.Save(r.currentTerm, r.votedFor); err != nil {
			r.logger.Error("failed to persist vote", "error", err)
			return resp, fmt.Errorf("raft: persist vote: %w", err)
		}
		resp.VoteGranted = true
		r.sendResetTimer()
		r.logger.Info("voted for candidate",
			"candidate", req.CandidateId,
			"term", r.currentTerm)
	}

	return resp, nil
}

// HandleAppendEntries processes an incoming AppendEntries RPC — both heartbeats
// (empty Entries) and genuine log entries.
func (r *RaftNode) HandleAppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &kvpb.AppendEntriesResponse{Term: r.currentTerm}

	// Reject messages from stale leaders.
	if req.Term < r.currentTerm {
		return resp, nil
	}

	// A valid message from an equal or higher-term leader — step down if needed.
	if req.Term > r.currentTerm || r.role == Candidate {
		r.stepDownLocked(req.Term)
		resp.Term = r.currentTerm
	}

	r.leaderID = req.LeaderId
	r.sendResetTimer()

	// Apply any non-heartbeat log entries to the state machine.
	for _, entry := range req.Entries {
		r.applyEntryLocked(ctx, entry)
	}

	resp.Success = true
	return resp, nil
}

// applyEntryLocked applies a single log entry to the store. Caller must hold r.mu.
func (r *RaftNode) applyEntryLocked(ctx context.Context, entry *kvpb.LogEntry) {
	var err error
	switch entry.Op {
	case "put":
		err = r.store.Put(ctx, entry.Key, entry.Value)
	case "delete":
		err = r.store.Delete(ctx, entry.Key)
	}
	if err != nil {
		r.logger.Error("failed to apply log entry",
			"op", entry.Op,
			"key", entry.Key,
			"error", err)
	}
}

// candidateLogUpToDateLocked returns true if the candidate's log is at least
// as up-to-date as ours, per §5.4.1 of the Raft paper:
// - Higher last term wins.
// - Equal last term: longer log wins.
// Caller must hold r.mu.
func (r *RaftNode) candidateLogUpToDateLocked(candidateLastIdx, candidateLastTerm uint64) bool {
	myLastIdx, myLastTerm := r.lastLogIndexAndTermLocked()
	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIdx >= myLastIdx
}

// lastLogIndexAndTermLocked returns the index and term of the last log entry,
// or (0, 0) if the log is empty. Caller must hold r.mu.
func (r *RaftNode) lastLogIndexAndTermLocked() (index, term uint64) {
	if len(r.log) == 0 {
		return 0, 0
	}
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

// randomElectionTimeout returns a uniformly random duration in
// [electionTimeoutMin, electionTimeoutMax).
func (r *RaftNode) randomElectionTimeout() time.Duration {
	spread := r.electionTimeoutMax - r.electionTimeoutMin
	//nolint:gosec // non-cryptographic randomness is correct here
	jitter := time.Duration(rand.Int63n(int64(spread)))
	return r.electionTimeoutMin + jitter
}

// --- Public read accessors (no lock needed for these — callers only read) ---

// IsLeader reports whether this node currently believes itself to be the leader.
func (r *RaftNode) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader
}

// CurrentTerm returns the node's current Raft term.
func (r *RaftNode) CurrentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// Leader returns the node ID of the node this node believes to be the leader.
// Returns an empty string if no leader is known.
func (r *RaftNode) Leader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// RoleString returns the current role as a human-readable string.
func (r *RaftNode) RoleString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role.String()
}

// ID returns this node's unique identifier.
func (r *RaftNode) ID() string { return r.nodeID }
