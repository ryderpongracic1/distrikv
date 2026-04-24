// Package raft implements Raft leader election, heartbeating, log compaction
// via snapshots, and a pre-vote phase to prevent partitioned nodes from
// disrupting the cluster on reconnect.
//
// # Deviations from the Raft paper
//
// 1. Data writes are NOT replicated through the Raft log. They flow through
// the consistent-hash ring's ReplicationManager. Raft is an election,
// failure-detection, and snapshot-delivery mechanism.
//
// 2. nextIndex/matchIndex are initialised on leader election for snapshot
// delivery; full pipelined AppendEntries replication is not implemented.
//
// 3. No membership change protocol. Cluster members are static (env-var config).
//
// 4. Pre-vote phase (§9.6 of Raft dissertation) prevents a partitioned node
// from incrementing its term and disrupting the cluster on reconnect.
//
// 5. Snapshots (§7) allow log compaction. A snapshot contains a full copy of
// the state machine serialised as map[string][]byte (not raw SSTable files).
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
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
	Follower  Role = iota
	Candidate
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

// StoreInterface is the subset of store.Store that RaftNode uses.
// Extends the original Put/Delete with Snapshot/RestoreFromSnapshot for §7.
type StoreInterface interface {
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	Snapshot(ctx context.Context) (map[string][]byte, error)
	RestoreFromSnapshot(ctx context.Context, data map[string][]byte) error
}

// Config carries RaftNode configuration.
type Config struct {
	NodeID              string
	DataDir             string
	ElectionTimeoutMin  time.Duration
	ElectionTimeoutMax  time.Duration
	HeartbeatInterval   time.Duration
	SnapshotThreshold   int // take snapshot when log exceeds this many entries
}

// RaftNode implements Raft leader election, heartbeating, snapshot delivery,
// and pre-vote. All mutable state is protected by mu.
//
// Goroutine model:
//   - Run() starts exactly one goroutine: runElectionTimer.
//   - runElectionTimer is the sole owner of all role transitions.
//   - When Leader, runElectionTimer starts runLeader in a new goroutine.
//   - stepDown closes leaderStop to terminate the leader goroutine.
//   - takeSnapshot runs in its own goroutine (started from applyEntryLocked).
type RaftNode struct {
	mu sync.Mutex

	// --- Persistent state ---
	currentTerm   uint64
	votedFor      string
	snapLastIndex uint64 // last log index captured in a snapshot
	snapLastTerm  uint64 // term of that entry

	// --- Volatile state ---
	role     Role
	leaderID string
	log      []LogEntry // entries after snapLastIndex

	// --- Leader volatile state (reset on each election) ---
	nextIndex  map[string]uint64 // peer → next log index to send
	matchIndex map[string]uint64 // peer → highest log index known replicated

	// --- Pre-vote: track when we last heard from a valid leader ---
	lastHeardFromLeader time.Time

	// --- Infrastructure ---
	nodeID        string
	peers         []PeerClient
	store         StoreInterface
	persist       *PersistentState
	snapshotStore *SnapshotStore
	metrics       metricsInterface
	logger        *slog.Logger

	snapshotThreshold int

	// --- Timing ---
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	resetTimerCh chan struct{}
	leaderStop   chan struct{}
}

// metricsInterface is the subset of metrics.Metrics that RaftNode increments.
type metricsInterface interface {
	IncRaftTerms()
	IncLeaderElections()
}

// New creates a RaftNode and loads any persisted state from disk.
func New(cfg Config, peers []PeerClient, store StoreInterface, m metricsInterface, logger *slog.Logger) (*RaftNode, error) {
	persistPath := cfg.DataDir + "/raft-state"
	ps := newPersistentState(persistPath)

	term, votedFor, snapLastIndex, snapLastTerm, err := ps.Load()
	if err != nil {
		return nil, fmt.Errorf("raft.New: load persistent state: %w", err)
	}

	ss := NewSnapshotStore(cfg.DataDir, logger)

	threshold := cfg.SnapshotThreshold
	if threshold <= 0 {
		threshold = 1000
	}

	r := &RaftNode{
		currentTerm:        term,
		votedFor:           votedFor,
		snapLastIndex:      snapLastIndex,
		snapLastTerm:       snapLastTerm,
		role:               Follower,
		nodeID:             cfg.NodeID,
		peers:              peers,
		store:              store,
		persist:            ps,
		snapshotStore:      ss,
		metrics:            m,
		logger:             logger.With("component", "raft", "node_id", cfg.NodeID),
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		snapshotThreshold:  threshold,
		resetTimerCh:       make(chan struct{}, 1),
	}

	// On restart, if a snapshot exists, restore the state machine.
	if snap, ok, err := ss.Load(); err != nil {
		return nil, fmt.Errorf("raft.New: load snapshot: %w", err)
	} else if ok && store != nil {
		if err := store.RestoreFromSnapshot(context.Background(), snap.Data); err != nil {
			return nil, fmt.Errorf("raft.New: restore snapshot on startup: %w", err)
		}
		logger.Info("raft: restored snapshot on startup",
			"last_index", snap.LastIncludedIndex,
			"last_term", snap.LastIncludedTerm)
	}

	return r, nil
}

// Run starts the Raft election timer goroutine and blocks until ctx is cancelled.
func (r *RaftNode) Run(ctx context.Context) {
	r.logger.Info("raft node starting", "term", r.currentTerm, "voted_for", r.votedFor)
	r.runElectionTimer(ctx)
	r.logger.Info("raft node stopped")
}

// runElectionTimer fires a new election if no heartbeat arrives within a random timeout.
func (r *RaftNode) runElectionTimer(ctx context.Context) {
	for {
		timeout := r.randomElectionTimeout()
		select {
		case <-ctx.Done():
			return
		case <-r.resetTimerCh:
			// Valid heartbeat or RPC — reset without electing.
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

// startElection runs a pre-vote phase, then (if successful) a real vote.
//
// Pre-vote (§9.6): solicits dry-run votes without incrementing currentTerm.
// If a majority grants pre-votes the node proceeds to the real election.
// A valid AppendEntries arriving mid-round updates lastHeardFromLeader, which
// the double-check under lock in startElection catches even if runPreVote
// already returned true.
func (r *RaftNode) startElection(ctx context.Context) {
	if !r.runPreVote(ctx) {
		r.logger.Info("pre-vote failed; staying follower")
		return
	}

	r.mu.Lock()
	// Final guard: did a valid leader contact us during the pre-vote round?
	if time.Since(r.lastHeardFromLeader) < r.electionTimeoutMin {
		r.mu.Unlock()
		r.logger.Info("leader heard during pre-vote; aborting election")
		return
	}

	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.nodeID
	term := r.currentTerm
	lastIdx := r.lastLogIndex()
	lastTerm := r.lastLogTerm()

	if err := r.persist.Save(r.currentTerm, r.votedFor, r.snapLastIndex, r.snapLastTerm); err != nil {
		r.logger.Error("failed to persist state before election", "error", err)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	if r.metrics != nil {
		r.metrics.IncRaftTerms()
	}
	r.logger.Info("starting real election", "term", term)

	votes := 1
	majority := (len(r.peers)+1)/2 + 1

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
				results <- voteResult{false}
				return
			}
			r.mu.Lock()
			if resp.Term > r.currentTerm {
				r.stepDownLocked(resp.Term)
			}
			r.mu.Unlock()
			results <- voteResult{resp.VoteGranted}
		}()
	}

	for range r.peers {
		res := <-results
		r.mu.Lock()
		if r.role != Candidate || r.currentTerm != term {
			r.mu.Unlock()
			return
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

// runPreVote sends PreVote RPCs without modifying currentTerm or role.
// Returns true if a majority grants the pre-vote.
func (r *RaftNode) runPreVote(ctx context.Context) bool {
	r.mu.Lock()
	nextTerm := r.currentTerm + 1
	lastIdx := r.lastLogIndex()
	lastTerm := r.lastLogTerm()
	peers := r.peers
	r.mu.Unlock()

	if len(peers) == 0 {
		return true // single-node: always proceed
	}

	majority := (len(peers)+1)/2 + 1
	granted := 1 // self

	results := make(chan bool, len(peers))
	pCtx, cancel := context.WithTimeout(ctx, r.electionTimeoutMin)
	defer cancel()

	for _, peer := range peers {
		peer := peer
		go func() {
			resp, err := peer.Client.PreVote(pCtx, &kvpb.PreVoteRequest{
				NextTerm:     nextTerm,
				CandidateId:  r.nodeID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				results <- false
				return
			}
			results <- resp.VoteGranted
		}()
	}

	for range peers {
		if <-results {
			granted++
			if granted >= majority {
				// Check if a leader appeared while we were collecting pre-votes.
				r.mu.Lock()
				leaderActive := time.Since(r.lastHeardFromLeader) < r.electionTimeoutMin
				r.mu.Unlock()
				return !leaderActive
			}
		}
	}
	return false
}

// becomeLeaderLocked transitions to Leader and starts the heartbeat goroutine.
// Caller must hold r.mu.
func (r *RaftNode) becomeLeaderLocked(ctx context.Context) {
	r.role = Leader
	r.leaderID = r.nodeID

	// Initialise nextIndex for each peer (standard Raft leader initialisation).
	r.nextIndex = make(map[string]uint64, len(r.peers))
	r.matchIndex = make(map[string]uint64, len(r.peers))
	for _, p := range r.peers {
		r.nextIndex[p.NodeID] = r.lastLogIndex() + 1
		r.matchIndex[p.NodeID] = 0
	}

	stop := make(chan struct{})
	r.leaderStop = stop

	if r.metrics != nil {
		r.metrics.IncLeaderElections()
	}
	r.logger.Info("became leader", "term", r.currentTerm)
	go r.runLeader(ctx, stop)
}

// runLeader sends periodic heartbeats until ctx is cancelled or leaderStop fires.
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

// broadcastHeartbeat sends AppendEntries (or InstallSnapshot) to every peer.
// For peers that have fallen behind the snapshot boundary, InstallSnapshot is
// sent instead of AppendEntries.
func (r *RaftNode) broadcastHeartbeat(ctx context.Context) {
	r.mu.Lock()
	term := r.currentTerm
	lastIdx := r.lastLogIndex()
	lastTerm := r.lastLogTerm()
	snapLastIndex := r.snapLastIndex
	peers := r.peers
	nextIndex := r.nextIndex
	r.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		peerNext := nextIndex[peer.NodeID]

		if peerNext > 0 && peerNext-1 <= snapLastIndex {
			// Peer needs log entries that were compacted — send snapshot instead.
			go r.sendInstallSnapshot(ctx, peer, term)
		} else {
			go func() {
				hbCtx, cancel := context.WithTimeout(ctx, r.heartbeatInterval)
				defer cancel()
				resp, err := peer.Client.AppendEntries(hbCtx, &kvpb.AppendEntriesRequest{
					Term:         term,
					LeaderId:     r.nodeID,
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
}

// sendInstallSnapshot sends a full snapshot to a peer.
func (r *RaftNode) sendInstallSnapshot(ctx context.Context, peer PeerClient, term uint64) {
	snap, ok, err := r.snapshotStore.Load()
	if err != nil || !ok {
		return
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap.Data); err != nil {
		r.logger.Error("sendInstallSnapshot: encode data", "err", err)
		return
	}

	isCtx, cancel := context.WithTimeout(ctx, 5*r.heartbeatInterval)
	defer cancel()

	resp, err := peer.Client.InstallSnapshot(isCtx, &kvpb.InstallSnapshotRequest{
		Term:               term,
		LeaderId:           r.nodeID,
		LastIncludedIndex:  snap.LastIncludedIndex,
		LastIncludedTerm:   snap.LastIncludedTerm,
		Data:               buf.Bytes(),
	})
	if err != nil {
		r.logger.Warn("InstallSnapshot RPC failed", "peer", peer.NodeID, "err", err)
		return
	}
	r.mu.Lock()
	if resp.Term > r.currentTerm {
		r.stepDownLocked(resp.Term)
	} else if resp.Success {
		r.nextIndex[peer.NodeID] = snap.LastIncludedIndex + 1
		r.matchIndex[peer.NodeID] = snap.LastIncludedIndex
	}
	r.mu.Unlock()
}

// stepDownLocked reverts to Follower with the given higher term.
// Caller must hold r.mu.
func (r *RaftNode) stepDownLocked(newTerm uint64) {
	wasLeader := r.role == Leader
	r.currentTerm = newTerm
	r.role = Follower
	r.votedFor = ""

	if err := r.persist.Save(r.currentTerm, r.votedFor, r.snapLastIndex, r.snapLastTerm); err != nil {
		r.logger.Error("failed to persist state on step-down", "error", err)
	}
	if wasLeader && r.leaderStop != nil {
		close(r.leaderStop)
		r.leaderStop = nil
	}
	r.logger.Info("stepped down to follower", "new_term", newTerm)
	r.drainResetTimer()
}

func (r *RaftNode) drainResetTimer() {
	select {
	case <-r.resetTimerCh:
	default:
	}
}

func (r *RaftNode) sendResetTimer() {
	select {
	case r.resetTimerCh <- struct{}{}:
	default:
	}
}

// HandleRequestVote processes an incoming RequestVote RPC (§5.2, §5.4.1).
func (r *RaftNode) HandleRequestVote(ctx context.Context, req *kvpb.RequestVoteRequest) (*kvpb.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &kvpb.RequestVoteResponse{Term: r.currentTerm}

	if req.Term < r.currentTerm {
		return resp, nil
	}
	if req.Term > r.currentTerm {
		r.stepDownLocked(req.Term)
		resp.Term = r.currentTerm
	}

	alreadyVoted := r.votedFor != "" && r.votedFor != req.CandidateId
	logOK := r.candidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)

	if !alreadyVoted && logOK {
		r.votedFor = req.CandidateId
		if err := r.persist.Save(r.currentTerm, r.votedFor, r.snapLastIndex, r.snapLastTerm); err != nil {
			r.logger.Error("failed to persist vote", "error", err)
			return resp, fmt.Errorf("raft: persist vote: %w", err)
		}
		resp.VoteGranted = true
		r.sendResetTimer()
		r.logger.Info("voted for candidate", "candidate", req.CandidateId, "term", r.currentTerm)
	}
	return resp, nil
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (r *RaftNode) HandleAppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &kvpb.AppendEntriesResponse{Term: r.currentTerm}

	if req.Term < r.currentTerm {
		return resp, nil
	}
	if req.Term > r.currentTerm || r.role == Candidate {
		r.stepDownLocked(req.Term)
		resp.Term = r.currentTerm
	}

	r.leaderID = req.LeaderId
	r.lastHeardFromLeader = time.Now() // used by pre-vote to detect active leader
	r.sendResetTimer()

	for _, entry := range req.Entries {
		r.applyEntryLocked(ctx, entry)
	}

	resp.Success = true
	return resp, nil
}

// HandlePreVote processes an incoming PreVote RPC (§9.6 of Raft dissertation).
// CRITICAL: must NOT update currentTerm or votedFor — this is a dry-run.
func (r *RaftNode) HandlePreVote(_ context.Context, req *kvpb.PreVoteRequest) (*kvpb.PreVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Grant if: (1) we haven't heard from a valid leader recently, AND
	//           (2) candidate's log is at least as up-to-date as ours, AND
	//           (3) candidate's next term is higher than our current term.
	leaderActive := time.Since(r.lastHeardFromLeader) < r.electionTimeoutMin
	logOK := req.LastLogTerm > r.lastLogTerm() ||
		(req.LastLogTerm == r.lastLogTerm() && req.LastLogIndex >= r.lastLogIndex())
	granted := !leaderActive && logOK && req.NextTerm > r.currentTerm

	return &kvpb.PreVoteResponse{Term: r.currentTerm, VoteGranted: granted}, nil
}

// HandleInstallSnapshot applies a full snapshot from the leader.
func (r *RaftNode) HandleInstallSnapshot(ctx context.Context, req *kvpb.InstallSnapshotRequest) (*kvpb.InstallSnapshotResponse, error) {
	r.mu.Lock()

	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()
		return &kvpb.InstallSnapshotResponse{Term: term, Success: false}, nil
	}
	if req.Term > r.currentTerm {
		r.stepDownLocked(req.Term)
	}
	r.leaderID = req.LeaderId
	r.lastHeardFromLeader = time.Now()
	r.sendResetTimer()
	r.mu.Unlock()

	// Decode snapshot data outside the lock (I/O).
	var data map[string][]byte
	if err := gob.NewDecoder(bytes.NewReader(req.Data)).Decode(&data); err != nil {
		return &kvpb.InstallSnapshotResponse{Term: req.Term, Success: false},
			fmt.Errorf("raft: InstallSnapshot decode: %w", err)
	}

	// Apply to the state machine (I/O, no lock held).
	if err := r.store.RestoreFromSnapshot(ctx, data); err != nil {
		return &kvpb.InstallSnapshotResponse{Term: req.Term, Success: false},
			fmt.Errorf("raft: InstallSnapshot restore: %w", err)
	}

	// Persist the snapshot.
	snap := Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              data,
	}
	if err := r.snapshotStore.Save(snap); err != nil {
		r.logger.Error("InstallSnapshot: save snapshot", "err", err)
	}

	// Reset Raft log and update snapshot metadata under lock.
	r.mu.Lock()
	r.log = nil
	r.snapLastIndex = req.LastIncludedIndex
	r.snapLastTerm = req.LastIncludedTerm
	if err := r.persist.Save(r.currentTerm, r.votedFor, r.snapLastIndex, r.snapLastTerm); err != nil {
		r.logger.Error("InstallSnapshot: persist state", "err", err)
	}
	r.mu.Unlock()

	r.logger.Info("snapshot installed",
		"last_index", req.LastIncludedIndex,
		"last_term", req.LastIncludedTerm)

	return &kvpb.InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

// takeSnapshot captures a snapshot of the state machine and compacts the log.
// Runs in a goroutine; must not hold r.mu on entry.
func (r *RaftNode) takeSnapshot(ctx context.Context) {
	r.mu.Lock()
	if len(r.log) == 0 {
		r.mu.Unlock()
		return
	}
	lastIdx := r.lastLogIndex()
	lastTerm := r.log[len(r.log)-1].Term
	r.mu.Unlock()

	data, err := r.store.Snapshot(ctx)
	if err != nil {
		r.logger.Error("takeSnapshot: store.Snapshot failed", "err", err)
		return
	}

	snap := Snapshot{LastIncludedIndex: lastIdx, LastIncludedTerm: lastTerm, Data: data}
	if err := r.snapshotStore.Save(snap); err != nil {
		r.logger.Error("takeSnapshot: save failed", "err", err)
		return
	}

	r.mu.Lock()
	// Discard log entries up to and including lastIdx.
	cutoff := r.logSliceIndex(lastIdx) + 1
	if cutoff > 0 && cutoff <= len(r.log) {
		r.log = r.log[cutoff:]
	}
	r.snapLastIndex = lastIdx
	r.snapLastTerm = lastTerm
	if err := r.persist.Save(r.currentTerm, r.votedFor, r.snapLastIndex, r.snapLastTerm); err != nil {
		r.logger.Error("takeSnapshot: persist state", "err", err)
	}
	r.mu.Unlock()

	r.logger.Info("snapshot taken", "last_index", lastIdx, "last_term", lastTerm)
}

// applyEntryLocked applies a single log entry to the store and appends to r.log.
// Caller must hold r.mu.
func (r *RaftNode) applyEntryLocked(ctx context.Context, entry *kvpb.LogEntry) {
	var err error
	switch entry.Op {
	case "put":
		err = r.store.Put(ctx, entry.Key, entry.Value)
	case "delete":
		err = r.store.Delete(ctx, entry.Key)
	}
	if err != nil {
		r.logger.Error("failed to apply log entry", "op", entry.Op, "key", entry.Key, "error", err)
	}

	r.log = append(r.log, LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
		Op:    entry.Op,
		Key:   entry.Key,
		Value: entry.Value,
	})

	if len(r.log) >= r.snapshotThreshold {
		go r.takeSnapshot(ctx)
	}
}

// candidateLogUpToDateLocked returns true if the candidate's log is at least
// as up-to-date as ours (§5.4.1). Caller must hold r.mu.
func (r *RaftNode) candidateLogUpToDateLocked(candidateLastIdx, candidateLastTerm uint64) bool {
	myLastIdx, myLastTerm := r.lastLogIndex(), r.lastLogTerm()
	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIdx >= myLastIdx
}

// --- Virtual log indexing (post-snapshot) ------------------------------------

// logSliceIndex converts an absolute Raft log index to a slice index.
// Caller must hold r.mu.
func (r *RaftNode) logSliceIndex(absIdx uint64) int {
	return int(absIdx - r.snapLastIndex - 1)
}

// lastLogIndex returns the absolute index of the last log entry.
// Returns snapLastIndex if the log is empty (all entries compacted).
func (r *RaftNode) lastLogIndex() uint64 {
	if len(r.log) == 0 {
		return r.snapLastIndex
	}
	return r.log[len(r.log)-1].Index
}

// lastLogTerm returns the term of the last log entry.
func (r *RaftNode) lastLogTerm() uint64 {
	if len(r.log) == 0 {
		return r.snapLastTerm
	}
	return r.log[len(r.log)-1].Term
}

// lastLogIndexAndTermLocked returns (lastIndex, lastTerm). Caller must hold r.mu.
// Kept for backward compatibility with existing callers.
func (r *RaftNode) lastLogIndexAndTermLocked() (index, term uint64) {
	return r.lastLogIndex(), r.lastLogTerm()
}

func (r *RaftNode) randomElectionTimeout() time.Duration {
	spread := r.electionTimeoutMax - r.electionTimeoutMin
	//nolint:gosec
	jitter := time.Duration(rand.Int63n(int64(spread)))
	return r.electionTimeoutMin + jitter
}

// --- Public accessors -------------------------------------------------------

func (r *RaftNode) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader
}

func (r *RaftNode) CurrentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

func (r *RaftNode) Leader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

func (r *RaftNode) RoleString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role.String()
}

func (r *RaftNode) ID() string { return r.nodeID }
