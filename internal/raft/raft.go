// Package raft implements Raft leader election, log replication, heartbeating,
// log compaction via snapshots, and a pre-vote phase to prevent partitioned
// nodes from disrupting the cluster on reconnect.
//
// # Deviations from the Raft paper
//
// 1. Client data writes are NOT replicated through the Raft log. They flow
// through the consistent-hash ring's ReplicationManager. The Raft log exists to
// carry cluster-control entries — node-health transitions — and never key/value
// data. Raft is an election, failure-detection, control-plane replication, and
// snapshot-delivery mechanism.
//
// 2. Log replication is complete (§5.3 log matching with conflict truncation and
// nextIndex backoff, §5.4.2 commit rule, apply-on-commit) and rides the
// heartbeat ticker rather than a separate pipelined replication loop. Entries
// are batched per heartbeat interval, which suits a log whose write rate is one
// entry per genuine change in a peer's reachability. There is no fast-backup
// hint on rejection: nextIndex walks back one index per interval, which is
// bounded by how far a follower's log can diverge and needs no wire change.
//
// 3. No membership change protocol. Cluster members are static (env-var config).
//
// 4. Pre-vote phase (§9.6 of Raft dissertation) prevents a partitioned node
// from incrementing its term and disrupting the cluster on reconnect.
//
// 5. Snapshots (§7) allow log compaction. A snapshot carries the opaque bytes
// the StateMachine produced; Raft never inspects the payload.
//
// 6. A new leader does not append a no-op entry to discover its commit index.
// The consequence is the one the paper describes for §5.4.2: entries carried
// over from a previous term stay uncommitted until the leader appends and
// commits an entry of its own term, at which point they commit indirectly. For
// a control-plane log this costs nothing but a bounded delay; the no-op
// optimisation would need the state machine to tolerate an entry with no
// meaning, so it is deliberately not taken.
package raft

import (
	"context"
	"errors"
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
	Follower Role = iota
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

// StateMachine is the application Raft replicates. It is the only thing Raft
// applies committed entries to, and the only thing it snapshots.
//
// Raft deliberately knows nothing else about it: no key/value semantics, no
// storage engine. That is what keeps this package free of a dependency on the
// data path, whose replication is the hash ring's business (see deviation 1).
//
// Contract:
//
//   - Apply is called once per entry, in log-index order, only after the entry
//     is committed. It is called without any Raft lock held, so an
//     implementation may block on I/O — but every subsequent apply waits behind
//     it, so it should be quick.
//
//   - Apply MUST be idempotent. commitIndex and lastApplied are volatile (see
//     persistedState), so entries after the snapshot point are applied again
//     after a restart. Apply may also be re-attempted after it returns an
//     error: Raft leaves lastApplied where it was and retries on the next
//     commit advance rather than skipping the entry.
//
//   - ReplayApply is Apply for an entry that was already on disk when this node
//     opened, and so may have been applied by a previous incarnation. Raft
//     routes every such entry here instead of to Apply; see replayFrontier.
//     State must move exactly as it would under Apply — replay is how the view
//     is rebuilt — but an implementation MUST NOT emit an effect that only live
//     evidence justifies. Idempotent *state* is not idempotent *effects*: a map
//     assignment converges on replay, an operator-facing warning or a
//     notification on a channel does not, and re-emitting one turns a restart
//     into a burst of announcements about transitions that are already history
//     (defect 14).
//
//   - SnapshotState must return a payload reflecting exactly the entries
//     applied so far, with no partial application in it. Raft pairs the bytes
//     with the index of the last applied entry.
//
//   - RestoreFromSnapshot replaces all state with the payload's. It is called
//     on startup when a snapshot is on disk, and when a leader installs one. A
//     snapshot describes a state rather than a transition into it, so like
//     ReplayApply it must be effect-free.
type StateMachine interface {
	Apply(ctx context.Context, entry LogEntry) error
	ReplayApply(ctx context.Context, entry LogEntry) error
	SnapshotState(ctx context.Context) ([]byte, error)
	RestoreFromSnapshot(ctx context.Context, data []byte) error
}

// ErrNotLeader is returned by Propose on a node that is not the current leader.
// The caller can find the leader it should retry against with Leader().
var ErrNotLeader = errors.New("raft: not leader")

// Config carries RaftNode configuration.
type Config struct {
	NodeID             string
	DataDir            string
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	SnapshotThreshold  int // take snapshot when log exceeds this many entries
}

// RaftNode implements Raft leader election, log replication, heartbeating,
// snapshot delivery, and pre-vote. All mutable state is protected by mu.
//
// Goroutine model:
//   - Run() starts exactly one goroutine: runElectionTimer.
//   - runElectionTimer is the sole owner of all role transitions.
//   - When Leader, runElectionTimer starts runLeader in a new goroutine.
//   - stepDown closes leaderStop to terminate the leader goroutine.
//   - takeSnapshot runs in its own goroutine (started from the apply path).
//
// Lock order: applyMu before mu, never the reverse. applyMu serialises the
// apply loop so that entries reach the state machine exactly once and in order
// without mu being held across StateMachine.Apply.
type RaftNode struct {
	mu sync.Mutex

	// --- Persistent state (survives a crash; see persistedState) ---
	currentTerm   uint64
	votedFor      string
	snapLastIndex uint64     // last log index captured in a snapshot
	snapLastTerm  uint64     // term of that entry
	log           []LogEntry // entries after snapLastIndex, contiguous and ascending

	// --- Volatile state ---
	role     Role
	leaderID string

	// commitIndex is the highest index known to be committed: replicated to a
	// majority under the current leader's term (§5.4.2). lastApplied is how far
	// the state machine has consumed. Both are volatile by design — see
	// persistedState — and both start at snapLastIndex, since a snapshot is by
	// construction a record of applied, committed entries.
	commitIndex uint64
	lastApplied uint64

	// replayFrontier is the highest log index that was already on disk when this
	// node opened. It never moves after New.
	//
	// It exists because lastApplied is volatile: an entry at or below the
	// frontier may have been applied — and had its effects emitted — by a
	// previous incarnation of this process, and applying it again is a replay
	// rather than news. Entries above the frontier were appended in this
	// incarnation, by Propose or by AppendEntries, so applying one is the first
	// time anything has happened. applyCommitted routes the two cases to
	// StateMachine.ReplayApply and StateMachine.Apply respectively.
	//
	// The boundary is deliberately the persisted log tail and not the pre-crash
	// lastApplied, which is not recorded anywhere (see persistedState). That
	// makes it conservative in one direction: an entry that was persisted but
	// never applied before the crash is treated as a replay, so its effects are
	// suppressed the one time it is applied for real. The cost is bounded and
	// falls on the safe side — a missed recovery notification delays a catch-up
	// pass, which the anti-entropy retry loop covers anyway, whereas the
	// alternative is the noise defect 14 records. Persisting lastApplied would
	// remove the imprecision at the cost of a disk write per apply and a
	// deviation from the paper this package otherwise follows.
	replayFrontier uint64

	// --- Leader volatile state (reset on each election) ---
	nextIndex  map[string]uint64 // peer → next log index to send
	matchIndex map[string]uint64 // peer → highest log index known replicated

	// --- Pre-vote: track when we last heard from a valid leader ---
	lastHeardFromLeader time.Time

	// --- Infrastructure ---
	nodeID        string
	peers         []PeerClient
	sm            StateMachine
	persist       *PersistentState
	snapshotStore *SnapshotStore
	metrics       metricsInterface
	logger        *slog.Logger

	// applyMu serialises the apply loop. Held across StateMachine.Apply, which
	// is why mu must never be acquired before it.
	applyMu sync.Mutex

	// peerHealth, when set, receives the outcome of every heartbeat RPC this
	// node sends as leader. Guarded by mu. See SetPeerHealthObserver.
	peerHealth PeerHealthObserver

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
func New(cfg Config, peers []PeerClient, sm StateMachine, m metricsInterface, logger *slog.Logger) (*RaftNode, error) {
	if sm == nil {
		return nil, fmt.Errorf("raft.New: nil StateMachine")
	}

	persistPath := cfg.DataDir + "/raft-state"
	ps := newPersistentState(persistPath)

	st, err := ps.Load()
	if err != nil {
		return nil, fmt.Errorf("raft.New: load persistent state: %w", err)
	}

	ss := NewSnapshotStore(cfg.DataDir, logger)

	threshold := cfg.SnapshotThreshold
	if threshold <= 0 {
		threshold = 1000
	}

	r := &RaftNode{
		currentTerm:        st.CurrentTerm,
		votedFor:           st.VotedFor,
		snapLastIndex:      st.SnapLastIndex,
		snapLastTerm:       st.SnapLastTerm,
		log:                st.Log,
		role:               Follower,
		nodeID:             cfg.NodeID,
		peers:              peers,
		sm:                 sm,
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

	// On restart, if a snapshot exists, restore the state machine from it.
	//
	// The snapshot file is written before the state file that records its
	// bounds (in both takeSnapshot and HandleInstallSnapshot), so a crash
	// between the two leaves a snapshot on disk that is ahead of the recorded
	// snapLastIndex. The snapshot is the more advanced durable record of
	// applied state, so it wins: its bounds are adopted and the entries it
	// already covers are dropped from the log.
	snap, hasSnap, err := ss.Load()
	if err != nil {
		return nil, fmt.Errorf("raft.New: load snapshot: %w", err)
	}
	if hasSnap {
		if err := sm.RestoreFromSnapshot(context.Background(), snap.Data); err != nil {
			return nil, fmt.Errorf("raft.New: restore snapshot on startup: %w", err)
		}
		if snap.LastIncludedIndex > r.snapLastIndex {
			r.snapLastIndex = snap.LastIncludedIndex
			r.snapLastTerm = snap.LastIncludedTerm
		}
		r.logger.Info("raft: restored snapshot on startup",
			"last_index", snap.LastIncludedIndex,
			"last_term", snap.LastIncludedTerm)
	}

	// Enforce the log invariants the virtual indexing depends on: every entry
	// sits above snapLastIndex, and indices are contiguous and ascending.
	//
	// A file that violates them is corrupt, not merely stale, and the longest
	// valid prefix is the most that can be trusted. Truncating it is strictly
	// better than either refusing to start — which would brick a node over a
	// log this cheap to rebuild from the leader — or carrying entries whose
	// index no longer locates them.
	if dropped := r.normaliseLogLocked(); dropped > 0 {
		r.logger.Warn("raft: dropped log entries that violate index invariants",
			"dropped", dropped, "snap_last_index", r.snapLastIndex, "kept", len(r.log))
	}

	// A snapshot's contents are applied, committed entries by construction, so
	// both indices start at its boundary. Entries above it are replayed to the
	// state machine once the leader tells us they are committed, which is why
	// Apply must be idempotent.
	r.commitIndex = r.snapLastIndex
	r.lastApplied = r.snapLastIndex

	// Everything the log holds right now predates this process, so applying any
	// of it is a replay rather than an observation. Entries appended from here on
	// are live. See replayFrontier.
	r.replayFrontier = r.lastLogIndex()
	if r.replayFrontier > r.snapLastIndex {
		r.logger.Info("raft: entries from the persisted log will be replayed to the state machine; "+
			"their side effects are suppressed because they may already have been emitted before the restart",
			"from_index", r.snapLastIndex+1, "replay_frontier", r.replayFrontier)
	}

	return r, nil
}

// normaliseLogLocked drops entries the snapshot already covers and any suffix
// following a break in the index sequence. Returns how many were dropped.
// Caller must hold r.mu, or hold no reference to the node yet (as in New).
func (r *RaftNode) normaliseLogLocked() int {
	kept := make([]LogEntry, 0, len(r.log))
	want := r.snapLastIndex + 1
	for _, e := range r.log {
		if e.Index < want {
			continue // already in the snapshot, or a duplicate of one kept
		}
		if e.Index != want {
			break // gap: everything from here on is unlocatable
		}
		kept = append(kept, e)
		want++
	}
	dropped := len(r.log) - len(kept)
	if len(kept) == 0 {
		kept = nil
	}
	r.log = kept
	return dropped
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

	if err := r.persistLocked(r.log); err != nil {
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

	// Assert authority immediately rather than waiting out the first tick. The
	// followers that just voted reset their election timers at vote time, so
	// idling for a full heartbeatInterval spends part of their election window
	// before the new leader has said anything.
	r.broadcastHeartbeat(ctx)

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

// appendArgs is one peer's AppendEntries payload, computed under r.mu so that
// the whole set — prev pointer, entries, commit index — is drawn from one
// consistent view of the log.
type appendArgs struct {
	term         uint64
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []*kvpb.LogEntry
	leaderCommit uint64

	// nextIndexSent is the nextIndex this payload was built from. The response
	// handler only adjusts nextIndex if it still holds that value, so two RPCs
	// in flight to the same peer cannot decrement it twice for one mismatch.
	nextIndexSent uint64
}

// broadcastHeartbeat sends an AppendEntries to every peer — carrying whatever
// entries that peer is missing — plus an InstallSnapshot to any peer whose next
// log entry has already been compacted.
//
// Every peer gets an AppendEntries on every tick, unconditionally. It is the
// only liveness signal a follower's election timer has, so it is never traded
// away for another RPC — doing so silently starves that follower into calling an
// election against a healthy leader. That is not hypothetical: it is the defect
// that produced a three-month election storm in this system.
//
// Each peer is served by its own goroutine, so a slow or unreachable peer can
// neither delay nor suppress the AppendEntries owed to any other peer.
func (r *RaftNode) broadcastHeartbeat(ctx context.Context) {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return
	}
	peers := r.peers
	args := make(map[string]appendArgs, len(peers))
	needSnapshot := make(map[string]bool, len(peers))

	for _, p := range peers {
		a, need := r.appendArgsForLocked(p.NodeID)
		args[p.NodeID] = a
		needSnapshot[p.NodeID] = need
	}
	r.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		a := args[peer.NodeID]
		go r.sendAppendEntries(ctx, peer, a)
		if needSnapshot[peer.NodeID] {
			go r.sendInstallSnapshot(ctx, peer, a.term)
		}
	}
}

// appendArgsForLocked builds the AppendEntries payload owed to one peer, and
// reports whether that peer also needs a snapshot.
//
// A peer needs a snapshot only when the entry it asks for next has already been
// compacted away. nextIndex == snapLastIndex+1 is the fully-caught-up case:
// PrevLogIndex is then snapLastIndex, whose term the leader still holds in
// snapLastTerm, so a plain AppendEntries carries everything the peer needs.
//
// Caller must hold r.mu.
func (r *RaftNode) appendArgsForLocked(peerID string) (a appendArgs, needSnapshot bool) {
	next := r.nextIndex[peerID]
	if next < 1 {
		next = 1 // log indices start at 1; guard against an unseeded peer
	}

	a = appendArgs{
		term:          r.currentTerm,
		leaderCommit:  r.commitIndex,
		nextIndexSent: next,
	}

	if prevTerm, ok := r.termAtLocked(next - 1); ok {
		a.prevLogIndex = next - 1
		a.prevLogTerm = prevTerm
		a.entries = r.entriesFromLocked(next)
	} else {
		// The position this peer asks about is compacted away, so no consistency
		// check can be built for it. Point at the snapshot boundary and send no
		// entries: the follower will reject, but it still resets its election
		// timer on a current-term request, and the InstallSnapshot going out
		// alongside is what actually repairs it.
		a.prevLogIndex = r.snapLastIndex
		a.prevLogTerm = r.snapLastTerm
	}

	return a, next <= r.snapLastIndex
}

// sendAppendEntries sends one AppendEntries to one peer and folds the reply into
// the leader's replication bookkeeping: a higher term steps us down, success
// advances matchIndex (and possibly commitIndex), and a log mismatch walks
// nextIndex back one position for the next tick.
func (r *RaftNode) sendAppendEntries(ctx context.Context, peer PeerClient, a appendArgs) {
	hbCtx, cancel := context.WithTimeout(ctx, r.heartbeatRPCTimeout())
	defer cancel()

	resp, err := peer.Client.AppendEntries(hbCtx, &kvpb.AppendEntriesRequest{
		Term:         a.term,
		LeaderId:     r.nodeID,
		PrevLogIndex: a.prevLogIndex,
		PrevLogTerm:  a.prevLogTerm,
		Entries:      a.entries,
		LeaderCommit: a.leaderCommit,
	})
	if err != nil {
		r.notePeerHeartbeat(peer.NodeID, false)
		r.logger.Warn("AppendEntries failed", "peer", peer.NodeID, "error", err)
		return
	}

	// A peer that answered is a peer that is up — including one that answered
	// "no". A log mismatch is a healthy follower disagreeing about history, and
	// reporting it as unreachable would poison the health signal this log
	// exists to carry: the peer would be marked down at the exact moment it is
	// proving it is alive. Only a transport failure is ok=false, above.
	r.notePeerHeartbeat(peer.NodeID, true)

	r.mu.Lock()

	if resp.Term > r.currentTerm {
		r.stepDownLocked(resp.Term)
		r.mu.Unlock()
		return
	}
	// Ignore a reply that belongs to a term or a role we have since left: its
	// bookkeeping would be about a log we no longer own.
	if r.role != Leader || r.currentTerm != a.term {
		r.mu.Unlock()
		return
	}

	if resp.Success {
		matched := a.prevLogIndex + uint64(len(a.entries))
		if matched > r.matchIndex[peer.NodeID] {
			r.matchIndex[peer.NodeID] = matched
		}
		if next := matched + 1; next > r.nextIndex[peer.NodeID] {
			r.nextIndex[peer.NodeID] = next
		}
		before := r.commitIndex
		r.advanceCommitIndexLocked()
		advanced := r.commitIndex > before
		r.mu.Unlock()

		if advanced {
			// Detached from the heartbeat deadline deliberately: that deadline
			// exists to abandon a stale RPC, and letting it also abort
			// application would make how far the state machine gets depend on
			// network timing.
			r.applyCommitted(context.WithoutCancel(ctx))
		}
		return
	}

	// Log mismatch. Walk back one index and retry on the next tick. Only adjust
	// if no other in-flight reply has already moved it, and never below the
	// first index (1) — a peer whose nextIndex reaches the snapshot boundary is
	// picked up by the InstallSnapshot path instead.
	if r.nextIndex[peer.NodeID] == a.nextIndexSent && a.nextIndexSent > 1 {
		r.nextIndex[peer.NodeID] = a.nextIndexSent - 1
		r.logger.Debug("AppendEntries rejected; backing off",
			"peer", peer.NodeID, "next_index", a.nextIndexSent-1)
	}
	r.mu.Unlock()
}

// SetPeerHealthObserver registers a sink for heartbeat outcomes. Passing nil
// clears it. Call before Run.
//
// This is how the leader's heartbeats — the most direct liveness signal the
// cluster has — reach the replica catch-up trigger, which lives outside Raft
// because data placement is the hash ring's business, not Raft's. The
// registration is optional so that Raft keeps no hard dependency on the
// replication layer, and so that raft's own tests need no sink.
func (r *RaftNode) SetPeerHealthObserver(o PeerHealthObserver) {
	r.mu.Lock()
	r.peerHealth = o
	r.mu.Unlock()
}

// PeerHealthObserver receives the outcome of each heartbeat RPC the leader
// sends. Implementations must not block: they are called on the per-peer
// heartbeat goroutine, whose promptness the cluster's stability depends on.
type PeerHealthObserver interface {
	ObserveHeartbeat(nodeID string, ok bool)
}

func (r *RaftNode) notePeerHeartbeat(nodeID string, ok bool) {
	r.mu.Lock()
	o := r.peerHealth
	r.mu.Unlock()
	if o != nil {
		o.ObserveHeartbeat(nodeID, ok)
	}
}

// heartbeatRPCTimeout is the per-RPC deadline for a heartbeat. It is
// deliberately decoupled from the send interval.
//
// Giving each heartbeat exactly one heartbeatInterval to complete — as this
// code used to — means any RPC delayed past a single interval by gRPC
// connection setup, a host scheduler pause, or ordinary network jitter is
// cancelled by its own deadline. The follower never sees it, its election timer
// never resets, and it calls an election against a perfectly healthy leader.
// That margin is far too tight to survive a containerised cluster.
//
// A heartbeat stays useful to a follower right up until that follower's
// election timer could expire, so the deadline is the minimum election timeout:
// long enough to ride out jitter of several send intervals, short enough that
// the RPC is abandoned once its arrival could no longer have prevented an
// election. The floor of two send intervals keeps the deadline wider than one
// interval even if the election timeouts are configured tighter than the
// heartbeat interval, which the config validator does not forbid.
//
// The send loop period stays at heartbeatInterval, so a merely slow peer still
// gets a fresh heartbeat every interval. Up to ceil(timeout/interval) may be in
// flight to one peer at once; that is bounded and deliberate. Skipping a tick
// while an earlier RPC is still outstanding would withhold the very signal the
// follower is waiting for, betting the cluster's stability on the stalled RPC
// eventually landing.
func (r *RaftNode) heartbeatRPCTimeout() time.Duration {
	timeout := r.electionTimeoutMin
	if floor := 2 * r.heartbeatInterval; timeout < floor {
		timeout = floor
	}
	return timeout
}

// sendInstallSnapshot sends a full snapshot to a peer.
func (r *RaftNode) sendInstallSnapshot(ctx context.Context, peer PeerClient, term uint64) {
	snap, ok, err := r.snapshotStore.Load()
	if err != nil || !ok {
		return
	}

	isCtx, cancel := context.WithTimeout(ctx, 5*r.heartbeatInterval)
	defer cancel()

	resp, err := peer.Client.InstallSnapshot(isCtx, &kvpb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          r.nodeID,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		// The state machine's own bytes, passed through untouched: Raft has no
		// business knowing their shape.
		Data: snap.Data,
	})
	if err != nil {
		r.logger.Warn("InstallSnapshot RPC failed", "peer", peer.NodeID, "err", err)
		return
	}
	r.mu.Lock()
	if resp.Term > r.currentTerm {
		r.stepDownLocked(resp.Term)
	} else if resp.Success && r.role == Leader && r.currentTerm == term {
		if next := snap.LastIncludedIndex + 1; next > r.nextIndex[peer.NodeID] {
			r.nextIndex[peer.NodeID] = next
		}
		if snap.LastIncludedIndex > r.matchIndex[peer.NodeID] {
			r.matchIndex[peer.NodeID] = snap.LastIncludedIndex
		}
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

	if err := r.persistLocked(r.log); err != nil {
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
		if err := r.persistLocked(r.log); err != nil {
			r.logger.Error("failed to persist vote", "error", err)
			return resp, fmt.Errorf("raft: persist vote: %w", err)
		}
		resp.VoteGranted = true
		r.sendResetTimer()
		r.logger.Info("voted for candidate", "candidate", req.CandidateId, "term", r.currentTerm)
	}
	return resp, nil
}

// HandleAppendEntries processes an incoming AppendEntries RPC (§5.3).
//
// The order of the steps is load-bearing:
//
//  1. A stale term is rejected outright.
//  2. Leader recognition — leaderID, lastHeardFromLeader, election-timer reset —
//     happens for every request from a current-term leader, *before* the log is
//     inspected and regardless of how the inspection turns out. A follower whose
//     log has diverged is still hearing from the legitimate leader, and starving
//     its election timer while the leader repairs it would make it stand for
//     election against the very node fixing it.
//  3. The consistency check, then the merge, then persistence, and only then
//     Success. Nothing is acknowledged before it is durable.
//  4. Applying happens after the lock is released, so a slow state machine
//     cannot delay the next heartbeat.
func (r *RaftNode) HandleAppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error) {
	r.mu.Lock()

	resp := &kvpb.AppendEntriesResponse{Term: r.currentTerm}

	// 1. Reply false if the leader's term is behind ours (§5.1).
	if req.Term < r.currentTerm {
		r.mu.Unlock()
		return resp, nil
	}
	if req.Term > r.currentTerm || r.role == Candidate {
		r.stepDownLocked(req.Term)
		resp.Term = r.currentTerm
	}

	// 2. Recognise the leader.
	r.leaderID = req.LeaderId
	r.lastHeardFromLeader = time.Now() // used by pre-vote to detect active leader
	r.sendResetTimer()

	// 3. Reply false if we have no entry at PrevLogIndex whose term matches
	//    PrevLogTerm (§5.3). termAtLocked reports the snapshot boundary, so a
	//    caught-up follower whose log was compacted answers correctly; an index
	//    below the boundary is unresolvable, and rejecting it is what makes the
	//    leader fall back to InstallSnapshot.
	localTerm, ok := r.termAtLocked(req.PrevLogIndex)
	if !ok || localTerm != req.PrevLogTerm {
		r.logger.Debug("AppendEntries consistency check failed",
			"prev_log_index", req.PrevLogIndex, "leader_prev_term", req.PrevLogTerm,
			"local_term", localTerm, "have_index", ok, "last_log_index", r.lastLogIndex())
		r.mu.Unlock()
		return resp, nil
	}

	newLog, changed, err := r.mergeEntriesLocked(req.PrevLogIndex, req.Entries)
	if err != nil {
		// An invariant violation, not a routine mismatch. Reject without
		// touching the log and let it be visible in the logs.
		r.logger.Error("AppendEntries merge refused", "error", err)
		r.mu.Unlock()
		return resp, nil
	}

	if changed {
		// Durable before acknowledged. Because mergeEntriesLocked built a copy,
		// a failed write leaves r.log exactly as the disk has it — so the
		// leader's retry re-derives the same append rather than finding the
		// entry already present and being acknowledged for it.
		if err := r.persistLocked(newLog); err != nil {
			r.logger.Error("AppendEntries: persist log", "error", err)
			r.mu.Unlock()
			return resp, nil
		}
		r.log = newLog
	}

	// 4. Advance commitIndex, bounded by the last entry this request vouches
	//    for. Bounding on lastLogIndex instead would be wrong when the merge
	//    kept a tail beyond the request's range: those entries came from some
	//    earlier leader and this request says nothing about them.
	lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, lastNewIndex)
	}
	resp.Success = true
	r.mu.Unlock()

	r.applyCommitted(ctx)
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

// HandleInstallSnapshot applies a full snapshot from the leader (§7).
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

	// A snapshot ending at or before our own boundary carries nothing we do not
	// already have applied. Installing it anyway would move the state machine
	// backwards.
	if req.LastIncludedIndex <= r.snapLastIndex {
		term := r.currentTerm
		r.mu.Unlock()
		r.logger.Debug("InstallSnapshot ignored: not newer than local snapshot",
			"req_last_index", req.LastIncludedIndex, "local_snap_index", r.snapLastIndex)
		return &kvpb.InstallSnapshotResponse{Term: term, Success: true}, nil
	}

	// §7's retention decision is deliberately NOT made here. The lock is about
	// to be dropped for the restore, and another AppendEntries could change the
	// log in between — so the decision is taken below, under the same lock hold
	// that acts on it.
	r.mu.Unlock()

	// Restore with no lock held: the payload is arbitrarily large and the
	// restore is I/O.
	if err := r.sm.RestoreFromSnapshot(ctx, req.Data); err != nil {
		return &kvpb.InstallSnapshotResponse{Term: req.Term, Success: false},
			fmt.Errorf("raft: InstallSnapshot restore: %w", err)
	}

	// Persist the snapshot before recording its bounds, so a crash in between
	// leaves a snapshot ahead of the state file rather than bounds pointing at a
	// payload that was never written. New() resolves that direction.
	snap := Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              cloneBytes(req.Data),
	}
	if err := r.snapshotStore.Save(snap); err != nil {
		r.logger.Error("InstallSnapshot: save snapshot", "err", err)
		return &kvpb.InstallSnapshotResponse{Term: req.Term, Success: false},
			fmt.Errorf("raft: InstallSnapshot save: %w", err)
	}

	r.mu.Lock()
	// §7: if our log holds an entry at the snapshot's last included index with
	// the same term, the entries after it are still valid and are kept.
	// Otherwise the whole log goes — it describes a history the leader has
	// already superseded.
	localTerm, haveIndex := r.termAtLocked(req.LastIncludedIndex)
	retainTail := haveIndex && localTerm == req.LastIncludedTerm

	r.snapLastIndex = req.LastIncludedIndex
	r.snapLastTerm = req.LastIncludedTerm
	if retainTail {
		r.normaliseLogLocked() // drops what the snapshot covers, keeps the rest
	} else {
		r.log = nil
	}
	// The snapshot's contents are committed and applied by definition, so both
	// frontiers move up to its boundary.
	if r.commitIndex < r.snapLastIndex {
		r.commitIndex = r.snapLastIndex
	}
	if r.lastApplied < r.snapLastIndex {
		r.lastApplied = r.snapLastIndex
	}
	if err := r.persistLocked(r.log); err != nil {
		r.logger.Error("InstallSnapshot: persist state", "err", err)
	}
	r.mu.Unlock()

	r.logger.Info("snapshot installed",
		"last_index", req.LastIncludedIndex,
		"last_term", req.LastIncludedTerm,
		"retained_tail", retainTail)

	return &kvpb.InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

// takeSnapshot captures the state machine and compacts the log.
// Runs in a goroutine; must not hold r.mu on entry.
func (r *RaftNode) takeSnapshot(ctx context.Context) {
	r.mu.Lock()
	// The cut point is lastApplied, not lastLogIndex. The payload reflects only
	// what the state machine has consumed, so claiming a higher index would hand
	// a follower a payload missing entries the index promises — and then discard
	// those entries locally as compacted.
	lastIdx := r.lastApplied
	if lastIdx <= r.snapLastIndex {
		r.mu.Unlock()
		return // nothing applied since the last snapshot
	}
	lastTerm, ok := r.termAtLocked(lastIdx)
	if !ok {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	data, err := r.sm.SnapshotState(ctx)
	if err != nil {
		r.logger.Error("takeSnapshot: SnapshotState failed", "err", err)
		return
	}

	snap := Snapshot{LastIncludedIndex: lastIdx, LastIncludedTerm: lastTerm, Data: data}
	if err := r.snapshotStore.Save(snap); err != nil {
		r.logger.Error("takeSnapshot: save failed", "err", err)
		return
	}

	r.mu.Lock()
	// Re-check under the lock: another snapshot may have advanced the boundary
	// past this one while SnapshotState was running.
	if lastIdx > r.snapLastIndex {
		r.snapLastIndex = lastIdx
		r.snapLastTerm = lastTerm
		r.normaliseLogLocked() // discards entries up to and including lastIdx
		if err := r.persistLocked(r.log); err != nil {
			r.logger.Error("takeSnapshot: persist state", "err", err)
		}
	}
	r.mu.Unlock()

	r.logger.Info("snapshot taken", "last_index", lastIdx, "last_term", lastTerm)
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

// CommitIndex is the highest log index known to be committed. A proposer can
// watch it against the index Propose returned to learn that its entry took
// effect cluster-wide.
func (r *RaftNode) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// LastLogIndex is the highest index this node stores, whether committed or not.
func (r *RaftNode) LastLogIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastLogIndex()
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
