package cluster

import (
	"log/slog"
	"sort"
	"sync"
	"time"
)

// DefaultStableChecks is how many consecutive healthy observations a peer must
// produce before it is declared healthy again.
//
// A single successful observation is not enough. A node that has just restarted
// accepts connections before it is useful — it still has a WAL to replay and a
// compaction backlog to arm — and a flapping peer would otherwise trigger a
// catch-up pass on every flap. Three observations is the same "wait for stable
// health" gate Raft's own election timing uses in miniature: long enough to
// exclude a single lucky success, short enough to react within a fraction of a
// second at a 150ms heartbeat interval.
const DefaultStableChecks = 3

// PeerHealth is this node's *local* view of which peers are reachable, and one
// of the two sources of the unreachable → healthy transition that triggers
// replica catch-up. The other is the committed, cluster-wide health view the
// Raft log carries; the two are merged by the anti-entropy engine
// (`antiEntropy.reachable`), not here.
//
// It observes two signals, both of which are outcomes of traffic this node was
// sending anyway. There is no dedicated probing machinery: a transport-level
// probe on a ticker used to be a third signal and has been removed, because the
// committed health view covers the case it uniquely covered (see
// docs/replication-and-anti-entropy.md).
//
//   - Raft heartbeat outcomes, via ObserveHeartbeat. The most direct signal
//     available — but only the Raft leader sends heartbeats, so a follower that
//     is nonetheless a ring-primary for some key range learns nothing from it.
//     Ring ownership and Raft leadership are deliberately unrelated in distrikv,
//     so health cannot depend on being the leader.
//   - Replication RPC outcomes, via ObserveReplication. Every ring-primary has
//     this signal for exactly the peers it replicates to, and it is the same
//     transport the catch-up pass will use — so it fails and recovers together
//     with the thing being triggered.
//
// PeerHealth is safe for concurrent use.
type PeerHealth struct {
	stableChecks int
	logger       *slog.Logger

	mu    sync.Mutex
	peers map[string]*peerState

	// recovered carries node IDs that just transitioned unreachable → healthy.
	// Buffered and lossy by design: a dropped transition costs a delayed pass,
	// never a missed repair, because the anti-entropy engine also retries any
	// replica it knows to be behind.
	recovered chan string
}

type peerState struct {
	healthy    bool
	consecOK   int
	lastChange time.Time

	// lastReplicationOK records this node's most recent replication RPC outcome
	// for the peer, which is *positive local evidence* about reachability rather
	// than the absence of bad news. `healthy` cannot serve that purpose: a peer
	// starts healthy and an untracked node reports healthy, so "healthy" also
	// means "no opinion". This field distinguishes the two, which is what lets
	// the anti-entropy engine veto a committed "down" only when this node has
	// actually reached the peer — and it starts false, so a peer no replication
	// has been attempted to carries no evidence at all.
	lastReplicationOK bool
}

// HealthConfig configures a PeerHealth. Every field has a default.
type HealthConfig struct {
	// StableChecks is the number of consecutive healthy observations required
	// before an unreachable peer is declared healthy. Defaults to
	// DefaultStableChecks.
	StableChecks int

	Logger *slog.Logger
}

// NewPeerHealth creates a tracker for the given peer node IDs.
//
// Peers start out considered healthy. Starting them unhealthy would make every
// node fire a catch-up pass for every replica the first time it saw them, which
// is exactly the thundering herd the stable-health gate exists to avoid; a peer
// that really is down is demoted by the first failed heartbeat or replication.
func NewPeerHealth(nodeIDs []string, cfg HealthConfig) *PeerHealth {
	if cfg.StableChecks <= 0 {
		cfg.StableChecks = DefaultStableChecks
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ph := &PeerHealth{
		stableChecks: cfg.StableChecks,
		logger:       cfg.Logger.With("component", "peer-health"),
		peers:        make(map[string]*peerState, len(nodeIDs)),
		recovered:    make(chan string, 2*len(nodeIDs)+4),
	}
	now := time.Now()
	for _, id := range nodeIDs {
		ph.peers[id] = &peerState{healthy: true, consecOK: cfg.StableChecks, lastChange: now}
	}
	return ph
}

// Recovered returns the channel of unreachable → healthy transitions.
func (ph *PeerHealth) Recovered() <-chan string { return ph.recovered }

// Healthy reports whether nodeID is currently considered reachable. Unknown
// nodes report true: a caller must not treat "we have no opinion" as "down".
func (ph *PeerHealth) Healthy(nodeID string) bool {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	st, ok := ph.peers[nodeID]
	if !ok {
		return true
	}
	return st.healthy
}

// ObserveHeartbeat records a Raft heartbeat outcome for a peer.
func (ph *PeerHealth) ObserveHeartbeat(nodeID string, ok bool) {
	ph.observe(nodeID, ok, false)
}

// ObserveReplication records a replication RPC outcome for a peer.
//
// Failures are what demote a peer. Successes count towards the stable-health
// gate and, separately, are recorded as the peer's most recent replication
// outcome so that LastReplicationSucceeded can report positive local evidence.
func (ph *PeerHealth) ObserveReplication(nodeID string, ok bool) {
	ph.observe(nodeID, ok, true)
}

// LastReplicationSucceeded reports whether this node's most recent replication
// RPC to nodeID succeeded.
//
// Unlike Healthy, it is *positive* evidence and false when there is none: a peer
// no replication has ever been attempted to reports false. That distinction is
// the whole point. Healthy conflates "reachable" with "no opinion" — a freshly
// tracked peer and an untracked node both report healthy — so it cannot be used
// to override a committed consensus "down" without letting a node with no
// evidence at all overrule the cluster. This can, because a true here means this
// node put bytes on the wire to that peer and got an answer.
//
// It is deliberately last-outcome-wins rather than time-windowed. `ReplicateWrite`
// attempts every replica in the ring regardless of health, so a peer that has
// genuinely gone away produces a failure on the next write to any key it replicates
// and the evidence flips back on its own. The residual case — a peer this node holds
// a stale success for and is currently sending no traffic to — is bounded and errs
// towards work rather than away from it: the retry loop schedules one catch-up pass
// that fails, which records the failure and withdraws the evidence.
func (ph *PeerHealth) LastReplicationSucceeded(nodeID string) bool {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	st, ok := ph.peers[nodeID]
	if !ok {
		return false
	}
	return st.lastReplicationOK
}

func (ph *PeerHealth) observe(nodeID string, ok, fromReplication bool) {
	ph.mu.Lock()
	st := ph.peers[nodeID]
	if st == nil {
		ph.mu.Unlock()
		return
	}
	if fromReplication {
		st.lastReplicationOK = ok
	}

	var transitioned bool
	if ok {
		st.consecOK++
		if !st.healthy && st.consecOK >= ph.stableChecks {
			st.healthy = true
			st.lastChange = time.Now()
			transitioned = true
		}
	} else {
		st.consecOK = 0
		if st.healthy {
			st.healthy = false
			st.lastChange = time.Now()
			ph.logger.Warn("peer marked unreachable", "peer", nodeID)
		}
	}
	ph.mu.Unlock()

	if !transitioned {
		return
	}
	ph.logger.Info("peer healthy again after being unreachable",
		"peer", nodeID, "stable_checks", ph.stableChecks)
	select {
	case ph.recovered <- nodeID:
	default:
		ph.logger.Warn("peer recovery notification dropped (queue full); "+
			"catch-up will still run from the retry loop", "peer", nodeID)
	}
}

// trackedPeers returns the tracked node IDs in sorted order.
//
// Unexported: its only production caller was the probe ticker, which is gone. It
// is kept because the sorted-order invariant is worth pinning and it is a useful
// handle when debugging a tracker, but a health tracker has no business offering
// the cluster's peer list as public API — the ring owns that.
func (ph *PeerHealth) trackedPeers() []string {
	ph.mu.Lock()
	out := make([]string, 0, len(ph.peers))
	for id := range ph.peers {
		out = append(out, id)
	}
	ph.mu.Unlock()
	sort.Strings(out)
	return out
}
