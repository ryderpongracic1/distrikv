package cluster

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// DefaultStableChecks is how many consecutive healthy observations a peer must
// produce before it is declared healthy again.
//
// A single successful probe is not enough. A node that has just restarted
// accepts connections before it is useful — it still has a WAL to replay and a
// compaction backlog to arm — and a flapping peer would otherwise trigger a
// catch-up pass on every flap. Three observations is the same "wait for stable
// health" gate Raft's own election timing uses in miniature: long enough to
// exclude a single lucky probe, short enough to react within a fraction of a
// second at a 150ms heartbeat interval.
const DefaultStableChecks = 3

// PeerHealth is the cluster's view of which peers are reachable, and the source
// of the unreachable → healthy transition that triggers replica catch-up.
//
// It merges three independent signals rather than relying on any one:
//
//   - Raft heartbeat outcomes, via ObserveHeartbeat. This is the signal the
//     design calls for, and it is the most direct one — but only the Raft leader
//     sends heartbeats, so a follower that is nonetheless a ring-primary for some
//     key range would learn nothing from it. Ring ownership and Raft leadership
//     are deliberately unrelated in distrikv, so health cannot depend on being
//     the leader.
//   - Replication RPC outcomes, via ObserveReplication. Every ring-primary has
//     this signal for exactly the peers it replicates to, and it is the same
//     transport the catch-up pass will use — so it fails and recovers together
//     with the thing being triggered.
//   - A transport-level probe run on a ticker, via the Probe function. This is
//     what lets a non-leader notice that a peer has come *back*: replication
//     failures tell you a peer is gone, but nothing on the write path tells you
//     it returned, because writes to it are being refused.
//
// PeerHealth is safe for concurrent use.
type PeerHealth struct {
	interval     time.Duration
	stableChecks int
	probe        func(nodeID string) bool
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
}

// HealthConfig configures a PeerHealth. Probe is required; the rest have
// defaults.
type HealthConfig struct {
	// Interval is how often Probe is called for each peer.
	Interval time.Duration

	// StableChecks is the number of consecutive healthy observations required
	// before an unreachable peer is declared healthy. Defaults to
	// DefaultStableChecks.
	StableChecks int

	// Probe reports whether a peer looks reachable right now. It must not block
	// for long: it is called for every peer on every tick. The intended
	// implementation is a transport-state read (e.g. a gRPC channel's
	// connectivity state), not an RPC.
	Probe func(nodeID string) bool

	Logger *slog.Logger
}

// NewPeerHealth creates a tracker for the given peer node IDs.
//
// Peers start out considered healthy. Starting them unhealthy would make every
// node fire a catch-up pass for every replica the first time it saw them, which
// is exactly the thundering herd the stable-health gate exists to avoid; a peer
// that really is down is demoted by the first failed probe one interval later.
func NewPeerHealth(nodeIDs []string, cfg HealthConfig) *PeerHealth {
	if cfg.Interval <= 0 {
		cfg.Interval = 250 * time.Millisecond
	}
	if cfg.StableChecks <= 0 {
		cfg.StableChecks = DefaultStableChecks
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ph := &PeerHealth{
		interval:     cfg.Interval,
		stableChecks: cfg.StableChecks,
		probe:        cfg.Probe,
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
func (ph *PeerHealth) ObserveHeartbeat(nodeID string, ok bool) { ph.observe(nodeID, ok) }

// ObserveReplication records a replication RPC outcome for a peer.
//
// Only failures are meaningful here in practice — a successful replication also
// counts towards stability, but during a fault there are no successes to count,
// which is why recovery detection needs the probe.
func (ph *PeerHealth) ObserveReplication(nodeID string, ok bool) { ph.observe(nodeID, ok) }

func (ph *PeerHealth) observe(nodeID string, ok bool) {
	ph.mu.Lock()
	st := ph.peers[nodeID]
	if st == nil {
		ph.mu.Unlock()
		return
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

// Run polls every peer on a ticker until ctx is cancelled. It is the only part
// of PeerHealth that needs a goroutine; the Observe methods are called from
// whichever goroutine produced the signal.
func (ph *PeerHealth) Run(ctx context.Context) {
	if ph.probe == nil {
		ph.logger.Warn("no peer probe configured; recovery will be detected only " +
			"from replication or heartbeat successes")
		<-ctx.Done()
		return
	}
	t := time.NewTicker(ph.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, id := range ph.Peers() {
				ph.observe(id, ph.probe(id))
			}
		}
	}
}

// Peers returns the tracked node IDs in sorted order.
func (ph *PeerHealth) Peers() []string {
	ph.mu.Lock()
	out := make([]string, 0, len(ph.peers))
	for id := range ph.peers {
		out = append(out, id)
	}
	ph.mu.Unlock()
	sort.Strings(out)
	return out
}
