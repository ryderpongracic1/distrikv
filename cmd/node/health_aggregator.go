package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// Hysteresis defaults for the health aggregator.
//
// Down is slower than up on purpose, and both are asymmetric to the cost of
// being wrong. Proposing "down" too eagerly writes a Raft entry that gates every
// ring-primary's retry loop against a peer that was merely slow for one tick;
// proposing "up" too eagerly only schedules a catch-up pass that finds nothing
// to ship. At the docker-compose heartbeat interval of 150 ms these are ~450 ms
// to declare a peer down and ~300 ms to declare it back.
//
// defaultHealthDownAfter matches cluster.DefaultStableChecks so the consensus
// signal and the local one do not disagree about how much evidence a transition
// needs.
const (
	defaultHealthDownAfter = 3
	defaultHealthUpAfter   = 2
)

// healthProposer is the slice of raft.RaftNode the aggregator needs. It is an
// interface so the aggregator's hysteresis, step-down reset and failure handling
// can be tested without standing up a Raft cluster.
type healthProposer interface {
	// Propose appends an entry to the leader's log. Returns raft.ErrNotLeader on
	// a non-leader.
	Propose(ctx context.Context, op, key string, value []byte) (uint64, error)
	IsLeader() bool
	CurrentTerm() uint64
}

// committedHealthView is the aggregator's read of the state its own proposals
// produce — the committed view, not its local observations.
type committedHealthView interface {
	Healthy(nodeID string) bool
}

// healthAggregatorConfig tunes the hysteresis. Zero values take the defaults.
type healthAggregatorConfig struct {
	// DownAfter is how many consecutive failed heartbeats to a peer must be
	// observed before "down" is proposed.
	DownAfter int

	// UpAfter is how many consecutive successful heartbeats must be observed
	// before "up" is proposed.
	UpAfter int

	// QueueDepth bounds the pending proposals buffer. It only needs to absorb
	// the transitions one heartbeat tick can produce.
	QueueDepth int
}

func (c healthAggregatorConfig) withDefaults(peerCount int) healthAggregatorConfig {
	if c.DownAfter <= 0 {
		c.DownAfter = defaultHealthDownAfter
	}
	if c.UpAfter <= 0 {
		c.UpAfter = defaultHealthUpAfter
	}
	if c.QueueDepth <= 0 {
		c.QueueDepth = 4*peerCount + 4
	}
	return c
}

// aggPeerState is the aggregator's per-peer hysteresis counters.
type aggPeerState struct {
	failures  int
	successes int

	// proposedHealthy is the state this aggregator last committed itself to
	// proposing for the peer. It is what stops a transition from being proposed
	// on every tick for as long as the condition holds — the log must carry one
	// entry per genuine change, not one per heartbeat.
	//
	// It is seeded from the *committed* view rather than assumed healthy. A
	// freshly elected leader that assumed "up" would sit with a peer its
	// predecessor marked down, observe nothing but successes, decide there is no
	// change to propose, and leave that stale "down" entry standing with nobody
	// left to correct it. Seeding from consensus makes the new leader propose the
	// correction on its first UpAfter successes.
	proposedHealthy bool
}

// healthProposal is one transition waiting to be written to the Raft log.
type healthProposal struct {
	op         string
	nodeID     string
	observedAt time.Time
}

// healthAggregator turns the Raft leader's heartbeat outcomes into committed
// health transitions.
//
// # Where it runs
//
// On the leader only, because heartbeat outcomes only exist there. On step-down
// it drops everything it had accumulated: those counters describe RPCs this node
// is no longer sending, and the next leader is the one whose observations should
// decide. A step-down race is harmless — Propose answers ErrNotLeader and the
// transition is dropped, because the new leader observes the same peer and
// proposes it too.
//
// # Why proposing is a separate goroutine
//
// ObserveHeartbeat is called on the per-peer heartbeat goroutine, and
// raft.Propose fsyncs the log before returning. Calling it inline would put a
// disk write on the one path every follower's election timer depends on — which
// is precisely the shape of the defect that cost this cluster three months of
// election storm. So ObserveHeartbeat only touches in-memory counters and hands
// the transition to Run's goroutine through a buffered channel.
//
// healthAggregator is safe for concurrent use.
type healthAggregator struct {
	raft    healthProposer
	view    committedHealthView
	metrics *metrics.Metrics
	logger  *slog.Logger
	cfg     healthAggregatorConfig

	mu sync.Mutex
	// leading and term together identify the leadership epoch the counters
	// belong to. Either changing resets them.
	leading bool
	term    uint64
	peers   map[string]*aggPeerState

	proposals chan healthProposal
}

func newHealthAggregator(
	r healthProposer,
	view committedHealthView,
	peerCount int,
	m *metrics.Metrics,
	logger *slog.Logger,
	cfg healthAggregatorConfig,
) *healthAggregator {
	cfg = cfg.withDefaults(peerCount)
	return &healthAggregator{
		raft:      r,
		view:      view,
		metrics:   m,
		logger:    logger.With("component", "health-aggregator"),
		cfg:       cfg,
		peers:     make(map[string]*aggPeerState, peerCount),
		proposals: make(chan healthProposal, cfg.QueueDepth),
	}
}

// ObserveHeartbeat implements raft.PeerHealthObserver. It must not block: see the
// type doc.
func (a *healthAggregator) ObserveHeartbeat(nodeID string, ok bool) {
	// Read leadership before taking a.mu. raft.notePeerHeartbeat has already
	// released the Raft lock by the time it calls an observer, so calling back in
	// is safe; doing it outside a.mu keeps the two locks from ever being held
	// together in either order.
	leading := a.raft.IsLeader()
	term := a.raft.CurrentTerm()

	a.mu.Lock()
	if !leading {
		if a.leading {
			a.resetLocked("no longer the leader")
		}
		a.mu.Unlock()
		return
	}
	if !a.leading || term != a.term {
		a.resetLocked("new leadership epoch")
		a.leading, a.term = true, term
	}

	st := a.peers[nodeID]
	if st == nil {
		st = &aggPeerState{proposedHealthy: a.view.Healthy(nodeID)}
		a.peers[nodeID] = st
	}

	var op string
	if ok {
		st.successes++
		st.failures = 0
		if !st.proposedHealthy && st.successes >= a.cfg.UpAfter {
			st.proposedHealthy = true
			op = opHealthUp
		}
	} else {
		st.failures++
		st.successes = 0
		if st.proposedHealthy && st.failures >= a.cfg.DownAfter {
			st.proposedHealthy = false
			op = opHealthDown
		}
	}
	a.mu.Unlock()

	if op == "" {
		return
	}
	a.enqueue(healthProposal{op: op, nodeID: nodeID, observedAt: time.Now()})
}

// resetLocked drops all accumulated hysteresis. Caller must hold a.mu.
//
// The map is cleared rather than zeroed per peer so that the next observation of
// each peer re-seeds proposedHealthy from the committed view — which is the whole
// point of resetting (see aggPeerState.proposedHealthy).
func (a *healthAggregator) resetLocked(reason string) {
	if len(a.peers) > 0 {
		a.logger.Info("dropping accumulated heartbeat observations",
			"reason", reason, "peers", len(a.peers))
	}
	a.leading = false
	a.peers = make(map[string]*aggPeerState, len(a.peers))
}

// enqueue hands a transition to Run's goroutine without blocking.
//
// A full queue reverts the peer's proposed state so the next tick that still
// sees the condition proposes it again. Dropping it silently would lose the
// transition permanently: the counters keep climbing past the threshold, but
// proposedHealthy would already say the transition was made.
func (a *healthAggregator) enqueue(p healthProposal) {
	select {
	case a.proposals <- p:
	default:
		a.revert(p)
		a.logger.Warn("health transition queue full; will re-propose on a later tick",
			"peer", p.nodeID, "op", p.op)
	}
}

// revert undoes the proposedHealthy update an attempt made, so the hysteresis
// re-fires. It is a no-op if the peer's state has already moved on, which keeps a
// late failure from overwriting a newer decision.
func (a *healthAggregator) revert(p healthProposal) {
	attempted := p.op == opHealthUp
	a.mu.Lock()
	defer a.mu.Unlock()
	if st := a.peers[p.nodeID]; st != nil && st.proposedHealthy == attempted {
		st.proposedHealthy = !attempted
	}
}

// Run drains queued transitions into the Raft log until ctx is cancelled.
//
// It is the only caller of Propose, so at most one log append is in flight at a
// time and the fsync it pays never lands on a heartbeat goroutine.
func (a *healthAggregator) Run(ctx context.Context) {
	a.logger.Info("health aggregator started",
		"down_after", a.cfg.DownAfter, "up_after", a.cfg.UpAfter)
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-a.proposals:
			a.propose(ctx, p)
		}
	}
}

func (a *healthAggregator) propose(ctx context.Context, p healthProposal) {
	// The value is diagnostic only — the state machine reads the op and the key.
	// It records when the leader decided, which is what a reader of the log wants
	// to correlate against the peer's own logs; the entry's own term and index
	// already say when it was committed.
	value := []byte(p.observedAt.UTC().Format(time.RFC3339Nano))

	index, err := a.raft.Propose(ctx, p.op, p.nodeID, value)
	switch {
	case err == nil:
		if a.metrics != nil {
			a.metrics.HealthTransitionsProposed.Add(1)
		}
		a.logger.Info("proposed health transition",
			"peer", p.nodeID, "op", p.op, "index", index)

	case errors.Is(err, raft.ErrNotLeader):
		// Lost leadership between the observation and the append. Not a fault and
		// not worth retrying here: this node is no longer the one observing that
		// peer, and the new leader will see the same condition and propose it.
		a.revert(p)
		a.logger.Debug("dropped health transition: no longer the leader",
			"peer", p.nodeID, "op", p.op)

	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		a.logger.Debug("dropped health transition: shutting down",
			"peer", p.nodeID, "op", p.op)

	default:
		// A persist failure. Revert so the hysteresis re-proposes on a later tick
		// rather than believing a transition was recorded that never reached disk.
		a.revert(p)
		a.logger.Error("could not append health transition to the Raft log; "+
			"will re-propose while the condition holds",
			"peer", p.nodeID, "op", p.op, "err", err)
	}
}

// multiPeerHealthObserver fans one heartbeat outcome out to several observers.
//
// raft.SetPeerHealthObserver holds a single sink, and this change must not take
// the existing one away: cluster.PeerHealth's local signals are what carry
// recovery detection during a leaderless window, when no new health entry can be
// committed. So the consensus aggregator is added alongside it rather than in
// place of it.
type multiPeerHealthObserver []raft.PeerHealthObserver

// ObserveHeartbeat forwards to every registered observer.
//
// Like the observers it wraps, this must not block — it is called on the per-peer
// heartbeat goroutine. Both current implementations only touch in-memory state.
func (m multiPeerHealthObserver) ObserveHeartbeat(nodeID string, ok bool) {
	for _, o := range m {
		if o != nil {
			o.ObserveHeartbeat(nodeID, ok)
		}
	}
}
