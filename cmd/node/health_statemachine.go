package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// Health transition ops. These are the only values the Raft log carries in this
// system: the log is a control plane for node reachability and never holds
// key/value data (see the internal/raft package doc, deviation 1).
const (
	opHealthDown = "health-down"
	opHealthUp   = "health-up"
)

// HealthStateMachine is the committed, cluster-wide view of which nodes are
// reachable. It is the raft.StateMachine every node applies committed entries
// to, and it is what makes "Raft provides node health" true of every node rather
// than only of the Raft leader.
//
// # Why this exists
//
// Only the Raft leader sends heartbeats, so only the leader ever observes a
// peer's reachability directly. Ring ownership and Raft leadership are
// deliberately unrelated in distrikv, so on a 3-node cluster two of the three
// ring-primaries used to learn nothing from Raft at all — which is why
// cluster.PeerHealth had to merge two further local signals to cover them. Here
// the leader's monopoly on heartbeats becomes a feature: the leader observes,
// proposes a transition through the log, and every node applies the same
// committed sequence. A follower that is a ring-primary now reads the same
// health view as the leader.
//
// # Absent means healthy
//
// The map holds only nodes a transition has been committed for. An absent node
// reports healthy, which is both the optimistic start the design calls for
// (nothing is down until something observes it failing) and the same convention
// cluster.PeerHealth already uses for an unknown node: a caller must not read
// "we have no opinion" as "down". It also means the state machine needs no peer
// list and cannot drift out of step with one.
//
// HealthStateMachine is safe for concurrent use.
type HealthStateMachine struct {
	// selfID is this node's own ID. Entries about it are ignored — see Apply.
	selfID  string
	metrics *metrics.Metrics
	logger  *slog.Logger

	mu    sync.Mutex
	state map[string]bool // nodeID → healthy; absent means healthy

	// recovered carries node IDs that just transitioned unhealthy → healthy in
	// the committed view. Buffered and lossy for the same reason
	// cluster.PeerHealth's channel is: a dropped transition costs a delayed
	// catch-up pass, never a missed repair, because the anti-entropy engine also
	// retries any replica it knows to be behind.
	recovered chan string
}

// newHealthStateMachine builds the state machine for a node whose own ID is
// selfID. peerCount only sizes the recovery channel; the state machine tracks
// whatever node IDs the log names.
func newHealthStateMachine(selfID string, peerCount int, m *metrics.Metrics, logger *slog.Logger) *HealthStateMachine {
	if peerCount < 1 {
		peerCount = 1
	}
	return &HealthStateMachine{
		selfID:    selfID,
		metrics:   m,
		logger:    logger.With("component", "health-state-machine"),
		state:     make(map[string]bool, peerCount),
		recovered: make(chan string, 2*peerCount+4),
	}
}

// applyOrigin says how a committed entry reached this state machine. It changes
// nothing about the view the entry produces — replay is how the view is rebuilt
// after a restart — and everything about whether the transition is announced.
//
// It mirrors lsm.writeOrigin, which draws the same distinction one layer down
// for the same reason: a replayed thing must not fire a signal that only live
// evidence justifies (defects 13 and 14).
type applyOrigin uint8

const (
	// originLive is an entry this node is applying for the first time: it was
	// appended to the log during this process's lifetime, so the transition it
	// describes is news.
	originLive applyOrigin = iota

	// originReplay is an entry that was already on disk when this node opened.
	// A previous incarnation may have applied it and already announced it, so
	// the state moves and nothing is emitted.
	originReplay
)

// Apply folds one committed entry into the health view, treating it as live.
//
// It satisfies raft.StateMachine's idempotence requirement by construction: the
// body of a health transition is a map assignment, so applying the same entry
// twice lands on the same state.
//
// An entry about this node itself is ignored. A leader can legitimately commit
// "node2 is down" while node2 is alive and applying that very entry (it was
// merely unreachable *from the leader*), and a node can neither act on nor
// usefully believe a claim about its own reachability. Recording it would also
// put this node's own ID on the recovery channel, where the anti-entropy engine
// would try to schedule a catch-up pass to a replica that does not exist.
//
// An unrecognised op is logged at debug and succeeds. Returning an error would
// be worse than useless: Raft retries a failed Apply forever rather than
// skipping it (see raft.StateMachine), so one entry from a future version would
// wedge the apply loop and freeze the health view of every node that received
// it.
func (h *HealthStateMachine) Apply(ctx context.Context, entry raft.LogEntry) error {
	return h.apply(ctx, entry, originLive)
}

// ReplayApply folds one committed entry into the health view without announcing
// it, for an entry that was on disk before this node opened.
//
// This is the effect gate defect 14 records. commitIndex and lastApplied are
// volatile (raft deviation 3), so a restarting node re-applies every entry above
// the snapshot boundary against a view that starts empty — and because absent
// means healthy, each historical down→up pair in that window looks like a fresh
// failure followed by a fresh recovery. Announcing those would emit an
// operator-facing "consensus marked peer unhealthy" for a peer that is fine and
// schedule a catch-up pass for a replica that never went anywhere, once per pair,
// on every restart.
//
// The state assignment is identical to Apply's: replay is how the committed view
// is reconstructed, and suppressing it would lose the history the log exists to
// carry. Only the announcements are withheld — the WARN, the recovery Info, and
// the send on the recovery channel — which is the distinction the raft.StateMachine
// contract draws between idempotent state and idempotent effects.
func (h *HealthStateMachine) ReplayApply(ctx context.Context, entry raft.LogEntry) error {
	return h.apply(ctx, entry, originReplay)
}

// apply is the shared body of Apply and ReplayApply. origin decides only whether
// a genuine transition is announced.
func (h *HealthStateMachine) apply(_ context.Context, entry raft.LogEntry, origin applyOrigin) error {
	live := origin == originLive

	// Recorded before every early return below, and for every op: this gauge
	// describes how far through the log this node is, not what the health view
	// says. It is the denominator HealthTransitionsCommitted has to be read
	// against — see metrics.RaftLastAppliedIndex. A plain Store is correct because
	// Raft's apply loop is serialised and hands entries over in index order, so
	// the last write is the highest index.
	if h.metrics != nil {
		h.metrics.RaftLastAppliedIndex.Store(entry.Index)
	}

	var healthy bool
	switch entry.Op {
	case opHealthDown:
		healthy = false
	case opHealthUp:
		healthy = true
	default:
		h.logger.Debug("ignoring committed entry with an unrecognised op",
			"index", entry.Index, "term", entry.Term, "op", entry.Op, "key", entry.Key)
		return nil
	}

	nodeID := entry.Key
	if nodeID == "" {
		h.logger.Warn("ignoring health entry with an empty node ID",
			"index", entry.Index, "term", entry.Term, "op", entry.Op)
		return nil
	}
	if nodeID == h.selfID {
		h.logger.Debug("ignoring committed health entry about this node",
			"index", entry.Index, "term", entry.Term, "op", entry.Op)
		return nil
	}

	h.mu.Lock()
	was, known := h.state[nodeID]
	if !known {
		was = true // absent means healthy
	}
	h.state[nodeID] = healthy
	changed := was != healthy
	h.mu.Unlock()

	if h.metrics != nil {
		h.metrics.HealthTransitionsCommitted.Add(1)
	}
	if !changed {
		// A re-applied or redundant entry. Not a fault: the leader only proposes
		// on a change it observed, but a restart replays the log and a new leader
		// may re-assert a transition its predecessor already committed.
		h.logger.Debug("committed health entry restates the view",
			"peer", nodeID, "healthy", healthy, "index", entry.Index, "replay", !live)
		return nil
	}

	if !live {
		// A transition, but one this node is only re-reading. Debug, not Warn:
		// the entry describes history, and a restart that walked a hundred of
		// these must not look like a hundred peers failing right now.
		h.logger.Debug("replayed committed health transition (not announced)",
			"peer", nodeID, "healthy", healthy, "index", entry.Index, "term", entry.Term)
		return nil
	}

	if !healthy {
		h.logger.Warn("consensus marked peer unhealthy",
			"peer", nodeID, "index", entry.Index, "term", entry.Term)
		return nil
	}

	h.logger.Info("consensus marked peer healthy again",
		"peer", nodeID, "index", entry.Index, "term", entry.Term)
	select {
	case h.recovered <- nodeID:
	default:
		h.logger.Warn("consensus recovery notification dropped (queue full); "+
			"catch-up will still run from the retry loop", "peer", nodeID)
	}
	return nil
}

// SnapshotState encodes the committed view as JSON.
//
// It is called from Raft's apply path with no entry in flight, so the map it
// reads under h.mu reflects exactly the entries applied so far — which is what
// raft.StateMachine requires of the payload it pairs with lastApplied.
func (h *HealthStateMachine) SnapshotState(_ context.Context) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	data, err := json.Marshal(h.state)
	if err != nil {
		return nil, fmt.Errorf("health state machine: encode snapshot: %w", err)
	}
	return data, nil
}

// RestoreFromSnapshot replaces the whole view with the payload's.
//
// Replace, not merge: a snapshot is a complete record of the applied entries at
// its index, so a node ID absent from the payload is absent from the state the
// snapshot describes. Merging would keep a stale entry from before the snapshot
// alive forever, with no entry left in the log to correct it.
//
// An empty payload restores an empty view, which is the correct reading of a
// snapshot taken before any transition was committed.
func (h *HealthStateMachine) RestoreFromSnapshot(_ context.Context, data []byte) error {
	restored := make(map[string]bool)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &restored); err != nil {
			return fmt.Errorf("health state machine: decode snapshot: %w", err)
		}
	}
	// A snapshot is not an observation, so it emits nothing on the recovery
	// channel: it describes the state at its index rather than a transition into
	// it, and the anti-entropy engine's retry loop is what covers a replica this
	// view now says is reachable.
	h.mu.Lock()
	h.state = restored
	h.mu.Unlock()

	h.logger.Info("restored committed health view from snapshot", "nodes", len(restored))
	return nil
}

// Healthy reports whether the committed view considers nodeID reachable.
// A node no transition has been committed for reports healthy — see the type
// doc.
func (h *HealthStateMachine) Healthy(nodeID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	healthy, known := h.state[nodeID]
	return !known || healthy
}

// Recovered returns the channel of committed unhealthy → healthy transitions.
// This is the signal that lets a ring-primary which is not the Raft leader
// schedule a replica catch-up on recovery.
func (h *HealthStateMachine) Recovered() <-chan string { return h.recovered }
