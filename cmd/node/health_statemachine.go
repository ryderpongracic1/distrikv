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

// Apply folds one committed entry into the health view.
//
// It satisfies raft.StateMachine's idempotence requirement by construction: the
// body of a health transition is a map assignment, so applying the same entry
// twice — which happens after every restart, because commitIndex and lastApplied
// are volatile — lands on the same state. The only non-idempotent side effect,
// the recovery notification, is emitted from inside the transition check, so a
// re-applied entry that changes nothing announces nothing.
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
func (h *HealthStateMachine) Apply(_ context.Context, entry raft.LogEntry) error {
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
			"peer", nodeID, "healthy", healthy, "index", entry.Index)
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
