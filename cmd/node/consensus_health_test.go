package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// The discriminating test for consensus-backed node health.
//
// Everything else in this feature can pass while the design premise stays false.
// The premise is that a ring-primary which is *not* the Raft leader gets a health
// view — and before this change it did not: only the leader sends heartbeats, so
// on a 3-node cluster two of three ring-primaries learned nothing from Raft and
// cluster.PeerHealth had to merge two local signals to cover them.
//
// So this test removes every local signal and leaves consensus as the only way
// health can travel:
//
//   - the transport probe is disabled (its ticker is never started and its
//     interval is set beyond the life of the test),
//   - no replication RPCs are issued, so there are no replication outcomes,
//   - the observed node is a follower, so its own heartbeat observations are
//     empty by construction — followers send no heartbeats.
//
// With those gone, a follower that still learns a peer went down, learns it came
// back, and schedules a catch-up pass for it can only have learned it from the
// committed Raft log.
//
// Revert-check: pass nil for the consensus argument of newAntiEntropy (or hand
// raft a state machine that ignores entries) and the health assertions and the
// scheduled-pass assertion all fail.

// ---------------------------------------------------------------------------
// In-process Raft cluster harness
// ---------------------------------------------------------------------------

// raftLink is a kvpb.KVServiceClient that dispatches straight into another
// RaftNode's handlers, and refuses to carry anything while either end is
// isolated.
//
// Isolation is bidirectional on purpose. Severing only the inbound direction
// would leave the "killed" node able to campaign against a cluster it cannot
// hear, which is a partition scenario rather than the node failure this test is
// about.
type raftLink struct {
	kvpb.KVServiceClient // nil: raft only calls the four RPCs below

	harness *raftHarness
	from    string
	to      string
	target  *raft.RaftNode
}

// errPeerDown stands in for a refused connection.
var errPeerDown = fmt.Errorf("in-process link: connection refused")

func (l *raftLink) up() error {
	if l.harness.isolated(l.from) || l.harness.isolated(l.to) {
		return errPeerDown
	}
	return nil
}

func (l *raftLink) PreVote(ctx context.Context, req *kvpb.PreVoteRequest, _ ...grpc.CallOption) (*kvpb.PreVoteResponse, error) {
	if err := l.up(); err != nil {
		return nil, err
	}
	return l.target.HandlePreVote(ctx, req)
}

func (l *raftLink) RequestVote(ctx context.Context, req *kvpb.RequestVoteRequest, _ ...grpc.CallOption) (*kvpb.RequestVoteResponse, error) {
	if err := l.up(); err != nil {
		return nil, err
	}
	return l.target.HandleRequestVote(ctx, req)
}

func (l *raftLink) AppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	if err := l.up(); err != nil {
		return nil, err
	}
	return l.target.HandleAppendEntries(ctx, req)
}

func (l *raftLink) InstallSnapshot(ctx context.Context, req *kvpb.InstallSnapshotRequest, _ ...grpc.CallOption) (*kvpb.InstallSnapshotResponse, error) {
	if err := l.up(); err != nil {
		return nil, err
	}
	return l.target.HandleInstallSnapshot(ctx, req)
}

// healthNode is one member of the harness: a Raft node with the real health state
// machine and the real leader-side aggregator wired exactly as cmd/node wires
// them in production.
type healthNode struct {
	id      string
	raft    *raft.RaftNode
	sm      *HealthStateMachine
	agg     *healthAggregator
	metrics *metrics.Metrics
}

type raftHarness struct {
	t     *testing.T
	nodes map[string]*healthNode
	ids   []string

	mu   sync.Mutex
	down map[string]bool
}

func (h *raftHarness) isolated(nodeID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.down[nodeID]
}

// isolate severs every link into and out of a node, which is what a stopped
// container looks like to its peers.
func (h *raftHarness) isolate(nodeID string) {
	h.mu.Lock()
	h.down[nodeID] = true
	h.mu.Unlock()
}

func (h *raftHarness) heal(nodeID string) {
	h.mu.Lock()
	h.down[nodeID] = false
	h.mu.Unlock()
}

// newRaftHarness stands up a 3-node in-process cluster and starts it.
//
// The nodes are built the way NewNode builds one — real HealthStateMachine, real
// healthAggregator registered through multiPeerHealthObserver, aggregator
// goroutine running — with one deliberate difference: no cluster.PeerHealth is
// registered as an observer, because this test's whole purpose is to leave
// consensus as the only path health can take.
func newRaftHarness(t *testing.T, hysteresis healthAggregatorConfig) *raftHarness {
	t.Helper()

	// Discard logs: an unstable cluster emits thousands of election lines, and a
	// handler bound to the test would outlive it via the RPC goroutines still
	// draining after cancellation.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	h := &raftHarness{
		t:     t,
		nodes: make(map[string]*healthNode, 3),
		ids:   []string{"node1", "node2", "node3"},
		down:  make(map[string]bool, 3),
	}

	// Timing chosen so a health transition resolves in well under a second while
	// leaving the election timeout a comfortable multiple of the heartbeat
	// interval — the margin whose absence caused the election storm.
	timing := raft.Config{
		ElectionTimeoutMin: 200 * time.Millisecond,
		ElectionTimeoutMax: 400 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		SnapshotThreshold:  1000,
	}

	// Links are created before the Raft nodes because raft.New takes the peer set;
	// their targets are filled in once every node exists, before anything starts.
	links := make(map[string][]*raftLink, 3)
	for _, id := range h.ids {
		for _, other := range h.ids {
			if other == id {
				continue
			}
			links[id] = append(links[id], &raftLink{harness: h, from: id, to: other})
		}
	}

	for _, id := range h.ids {
		cfg := timing
		cfg.NodeID = id
		cfg.DataDir = t.TempDir()

		m := &metrics.Metrics{}
		sm := newHealthStateMachine(id, len(h.ids)-1, m, logger)

		peers := make([]raft.PeerClient, 0, len(links[id]))
		for _, l := range links[id] {
			peers = append(peers, raft.PeerClient{NodeID: l.to, Client: l})
		}

		rn, err := raft.New(cfg, peers, sm, &metricsAdapter{m}, logger)
		if err != nil {
			t.Fatalf("init raft %s: %v", id, err)
		}

		agg := newHealthAggregator(rn, sm, len(h.ids)-1, m, logger, hysteresis)
		// Exactly the production registration, minus the local tracker.
		rn.SetPeerHealthObserver(multiPeerHealthObserver{agg})

		h.nodes[id] = &healthNode{id: id, raft: rn, sm: sm, agg: agg, metrics: m}
	}

	// Resolve link targets now that every node exists. Nothing is running yet, so
	// the writes are ordered before the reads by the goroutine starts below.
	for _, ls := range links {
		for _, l := range ls {
			l.target = h.nodes[l.to].raft
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, n := range h.nodes {
		n := n
		wg.Add(2)
		go func() { defer wg.Done(); n.raft.Run(ctx) }()
		go func() { defer wg.Done(); n.agg.Run(ctx) }()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	return h
}

// leader blocks until exactly one node claims leadership and every node agrees on
// it, then returns the leader and the two followers.
func (h *raftHarness) leader(timeout time.Duration) (leader *healthNode, followers []*healthNode) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*healthNode
		for _, id := range h.ids {
			if h.nodes[id].raft.IsLeader() {
				leaders = append(leaders, h.nodes[id])
			}
		}
		if len(leaders) == 1 {
			leader = leaders[0]
			for _, id := range h.ids {
				if id != leader.id {
					followers = append(followers, h.nodes[id])
				}
			}
			return leader, followers
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("no single leader within %s", timeout)
	return nil, nil
}

// awaitHealth waits for a node's committed view of peerID to reach want.
func awaitHealth(t *testing.T, n *healthNode, peerID string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.sm.Healthy(peerID) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s still reports Healthy(%s)=%v after %s, want %v",
		n.id, peerID, n.sm.Healthy(peerID), timeout, want)
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

// TestConsensusHealthReachesANonLeader is the feature's reason for existing.
//
// A follower — which sends no heartbeats and so has no direct observation of
// anything — must learn that a peer failed and that it recovered, purely from the
// committed Raft log, and must agree with the leader that proposed it.
func TestConsensusHealthReachesANonLeader(t *testing.T) {
	h := newRaftHarness(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	leader, followers := h.leader(5 * time.Second)
	termAtStart := leader.raft.CurrentTerm()

	// The victim must be a follower: isolating the leader would test an election,
	// not a health transition. The observer is the other follower — the node that
	// before this change could learn nothing from Raft.
	victim, observer := followers[0], followers[1]

	if !observer.sm.Healthy(victim.id) {
		t.Fatalf("precondition: %s should start out considering %s healthy", observer.id, victim.id)
	}

	h.isolate(victim.id)

	// The leader observes DownAfter failed heartbeats and proposes; the entry
	// commits; every surviving node applies it.
	awaitHealth(t, leader, victim.id, false, 5*time.Second)
	awaitHealth(t, observer, victim.id, false, 5*time.Second)

	// The load-bearing assertion, stated on its own: the node that learned this is
	// not the leader, and it has no other source for the information.
	if observer.raft.IsLeader() {
		t.Fatal("the observing node must not be the leader — that is the whole point")
	}
	if observer.metrics.HealthTransitionsProposed.Load() != 0 {
		t.Error("the observing node must not have proposed anything: only the leader " +
			"observes heartbeats, so a non-zero count here would mean the test is " +
			"not measuring what it claims")
	}
	if got := observer.metrics.HealthTransitionsCommitted.Load(); got == 0 {
		t.Error("the observing node applied no health entries, so whatever it knows " +
			"did not come from consensus")
	}
	if got := leader.metrics.HealthTransitionsProposed.Load(); got == 0 {
		t.Errorf("the leader proposed %d health transitions, want at least 1", got)
	}

	// Recovery: the same path in the other direction.
	h.heal(victim.id)
	awaitHealth(t, leader, victim.id, true, 5*time.Second)
	awaitHealth(t, observer, victim.id, true, 5*time.Second)

	// The healed node catches up on the log and ends with the same view as
	// everyone else — including about the peer it never knew was reported down.
	awaitHealth(t, victim, leader.id, true, 5*time.Second)

	// Election-storm regression, asserted in the presence of a flowing log rather
	// than on an idle cluster: proposing health transitions must not destabilise
	// Raft. Isolating a follower costs no election — the remaining two nodes are a
	// majority of three — so the term must not have moved at all.
	if got := leader.raft.CurrentTerm(); got != termAtStart {
		t.Errorf("leader term moved %d → %d while health transitions were flowing; "+
			"the aggregator must not cost an election", termAtStart, got)
	}
}

// TestConsensusHealthSchedulesCatchUpOnANonLeader is the second half of the
// premise. Knowing a peer recovered is only useful if it makes the non-leader
// ring-primary *act*: schedule the replica catch-up pass that repairs the writes
// the peer missed while it was down.
//
// Every other trigger is disabled, so a pass that runs can only have been
// scheduled by the consensus recovery signal:
//
//   - the transport probe is never started and its interval is beyond the test,
//   - the local tracker is never fed an observation,
//   - nothing is behind at startup, so the startup sweep enqueues nothing,
//   - RetryInterval is set beyond the life of the test, so the retry ticker cannot
//     fire.
func TestConsensusHealthSchedulesCatchUpOnANonLeader(t *testing.T) {
	h := newRaftHarness(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	leader, followers := h.leader(5 * time.Second)
	victim, observer := followers[0], followers[1]

	if observer.id == leader.id {
		t.Fatal("the observing node must be a follower")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()

	st, err := store.New(dir, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// A few writes so the WAL exists and a pass has a log to read.
	for i := 0; i < 5; i++ {
		if _, err := st.Put(context.Background(), fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}

	cursors, err := store.OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}

	ring := cluster.New()
	for _, id := range h.ids {
		ring.AddNode(id, id+":9000")
	}

	// The transport probe: configured, and disabled. An hour-long interval and a
	// Run() that is never started mean it cannot contribute a single observation.
	localHealth := cluster.NewPeerHealth([]string{victim.id}, cluster.HealthConfig{
		Interval: time.Hour,
		Probe:    func(string) bool { t.Error("the transport probe must not run in this test"); return true },
		Logger:   logger,
	})

	ae := newAntiEntropy(
		observer.id, 2, []string{victim.id},
		st, cursors, ring,
		map[string]kvpb.KVServiceClient{victim.id: &fakePeer{nodeID: victim.id}},
		localHealth,
		observer.sm, // the consensus signal under test
		observer.raft.CurrentTerm,
		observer.metrics, logger,
		antiEntropyConfig{
			RetryInterval: time.Hour, // the retry ticker must not fire
			SettleDelay:   time.Millisecond,
		},
	)

	if ae.behindReplicas() != nil {
		t.Fatalf("precondition: nothing may be behind at startup, or the startup " +
			"sweep would schedule the pass this test attributes to consensus")
	}

	aeCtx, cancelAE := context.WithCancel(context.Background())
	aeDone := make(chan struct{})
	go func() { defer close(aeDone); ae.Run(aeCtx) }()
	t.Cleanup(func() { cancelAE(); <-aeDone })

	// Let the engine reach its select before the transition arrives, so a missed
	// notification is a real failure rather than a startup race. The channel is
	// buffered, so this is belt-and-braces.
	time.Sleep(50 * time.Millisecond)

	passesBefore := observer.metrics.AntiEntropyPasses.Load()

	// Fail the peer, then heal it: only the healthy → unhealthy → healthy
	// round trip produces a recovery notification.
	h.isolate(victim.id)
	awaitHealth(t, observer, victim.id, false, 5*time.Second)
	h.heal(victim.id)
	awaitHealth(t, observer, victim.id, true, 5*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if observer.metrics.AntiEntropyPasses.Load() > passesBefore {
			// A ring-primary that is not the Raft leader scheduled a replica
			// catch-up from consensus alone. That is the capability this feature
			// was missing.
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no catch-up pass ran on %s (a non-leader) after the consensus view "+
		"reported %s recovered: passes still %d. Every other trigger is disabled, "+
		"so the consensus signal is not reaching the anti-entropy scheduler",
		observer.id, victim.id, passesBefore)
}

// TestConsensusHealthGatesTheRetryLoop pins the precedence rule: a committed
// "down" is sufficient on its own to hold the retry loop back, even when every
// local signal says the peer is fine.
//
// It matters because the local signals are weaker evidence than they look. A gRPC
// channel reports Ready or Idle for a peer this node simply has not spoken to
// recently, whereas a committed transition is the leader's own heartbeat outcome
// against that peer, agreed cluster-wide.
func TestConsensusHealthGatesTheRetryLoop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()

	st, err := store.New(dir, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cursors, err := store.OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}

	ring := cluster.New()
	ring.AddNode("node1", "node1:9000")
	ring.AddNode("node2", "node2:9000")

	// Local signals: unanimously healthy.
	localHealth := cluster.NewPeerHealth([]string{"node2"}, cluster.HealthConfig{
		Interval: time.Hour,
		Probe:    func(string) bool { return true },
		Logger:   logger,
	})
	sm := newHealthStateMachine("node1", 1, &metrics.Metrics{}, logger)

	ae := newAntiEntropy(
		"node1", 2, []string{"node2"},
		st, cursors, ring,
		map[string]kvpb.KVServiceClient{"node2": &fakePeer{nodeID: "node2"}},
		localHealth, sm,
		func() uint64 { return 1 },
		&metrics.Metrics{}, logger, antiEntropyConfig{},
	)

	if !ae.reachable("node2") {
		t.Fatal("with every signal healthy, the replica must be considered reachable")
	}

	if err := sm.Apply(context.Background(), healthEntry(1, opHealthDown, "node2")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if ae.reachable("node2") {
		t.Error("a committed health-down must be sufficient to hold the retry loop " +
			"back on its own: consensus is cluster-wide knowledge, a local channel " +
			"state is not")
	}

	if err := sm.Apply(context.Background(), healthEntry(2, opHealthUp, "node2")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !ae.reachable("node2") {
		t.Error("a committed health-up must release the gate again")
	}
}

// TestLocalSignalStillVetoesWithoutConsensus asserts the additive property from
// the other side: wiring the consensus signal in must not weaken the local ones. A
// local failure still gates the retry loop while consensus says nothing at all —
// which is the state of every node during a leaderless window.
func TestLocalSignalStillVetoesWithoutConsensus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()

	st, err := store.New(dir, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cursors, err := store.OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}

	ring := cluster.New()
	ring.AddNode("node1", "node1:9000")
	ring.AddNode("node2", "node2:9000")

	localHealth := cluster.NewPeerHealth([]string{"node2"}, cluster.HealthConfig{
		Interval: time.Hour,
		Probe:    func(string) bool { return true },
		Logger:   logger,
	})
	// Consensus knows nothing about node2 — absent means healthy.
	sm := newHealthStateMachine("node1", 1, &metrics.Metrics{}, logger)

	ae := newAntiEntropy(
		"node1", 2, []string{"node2"},
		st, cursors, ring,
		map[string]kvpb.KVServiceClient{"node2": &fakePeer{nodeID: "node2"}},
		localHealth, sm,
		func() uint64 { return 1 },
		&metrics.Metrics{}, logger, antiEntropyConfig{},
	)

	// One local replication failure is enough to demote a healthy peer.
	localHealth.ObserveReplication("node2", false)
	if ae.reachable("node2") {
		t.Error("a local replication failure must still gate the retry loop while " +
			"consensus has no opinion; the consensus signal is additive, not a " +
			"replacement")
	}
}
