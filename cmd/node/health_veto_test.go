package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// The healthy-direction veto, from both sides.
//
// With the transport probe removed, health reaches a ring-primary that is not the
// Raft leader through the committed log. That view is the *leader's* vantage,
// which is the right default and the wrong answer in exactly one case: an
// asymmetric partition, where the leader cannot reach a peer that this
// ring-primary reaches perfectly well — and this ring-primary is the node that
// owes that peer its data.
//
// So a replication success this node performed itself overrides a committed
// "down", for repair scheduling only. The two tests below pin the two halves that
// have to hold together:
//
//   - it fires (TestReplicationSuccessVetoesACommittedDown), or the asymmetric
//     partition costs convergence that the local transport could have delivered;
//   - it operates in the healthy direction only
//     (TestVetoNeverOperatesInTheUnhealthyDirection), or a committed "up" masks a
//     local failure and the engine sends a pass down a transport it already knows
//     is broken — or worse, stops marking the replica behind.
//
// Neither test stands up a Raft cluster, so neither is exposed to the mid-test
// re-election that three earlier tests in this package had to be taught to
// tolerate: the committed view is driven by applying entries to a
// HealthStateMachine directly.

// vetoFixture is an anti-entropy engine wired to a local tracker and a committed
// health view the test drives by hand, over a store with a WAL worth shipping.
type vetoFixture struct {
	ae     *antiEntropy
	local  *cluster.PeerHealth
	sm     *HealthStateMachine
	m      *metrics.Metrics
	peerID string
}

func newVetoFixture(t *testing.T, cfg antiEntropyConfig) *vetoFixture {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()

	st, err := store.New(dir, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// Enough writes that whichever keys this node is ring-primary for, a pass has
	// something to read.
	for i := 0; i < 20; i++ {
		if _, err := st.Put(context.Background(), fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}

	cursors, err := store.OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}

	ring := cluster.New()
	ring.AddNode("node1", "node1:9000")
	ring.AddNode("node2", "node2:9000")

	local := cluster.NewPeerHealth([]string{"node2"}, cluster.HealthConfig{
		StableChecks: 3,
		Logger:       logger,
	})
	m := &metrics.Metrics{}
	sm := newHealthStateMachine("node1", 1, m, logger)

	ae := newAntiEntropy(
		"node1", 2, []string{"node2"},
		st, cursors, ring,
		map[string]kvpb.KVServiceClient{"node2": &fakePeer{nodeID: "node2"}},
		local, sm,
		func() uint64 { return 1 },
		m, logger, cfg,
	)

	if ae.behindReplicas() != nil {
		t.Fatal("precondition: nothing may be behind at startup, or the startup " +
			"sweep would schedule a pass this test attributes to the retry loop")
	}
	return &vetoFixture{ae: ae, local: local, sm: sm, m: m, peerID: "node2"}
}

// markBehindDirectly puts the replica in the "behind" state without touching the
// health tracker.
//
// Going through NoteReplicationFailure would be more natural and would defeat the
// test: a failure demotes the peer locally, so restoring it takes StableChecks
// successes, and that unhealthy → healthy transition enqueues a pass through the
// local recovery channel — a path that does not consult reachable() at all. The
// pass would then run whether or not the veto works.
func (f *vetoFixture) markBehindDirectly(t *testing.T) {
	t.Helper()
	f.ae.mu.Lock()
	defer f.ae.mu.Unlock()
	st := f.ae.replica[f.peerID]
	if st == nil {
		t.Fatalf("no replica state for %s", f.peerID)
	}
	st.behind = true
}

// assertNoRecoveryPending fails if either recovery channel is holding a
// transition, because either would enqueue a pass without consulting reachable().
func (f *vetoFixture) assertNoRecoveryPending(t *testing.T) {
	t.Helper()
	select {
	case id := <-f.local.Recovered():
		t.Fatalf("a local recovery for %q is pending; it would schedule a pass "+
			"without consulting the reachability gate this test is about", id)
	default:
	}
	select {
	case id := <-f.sm.Recovered():
		t.Fatalf("a committed recovery for %q is pending; it would schedule a pass "+
			"without consulting the reachability gate this test is about", id)
	default:
	}
}

// TestReplicationSuccessVetoesACommittedDown is the veto firing.
//
// Consensus says the peer is down — the leader's honest view. This node has
// replicated to that peer successfully, which is positive evidence about the one
// transport a catch-up pass would actually use. Repair scheduling must then treat
// the peer as reachable and run the pass.
//
// Every other way a pass could be scheduled is closed: the startup sweep found
// nothing behind (checked in the fixture), neither recovery channel holds anything
// (checked below), and the local tracker is never demoted so it cannot produce a
// transition. The retry ticker consulting reachable() is the only path left.
//
// Revert-check: make the committed "down" unconditional again —
//
//	if ae.consensus != nil && !ae.consensus.Healthy(nodeID) { return false }
//
// — and both the reachable() assertion and the pass assertion fail.
func TestReplicationSuccessVetoesACommittedDown(t *testing.T) {
	f := newVetoFixture(t, antiEntropyConfig{
		RetryInterval: 20 * time.Millisecond,
		SettleDelay:   time.Millisecond,
	})

	// The committed view: the leader cannot reach node2 and has said so.
	if err := f.sm.Apply(context.Background(), healthEntry(1, opHealthDown, f.peerID)); err != nil {
		t.Fatalf("apply health-down: %v", err)
	}
	if f.sm.Healthy(f.peerID) {
		t.Fatal("precondition: the committed view should report node2 unhealthy")
	}

	// No local evidence yet. The peer still reports Healthy locally — it has never
	// been seen to fail — and that must not be enough to override the cluster.
	if !f.local.Healthy(f.peerID) {
		t.Fatal("precondition: the local tracker should still report node2 healthy")
	}
	if f.ae.reachable(f.peerID) {
		t.Fatal("a committed health-down was overridden with no positive local " +
			"evidence: 'this node has never seen the peer fail' is not 'this node " +
			"has reached the peer'")
	}

	// The asymmetric partition: this node reaches the peer even though the leader
	// cannot. One success, deliberately — fewer than StableChecks, so the local
	// tracker records no transition and the recovery channel stays empty.
	f.local.ObserveReplication(f.peerID, true)

	if !f.ae.reachable(f.peerID) {
		t.Fatal("a replication success this node performed did not override the " +
			"committed health-down; an asymmetric partition would cost convergence " +
			"the local transport could have delivered")
	}

	// And the scheduling consequence, which is what the veto is for.
	f.markBehindDirectly(t)
	f.assertNoRecoveryPending(t)
	passesBefore := f.m.AntiEntropyPasses.Load()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.ae.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.m.AntiEntropyPasses.Load() > passesBefore {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no catch-up pass ran for a replica the committed view calls down and "+
		"this node has replicated to successfully: passes still %d. The retry "+
		"ticker is the only enabled scheduler, so the veto is not reaching it",
		passesBefore)
}

// TestVetoNeverOperatesInTheUnhealthyDirection is the other half, and the one that
// protects correctness rather than convergence speed.
//
// Consensus says the peer is up — committed, explicit, not merely absent. This
// node's own replication to it just failed. Two things must hold:
//
//  1. the replica is still marked behind. The behind-marking is what makes the
//     write recoverable at all, and nothing about health may suppress it.
//  2. the committed "up" does not make the peer reachable. A pass down a transport
//     this node already knows is broken burns the cycle and ships nothing.
//
// And repair must not be *delayed* either: once the local transport recovers, the
// pass runs. That is asserted last, so a fix that satisfied (1) and (2) by
// wedging the engine would still fail.
//
// Revert-check: put the committed view first and let it short-circuit —
//
//	if ae.consensus != nil && ae.consensus.Healthy(nodeID) { return true }
//
// — and the reachability assertion fails while the local transport is broken.
func TestVetoNeverOperatesInTheUnhealthyDirection(t *testing.T) {
	f := newVetoFixture(t, antiEntropyConfig{
		RetryInterval: 20 * time.Millisecond,
		SettleDelay:   time.Millisecond,
	})

	// Drive the committed view to an explicit "up" rather than relying on absence,
	// so the test really is about a committed health-up.
	ctxb := context.Background()
	if err := f.sm.Apply(ctxb, healthEntry(1, opHealthDown, f.peerID)); err != nil {
		t.Fatalf("apply health-down: %v", err)
	}
	if err := f.sm.Apply(ctxb, healthEntry(2, opHealthUp, f.peerID)); err != nil {
		t.Fatalf("apply health-up: %v", err)
	}
	if !f.sm.Healthy(f.peerID) {
		t.Fatal("precondition: the committed view should report node2 healthy")
	}
	// That up transition queued a committed recovery. Drain it: this test is about
	// the gate, and a pending recovery would schedule a pass without consulting it.
	select {
	case <-f.sm.Recovered():
	default:
		t.Fatal("precondition: the committed health-up should have queued a recovery")
	}

	// This node's own replication to the peer fails.
	f.ae.NoteReplicationFailure(f.peerID)

	// (1) The behind-marking survives. This is the assertion that would catch a
	// veto implemented anywhere near the failure path.
	behind := f.ae.behindReplicas()
	if len(behind) != 1 || behind[0] != f.peerID {
		t.Fatalf("behindReplicas() = %v, want [%s]: a local replication failure must "+
			"mark the replica behind whatever the committed view says", behind, f.peerID)
	}

	// (2) The committed "up" does not mask the local failure.
	if f.local.Healthy(f.peerID) {
		t.Fatal("precondition: one local replication failure should demote the peer")
	}
	if f.ae.reachable(f.peerID) {
		t.Fatal("a committed health-up masked this node's own replication failure; " +
			"the veto must operate in the healthy direction only")
	}

	// (3) Repair is not delayed: when the local transport comes back, the pass runs.
	passesBefore := f.m.AntiEntropyPasses.Load()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.ae.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	for i := 0; i < 3; i++ { // StableChecks
		f.local.ObserveReplication(f.peerID, true)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.m.AntiEntropyPasses.Load() > passesBefore {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no catch-up pass ran after the local transport recovered: passes still "+
		"%d. Marking a replica behind and then never repairing it is worse than not "+
		"marking it", passesBefore)
}
