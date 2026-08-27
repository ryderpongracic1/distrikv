package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// healthAggregator tests.
//
// These drive ObserveHeartbeat and inspect the queue directly rather than running
// Run() and waiting, so each assertion is about a specific decision instead of a
// race against a goroutine. That split is also the point of the design: the
// observation path is pure in-memory bookkeeping, and the Raft append happens
// somewhere else entirely.

// fakeProposer stands in for raft.RaftNode. It records what was proposed and can
// be made to fail, which is the only way to exercise the revert path — a real
// Propose fails on step-down or a disk error, neither of which a test can arrange
// reliably.
type fakeProposer struct {
	mu        sync.Mutex
	leader    bool
	term      uint64
	err       error
	proposed  []healthProposal
	callCount int
}

func (f *fakeProposer) Propose(_ context.Context, op, key string, _ []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	if f.err != nil {
		return 0, f.err
	}
	f.proposed = append(f.proposed, healthProposal{op: op, nodeID: key})
	return uint64(len(f.proposed)), nil
}

func (f *fakeProposer) Leadership() (bool, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader, f.term
}

func (f *fakeProposer) setLeader(leader bool, term uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leader, f.term = leader, term
}

func (f *fakeProposer) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeProposer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

func (f *fakeProposer) records() []healthProposal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]healthProposal(nil), f.proposed...)
}

// fakeHealthView is the committed view the aggregator seeds itself from.
type fakeHealthView struct {
	mu        sync.Mutex
	unhealthy map[string]bool
}

func (v *fakeHealthView) Healthy(nodeID string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return !v.unhealthy[nodeID]
}

func (v *fakeHealthView) markUnhealthy(nodeID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.unhealthy == nil {
		v.unhealthy = make(map[string]bool)
	}
	v.unhealthy[nodeID] = true
}

func testAggregator(t *testing.T, cfg healthAggregatorConfig) (*healthAggregator, *fakeProposer, *fakeHealthView, *metrics.Metrics) {
	t.Helper()
	p := &fakeProposer{leader: true, term: 7}
	v := &fakeHealthView{}
	m := &metrics.Metrics{}
	a := newHealthAggregator(p, v, 2, m, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	return a, p, v, m
}

// queued drains and returns everything the aggregator has handed to Run.
func queued(a *healthAggregator) []healthProposal {
	var out []healthProposal
	for {
		select {
		case p := <-a.proposals:
			out = append(out, p)
		default:
			return out
		}
	}
}

// observe feeds n heartbeat outcomes of the same kind for one peer.
func observe(a *healthAggregator, nodeID string, ok bool, n int) {
	for i := 0; i < n; i++ {
		a.ObserveHeartbeat(nodeID, ok)
	}
}

// TestAggregatorProposesDownAfterThreshold asserts the down hysteresis: nothing is
// proposed until DownAfter consecutive failures, and then exactly one entry.
func TestAggregatorProposesDownAfterThreshold(t *testing.T) {
	a, _, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 2)
	if got := queued(a); len(got) != 0 {
		t.Fatalf("proposed %v after 2 of 3 failures; the threshold exists so one "+
			"slow tick does not write a Raft entry", got)
	}

	a.ObserveHeartbeat("node2", false)
	got := queued(a)
	if len(got) != 1 || got[0].op != opHealthDown || got[0].nodeID != "node2" {
		t.Fatalf("after the third failure = %v, want one health-down for node2", got)
	}
}

// TestAggregatorSuccessResetsFailureRun asserts the counters require *consecutive*
// failures. A peer that fails, answers, then fails again is flapping, not down.
func TestAggregatorSuccessResetsFailureRun(t *testing.T) {
	a, _, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 2)
	a.ObserveHeartbeat("node2", true) // breaks the run
	observe(a, "node2", false, 2)

	if got := queued(a); len(got) != 0 {
		t.Errorf("proposed %v: a success between failures must reset the run, so "+
			"2+2 failures is not 3 consecutive ones", got)
	}
}

// TestAggregatorProposesUpAfterThreshold covers the recovery direction, including
// that a success run shorter than UpAfter proposes nothing.
func TestAggregatorProposesUpAfterThreshold(t *testing.T) {
	a, _, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 3)
	if got := queued(a); len(got) != 1 {
		t.Fatalf("setup: want the down transition queued, got %v", got)
	}

	a.ObserveHeartbeat("node2", true)
	if got := queued(a); len(got) != 0 {
		t.Fatalf("proposed %v after 1 of 2 successes", got)
	}

	a.ObserveHeartbeat("node2", true)
	got := queued(a)
	if len(got) != 1 || got[0].op != opHealthUp || got[0].nodeID != "node2" {
		t.Fatalf("after the second success = %v, want one health-up for node2", got)
	}
}

// TestAggregatorDoesNotDoublePropose is the log-volume pin. The condition holding
// for twenty ticks is one entry, not twenty: the Raft log must carry one entry per
// genuine change in reachability, which is the assumption the full-rewrite
// persistence format is built on.
//
// Revert-check: drop the `st.proposedHealthy` guard from either branch of
// ObserveHeartbeat and this fails with 18 or 19 proposals.
func TestAggregatorDoesNotDoublePropose(t *testing.T) {
	a, _, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2, QueueDepth: 64})

	observe(a, "node2", false, 20)
	if got := queued(a); len(got) != 1 {
		t.Errorf("20 consecutive failures proposed %d entries, want 1", len(got))
	}

	observe(a, "node2", true, 20)
	if got := queued(a); len(got) != 1 {
		t.Errorf("20 consecutive successes proposed %d entries, want 1", len(got))
	}
}

// TestAggregatorNeverProposesInline is the defect-6 pin, and the reason the
// aggregator has a goroutine at all.
//
// ObserveHeartbeat runs on the per-peer heartbeat goroutine, and raft.Propose
// fsyncs the log before it returns. A disk write on that path is exactly the shape
// of the defect that cost this cluster three months of election storm, so the
// observation path must reach the threshold, decide, and return without ever
// touching Raft's proposal API.
func TestAggregatorNeverProposesInline(t *testing.T) {
	a, p, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	// Run is deliberately not started, so nothing can drain the queue.
	observe(a, "node2", false, 10)
	observe(a, "node2", true, 10)

	if got := p.calls(); got != 0 {
		t.Errorf("Propose called %d times from the heartbeat path, want 0: Propose "+
			"fsyncs, and the heartbeat path is what every follower's election "+
			"timer depends on", got)
	}
	if got := queued(a); len(got) == 0 {
		t.Error("the transitions must still be queued for the aggregator's own goroutine")
	}
}

// TestAggregatorIgnoresObservationsWhenNotLeader asserts the aggregator is
// leader-only. A follower sends no heartbeats, so any outcome reaching it is stale
// and must not become a proposal.
func TestAggregatorIgnoresObservationsWhenNotLeader(t *testing.T) {
	a, p, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})
	p.setLeader(false, 7)

	observe(a, "node2", false, 10)
	if got := queued(a); len(got) != 0 {
		t.Errorf("a non-leader proposed %v", got)
	}
}

// TestAggregatorStepDownResetsState asserts the accumulated counters are dropped
// on step-down: they describe RPCs this node is no longer sending, and the next
// leader's own observations are what should decide.
//
// Revert-check: remove the reset from the !leading branch and the post-step-down
// failure alone completes the old run of two, proposing immediately.
func TestAggregatorStepDownResetsState(t *testing.T) {
	a, p, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 2) // two thirds of the way to a down proposal

	p.setLeader(false, 7)
	a.ObserveHeartbeat("node2", false) // observed while a follower: dropped
	p.setLeader(true, 8)               // re-elected in a later term

	a.ObserveHeartbeat("node2", false)
	if got := queued(a); len(got) != 0 {
		t.Fatalf("proposed %v after one failure in a new leadership epoch; the "+
			"pre-step-down counters must not carry over", got)
	}

	observe(a, "node2", false, 2)
	if got := queued(a); len(got) != 1 {
		t.Errorf("after three failures as leader again = %d proposals, want 1", len(got))
	}
}

// TestAggregatorTermChangeResetsState covers the same reset through the other
// trigger: leadership regained in a new term without this node ever being observed
// as a follower in between.
func TestAggregatorTermChangeResetsState(t *testing.T) {
	a, p, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 2)
	p.setLeader(true, 9) // same node, new term

	a.ObserveHeartbeat("node2", false)
	if got := queued(a); len(got) != 0 {
		t.Errorf("proposed %v: a new term is a new set of observations", got)
	}
}

// TestAggregatorSeedsFromCommittedView is the subtle one, and it is a correctness
// bug rather than a tidiness point.
//
// A newly elected leader inherits a committed view its predecessor wrote. If it
// assumed every peer was healthy, then for a peer the predecessor marked *down*
// that is now back it would observe nothing but successes, conclude there is no
// change to propose, and leave the stale "down" entry standing — with nobody left
// to correct it, and every ring-primary's retry loop gated against a peer that is
// fine. Seeding from the committed view makes the new leader propose the
// correction on its first UpAfter successes.
//
// Revert-check: seed proposedHealthy to a literal true and this fails with zero
// proposals.
func TestAggregatorSeedsFromCommittedView(t *testing.T) {
	a, _, view, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})
	view.markUnhealthy("node2") // the previous leader committed "node2 is down"

	observe(a, "node2", true, 2)
	got := queued(a)
	if len(got) != 1 || got[0].op != opHealthUp {
		t.Fatalf("a new leader observing a peer the committed view calls down "+
			"proposed %v, want one health-up: nothing else will correct that entry", got)
	}
}

// TestAggregatorRevertsOnNotLeader asserts a transition lost to a step-down race
// leaves the hysteresis able to fire again, rather than believing it already
// proposed.
func TestAggregatorRevertsOnNotLeader(t *testing.T) {
	a, p, _, m := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 3)
	got := queued(a)
	if len(got) != 1 {
		t.Fatalf("setup: want one queued transition, got %v", got)
	}

	p.setErr(raft.ErrNotLeader)
	a.propose(context.Background(), got[0])

	if n := m.HealthTransitionsProposed.Load(); n != 0 {
		t.Errorf("health_transitions_proposed = %d after a rejected append, want 0", n)
	}

	// The condition still holds, so the next observation must re-propose.
	p.setErr(nil)
	a.ObserveHeartbeat("node2", false)
	if again := queued(a); len(again) != 1 || again[0].op != opHealthDown {
		t.Errorf("re-queued %v after a dropped transition, want one health-down: a "+
			"failed append must not be remembered as a successful one", again)
	}
}

// TestAggregatorRevertsOnPersistError is the same recovery for the other failure
// mode — a disk error rather than a lost election.
func TestAggregatorRevertsOnPersistError(t *testing.T) {
	a, p, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})

	observe(a, "node2", false, 3)
	got := queued(a)
	if len(got) != 1 {
		t.Fatalf("setup: want one queued transition, got %v", got)
	}

	p.setErr(errors.New("raft: persist proposed entry: no space left on device"))
	a.propose(context.Background(), got[0])
	p.setErr(nil)

	a.ObserveHeartbeat("node2", false)
	if again := queued(a); len(again) != 1 {
		t.Errorf("re-queued %d transitions after a persist failure, want 1", len(again))
	}
}

// TestAggregatorRevertsOnFullQueue covers the third way a transition can be lost.
// The queue only has to absorb one tick's worth of transitions, so filling it means
// the proposer goroutine is wedged — and the recovery is the same: re-propose while
// the condition holds rather than record a transition that never happened.
func TestAggregatorRevertsOnFullQueue(t *testing.T) {
	a, _, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 1, UpAfter: 1, QueueDepth: 1})

	a.ObserveHeartbeat("node2", false) // fills the queue of depth 1
	a.ObserveHeartbeat("node3", false) // dropped: queue full

	// node3's transition was reverted, so a later observation proposes it again
	// once there is room.
	if got := queued(a); len(got) != 1 || got[0].nodeID != "node2" {
		t.Fatalf("queued = %v, want only node2's transition", got)
	}
	a.ObserveHeartbeat("node3", false)
	if got := queued(a); len(got) != 1 || got[0].nodeID != "node3" {
		t.Errorf("queued = %v after room freed, want node3's transition re-proposed", got)
	}
}

// TestAggregatorProposeSucceedsAndCounts asserts the happy path reaches Raft with
// the op and key the state machine reads, and that the proposal counter moves.
func TestAggregatorProposeSucceedsAndCounts(t *testing.T) {
	a, p, _, m := testAggregator(t, healthAggregatorConfig{DownAfter: 1, UpAfter: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()

	a.ObserveHeartbeat("node2", false)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && m.HealthTransitionsProposed.Load() == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if n := m.HealthTransitionsProposed.Load(); n != 1 {
		t.Fatalf("health_transitions_proposed = %d, want 1", n)
	}
	rec := p.records()
	if len(rec) != 1 || rec[0].op != opHealthDown || rec[0].nodeID != "node2" {
		t.Errorf("proposed %v, want one health-down keyed on node2", rec)
	}
}

// TestAggregatorTracksPeersIndependently asserts one peer's run cannot advance
// another's — the counters are per peer, not per aggregator.
func TestAggregatorTracksPeersIndependently(t *testing.T) {
	a, _, _, _ := testAggregator(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2, QueueDepth: 16})

	a.ObserveHeartbeat("node2", false)
	a.ObserveHeartbeat("node3", false)
	a.ObserveHeartbeat("node2", false)
	a.ObserveHeartbeat("node3", false)
	if got := queued(a); len(got) != 0 {
		t.Fatalf("proposed %v after two failures each", got)
	}

	observe(a, "node2", false, 1)
	got := queued(a)
	if len(got) != 1 || got[0].nodeID != "node2" {
		t.Errorf("queued = %v, want only node2 at its threshold", got)
	}
}

// TestMultiObserverFansOutToEveryObserver pins the additive requirement at the
// registration seam: raft holds one observer slot, and the consensus aggregator is
// added *beside* the local tracker, never in place of it. The local signals are
// what cover a leaderless window, when no health entry can be committed at all.
func TestMultiObserverFansOutToEveryObserver(t *testing.T) {
	var first, second []bool
	obs := multiPeerHealthObserver{
		observerFunc(func(_ string, ok bool) { first = append(first, ok) }),
		nil, // a nil member must be skipped, not panic
		observerFunc(func(_ string, ok bool) { second = append(second, ok) }),
	}

	obs.ObserveHeartbeat("node2", false)
	obs.ObserveHeartbeat("node2", true)

	want := []bool{false, true}
	for name, got := range map[string][]bool{"first": first, "second": second} {
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("%s observer saw %v, want %v: every registered observer must "+
				"receive every outcome", name, got, want)
		}
	}
}

// observerFunc adapts a function to raft.PeerHealthObserver.
type observerFunc func(nodeID string, ok bool)

func (f observerFunc) ObserveHeartbeat(nodeID string, ok bool) { f(nodeID, ok) }
