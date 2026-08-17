package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func quietHealth(peers []string, cfg HealthConfig) *PeerHealth {
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewPeerHealth(peers, cfg)
}

// TestPeerStartsHealthyAndIsDemotedByAFailure covers the initial state. Starting
// peers unhealthy would make every node fire a catch-up pass for every replica the
// first time it looked — the thundering herd the stable-health gate exists to
// avoid.
func TestPeerStartsHealthyAndIsDemotedByAFailure(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{Probe: func(string) bool { return true }})

	if !ph.Healthy("node2") {
		t.Error("a freshly tracked peer should start healthy")
	}
	if !ph.Healthy("node-unknown") {
		t.Error("an untracked node should not be reported as down; no opinion is not 'unreachable'")
	}

	ph.ObserveReplication("node2", false)
	if ph.Healthy("node2") {
		t.Error("a peer that failed a replication is still reported healthy")
	}
	select {
	case id := <-ph.Recovered():
		t.Fatalf("a failure produced a recovery notification for %q", id)
	default:
	}
}

// TestRecoveryRequiresStableHealth is the gate the design calls for: a node that
// has just restarted accepts connections before it is useful, so one lucky probe
// must not trigger a catch-up.
func TestRecoveryRequiresStableHealth(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{
		StableChecks: 3,
		Probe:        func(string) bool { return true },
	})

	ph.ObserveReplication("node2", false)

	for i := 1; i <= 2; i++ {
		ph.ObserveHeartbeat("node2", true)
		if ph.Healthy("node2") {
			t.Fatalf("peer declared healthy after %d of 3 required observations", i)
		}
		select {
		case <-ph.Recovered():
			t.Fatalf("recovery notified after %d of 3 required observations", i)
		default:
		}
	}

	ph.ObserveHeartbeat("node2", true)
	if !ph.Healthy("node2") {
		t.Fatal("peer not healthy after the third consecutive good observation")
	}
	select {
	case id := <-ph.Recovered():
		if id != "node2" {
			t.Errorf("recovery notified for %q, want node2", id)
		}
	default:
		t.Fatal("no recovery notification after the stable-health gate was met")
	}
}

// TestFlappingPeerNeverGraduates pins the other half of the gate: a peer that
// alternates must not accumulate credit towards recovery.
func TestFlappingPeerNeverGraduates(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{
		StableChecks: 3,
		Probe:        func(string) bool { return true },
	})

	ph.ObserveReplication("node2", false)
	for i := 0; i < 10; i++ {
		ph.ObserveHeartbeat("node2", true)
		ph.ObserveHeartbeat("node2", true)
		ph.ObserveHeartbeat("node2", false)
	}
	if ph.Healthy("node2") {
		t.Error("a flapping peer was declared healthy")
	}
	select {
	case id := <-ph.Recovered():
		t.Fatalf("a flapping peer produced a recovery notification for %q", id)
	default:
	}
}

// TestHealthyPeerDoesNotRe-notify guards against a stream of transitions for a
// peer that is simply working: only a recovery from unreachable is an event.
func TestHealthyPeerDoesNotRenotify(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{Probe: func(string) bool { return true }})
	for i := 0; i < 20; i++ {
		ph.ObserveReplication("node2", true)
	}
	select {
	case id := <-ph.Recovered():
		t.Fatalf("a peer that never failed produced a recovery notification for %q", id)
	default:
	}
}

// TestProbeLoopDetectsRecoveryWithoutAnyTraffic is why the probe exists at all.
// During a fault, writes to the replica are refused, so the write path produces no
// successes to count — and only the Raft leader sends heartbeats. A ring-primary
// that is not the leader would otherwise never learn that its replica came back.
func TestProbeLoopDetectsRecoveryWithoutAnyTraffic(t *testing.T) {
	var reachable atomic.Bool
	ph := quietHealth([]string{"node2"}, HealthConfig{
		Interval:     time.Millisecond,
		StableChecks: 3,
		Probe:        func(string) bool { return reachable.Load() },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ph.Run(ctx)

	// The probe reports the peer down; it must be demoted with no other signal.
	deadline := time.Now().Add(2 * time.Second)
	for ph.Healthy("node2") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ph.Healthy("node2") {
		t.Fatal("the probe loop never demoted an unreachable peer")
	}

	reachable.Store(true)
	select {
	case id := <-ph.Recovered():
		if id != "node2" {
			t.Errorf("recovered %q, want node2", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the probe loop never reported the peer's recovery")
	}
}

// TestRunWithoutAProbeDoesNotSpin covers the degraded configuration: no probe
// means recovery is detected only from traffic, and Run must simply wait rather
// than busy-looping on a nil function.
func TestRunWithoutAProbeDoesNotSpin(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{Interval: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { ph.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return when its context was cancelled")
	}
}

func TestPeersIsSorted(t *testing.T) {
	ph := quietHealth([]string{"node3", "node1", "node2"}, HealthConfig{Probe: func(string) bool { return true }})
	got := ph.Peers()
	if len(got) != 3 || got[0] != "node1" || got[1] != "node2" || got[2] != "node3" {
		t.Errorf("Peers() = %v, want sorted", got)
	}
}
