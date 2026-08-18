package cluster

import (
	"io"
	"log/slog"
	"testing"
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
	ph := quietHealth([]string{"node2"}, HealthConfig{})

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
// has just restarted accepts connections before it is useful, so one lucky
// success must not trigger a catch-up.
func TestRecoveryRequiresStableHealth(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{StableChecks: 3})

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
	ph := quietHealth([]string{"node2"}, HealthConfig{StableChecks: 3})

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
	ph := quietHealth([]string{"node2"}, HealthConfig{})
	for i := 0; i < 20; i++ {
		ph.ObserveReplication("node2", true)
	}
	select {
	case id := <-ph.Recovered():
		t.Fatalf("a peer that never failed produced a recovery notification for %q", id)
	default:
	}
}

// TestLastReplicationSucceededIsPositiveEvidenceOnly is the distinction the
// healthy-direction veto rests on. Healthy() conflates "reachable" with "no
// opinion" — a freshly tracked peer and an untracked node both report healthy — so
// it cannot be used to override the cluster's committed view. This must report
// false until this node has actually replicated to the peer and got an answer.
func TestLastReplicationSucceededIsPositiveEvidenceOnly(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{})

	if !ph.Healthy("node2") {
		t.Fatal("precondition: a freshly tracked peer should report healthy")
	}
	if ph.LastReplicationSucceeded("node2") {
		t.Error("a peer no replication has been attempted to reports a success; " +
			"'no opinion' must not read as positive evidence")
	}
	if ph.LastReplicationSucceeded("node-unknown") {
		t.Error("an untracked node reports a replication success")
	}

	// A heartbeat success is not replication evidence: only the leader sends
	// heartbeats, and the veto is about the transport a catch-up pass would use.
	ph.ObserveHeartbeat("node2", true)
	if ph.LastReplicationSucceeded("node2") {
		t.Error("a heartbeat success was recorded as a replication success")
	}

	ph.ObserveReplication("node2", true)
	if !ph.LastReplicationSucceeded("node2") {
		t.Error("a successful replication was not recorded as positive evidence")
	}
}

// TestLastReplicationSucceededFollowsTheMostRecentOutcome pins the self-correcting
// half: the evidence is last-outcome-wins, so a peer that has genuinely gone away
// withdraws its own veto on the next failed write, and one that comes back
// restores it.
func TestLastReplicationSucceededFollowsTheMostRecentOutcome(t *testing.T) {
	ph := quietHealth([]string{"node2"}, HealthConfig{StableChecks: 3})

	ph.ObserveReplication("node2", true)
	ph.ObserveReplication("node2", false)
	if ph.LastReplicationSucceeded("node2") {
		t.Error("a failure did not withdraw the previous success")
	}

	// One success is enough to restore the evidence, and deliberately so: this is
	// not the stable-health gate. The peer is still not Healthy here — that needs
	// StableChecks consecutive observations — which is exactly the asymmetry the
	// veto exploits, since a node that just reached a peer may catch it up.
	ph.ObserveReplication("node2", true)
	if !ph.LastReplicationSucceeded("node2") {
		t.Error("a success after a failure did not restore the evidence")
	}
	if ph.Healthy("node2") {
		t.Error("one success graduated the peer past the stable-health gate")
	}
}

func TestPeersIsSorted(t *testing.T) {
	ph := quietHealth([]string{"node3", "node1", "node2"}, HealthConfig{})
	got := ph.trackedPeers()
	if len(got) != 3 || got[0] != "node1" || got[1] != "node2" || got[2] != "node3" {
		t.Errorf("trackedPeers() = %v, want sorted", got)
	}
}
