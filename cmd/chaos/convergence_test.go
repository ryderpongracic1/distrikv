package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
)

// convergedOK is a passing convergence result, for tests about other things.
var convergedOK = convergenceResult{Checked: true, Converged: true}

// fakeNode is a stand-in distrikv node: it answers /status with its node ID and
// GET /keys/{k}?local=true from its own map, which is exactly the surface the
// convergence check depends on.
type fakeNode struct {
	id  string
	srv *httptest.Server

	mu     sync.Mutex
	values map[string]string
	// localReads counts reads that asked for local-only answers, so a test can
	// prove the check never falls back to a forwarding read — which would report
	// the primary's value from every node and make every run "converge".
	localReads   int
	forwardReads int

	// failKeyReads, when > 0, makes that many key reads answer 500 before the node
	// starts serving normally again. /status is unaffected, which matters: node
	// discovery happens before the check's retry loop and returns early on
	// failure, so a *transient* read error is only reachable from inside the loop.
	failKeyReads int
}

func newFakeNode(id string) *fakeNode {
	n := &fakeNode{id: id, values: make(map[string]string)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"node_id": n.id})
	})
	mux.HandleFunc("GET /keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		n.mu.Lock()
		if r.URL.Query().Get("local") == "true" {
			n.localReads++
		} else {
			n.forwardReads++
		}
		v, ok := n.values[key]
		failing := n.failKeyReads > 0
		if failing {
			n.failKeyReads--
		}
		n.mu.Unlock()

		if failing {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"value": v})
	})
	n.srv = httptest.NewServer(mux)
	return n
}

func (n *fakeNode) addr() string { return strings.TrimPrefix(n.srv.URL, "http://") }

func (n *fakeNode) set(key, value string) {
	n.mu.Lock()
	n.values[key] = value
	n.mu.Unlock()
}

func (n *fakeNode) del(key string) {
	n.mu.Lock()
	delete(n.values, key)
	n.mu.Unlock()
}

// failNextKeyReads makes the next count key reads answer 500, modelling a
// transient read error inside the check's grace window.
func (n *fakeNode) failNextKeyReads(count int) {
	n.mu.Lock()
	n.failKeyReads = count
	n.mu.Unlock()
}

func (n *fakeNode) reads() (local, forward int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.localReads, n.forwardReads
}

// startCluster spins up n fake nodes and returns them keyed by node ID plus the
// address list the checker takes.
func startCluster(t *testing.T, ids ...string) (map[string]*fakeNode, []string) {
	t.Helper()
	nodes := make(map[string]*fakeNode, len(ids))
	addrs := make([]string, 0, len(ids))
	for _, id := range ids {
		n := newFakeNode(id)
		t.Cleanup(n.srv.Close)
		nodes[id] = n
		addrs = append(addrs, n.addr())
	}
	return nodes, addrs
}

// writeToOwners puts key on every node the ring says should hold it, which is how
// a converged cluster looks.
func writeToOwners(t *testing.T, nodes map[string]*fakeNode, addrs []string, key, value string, replicas int) []string {
	t.Helper()
	owners := ownersFor(t, nodes, addrs, key, replicas)
	for _, id := range owners {
		nodes[id].set(key, value)
	}
	return owners
}

func ownersFor(t *testing.T, nodes map[string]*fakeNode, _ []string, key string, replicas int) []string {
	t.Helper()
	ring := newRingOf(nodes)
	vns, err := ring.GetN(key, replicas)
	if err != nil {
		t.Fatalf("ring.GetN: %v", err)
	}
	out := make([]string, 0, len(vns))
	for _, vn := range vns {
		out = append(out, vn.NodeID)
	}
	return out
}

// newRingOf builds the same ring the checker builds — placement is a hash of the
// node ID alone, so the address it is registered under does not matter.
func newRingOf(nodes map[string]*fakeNode) *cluster.Ring {
	ring := cluster.New()
	for id, n := range nodes {
		ring.AddNode(id, n.addr())
	}
	return ring
}

// TestConvergenceDetectsADivergentReplica is the check's reason to exist: the
// primary kept a refused write and the replica never got it.
func TestConvergenceDetectsADivergentReplica(t *testing.T) {
	nodes, addrs := startCluster(t, "node1", "node2", "node3")
	const key = "chaos-m1-k0"

	owners := writeToOwners(t, nodes, addrs, key, "agreed", 2)
	// Take the value away from the replica, leaving the primary ahead — exactly
	// the refused-but-applied shape.
	nodes[owners[1]].del(key)

	res := checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 0, Nodes: addrs, Replicas: 2, PollInterval: time.Millisecond,
	}, []string{key}, http.DefaultClient)

	if !res.Checked {
		t.Fatalf("check was skipped: %s", res.Skipped)
	}
	if res.Converged {
		t.Fatal("a replica missing the primary's value was reported as converged")
	}
	if res.Divergent != 1 {
		t.Errorf("divergent keys = %d, want 1", res.Divergent)
	}
	if len(res.Examples) != 1 || !strings.Contains(res.Examples[0], "<absent>") {
		t.Errorf("examples = %v, want one naming the absent replica", res.Examples)
	}

	// A conflicting value, not just an absent one, must also be caught.
	nodes[owners[1]].set(key, "stale")
	res = checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 0, Nodes: addrs, Replicas: 2, PollInterval: time.Millisecond,
	}, []string{key}, http.DefaultClient)
	if res.Converged {
		t.Fatal("replicas holding different values were reported as converged")
	}
}

// TestConvergenceReadsLocallyOnly pins the detail the whole check hinges on: a
// plain GET on a non-owning node forwards to the ring-primary, so it would report
// the primary's value from every node and every run would converge.
func TestConvergenceReadsLocallyOnly(t *testing.T) {
	nodes, addrs := startCluster(t, "node1", "node2", "node3")
	const key = "chaos-m1-k1"
	writeToOwners(t, nodes, addrs, key, "v", 2)

	checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 0, Nodes: addrs, Replicas: 2, PollInterval: time.Millisecond,
	}, []string{key}, http.DefaultClient)

	var totalLocal, totalForward int
	for _, n := range nodes {
		l, f := n.reads()
		totalLocal += l
		totalForward += f
	}
	if totalForward != 0 {
		t.Errorf("%d forwarding reads were issued; every convergence read must be local", totalForward)
	}
	if totalLocal == 0 {
		t.Error("no local reads were issued at all")
	}
}

// TestConvergencePassesOnAnAgreeingCluster is the shape of a healthy run.
func TestConvergencePassesOnAnAgreeingCluster(t *testing.T) {
	nodes, addrs := startCluster(t, "node1", "node2", "node3")
	keys := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("chaos-m1-k%d", i)
		keys = append(keys, key)
		writeToOwners(t, nodes, addrs, key, fmt.Sprintf("v%d", i), 2)
	}

	res := checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: time.Second, Nodes: addrs, Replicas: 2, PollInterval: time.Millisecond,
	}, keys, http.DefaultClient)

	if !res.Converged {
		t.Fatalf("an agreeing cluster reported divergent: %d keys, examples %v", res.Divergent, res.Examples)
	}
	if res.KeysChecked != len(keys) {
		t.Errorf("keys checked = %d, want %d", res.KeysChecked, len(keys))
	}
	if res.NodeReads != len(keys)*2 {
		t.Errorf("node reads = %d, want %d (one per key per replica)", res.NodeReads, len(keys)*2)
	}
	// A key absent from every replica agrees too: convergence is about the replicas
	// matching, not about the key existing.
	res = checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 0, Nodes: addrs, Replicas: 2, PollInterval: time.Millisecond,
	}, []string{"never-written"}, http.DefaultClient)
	if !res.Converged {
		t.Error("a key absent from every replica was reported divergent")
	}
}

// TestConvergenceClearsUnreachableBetweenAttempts pins the report's internal
// consistency. Unreachable is per-attempt evidence, but it was accumulated across
// the whole grace window and never cleared — so a transient read error on an early
// attempt survived into a later attempt that read every replica cleanly, and
// convergenceLines printed "converged: true" with unreachable nodes listed
// underneath it. A summary contradicted by its own detail lines is exactly the
// kind of claim this harness exists to prevent.
func TestConvergenceClearsUnreachableBetweenAttempts(t *testing.T) {
	nodes, addrs := startCluster(t, "node1", "node2", "node3")
	const key = "chaos-m1-k3"
	owners := writeToOwners(t, nodes, addrs, key, "agreed", 2)

	// One replica answers 500 for its first read, then serves normally — the shape
	// of a node that is briefly busy mid-grace-window rather than gone.
	nodes[owners[1]].failNextKeyReads(1)

	res := checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 3 * time.Second, Nodes: addrs, Replicas: 2,
		PollInterval: 10 * time.Millisecond,
	}, []string{key}, http.DefaultClient)

	if !res.Converged {
		t.Fatalf("a cluster that agrees on every key after one transient read error "+
			"was reported unconverged: %+v", res)
	}
	if len(res.Unreachable) != 0 {
		t.Errorf("converged=true was returned with %d unreachable entries still "+
			"attached from an earlier attempt: %v", len(res.Unreachable), res.Unreachable)
	}
	if res.Attempts < 2 {
		t.Fatalf("attempts = %d: the transient error did not force a retry, so this "+
			"test is not exercising the accumulation it is about", res.Attempts)
	}

	// The rendered report is what an operator actually reads, so assert there too.
	for _, line := range res.convergenceLines() {
		if strings.Contains(line, "unreachable:") {
			t.Errorf("the report prints %q under a converged summary", strings.TrimSpace(line))
		}
	}
}

// TestConvergenceWaitsWithinItsGrace covers the grace window: catch-up is
// after-the-fact, so the check has to give repair time to happen.
func TestConvergenceWaitsWithinItsGrace(t *testing.T) {
	nodes, addrs := startCluster(t, "node1", "node2", "node3")
	const key = "chaos-m1-k2"
	owners := ownersFor(t, nodes, addrs, key, 2)
	nodes[owners[0]].set(key, "primary-value")

	// The replica is repaired shortly after the check starts, the way a catch-up
	// pass would repair it.
	go func() {
		time.Sleep(150 * time.Millisecond)
		nodes[owners[1]].set(key, "primary-value")
	}()

	res := checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 3 * time.Second, Nodes: addrs, Replicas: 2, PollInterval: 10 * time.Millisecond,
	}, []string{key}, http.DefaultClient)

	if !res.Converged {
		t.Fatalf("the check gave up before the repair landed: %v", res)
	}
	if res.Attempts < 2 {
		t.Errorf("attempts = %d; the check should have retried while the cluster was divergent", res.Attempts)
	}
}

// TestConvergenceRefusesToPassWithAnUnreachableNode is the failure mode that would
// make the whole gate worthless: a node that cannot be asked cannot be shown to
// have converged.
func TestConvergenceRefusesToPassWithAnUnreachableNode(t *testing.T) {
	nodes, addrs := startCluster(t, "node1", "node2")
	nodes["node2"].srv.Close()

	res := checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 0, Nodes: addrs, Replicas: 2, PollInterval: time.Millisecond,
	}, []string{"chaos-m1-k3"}, http.DefaultClient)

	if res.Converged {
		t.Fatal("reported converged while a node was unreachable")
	}
	if len(res.Unreachable) == 0 {
		t.Error("the unreachable node was not reported")
	}
	if !strings.Contains(strings.Join(res.convergenceLines(), "\n"), "could not verify") {
		t.Errorf("the report does not distinguish 'unverified' from 'divergent': %v", res.convergenceLines())
	}
}

// TestConvergenceSkipsWithoutPeerAddresses pins the honest skip: the check needs
// every node's address, and silently passing without them would be the worst
// outcome.
func TestConvergenceSkipsWithoutPeerAddresses(t *testing.T) {
	res := checkConvergence(context.Background(), convergenceConfig{
		Enabled: true, Grace: 0, Nodes: []string{"localhost:8001"}, Replicas: 2,
	}, []string{"k"}, http.DefaultClient)

	if res.Checked {
		t.Fatal("the check ran with only one node's address")
	}
	if !strings.Contains(res.Skipped, "--peers") {
		t.Errorf("skip reason %q does not tell the operator what to pass", res.Skipped)
	}
	if !strings.Contains(strings.Join(res.convergenceLines(), "\n"), "skipped") {
		t.Errorf("the report does not mark the check as skipped: %v", res.convergenceLines())
	}
}

func TestParseNodeAddrs(t *testing.T) {
	got := parseNodeAddrs("localhost:8001", " localhost:8002 , localhost:8003 ,,localhost:8001")
	want := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	if len(got) != len(want) {
		t.Fatalf("parseNodeAddrs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parseNodeAddrs = %v, want %v", got, want)
		}
	}
	if got := parseNodeAddrs("localhost:8001", ""); len(got) != 1 {
		t.Errorf("parseNodeAddrs with no peers = %v, want just the target", got)
	}
}

// TestDivergenceFailNoteIsAboutDivergence guards against the operator being told
// to look for a consistency anomaly when the finding is unrepaired divergence.
func TestDivergenceFailNoteIsAboutDivergence(t *testing.T) {
	diverged := convergenceResult{Checked: true, Converged: false, Divergent: 4, KeysChecked: 20}
	note := strings.Join(verdictNotes(1, 100, 0, diverged), "\n")

	for _, want := range []string{"replicas did not converge", "anti-entropy", "anti_entropy_passes", "--convergence-grace"} {
		if !strings.Contains(note, want) {
			t.Errorf("the divergence note should mention %q:\n%s", want, note)
		}
	}
	if strings.Contains(note, "real consistency anomaly") {
		t.Error("the divergence note reuses the illegal-history explanation, which does not apply")
	}
}
