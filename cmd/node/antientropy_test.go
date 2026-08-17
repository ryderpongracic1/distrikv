package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// Anti-entropy tests. The shape of every one of them is the fault the feature
// exists for: this node accepts a write, its replica does not, the client is told
// 503, and the cluster is left divergent until a catch-up pass repairs it.
//
// They drive repair() directly rather than waiting on Run()'s tickers, so each
// test asserts a specific pass outcome instead of racing a background loop.

// withAntiEntropy attaches the convergence subsystems to a test Node and returns
// the engine. SettleDelay is squeezed to a millisecond: a repair cycle always
// runs a second pass to confirm there is nothing left, and the production delay
// exists to let in-flight replication RPCs resolve, which a fake peer does
// synchronously.
func withAntiEntropy(t *testing.T, n *Node, peerIDs ...string) *antiEntropy {
	t.Helper()

	cursors, err := store.OpenCursorStore(n.cfg.DataDir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}
	n.cursors = cursors

	n.health = cluster.NewPeerHealth(peerIDs, cluster.HealthConfig{
		Interval: time.Hour, // the probe loop is not run in these tests
		Probe:    func(string) bool { return true },
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	n.antiEntropy = newAntiEntropy(
		n.cfg.NodeID, n.cfg.ReplicaCount, peerIDs,
		n.store, cursors, n.ring, n.peerClients, n.health,
		n.raft.CurrentTerm, n.metrics, n.logger,
		antiEntropyConfig{SettleDelay: time.Millisecond},
	)
	return n.antiEntropy
}

// keysOwedTo returns n keys for which this node is the ring-primary and target is
// a replica — the only keys a catch-up pass to target may ship.
func keysOwedTo(t *testing.T, ae *antiEntropy, target string, count int) []string {
	t.Helper()
	var out []string
	for i := 0; len(out) < count && i < 10_000; i++ {
		key := fmt.Sprintf("owed-%d", i)
		if ae.owedTo(key, target) {
			out = append(out, key)
		}
	}
	if len(out) < count {
		t.Fatalf("found only %d of %d keys owned by this node with %s as replica", len(out), count, target)
	}
	return out
}

// keyOwnedElsewhere returns a key this node is NOT the ring-primary for.
func keyOwnedElsewhere(t *testing.T, n *Node) string {
	t.Helper()
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("elsewhere-%d", i)
		owners, err := n.ring.GetN(key, n.cfg.ReplicaCount)
		if err != nil {
			t.Fatalf("ring.GetN: %v", err)
		}
		if len(owners) > 0 && owners[0].NodeID != n.cfg.NodeID {
			return key
		}
	}
	t.Fatal("no key owned by another node")
	return ""
}

// primaryPut performs the write the HTTP primary path performs: durable locally,
// then replicated. It returns the replication error so a test can assert the CP
// refusal, exactly as a client would see it.
func primaryPut(t *testing.T, n *Node, key, value string) error {
	t.Helper()
	if err := n.store.Put(context.Background(), key, []byte(value)); err != nil {
		t.Fatalf("local put %q: %v", key, err)
	}
	return n.ReplicateWrite(context.Background(), server.OpPut, key, []byte(value))
}

// putsByKey collapses a fake peer's recorded requests into key → last value.
func putsByKey(p *fakePeer) map[string]string {
	out := make(map[string]string)
	for _, r := range p.requests() {
		out[r.Key] = string(r.Value)
	}
	return out
}

// TestAntiEntropyConvergesReplicaAfterFault is the end-to-end property: writes
// refused during a fault are replayed to the replica once it is healthy again.
func TestAntiEntropyConvergesReplicaAfterFault(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	keys := keysOwedTo(t, ae, "node2", 3)

	// Healthy write: replicated live, so catch-up must not need to resend it.
	if err := primaryPut(t, n, keys[0], "before-fault"); err != nil {
		t.Fatalf("healthy write: %v", err)
	}
	// Let the cursor record the healthy prefix, the way the quiet-cursor ticker
	// does on a cluster that is simply working. Without a cursor a pass starts at
	// the oldest retained segment and re-sends everything, which is correct but
	// says nothing about whether the cursor works.
	ae.repair(context.Background(), "node2")
	if n.cursors.Get("node2").IsZero() {
		t.Fatal("no cursor recorded after a clean catch-up cycle")
	}

	// Fault: the replica stops answering. The write is refused — CP semantics —
	// but kept locally, which is the divergence being repaired.
	replica.setErr(errors.New("connection refused"))
	for i, key := range keys[1:] {
		if err := primaryPut(t, n, key, fmt.Sprintf("during-fault-%d", i)); err == nil {
			t.Fatalf("write to %q during the fault returned nil; a replica that did not "+
				"ACK must still fail the client's write", key)
		}
	}
	// The last key is written twice, so the pass has something to deduplicate.
	if err := primaryPut(t, n, keys[2], "during-fault-final"); err == nil {
		t.Fatal("second write during the fault returned nil")
	}
	if !ae.behindReplicas0(t, "node2") {
		t.Fatal("the replica is not marked behind after replication failures")
	}

	// Heal, and forget everything the replica saw before, so the assertions below
	// are about the catch-up alone.
	replica.setErr(nil)
	replica.reset()

	ae.repair(context.Background(), "node2")

	got := putsByKey(replica)
	want := map[string]string{
		keys[1]: "during-fault-0",
		keys[2]: "during-fault-final",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("catch-up delivered %q = %q, want %q", k, got[k], v)
		}
	}
	if _, resent := got[keys[0]]; resent {
		t.Errorf("catch-up resent %q, which the replica already had: the cursor did not "+
			"account for writes that succeeded before the fault", keys[0])
	}
	if len(got) != len(want) {
		t.Errorf("catch-up delivered %d distinct keys, want %d: %v", len(got), len(want), got)
	}
	// Deduplication: the twice-written key must cost one entry, not two.
	var sentForFinal int
	for _, r := range replica.requests() {
		if r.Key == keys[2] {
			sentForFinal++
		}
	}
	if sentForFinal != 1 {
		t.Errorf("the twice-written key was sent %d times, want 1: a pass must ship only "+
			"each key's newest value", sentForFinal)
	}

	if ae.behindReplicas0(t, "node2") {
		t.Error("the replica is still marked behind after a clean catch-up cycle")
	}
}

// behindReplicas0 reports whether nodeID is currently marked behind.
func (ae *antiEntropy) behindReplicas0(t *testing.T, nodeID string) bool {
	t.Helper()
	for _, id := range ae.behindReplicas() {
		if id == nodeID {
			return true
		}
	}
	return false
}

// TestAntiEntropySkipsKeysThisNodeDoesNotOwn covers the filter that keeps a pass
// from speaking for a range this node is not the primary of. A node's WAL also
// holds the writes it accepted *as* a replica; replaying those to a third node
// would have it act as a primary for keys it does not own.
func TestAntiEntropySkipsKeysThisNodeDoesNotOwn(t *testing.T) {
	n, peers := testNode(t, 2, "node2", "node3")
	ae := withAntiEntropy(t, n, "node2", "node3")
	replica := peers["node2"]

	foreign := keyOwnedElsewhere(t, n)
	// Applied the way a replica applies a write it was sent.
	if err := n.ApplyReplica(context.Background(), server.OpPut, foreign, []byte("not mine")); err != nil {
		t.Fatalf("ApplyReplica: %v", err)
	}

	ae.repair(context.Background(), "node2")

	for _, r := range replica.requests() {
		if r.Key == foreign {
			t.Fatalf("catch-up sent %q, a key this node is not the ring-primary for", foreign)
		}
	}
}

// TestAntiEntropyResumesWhereAReplicaDiedMidSync covers the explicit edge case:
// a replica that stops accepting entries part-way through a pass must be resumed
// from the entry after the last one it ACKed, not from the beginning.
func TestAntiEntropyResumesWhereAReplicaDiedMidSync(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	keys := keysOwedTo(t, ae, "node2", 4)

	replica.setErr(errors.New("connection refused"))
	for i, key := range keys {
		_ = primaryPut(t, n, key, fmt.Sprintf("v%d", i))
	}

	// The replica comes back but dies again after taking two entries.
	replica.setErr(nil)
	replica.failAfter(2, errors.New("replica died mid-sync"))
	replica.reset()

	ae.repair(context.Background(), "node2")

	first := putsByKey(replica)
	if len(first) != 2 {
		t.Fatalf("the interrupted pass delivered %d entries, want 2: %v", len(first), first)
	}
	if !ae.behindReplicas0(t, "node2") {
		t.Error("a replica that died mid-sync must stay marked behind")
	}

	// Second attempt: only the entries the replica never took.
	replica.failAfter(0, nil)
	replica.reset()
	ae.repair(context.Background(), "node2")

	second := putsByKey(replica)
	for k := range first {
		if _, resent := second[k]; resent {
			t.Errorf("the resumed pass resent %q, which the replica had already ACKed", k)
		}
	}
	if len(second) != len(keys)-2 {
		t.Errorf("the resumed pass delivered %d entries, want %d: %v", len(second), len(keys)-2, second)
	}
	// Every key ends up delivered exactly once across the two attempts.
	for _, k := range keys {
		_, a := first[k]
		_, b := second[k]
		if a == b {
			t.Errorf("key %q was delivered %v times across the two passes; want exactly once",
				k, map[bool]string{true: "two", false: "zero"}[a])
		}
	}
}

// TestAntiEntropyCursorSurvivesRestart pins the durability of the cursor: a
// primary that restarts must still know which replicas it is ahead of. Without
// this, a restart silently forgets the divergence, and nothing ever repairs it.
func TestAntiEntropyCursorSurvivesRestart(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	keys := keysOwedTo(t, ae, "node2", 2)

	if err := primaryPut(t, n, keys[0], "replicated"); err != nil {
		t.Fatalf("healthy write: %v", err)
	}
	// Let the cursor record the healthy prefix, then diverge.
	ae.repair(context.Background(), "node2")
	cursorAfterCatchUp := n.cursors.Get("node2")
	if cursorAfterCatchUp.IsZero() {
		t.Fatal("no cursor recorded after a catch-up cycle")
	}

	peers["node2"].setErr(errors.New("connection refused"))
	_ = primaryPut(t, n, keys[1], "diverged")

	if err := n.cursors.Flush(); err != nil {
		t.Fatalf("flush cursors: %v", err)
	}

	// Reopen from disk, the way a restarted process would.
	reopened, err := store.OpenCursorStore(n.cfg.DataDir)
	if err != nil {
		t.Fatalf("reopen cursor store: %v", err)
	}
	if got := reopened.Get("node2"); got != cursorAfterCatchUp {
		t.Errorf("cursor after restart = %s, want %s", got, cursorAfterCatchUp)
	}
	if floor, ok := reopened.RetentionFloor(); !ok || floor != cursorAfterCatchUp.Segment {
		t.Errorf("retention floor after restart = (%d, %t), want (%d, true)",
			floor, ok, cursorAfterCatchUp.Segment)
	}

	// A restarted engine must conclude, from the durable cursor alone, that the
	// replica is owed writes — no health transition is coming to tell it.
	restarted := newAntiEntropy(
		n.cfg.NodeID, n.cfg.ReplicaCount, []string{"node2"},
		n.store, reopened, n.ring, n.peerClients, nil,
		n.raft.CurrentTerm, n.metrics, n.logger, antiEntropyConfig{},
	)
	if !restarted.behindReplicas0(t, "node2") {
		t.Error("a replica whose durable cursor is behind the WAL tip must be repaired " +
			"after a restart; the engine did not notice")
	}
}

// TestAntiEntropyAdvancesCursorOnAQuietCluster covers the happy path nobody
// watches: with no faults, no pass ever runs, so something else has to keep the
// cursor moving — otherwise WAL retention pins every segment since startup.
func TestAntiEntropyAdvancesCursorOnAQuietCluster(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	ae.cfg.CursorHoldback = 10 * time.Millisecond
	keys := keysOwedTo(t, ae, "node2", 1)

	if err := primaryPut(t, n, keys[0], "v"); err != nil {
		t.Fatalf("healthy write: %v", err)
	}

	ae.advanceQuietCursors() // captures a candidate tip
	if got := n.cursors.Get("node2"); !got.IsZero() {
		t.Fatalf("cursor moved to %s on the first tick; the candidate tip must age first", got)
	}
	time.Sleep(2 * ae.cfg.CursorHoldback)
	ae.advanceQuietCursors() // adopts it: the window held no failures

	tip := n.store.WALTip()
	adopted := n.cursors.Get("node2")
	if adopted.IsZero() {
		t.Errorf("cursor is still unset after a fault-free window; tip is %s", tip)
	}

	// A failure inside the window must veto adoption: the failed write sits before
	// the candidate tip, so adopting it would step over an entry the replica never
	// received.
	ae.NoteReplicationFailure("node2")
	before := n.cursors.Get("node2")
	ae.advanceQuietCursors()
	time.Sleep(2 * ae.cfg.CursorHoldback)
	ae.advanceQuietCursors()
	if got := n.cursors.Get("node2"); got != before {
		t.Errorf("cursor advanced to %s over a window containing a replication failure "+
			"(was %s); the failed write would be skipped", got, before)
	}
}

// TestAntiEntropyPassIsBoundedByTheTipItPinned proves a pass cannot be chased
// forward for ever by concurrent writes: it covers the range that existed when it
// started and leaves the rest to the next pass.
func TestAntiEntropyPassIsBoundedByTheTipItPinned(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	keys := keysOwedTo(t, ae, "node2", 2)

	replica.setErr(errors.New("connection refused"))
	_ = primaryPut(t, n, keys[0], "first")
	pinned := n.store.WALTip()

	// A write that lands after the tip was pinned.
	_ = primaryPut(t, n, keys[1], "after-the-tip")

	replica.setErr(nil)
	replica.reset()
	sent, err := ae.runPass(context.Background(), "node2", storewal.Position{}, pinned)
	if err != nil {
		t.Fatalf("runPass: %v", err)
	}
	if sent != 1 {
		t.Fatalf("pass shipped %d entries, want 1 (only the write inside the pinned range)", sent)
	}
	if got := putsByKey(replica); got[keys[1]] != "" {
		t.Errorf("pass shipped %q, which was written after the pinned tip", keys[1])
	}
}
