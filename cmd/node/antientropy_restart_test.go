package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// Convergence across a primary restart.
//
// A live 3-node cluster failed the chaos harness's --check-convergence gate
// deterministically under the *graceful* stop-restart nemesis while passing under
// kill-restart. That asymmetry is the whole clue: a clean shutdown flushes the
// active memtable into an SSTable and releases its WAL segment, so a gracefully
// stopped primary could reopen with its segment numbering restarted — and every
// replica cursor it had persisted then addressed a different log. A SIGKILL leaves
// the segment on disk, numbering continues past it, and the cursors stay valid.
//
// The tests below cover the primary's obligation in both directions: what a pass
// must ship (including tombstones), and what it must refuse to claim.

// primaryDelete performs the delete the HTTP primary path performs: a blind
// tombstone locally, then replicated. It returns the replication error, which is
// what the client sees as a 503.
func primaryDelete(t *testing.T, n *Node, key string) error {
	t.Helper()
	if _, err := n.store.Delete(context.Background(), key); err != nil {
		t.Fatalf("local delete %q: %v", key, err)
	}
	return n.ReplicateWrite(context.Background(), server.OpDelete, key, nil, 0)
}

// lastOpByKey collapses a fake peer's recorded requests into key → the last
// request it received for that key, so a test can assert the *final* state a
// catch-up pass left the replica in.
func lastOpByKey(p *fakePeer) map[string]*kvpb.ReplicateRequest {
	out := make(map[string]*kvpb.ReplicateRequest)
	for _, r := range p.requests() {
		out[r.Key] = r
	}
	return out
}

// restartPrimary simulates this node stopping cleanly and coming back: cursors are
// persisted, the store is closed (which flushes the memtable and releases its WAL
// segment), and a fresh store, cursor store and anti-entropy engine are opened over
// the same data directory — exactly what the process does on a restart.
func restartPrimary(t *testing.T, n *Node, peerIDs ...string) *antiEntropy {
	t.Helper()

	if err := n.cursors.Flush(); err != nil {
		t.Fatalf("flush cursors before restart: %v", err)
	}
	if err := n.store.Close(); err != nil {
		t.Fatalf("close store for restart: %v", err)
	}

	reopened, err := store.New(n.cfg.DataDir, n.logger)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	n.store = reopened

	cursors, err := store.OpenCursorStore(n.cfg.DataDir)
	if err != nil {
		t.Fatalf("reopen cursor store: %v", err)
	}
	n.cursors = cursors

	ae := newAntiEntropy(
		n.cfg.NodeID, n.cfg.ReplicaCount, peerIDs,
		n.store, cursors, n.ring, n.peerClients, n.health,
		nil, // no consensus health signal: these tests drive the local signals
		n.raft.CurrentTerm, n.metrics, n.logger,
		antiEntropyConfig{SettleDelay: time.Millisecond},
	)
	n.antiEntropy = ae
	return ae
}

// TestAntiEntropyShipsTombstonesToARecoveringReplica pins the tombstone half of a
// pass: a delete refused during a fault must be replayed as a delete, so the
// replica converges to *absent* rather than keeping the value it last saw.
//
// This passes on the pre-fix engine too — the WAL reader surfaces delete entries,
// the dedup keeps each key's newest entry whichever op it is, and the Replicate RPC
// carries the op — so it is a regression pin rather than a repro. It is here
// because "the tombstone is dropped somewhere in the ship path" was the leading
// hypothesis for the live failure and needed a standing answer.
func TestAntiEntropyShipsTombstonesToARecoveringReplica(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	keys := keysOwedTo(t, ae, "node2", 2)
	deleted, kept := keys[0], keys[1]

	// Both keys exist on both nodes before the fault.
	for _, k := range keys {
		if err := primaryPut(t, n, k, "before-fault"); err != nil {
			t.Fatalf("healthy write %q: %v", k, err)
		}
	}
	ae.repair(context.Background(), "node2")

	// Fault: the primary overwrites one key and deletes the other. Both writes are
	// refused to the client and kept locally.
	replica.setErr(errors.New("connection refused"))
	if err := primaryPut(t, n, kept, "after-fault"); err == nil {
		t.Fatal("write during the fault returned nil; a replica that did not ACK must fail the write")
	}
	if err := primaryDelete(t, n, deleted); err == nil {
		t.Fatal("delete during the fault returned nil; a replica that did not ACK must fail the delete")
	}

	replica.setErr(nil)
	replica.reset()
	ae.repair(context.Background(), "node2")

	got := lastOpByKey(replica)
	if r := got[deleted]; r == nil {
		t.Errorf("catch-up never shipped anything for %q: the primary deleted it, so a "+
			"replica still holding the old value is divergent", deleted)
	} else if r.Op != server.OpDelete {
		t.Errorf("catch-up shipped op %q for %q, want %q: replaying a tombstone as a put "+
			"would resurrect a deleted key on the replica", r.Op, deleted, server.OpDelete)
	}
	if r := got[kept]; r == nil || string(r.Value) != "after-fault" {
		t.Errorf("catch-up delivered %q = %v, want %q", kept, r, "after-fault")
	}
}

// TestAntiEntropyConvergesAfterAGracefulRestart is the repro for the live failure.
// The primary is restarted between the fault and the repair, which is what the
// stop-restart nemesis does to it.
func TestAntiEntropyConvergesAfterAGracefulRestart(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	keys := keysOwedTo(t, ae, "node2", 2)
	deleted, updated := keys[0], keys[1]

	for _, k := range keys {
		if err := primaryPut(t, n, k, "before-fault"); err != nil {
			t.Fatalf("healthy write %q: %v", k, err)
		}
	}
	// A completed cycle records the cursor, which is the state that goes on to
	// address the wrong log if numbering repeats.
	ae.repair(context.Background(), "node2")
	cursorBefore := n.cursors.Get("node2")
	if cursorBefore.IsZero() {
		t.Fatal("no cursor recorded after a clean catch-up cycle")
	}

	// Fault: one key updated, one deleted, both refused and both kept locally.
	replica.setErr(errors.New("connection refused"))
	if err := primaryPut(t, n, updated, "during-fault"); err == nil {
		t.Fatal("write during the fault returned nil")
	}
	if err := primaryDelete(t, n, deleted); err == nil {
		t.Fatal("delete during the fault returned nil")
	}

	// The primary stops cleanly and comes back, the way `docker compose stop` then
	// `start` restarts it.
	restarted := restartPrimary(t, n, "node2")

	tip := n.store.WALTip()
	if tip.Before(cursorBefore) || tip == cursorBefore {
		// Reported rather than fatal so the assertions below still run: they are what
		// show the consequence — the pass ships nothing and the replica is left
		// divergent while the engine logs "replica caught up".
		t.Errorf("after the restart the WAL tip is %s but a cursor recorded before the "+
			"restart is %s: the new log has reused positions the old one already used, so "+
			"no cursor can be trusted", tip, cursorBefore)
	}

	replica.setErr(nil)
	replica.reset()
	restarted.repair(context.Background(), "node2")

	got := lastOpByKey(replica)
	if r := got[updated]; r == nil || string(r.Value) != "during-fault" {
		t.Errorf("after a graceful restart, catch-up delivered %q = %v, want %q. The "+
			"restarted primary is still ahead of the replica for every write refused "+
			"during the fault, and the persisted cursor is what tells it so",
			updated, r, "during-fault")
	}
	if r := got[deleted]; r == nil {
		t.Errorf("after a graceful restart, catch-up shipped nothing for the deleted key "+
			"%q; the replica is left holding a value the primary no longer has", deleted)
	} else if r.Op != server.OpDelete {
		t.Errorf("after a graceful restart, catch-up shipped op %q for %q, want %q",
			r.Op, deleted, server.OpDelete)
	}
}

// TestAntiEntropyWithholdsTheClaimWhenACursorCannotDescribeThisLog covers the
// defensive half. If a cursor somehow orders after the tip, the log it described is
// gone; the engine must drop it and refuse to report convergence rather than read
// past the end of a shorter log and call that "caught up".
func TestAntiEntropyWithholdsTheClaimWhenACursorCannotDescribeThisLog(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	keys := keysOwedTo(t, ae, "node2", 1)

	if err := primaryPut(t, n, keys[0], "v"); err != nil {
		t.Fatalf("healthy write: %v", err)
	}

	// A cursor from a log that no longer exists: far past this node's tip.
	tip := n.store.WALTip()
	bogus := storewal.Position{Segment: tip.Segment, Offset: tip.Offset + 1_000_000}
	if !n.cursors.Advance("node2", bogus) {
		t.Fatalf("could not plant a cursor at %s", bogus)
	}

	// A fresh engine over the same store, the way a restarted process opens one.
	// The store itself is untouched: what is being tested is how the engine reads a
	// cursor the log cannot account for, not the restart mechanics.
	reopened := newAntiEntropy(
		n.cfg.NodeID, n.cfg.ReplicaCount, []string{"node2"},
		n.store, n.cursors, n.ring, n.peerClients, nil, nil,
		n.raft.CurrentTerm, n.metrics, n.logger, antiEntropyConfig{},
	)

	if got := n.cursors.Get("node2"); !got.IsZero() {
		t.Errorf("cursor after opening with an impossible cursor = %s, want dropped: a "+
			"cursor ahead of the tip freezes WAL retention on a segment that does not "+
			"exist and cannot be moved back", got)
	}
	if !reopened.behindReplicas0(t, "node2") {
		t.Error("a replica whose cursor cannot describe this log must be examined, not " +
			"assumed up to date")
	}
	blocked, reason := reopened.convergenceClaimBlocked("node2")
	if !blocked {
		t.Error("the engine is still willing to claim this replica is caught up, even " +
			"though the log it would have to prove that from is gone")
	}
	if reason == "" {
		t.Error("the blocked claim carries no reason")
	}
	if n.metrics.AntiEntropyFullSyncRequired.Load() != 1 {
		t.Error("anti_entropy_full_sync_required was not raised for a log that cannot " +
			"account for what this node holds")
	}
}

// TestCatchUpCyclesAreCoalescedPerReplica is the hot-loop regression. Three
// independent schedulers can want a cycle for the same replica at the same time;
// running one cycle per request produced bursts of instantly-completing passes,
// each logging "replica caught up" for the same fact.
func TestCatchUpCyclesAreCoalescedPerReplica(t *testing.T) {
	n, _ := testNode(t, 2, "node2", "node3")
	ae := withAntiEntropy(t, n, "node2", "node3")

	const attempts = 6
	accepted := 0
	for i := 0; i < attempts; i++ {
		if ae.tryEnqueue("node2") {
			accepted++
		}
	}
	if accepted != 1 {
		t.Errorf("%d of %d schedule attempts for one replica were accepted, want 1: "+
			"duplicate cycles all find nothing to ship and complete instantly, which is "+
			"the sub-millisecond burst of 'replica caught up' lines this bounds",
			accepted, attempts)
	}

	// A different replica is independent — coalescing must not serialise the cluster.
	if !ae.tryEnqueue("node3") {
		t.Error("a pending cycle for one replica blocked scheduling for another")
	}

	// Once the cycle has run, the replica can be scheduled again.
	ae.repair(context.Background(), "node2")
	if !ae.tryEnqueue("node2") {
		t.Error("the replica could not be rescheduled after its cycle finished; the " +
			"pending slot was never released")
	}
}

// TestCollectKeepsTheNewestEntryPerKey pins the deduplication direction. Keeping
// the *first* entry per key would have a pass ship a stale value that overwrites a
// newer one on the replica — divergence caused by the repair itself.
//
// Like the tombstone test this passes pre-fix; it is a standing answer to a
// hypothesis the live failure invited, not a repro.
func TestCollectKeepsTheNewestEntryPerKey(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	key := keysOwedTo(t, ae, "node2", 1)[0]

	replica.setErr(errors.New("connection refused"))
	for i := 0; i < 3; i++ {
		_ = primaryPut(t, n, key, fmt.Sprintf("v%d", i))
	}

	replica.setErr(nil)
	replica.reset()
	sent, err := ae.runPass(context.Background(), "node2", storewal.Position{}, n.store.WALTip())
	if err != nil {
		t.Fatalf("runPass: %v", err)
	}
	if sent != 1 {
		t.Errorf("pass shipped %d entries for one thrice-written key, want 1", sent)
	}
	if got := putsByKey(replica)[key]; got != "v2" {
		t.Errorf("pass shipped %q = %q, want %q (the newest value): shipping an older "+
			"entry would overwrite a newer value on the replica", key, got, "v2")
	}
}
