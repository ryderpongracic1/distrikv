package main

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// These tests cover H2: two writes to one key can reach a replica in the
// opposite order to the one the primary wrote them in, because replication is a
// fan-out of independent RPCs. Applying them in arrival order leaves the replica
// holding the earlier write, and nothing afterwards notices — the replica ACKed
// both, so it is not behind, and an anti-entropy pass replays gaps rather than
// inversions. Every test here asserts on the value the replica ends up holding,
// because that is the divergence a convergence check reports.

// deliver applies one replicated mutation the way GRPCServer.Replicate does.
func deliverPut(t *testing.T, n *Node, key, value string, seq uint64) {
	t.Helper()
	if err := n.ApplyReplica(context.Background(), server.OpPut, key, []byte(value), seq); err != nil {
		t.Fatalf("ApplyReplica put %q seq=%d: %v", key, seq, err)
	}
}

func deliverDelete(t *testing.T, n *Node, key string, seq uint64) {
	t.Helper()
	if err := n.ApplyReplica(context.Background(), server.OpDelete, key, nil, seq); err != nil {
		t.Fatalf("ApplyReplica delete %q seq=%d: %v", key, seq, err)
	}
}

// wantValue asserts the replica's local state for key.
func wantValue(t *testing.T, n *Node, key, want string) {
	t.Helper()
	got, err := n.store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	if string(got) != want {
		t.Errorf("replica holds %q for %q, want %q", got, key, want)
	}
}

func wantAbsent(t *testing.T, n *Node, key string) {
	t.Helper()
	_, err := n.store.Get(context.Background(), key)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get %q = %v, want ErrNotFound", key, err)
	}
}

// TestApplyReplicaResolvesArrivalOrderInversion is the H2 regression: the same
// two writes are delivered in both orders and must settle on the same value.
//
// The reversed case is the one that used to diverge — "A" arriving last won the
// key even though "B" was written later.
func TestApplyReplicaResolvesArrivalOrderInversion(t *testing.T) {
	const key = "x"

	t.Run("in order", func(t *testing.T) {
		n, _ := testNode(t, 2, "node2")
		deliverPut(t, n, key, "A", 41)
		deliverPut(t, n, key, "B", 42)
		wantValue(t, n, key, "B")
	})

	t.Run("reversed", func(t *testing.T) {
		n, _ := testNode(t, 2, "node2")
		deliverPut(t, n, key, "B", 42) // the later write arrives first
		deliverPut(t, n, key, "A", 41) // the earlier write arrives second
		wantValue(t, n, key, "B")      // …and loses, because its sequence is lower
	})
}

// TestApplyReplicaOrdersDeletesAgainstPuts pins that a tombstone is ordered like
// any other version: the higher sequence wins whichever op it belongs to.
//
// Without this, a delete and a put racing on one key resolve by arrival order,
// which is how one replica ends up holding a value while another holds none —
// the `node2=<absent> node3="w7-…"` shape a convergence check reports.
func TestApplyReplicaOrdersDeletesAgainstPuts(t *testing.T) {
	const key = "y"

	t.Run("delete is newer and arrives first", func(t *testing.T) {
		n, _ := testNode(t, 2, "node2")
		deliverPut(t, n, key, "seed", 10)
		deliverDelete(t, n, key, 12)   // the delete was written last
		deliverPut(t, n, key, "B", 11) // an older put arrives after it
		wantAbsent(t, n, key)          // the delete still wins
	})

	t.Run("put is newer and arrives first", func(t *testing.T) {
		n, _ := testNode(t, 2, "node2")
		deliverPut(t, n, key, "seed", 10)
		deliverPut(t, n, key, "B", 12) // the put was written last
		deliverDelete(t, n, key, 11)   // an older delete arrives after it
		wantValue(t, n, key, "B")      // the put still wins
	})
}

// TestApplyReplicaSequenceComparisonSurvivesRestart checks the node-level
// property: the comparison is against the sequence this node has stored for the
// key, so that sequence has to mean the same thing after a restart as before it.
//
// This covers a clean restart, where the sequence comes back from the SSTable the
// memtable was flushed into on close. The harder path — recovery from an
// unflushed WAL, which is what a SIGKILLed replica actually does and which used
// to lose the sequence entirely — is pinned in the engine, where a crash can be
// simulated: see lsm.TestStoredSequenceSurvivesCrashRecovery.
func TestApplyReplicaSequenceComparisonSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	const key = "restart-key"

	n, _ := testNodeInDir(t, dir, 2, "node2")
	deliverPut(t, n, key, "at-100", 100)
	wantValue(t, n, key, "at-100")
	if err := n.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Same data directory, fresh process.
	n2, _ := testNodeInDir(t, dir, 2, "node2")
	wantValue(t, n2, key, "at-100")

	// A write the primary sent before the one this node already holds.
	deliverPut(t, n2, key, "at-50", 50)
	wantValue(t, n2, key, "at-100")

	// …and one it sent after, which must still be taken. This is the half that
	// fails when the stored sequence has been replaced by a local one: the
	// counter would have been seeded above 100, so 101 would look stale.
	deliverPut(t, n2, key, "at-101", 101)
	wantValue(t, n2, key, "at-101")
}

// TestApplyReplicaWithoutSequenceAppliesUnconditionally pins the compatibility
// path: a peer that predates the wire field sends 0, which must behave exactly
// as replication did before sequences existed rather than being read as an
// ordering below every real write.
func TestApplyReplicaWithoutSequenceAppliesUnconditionally(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	const key = "legacy"

	deliverPut(t, n, key, "sequenced", 500)
	deliverPut(t, n, key, "unsequenced", 0)
	wantValue(t, n, key, "unsequenced")

	deliverDelete(t, n, key, 0)
	wantAbsent(t, n, key)
}

// TestApplyReplicaAcksAStaleWrite pins that discarding is not an error.
//
// The primary asked this node to reach a state it is already at or past, which
// is a satisfied request. Answering with a failure would refuse the client's
// write on the primary and mark this node behind for a divergence that does not
// exist — turning the fix into a new availability bug.
func TestApplyReplicaAcksAStaleWrite(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	const key = "stale-ack"

	deliverPut(t, n, key, "new", 20)
	if err := n.ApplyReplica(context.Background(), server.OpPut, key, []byte("old"), 19); err != nil {
		t.Fatalf("a discarded write must still ACK, got: %v", err)
	}
	if err := n.ApplyReplica(context.Background(), server.OpDelete, key, nil, 19); err != nil {
		t.Fatalf("a discarded delete must still ACK, got: %v", err)
	}
	wantValue(t, n, key, "new")
}

// TestReplicateWriteCarriesTheLocalSequence asserts the primary side of the
// contract: the sequence its own store assigned is what goes on the wire, the
// same value to every replica.
func TestReplicateWriteCarriesTheLocalSequence(t *testing.T) {
	n, peers := testNode(t, 3, "node2", "node3")
	ctx := context.Background()
	const key = "alpha"

	first, err := n.store.Put(ctx, key, []byte("one"))
	if err != nil {
		t.Fatalf("local put: %v", err)
	}
	if first == 0 {
		t.Fatal("store.Put returned sequence 0; 0 means \"unknown\" on the wire and would disable ordering")
	}
	if err := n.ReplicateWrite(ctx, server.OpPut, key, []byte("one"), first); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	second, err := n.store.Put(ctx, key, []byte("two"))
	if err != nil {
		t.Fatalf("local put: %v", err)
	}
	if second <= first {
		t.Errorf("sequences must increase: first=%d second=%d", first, second)
	}
	if err := n.ReplicateWrite(ctx, server.OpPut, key, []byte("two"), second); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	targets := replicaIDsFor(t, n, key)
	if len(targets) == 0 {
		t.Fatal("precondition: the ring selected no peer replica for this key")
	}
	for _, id := range targets {
		reqs := peers[id].requests()
		if len(reqs) != 2 {
			t.Fatalf("replica %s saw %d requests, want 2", id, len(reqs))
		}
		if reqs[0].Seq != first || reqs[1].Seq != second {
			t.Errorf("replica %s received seqs (%d, %d), want (%d, %d)",
				id, reqs[0].Seq, reqs[1].Seq, first, second)
		}
	}
}

// TestReplicateRequestCarriesSeqOnTheWire guards the generated protobuf code.
//
// The descriptor and the struct are two separate declarations of the same field,
// and a Go struct field alone is not on the wire — if they disagree the value is
// dropped in transit, every ordering decision silently falls back to "unknown",
// and no other test in this package would notice because they exercise
// ApplyReplica directly rather than through a marshalled request.
func TestReplicateRequestCarriesSeqOnTheWire(t *testing.T) {
	in := &kvpb.ReplicateRequest{
		Op:    server.OpPut,
		Key:   "x",
		Value: []byte("B"),
		Term:  7,
		Seq:   1234567890,
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out kvpb.ReplicateRequest
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Seq != in.Seq {
		t.Errorf("seq did not survive the wire: got %d, want %d", out.Seq, in.Seq)
	}
	if out.Op != in.Op || out.Key != in.Key || string(out.Value) != string(in.Value) || out.Term != in.Term {
		t.Errorf("adding seq disturbed an existing field: %+v", &out)
	}

	// A request from a peer that predates the field carries no seq at all, which
	// must decode as 0 rather than as a parse failure.
	legacy := &kvpb.ReplicateRequest{Op: server.OpPut, Key: "x", Value: []byte("B"), Term: 7}
	rawLegacy, err := proto.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	var decoded kvpb.ReplicateRequest
	if err := proto.Unmarshal(rawLegacy, &decoded); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if decoded.Seq != 0 {
		t.Errorf("a request without seq decoded as %d, want 0", decoded.Seq)
	}
}
