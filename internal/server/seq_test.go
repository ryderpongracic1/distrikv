package server

import (
	"context"
	"net/http"
	"testing"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// TestPrimaryWritePutCarriesStoreSeq pins the sending half of the wiring: the
// sequence the primary's local write was assigned is the one that goes out on the
// fan-out. If the two ever drifted apart, replicas would compare versions against
// a number that describes a different write.
func TestPrimaryWritePutCarriesStoreSeq(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if rec := h.do(t, http.MethodPut, "/keys/alpha", `{"value":"one"}`); rec.Code != http.StatusOK {
		t.Fatalf("PUT alpha status = %d, want 200", rec.Code)
	}

	writes := h.repl.writeCalls()
	if len(writes) != 1 {
		t.Fatalf("fan-out calls = %d, want 1", len(writes))
	}
	if writes[0].Seq == 0 {
		t.Fatal("fan-out carried seq 0; the primary's write was sequenced and that sequence must travel")
	}

	// The recorded sequence must be exactly the one the store handed out for that
	// write. Nothing else has written since, so the very next write's sequence is
	// one above it.
	next, err := h.store.Put(ctx, "probe", []byte("v"))
	if err != nil {
		t.Fatalf("probe Put: %v", err)
	}
	if writes[0].Seq != next-1 {
		t.Errorf("fan-out seq = %d, want %d (the sequence the store assigned the PUT)", writes[0].Seq, next-1)
	}
}

// TestPrimaryWriteSeqIncreasesAcrossWrites checks that each mutation carries its
// own sequence — a cached or reused one would make two writes look like one
// version to every replica.
func TestPrimaryWriteSeqIncreasesAcrossWrites(t *testing.T) {
	h := newHarness(t)

	for _, target := range []string{"/keys/a", "/keys/b"} {
		if rec := h.do(t, http.MethodPut, target, `{"value":"v"}`); rec.Code != http.StatusOK {
			t.Fatalf("PUT %s status = %d, want 200", target, rec.Code)
		}
	}
	if rec := h.do(t, http.MethodDelete, "/keys/a", ""); rec.Code != http.StatusOK {
		t.Fatalf("DELETE /keys/a status = %d, want 200", rec.Code)
	}

	writes := h.repl.writeCalls()
	if len(writes) != 3 {
		t.Fatalf("fan-out calls = %d, want 3", len(writes))
	}
	var last uint64
	for i, w := range writes {
		if w.Seq <= last {
			t.Fatalf("fan-out call %d (%s %s): seq = %d, want > %d", i, w.Op, w.Key, w.Seq, last)
		}
		last = w.Seq
	}
}

// TestReplicateHandlerPassesSeq pins the receiving half: the sequence on the wire
// reaches ApplyReplica unchanged, which is where the apply-if-newer comparison
// happens.
func TestReplicateHandlerPassesSeq(t *testing.T) {
	h := newHarness(t)

	const wireSeq = 4242
	resp, err := h.grpc.Replicate(context.Background(), &kvpb.ReplicateRequest{
		Op: OpPut, Key: "alpha", Value: []byte("one"), Term: 7, Seq: wireSeq,
	})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if !resp.Success {
		t.Fatal("Replicate success = false, want true")
	}

	applies := h.repl.applyCalls()
	if len(applies) != 1 {
		t.Fatalf("ApplyReplica calls = %d, want 1", len(applies))
	}
	if applies[0].Seq != wireSeq {
		t.Errorf("ApplyReplica seq = %d, want %d (req.Seq must pass through unchanged)", applies[0].Seq, wireSeq)
	}
}

// TestReplicateHandlerPassesZeroSeq covers a sender that predates the field: the
// zero value has to reach the apply path intact so it can take the unconditional
// path rather than being mistaken for a real sequence.
func TestReplicateHandlerPassesZeroSeq(t *testing.T) {
	h := newHarness(t)

	if _, err := h.grpc.Replicate(context.Background(), &kvpb.ReplicateRequest{
		Op: OpPut, Key: "alpha", Value: []byte("one"),
	}); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	applies := h.repl.applyCalls()
	if len(applies) != 1 {
		t.Fatalf("ApplyReplica calls = %d, want 1", len(applies))
	}
	if applies[0].Seq != 0 {
		t.Errorf("ApplyReplica seq = %d, want 0", applies[0].Seq)
	}
}
