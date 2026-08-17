package server

import (
	"context"
	"testing"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// The replay marker is only useful if it survives the wire-to-apply hop intact.
// It carries no data and changes no value, so nothing downstream would fail if it
// were dropped here — the only symptom would be an epoch-regression alarm that
// fires on ordinary restarts, which is exactly the defect it exists to close.

// TestReplicateHandlerPassesReplayMarker pins that a catch-up replay reaches the
// apply path marked as one.
func TestReplicateHandlerPassesReplayMarker(t *testing.T) {
	h := newHarness(t)

	resp, err := h.grpc.Replicate(context.Background(), &kvpb.ReplicateRequest{
		Op: OpPut, Key: "alpha", Value: []byte("one"), Term: 7, Seq: 99, Replay: true,
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
	if !applies[0].Replay {
		t.Error("ApplyReplica replay = false for a request marked Replay: the receiver " +
			"then classifies a catch-up replay as a live write and can report a healthy " +
			"restart as an epoch regression")
	}
}

// TestReplicateHandlerDefaultsToLive covers a sender that predates the field, and
// the property that makes the marker safe: the zero value means live, so the
// epoch-regression classification stays on unless a sender explicitly asks for it
// to be relaxed.
func TestReplicateHandlerDefaultsToLive(t *testing.T) {
	h := newHarness(t)

	if _, err := h.grpc.Replicate(context.Background(), &kvpb.ReplicateRequest{
		Op: OpPut, Key: "alpha", Value: []byte("one"), Seq: 99,
	}); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	applies := h.repl.applyCalls()
	if len(applies) != 1 {
		t.Fatalf("ApplyReplica calls = %d, want 1", len(applies))
	}
	if applies[0].Replay {
		t.Error("a request with no replay marker reached ApplyReplica as a replay; the " +
			"absent field must read as live so an old peer keeps the full classification")
	}
}
