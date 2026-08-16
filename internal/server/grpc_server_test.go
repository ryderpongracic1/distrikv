package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// forwardErrorMessage extracts the JSON error string from a ForwardKeyResponse.
func forwardErrorMessage(t *testing.T, resp *kvpb.ForwardKeyResponse) string {
	t.Helper()
	var body struct{ Error string }
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		t.Fatalf("decode forward error body %q: %v", resp.Body, err)
	}
	return body.Error
}

// TestForwardKeyPutReplicates covers the second write entry point: a peer that
// is not the ring-primary forwards the mutation here, and this node — the
// primary — must replicate it exactly as it would a direct client write.
func TestForwardKeyPutReplicates(t *testing.T) {
	h := newHarness(t)

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "PUT", Key: "alpha", Value: []byte("one"),
	})
	if err != nil {
		t.Fatalf("ForwardKey PUT: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ForwardKey PUT status = %d, want 200 (body %q)", resp.StatusCode, resp.Body)
	}

	assertWrites(t, h.repl.writeCalls(), replCall{Op: OpPut, Key: "alpha", Value: []byte("one")})

	got, err := h.store.Get(context.Background(), "alpha")
	if err != nil || string(got) != "one" {
		t.Fatalf("local store after forwarded PUT = (%q, %v), want (\"one\", nil)", got, err)
	}
}

func TestForwardKeyDeleteReplicates(t *testing.T) {
	h := newHarness(t)

	if err := h.store.Put(context.Background(), "alpha", []byte("one")); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "DELETE", Key: "alpha",
	})
	if err != nil {
		t.Fatalf("ForwardKey DELETE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ForwardKey DELETE status = %d, want 200 (body %q)", resp.StatusCode, resp.Body)
	}

	assertWrites(t, h.repl.writeCalls(), replCall{Op: OpDelete, Key: "alpha"})
}

func TestForwardKeyDeleteMissingKeyIsIdempotentAndReplicates(t *testing.T) {
	h := newHarness(t)

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "DELETE", Key: "ghost",
	})
	if err != nil {
		t.Fatalf("ForwardKey DELETE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ForwardKey DELETE of absent key status = %d, want 200 (blind tombstone)", resp.StatusCode)
	}

	assertWrites(t, h.repl.writeCalls(), replCall{Op: OpDelete, Key: "ghost"})
}

func TestForwardKeyGetDoesNotReplicate(t *testing.T) {
	h := newHarness(t)

	if err := h.store.Put(context.Background(), "alpha", []byte("one")); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "GET", Key: "alpha",
	})
	if err != nil {
		t.Fatalf("ForwardKey GET: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ForwardKey GET status = %d, want 200", resp.StatusCode)
	}

	assertWrites(t, h.repl.writeCalls())
}

func TestForwardKeyPutReplicationFailureRefusesWrite(t *testing.T) {
	h := newHarness(t)
	h.repl.writeErr = errors.New("replicate to node2: connection refused")

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "PUT", Key: "alpha", Value: []byte("one"),
	})
	if err != nil {
		t.Fatalf("ForwardKey PUT: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ForwardKey PUT status = %d, want 503", resp.StatusCode)
	}
	if msg := forwardErrorMessage(t, resp); msg == "" {
		t.Error("forwarded replication failure carried no error message")
	}
}

// TestReplicateHandlerDoesNotFanOut is the loop guard. A replica applies an
// incoming mutation to its local store and stops there; if it also fanned out,
// every write would replicate forever.
func TestReplicateHandlerDoesNotFanOut(t *testing.T) {
	h := newHarness(t)

	resp, err := h.grpc.Replicate(context.Background(), &kvpb.ReplicateRequest{
		Op: OpPut, Key: "alpha", Value: []byte("one"), Term: 7,
	})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Replicate success = false, want true")
	}

	applies := h.repl.applyCalls()
	if len(applies) != 1 || applies[0].Op != OpPut || applies[0].Key != "alpha" {
		t.Fatalf("ApplyReplica calls = %+v, want exactly one put of \"alpha\"", applies)
	}

	assertWrites(t, h.repl.writeCalls()) // must be empty — no onward fan-out
}

func TestForwardKeyUnsupportedMethod(t *testing.T) {
	h := newHarness(t)

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "PATCH", Key: "alpha",
	})
	if err != nil {
		t.Fatalf("ForwardKey PATCH: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("ForwardKey PATCH status = %d, want 400", resp.StatusCode)
	}
	assertWrites(t, h.repl.writeCalls())
}

func TestForwardKeyGetMissingKey(t *testing.T) {
	h := newHarness(t)

	resp, err := h.grpc.ForwardKey(context.Background(), &kvpb.ForwardKeyRequest{
		Method: "GET", Key: "ghost",
	})
	if err != nil {
		t.Fatalf("ForwardKey GET: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ForwardKey GET of absent key status = %d, want 404", resp.StatusCode)
	}
	if _, err := h.store.Get(context.Background(), "ghost"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("store.Get(ghost) err = %v, want ErrNotFound", err)
	}
}
