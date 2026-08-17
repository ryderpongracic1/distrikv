package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// replCall records one call observed by fakeReplicator.
type replCall struct {
	Op    string
	Key   string
	Value []byte
}

// fakeReplicator stands in for cmd/node.Node as the ReplicationManager. It
// records every fan-out and every replica-apply so tests can assert exactly
// which mutations left this node.
type fakeReplicator struct {
	mu       sync.Mutex
	writes   []replCall
	applies  []replCall
	writeErr error
}

func (f *fakeReplicator) ReplicateWrite(_ context.Context, op, key string, value []byte) error {
	f.mu.Lock()
	f.writes = append(f.writes, replCall{Op: op, Key: key, Value: value})
	f.mu.Unlock()
	return f.writeErr
}

func (f *fakeReplicator) ApplyReplica(_ context.Context, op, key string, value []byte) error {
	f.mu.Lock()
	f.applies = append(f.applies, replCall{Op: op, Key: key, Value: value})
	f.mu.Unlock()
	return nil
}

func (f *fakeReplicator) writeCalls() []replCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]replCall(nil), f.writes...)
}

func (f *fakeReplicator) applyCalls() []replCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]replCall(nil), f.applies...)
}

func (f *fakeReplicator) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = nil
	f.applies = nil
}

// stubRaft satisfies RaftInterface. Only ID and CurrentTerm are exercised by
// these tests; the RPC handlers are never reached because no Raft traffic is
// generated.
type stubRaft struct{ id string }

func (s stubRaft) HandleRequestVote(context.Context, *kvpb.RequestVoteRequest) (*kvpb.RequestVoteResponse, error) {
	return nil, errors.New("stubRaft: not exercised")
}

func (s stubRaft) HandleAppendEntries(context.Context, *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error) {
	return nil, errors.New("stubRaft: not exercised")
}

func (s stubRaft) HandlePreVote(context.Context, *kvpb.PreVoteRequest) (*kvpb.PreVoteResponse, error) {
	return nil, errors.New("stubRaft: not exercised")
}

func (s stubRaft) HandleInstallSnapshot(context.Context, *kvpb.InstallSnapshotRequest) (*kvpb.InstallSnapshotResponse, error) {
	return nil, errors.New("stubRaft: not exercised")
}

func (s stubRaft) IsLeader() bool      { return true }
func (s stubRaft) CurrentTerm() uint64 { return 1 }
func (s stubRaft) Leader() string      { return s.id }
func (s stubRaft) RoleString() string  { return "leader" }
func (s stubRaft) ID() string          { return s.id }

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const testNodeID = "node1"

// harness builds an HTTP server and a gRPC server that share one store and one
// replication manager, mirroring how cmd/node wires them. The ring contains
// only this node, so every key is locally owned and no request is forwarded.
type harness struct {
	http  *HTTPServer
	grpc  *GRPCServer
	repl  *fakeReplicator
	store *store.Store
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(testNodeID, "node1:9001")

	repl := &fakeReplicator{}
	raft := stubRaft{id: testNodeID}

	return &harness{
		http: NewHTTPServer(":0", st, raft, ring, map[string]kvpb.KVServiceClient{},
			repl, &metrics.Metrics{}, logger),
		grpc:  NewGRPCServer(":0", st, raft, ring, repl, logger),
		repl:  repl,
		store: st,
	}
}

// do issues an HTTP request against the server's registered routes.
func (h *harness) do(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	return serveHTTP(t, h.http, method, target, body)
}

// assertWrites checks the exact set of fan-out calls the primary made.
func assertWrites(t *testing.T, got []replCall, want ...replCall) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("replication fan-out: got %d call(s) %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Errorf("fan-out call %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func errorBody(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct{ Error string }
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return body.Error
}

// ---------------------------------------------------------------------------
// HTTP write path
// ---------------------------------------------------------------------------

func TestHandlePutReplicatesToReplicas(t *testing.T) {
	h := newHarness(t)

	rec := h.do(t, http.MethodPut, "/keys/alpha", `{"value":"one"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	assertWrites(t, h.repl.writeCalls(), replCall{Op: OpPut, Key: "alpha", Value: []byte("one")})

	got, err := h.store.Get(context.Background(), "alpha")
	if err != nil || string(got) != "one" {
		t.Fatalf("local store after PUT = (%q, %v), want (\"one\", nil)", got, err)
	}
}

func TestHandleDeleteReplicatesTombstone(t *testing.T) {
	h := newHarness(t)

	if rec := h.do(t, http.MethodPut, "/keys/alpha", `{"value":"one"}`); rec.Code != http.StatusOK {
		t.Fatalf("seed PUT status = %d, want 200", rec.Code)
	}
	h.repl.reset()

	rec := h.do(t, http.MethodDelete, "/keys/alpha", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	assertWrites(t, h.repl.writeCalls(), replCall{Op: OpDelete, Key: "alpha"})

	if _, err := h.store.Get(context.Background(), "alpha"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("local store after DELETE: err = %v, want ErrNotFound", err)
	}
}

func TestHandleDeleteMissingKeyIsIdempotentAndReplicates(t *testing.T) {
	h := newHarness(t)

	rec := h.do(t, http.MethodDelete, "/keys/ghost", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE of absent key status = %d, want 200 (blind tombstone)", rec.Code)
	}

	// The tombstone is a durable local write, so it replicates like any other.
	assertWrites(t, h.repl.writeCalls(), replCall{Op: OpDelete, Key: "ghost"})
}

func TestHandleGetDoesNotReplicate(t *testing.T) {
	h := newHarness(t)

	if rec := h.do(t, http.MethodPut, "/keys/alpha", `{"value":"one"}`); rec.Code != http.StatusOK {
		t.Fatalf("seed PUT status = %d, want 200", rec.Code)
	}
	h.repl.reset()

	rec := h.do(t, http.MethodGet, "/keys/alpha", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", rec.Code)
	}

	assertWrites(t, h.repl.writeCalls())
}

// TestHandlePutReplicationFailureRefusesWrite pins the CP failure semantics:
// the client is told the write was refused, and — because no rollback exists —
// the value is still present on the primary. Both halves of that are the
// documented contract in the CAP Position section of docs/architecture.md.
func TestHandlePutReplicationFailureRefusesWrite(t *testing.T) {
	h := newHarness(t)
	h.repl.writeErr = errors.New("replicate to node2: connection refused")

	rec := h.do(t, http.MethodPut, "/keys/alpha", `{"value":"one"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	if msg := errorBody(t, rec); !strings.Contains(msg, "replication") {
		t.Errorf("error body = %q, want it to mention replication", msg)
	}

	got, err := h.store.Get(context.Background(), "alpha")
	if err != nil || string(got) != "one" {
		t.Fatalf("primary retains the write: got (%q, %v), want (\"one\", nil)", got, err)
	}
}

func TestHandleDeleteReplicationFailureRefusesWrite(t *testing.T) {
	h := newHarness(t)

	if rec := h.do(t, http.MethodPut, "/keys/alpha", `{"value":"one"}`); rec.Code != http.StatusOK {
		t.Fatalf("seed PUT status = %d, want 200", rec.Code)
	}
	h.repl.reset()
	h.repl.writeErr = errors.New("replicate to node2: connection refused")

	rec := h.do(t, http.MethodDelete, "/keys/alpha", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("DELETE status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}

	// The tombstone is retained locally even though the fan-out failed.
	if _, err := h.store.Get(context.Background(), "alpha"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("local store after failed-replication DELETE: err = %v, want ErrNotFound", err)
	}
}

// TestMissingReplicationManagerRefusesWrite guards against regressing to the
// original defect: an unwired replication path must fail loudly rather than
// silently accept single-copy writes.
func TestMissingReplicationManagerRefusesWrite(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(testNodeID, "node1:9001")

	h := NewHTTPServer(":0", st, stubRaft{id: testNodeID}, ring,
		map[string]kvpb.KVServiceClient{}, nil, &metrics.Metrics{}, logger)

	req := httptest.NewRequest(http.MethodPut, "/keys/alpha", strings.NewReader(`{"value":"one"}`))
	rec := httptest.NewRecorder()
	h.srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT with no replication manager: status = %d, want 503", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// statusForWriteError
// ---------------------------------------------------------------------------

func TestStatusForWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"not found", store.ErrNotFound, http.StatusNotFound},
		{"replication", errors.Join(ErrReplication, errors.New("node2 down")), http.StatusServiceUnavailable},
		{"other", errors.New("disk full"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := statusForWriteError(tc.err); got != tc.want {
				t.Errorf("statusForWriteError(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}
