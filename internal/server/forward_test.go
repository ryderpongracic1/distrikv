package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// remoteNodeID is the ring-primary that this node forwards to in these tests.
const remoteNodeID = "node2"

// forwardBudget is the stopwatch bound for a forward to an unreachable primary.
//
// The bound that actually matters is the HTTP server's WriteTimeout: past it the
// connection is closed with no response written at all, which is the defect
// these tests pin (curl reports exit code 000). forwardBudget sits comfortably
// above defaultForwardTimeout — leaving headroom for a loaded CI box — and
// comfortably below WriteTimeout, which the tests assert rather than assume.
const forwardBudget = 5 * time.Second

// ---------------------------------------------------------------------------
// Forward-path harness
// ---------------------------------------------------------------------------

// newForwardServer builds an HTTPServer whose ring holds this node plus one
// remote node reachable at remoteAddr, and returns it together with a key the
// ring assigns to that remote node — so every request for that key takes the
// forward path.
//
// The peer client is a real gRPC client built exactly as production builds it
// (see DialPeerWithRetry): grpc.NewClient is lazy, so it is created
// successfully even when remoteAddr is a blackhole, and the connection is only
// attempted when the first RPC is issued.
func newForwardServer(t *testing.T, remoteAddr string) (*HTTPServer, string) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(testNodeID, "node1:9001")
	ring.AddNode(remoteNodeID, remoteAddr)

	conn, err := grpc.NewClient(remoteAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("build peer client for %s: %v", remoteAddr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	h := NewHTTPServer(":0", st, stubRaft{id: testNodeID}, ring,
		map[string]kvpb.KVServiceClient{remoteNodeID: NewPeerClient(conn)},
		&fakeReplicator{}, &metrics.Metrics{}, logger)

	return h, keyOwnedBy(t, ring, remoteNodeID)
}

// keyOwnedBy returns a key whose ring-primary is nodeID. Searching for one
// keeps these tests independent of the ring's hash function: a hard-coded key
// would silently stop exercising the forward path if hashing ever changed.
func keyOwnedBy(t *testing.T, ring *cluster.Ring, nodeID string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("forward-key-%d", i)
		primary, err := ring.Get(key)
		if err != nil {
			t.Fatalf("ring lookup %q: %v", key, err)
		}
		if primary.NodeID == nodeID {
			return key
		}
	}
	t.Fatalf("no key among 1000 candidates is owned by %s", nodeID)
	return ""
}

// blackholeAddr returns the address of a TCP listener that is never accepted on.
//
// This reproduces the production hang faithfully. The kernel completes the TCP
// handshake from the listen backlog, but because nothing ever calls Accept the
// gRPC HTTP/2 handshake never finishes, so the channel stays in CONNECTING —
// the same state a stopped container's address produces, and the state in which
// a fail-fast RPC blocks instead of failing. A closed port would be no test at
// all: it answers with an immediate RST, which fails the RPC promptly even
// without a deadline. A non-routable address would depend on packets being
// silently dropped rather than rejected, which a container's routing table does
// not guarantee; a local unaccepted listener needs no network at all.
func blackholeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("open blackhole listener: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return lis.Addr().String()
}

// startPeerServer serves a real KVService on a loopback port, standing in for a
// healthy ring-primary. Its ring contains only itself, so it owns every key it
// is asked about and handles forwarded writes on its local primary-write path.
func startPeerServer(t *testing.T) string {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open peer store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(remoteNodeID, "node2:9002")

	peer := NewGRPCServer(":0", st, stubRaft{id: remoteNodeID}, ring, &fakeReplicator{}, logger)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for peer server: %v", err)
	}
	go func() { _ = peer.srv.Serve(lis) }()
	t.Cleanup(peer.srv.Stop)

	return lis.Addr().String()
}

// serveHTTP issues a request against a server's registered routes.
func serveHTTP(t *testing.T, h *HTTPServer, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.srv.Handler.ServeHTTP(rec, req)
	return rec
}

// closedPortAddr returns the address of a listener that has been closed, so
// connecting to it is refused immediately with ECONNREFUSED.
//
// This is the complement of blackholeAddr, and the two exist for opposite
// reasons. A closed port is useless for testing the forward *deadline* — it
// answers with an RST, so the RPC fails promptly even with no deadline at all —
// but it is the only way to produce the one outcome that is provably never-sent:
// a connection that was refused before any byte of the RPC could be written.
func closedPortAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("open listener to close: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// forwardOutcomeOf decodes the forward_outcome field from a 502 body, failing the
// test if it is absent — a 502 without it is the regression this pins.
func forwardOutcomeOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error          string  `json:"error"`
		ForwardOutcome *string `json:"forward_outcome"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode 502 body %q: %v", rec.Body.String(), err)
	}
	if body.ForwardOutcome == nil {
		t.Fatalf("502 body %q has no forward_outcome field: the chaos runner has to "+
			"fall back to scanning prose, which classes every forward to a dead "+
			"primary as unknown and times the linearizability check out",
			rec.Body.String())
	}
	return *body.ForwardOutcome
}

// ---------------------------------------------------------------------------
// Unreachable primary
// ---------------------------------------------------------------------------

// TestForwardToUnreachablePrimaryFailsFast pins the defect: a request for a key
// whose primary has vanished must come back 502 well inside the HTTP
// WriteTimeout. Before the forward RPC carried its own deadline it inherited the
// request context, which has none, so the call blocked until gRPC's own ~20s
// connect timeout — past WriteTimeout, at which point the connection was closed
// with nothing written and the client saw no response at all.
func TestForwardToUnreachablePrimaryFailsFast(t *testing.T) {
	for _, tc := range []struct {
		method string
		body   string
	}{
		{method: http.MethodPut, body: `{"value":"one"}`},
		{method: http.MethodGet},
		{method: http.MethodDelete},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()

			h, key := newForwardServer(t, blackholeAddr(t))

			// Keep the budget honest if the server's timeouts ever change:
			// a budget at or above WriteTimeout would assert nothing.
			if forwardBudget >= h.srv.WriteTimeout {
				t.Fatalf("forwardBudget (%s) must be below the server WriteTimeout (%s)",
					forwardBudget, h.srv.WriteTimeout)
			}

			start := time.Now()
			rec := serveHTTP(t, h, tc.method, "/keys/"+key, tc.body)
			elapsed := time.Since(start)

			if rec.Code != http.StatusBadGateway {
				t.Fatalf("%s forwarded to blackholed primary: status = %d, want 502 (body %q)",
					tc.method, rec.Code, rec.Body.String())
			}
			if elapsed > forwardBudget {
				t.Fatalf("%s forwarded to blackholed primary answered after %s, want < %s "+
					"(WriteTimeout is %s — past it the client gets no response at all)",
					tc.method, elapsed, forwardBudget, h.srv.WriteTimeout)
			}
			if msg := errorBody(t, rec); !strings.Contains(msg, remoteNodeID) {
				t.Errorf("error body %q does not name the unreachable primary %q", msg, remoteNodeID)
			}

			// A blackholed address is the ambiguous case, and it must say so. The
			// deadline fires while the channel is still CONNECTING, and gRPC
			// reports that as DeadlineExceeded with a message about waiting for a
			// load-balancer update — it names no transport failure, so nothing
			// about delivery is provable.
			if got := forwardOutcomeOf(t, rec); got != string(forwardUnknown) {
				t.Errorf("%s forwarded to blackholed primary: forward_outcome = %q, want %q "+
					"(claiming never-sent here would tell the model a write did not "+
					"happen when it may have)",
					tc.method, got, forwardUnknown)
			}
		})
	}
}

// TestForwardToRefusedPrimaryIsNeverSent is the other half of the pair: a primary
// whose port refuses the connection could not have received the request, and the
// 502 must say so in a form a consumer can read without parsing prose.
//
// This is the class that dominates a real fault window — a stopped container's
// address refuses connections — so it is the class whose misclassification pushed
// the chaos checker from a verdict into a timeout: tens of thousands of pending
// operations, each overlapping every later operation on its key.
func TestForwardToRefusedPrimaryIsNeverSent(t *testing.T) {
	for _, tc := range []struct {
		method string
		body   string
	}{
		{method: http.MethodPut, body: `{"value":"one"}`},
		{method: http.MethodGet},
		{method: http.MethodDelete},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()

			h, key := newForwardServer(t, closedPortAddr(t))

			rec := serveHTTP(t, h, tc.method, "/keys/"+key, tc.body)

			if rec.Code != http.StatusBadGateway {
				t.Fatalf("%s forwarded to refused primary: status = %d, want 502 (body %q)",
					tc.method, rec.Code, rec.Body.String())
			}
			if got := forwardOutcomeOf(t, rec); got != string(forwardNeverSent) {
				t.Errorf("%s forwarded to refused primary: forward_outcome = %q, want %q "+
					"(a refused connection got no SYN-ACK, so no byte of the RPC "+
					"could have been written)",
					tc.method, got, forwardNeverSent)
			}
			if msg := errorBody(t, rec); !strings.Contains(msg, remoteNodeID) {
				t.Errorf("error body %q does not name the unreachable primary %q", msg, remoteNodeID)
			}
		})
	}
}

// TestForwardWithNoPeerClientIsNeverSent covers the structural never-sent case:
// the ring names a primary this node holds no gRPC client for, so no RPC is
// attempted at all.
func TestForwardWithNoPeerClientIsNeverSent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(testNodeID, "node1:9001")
	ring.AddNode(remoteNodeID, "node2:9002")

	// Empty peers map: the ring routes to remoteNodeID, and there is no client.
	h := NewHTTPServer(":0", st, stubRaft{id: testNodeID}, ring,
		map[string]kvpb.KVServiceClient{}, &fakeReplicator{}, &metrics.Metrics{}, logger)

	rec := serveHTTP(t, h, http.MethodPut, "/keys/"+keyOwnedBy(t, ring, remoteNodeID), `{"value":"one"}`)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("PUT with no peer client: status = %d, want 502 (body %q)", rec.Code, rec.Body.String())
	}
	if got := forwardOutcomeOf(t, rec); got != string(forwardNeverSent) {
		t.Errorf("PUT with no peer client: forward_outcome = %q, want %q (no RPC was attempted)",
			got, forwardNeverSent)
	}
}

// TestForwardOutcomeWireStringsAreFrozen pins the producing half of a contract
// that is duplicated rather than shared.
//
// cmd/chaos consumes this field over HTTP and cannot import these constants —
// they are unexported, and the runner deliberately talks to a cluster the way a
// client does rather than by importing its internals. So it declares its own
// copies, and a rename here would otherwise satisfy every test on both sides
// while silently breaking the running system: the tests above compare against
// the constant, so they move with it.
//
// Asserting the literal strings is what makes that impossible. Change a value
// here and this test fails, naming cmd/chaos as the other place to change.
func TestForwardOutcomeWireStringsAreFrozen(t *testing.T) {
	for _, tc := range []struct {
		got  forwardOutcome
		want string
	}{
		{forwardNeverSent, "never-sent"},
		{forwardUnknown, "unknown"},
	} {
		if string(tc.got) != tc.want {
			t.Errorf("forward_outcome wire value = %q, want %q — cmd/chaos hard-codes "+
				"this string (see forwardOutcomeNeverSent/forwardOutcomeUnknown there) "+
				"and must change with it", tc.got, tc.want)
		}
	}
}

// TestForwardErrorsCarryNoTypedCause asserts the premise the whole design rests
// on, and the reason the classification cannot simply be moved into the client:
// a grpc-go RPC error is a *status.Error carrying a code and a string, and it
// does not wrap the transport failure underneath it.
//
// If a future grpc-go starts preserving that chain, this test fails — and that
// failure is the signal to replace the message matching in classifyForwardError
// with errors.Is/errors.As, which would be strictly better.
func TestForwardErrorsCarryNoTypedCause(t *testing.T) {
	conn, err := grpc.NewClient(closedPortAddr(t), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("build peer client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), forwardBudget)
	defer cancel()

	_, rpcErr := NewPeerClient(conn).ForwardKey(ctx, &kvpb.ForwardKeyRequest{
		Method: "PUT", Key: "k", Value: []byte("v"),
	})
	if rpcErr == nil {
		t.Fatal("ForwardKey to a refused port returned no error")
	}

	// The code and message are the entire evidence available.
	if got := status.Code(rpcErr); got != codes.Unavailable {
		t.Errorf("refused dial: code = %v, want %v", got, codes.Unavailable)
	}
	if msg := status.Convert(rpcErr).Message(); !strings.Contains(msg, "connection refused") {
		t.Errorf("refused dial: message = %q, want it to name the refusal", msg)
	}

	if unwrapped := errors.Unwrap(rpcErr); unwrapped != nil {
		t.Errorf("errors.Unwrap = %v, want nil — grpc-go now preserves the cause, so "+
			"classifyForwardError should match on the chain instead of the message", unwrapped)
	}
	if errors.Is(rpcErr, syscall.ECONNREFUSED) {
		t.Error("errors.Is(err, ECONNREFUSED) is now true — classifyForwardError should " +
			"use the typed chain instead of the message")
	}
	var opErr *net.OpError
	if errors.As(rpcErr, &opErr) {
		t.Error("errors.As(*net.OpError) is now true — classifyForwardError should " +
			"use the typed chain instead of the message")
	}
}

// TestClassifyForwardError is the decision table, exercised directly so the
// cases that are awkward to provoke through a real network are still pinned —
// above all the ones that must NOT be read as never-sent.
func TestClassifyForwardError(t *testing.T) {
	// Verbatim messages observed from grpc-go v1.x against a real cluster.
	const (
		refusedDial  = `connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:45983: connect: connection refused"`
		blackholeMsg = "received context error while waiting for new LB policy update: context deadline exceeded"
		resolverMsg  = "name resolver error: produced zero addresses"
	)

	for _, tc := range []struct {
		name string
		err  error
		want forwardOutcome
	}{
		{"refused dial", status.Error(codes.Unavailable, refusedDial), forwardNeverSent},
		{"unroutable dial", status.Error(codes.Unavailable,
			`connection error: desc = "transport: Error while dialing: dial tcp 192.0.2.1:9002: connect: no route to host"`),
			forwardNeverSent},
		{"unresolvable target", status.Error(codes.Unavailable, resolverMsg), forwardNeverSent},

		// Ambiguous: the request may have been written before the stream broke.
		{"stream broken after send", status.Error(codes.Unavailable, "transport is closing"), forwardUnknown},
		{"reset after send", status.Error(codes.Unavailable,
			"error reading from server: read tcp 127.0.0.1:5: connection reset by peer"), forwardUnknown},
		{"server draining", status.Error(codes.Unavailable, "the connection is draining"), forwardUnknown},

		// The trap. codes.Unavailable is a legal application code, and a primary
		// that could not reach a replica can produce a message quoting a refused
		// connection from its *own* fan-out. Reading the marker without the dial
		// framing would call that never-sent — the exact mistake that made the
		// chaos runner classify refused-but-applied writes as no-ops.
		{"remote status quoting a refused replica", status.Error(codes.Unavailable,
			"replicate to node3: connect: connection refused"), forwardUnknown},

		// The deadline can fire after the primary applied the mutation.
		{"deadline while connecting", status.Error(codes.DeadlineExceeded, blackholeMsg), forwardUnknown},
		{"deadline mid-RPC", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), forwardUnknown},
		{"cancelled", status.Error(codes.Canceled, "context canceled"), forwardUnknown},

		// A code only the remote can produce implies the request arrived.
		{"remote internal error", status.Error(codes.Internal, "boom"), forwardUnknown},
		{"unimplemented", status.Error(codes.Unimplemented, "unknown method ForwardKey"), forwardUnknown},

		// A dial framing with a failure we do not recognise is deliberately not
		// claimed: probably never sent, but "probably" is not the bar, and the
		// only cost of declining is checker time.
		{"dial timeout", status.Error(codes.Unavailable,
			`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:9002: i/o timeout"`),
			forwardUnknown},

		// Not a status error at all, and no error at all.
		{"plain error", errors.New("connection refused"), forwardUnknown},
		{"nil", nil, forwardUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyForwardError(tc.err); got != tc.want {
				t.Errorf("classifyForwardError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Healthy primary (regression)
// ---------------------------------------------------------------------------

// TestForwardToHealthyPrimarySucceeds is the regression guard for the deadline:
// a live primary must still serve forwarded writes and reads normally.
func TestForwardToHealthyPrimarySucceeds(t *testing.T) {
	h, key := newForwardServer(t, startPeerServer(t))

	rec := serveHTTP(t, h, http.MethodPut, "/keys/"+key, `{"value":"one"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT forwarded to healthy primary: status = %d, want 200 (body %q)",
			rec.Code, rec.Body.String())
	}

	rec = serveHTTP(t, h, http.MethodGet, "/keys/"+key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET forwarded to healthy primary: status = %d, want 200 (body %q)",
			rec.Code, rec.Body.String())
	}

	var got struct{ Value string }
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode forwarded GET body %q: %v", rec.Body.String(), err)
	}
	if got.Value != "one" {
		t.Errorf("forwarded GET value = %q, want %q", got.Value, "one")
	}

	rec = serveHTTP(t, h, http.MethodDelete, "/keys/"+key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE forwarded to healthy primary: status = %d, want 200 (body %q)",
			rec.Code, rec.Body.String())
	}
}
