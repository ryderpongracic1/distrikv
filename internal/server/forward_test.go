package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
