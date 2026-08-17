package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// ?local=true is the only way to ask a *replica* what it holds. Every other read
// path forwards a key to its ring-primary, which is correct for clients and
// useless for verifying convergence: it would answer with the primary's value
// from whichever node was asked, so a divergent replica would look identical to a
// converged one and every chaos run would pass.
//
// These tests pin that behaviour, because the chaos harness's convergence gate is
// only as trustworthy as this flag.

// localReadHarness builds a server that owns nothing: its ring has a remote
// primary for the test key and no peer client for it, so any forward attempt fails
// loudly rather than quietly succeeding.
func localReadHarness(t *testing.T) (*HTTPServer, *store.Store, string) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(testNodeID, "node1:9001")
	ring.AddNode(remoteNodeID, "node2:9002")

	h := NewHTTPServer(":0", st, stubRaft{id: testNodeID}, ring,
		map[string]kvpb.KVServiceClient{}, &fakeReplicator{}, &metrics.Metrics{}, logger)

	return h, st, keyOwnedBy(t, ring, remoteNodeID)
}

func TestLocalReadAnswersFromThisNodeWithoutForwarding(t *testing.T) {
	h, st, foreignKey := localReadHarness(t)

	// The value is present locally but this node is not the key's primary — the
	// exact position a replica is in.
	if err := st.Put(context.Background(), foreignKey, []byte("replica-copy")); err != nil {
		t.Fatalf("local put: %v", err)
	}

	rec := serveHTTP(t, h, http.MethodGet, "/keys/"+foreignKey+"?local=true", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("local read = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	var body struct{ Value string }
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Value != "replica-copy" {
		t.Errorf("local read returned %q, want the value this node holds", body.Value)
	}

	// The same request without the flag must forward, and with no client for the
	// primary it must fail — which is what proves the flag changed the path rather
	// than the answer happening to match.
	forwarded := serveHTTP(t, h, http.MethodGet, "/keys/"+foreignKey, "")
	if forwarded.Code == http.StatusOK {
		t.Errorf("a plain GET for a key this node does not own returned 200; it must forward")
	}
}

func TestLocalReadReportsAbsenceRatherThanForwarding(t *testing.T) {
	h, _, foreignKey := localReadHarness(t)

	rec := serveHTTP(t, h, http.MethodGet, "/keys/"+foreignKey+"?local=true", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("local read of an absent key = %d, want 404 (a replica that is missing a "+
			"key must say so, not forward); body %s", rec.Code, rec.Body.String())
	}
}

func TestLocalReadFlagSpellings(t *testing.T) {
	h, st, foreignKey := localReadHarness(t)
	if err := st.Put(context.Background(), foreignKey, []byte("v")); err != nil {
		t.Fatalf("local put: %v", err)
	}

	for _, query := range []string{"?local=true", "?local=1", "?local=yes", "?local", "?local="} {
		rec := serveHTTP(t, h, http.MethodGet, "/keys/"+foreignKey+query, "")
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", query, rec.Code)
		}
	}
	for _, query := range []string{"?local=false", "?local=0", "?local=no"} {
		rec := serveHTTP(t, h, http.MethodGet, "/keys/"+foreignKey+query, "")
		if rec.Code == http.StatusOK {
			t.Errorf("GET %s = 200; an explicit false must not switch off forwarding", query)
		}
	}
}

func TestLocalReadOnAnOwnedKeyIsUnchanged(t *testing.T) {
	h, st, _ := localReadHarness(t)
	owned := keyOwnedBy(t, h.ring, testNodeID)

	if err := st.Put(context.Background(), owned, []byte("mine")); err != nil {
		t.Fatalf("local put: %v", err)
	}
	for _, target := range []string{"/keys/" + owned, "/keys/" + owned + "?local=true"} {
		rec := serveHTTP(t, h, http.MethodGet, target, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", target, rec.Code)
		}
	}
}
