package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

func TestOperationalEndpoints(t *testing.T) {
	h := newHarness(t)

	health := h.do(t, http.MethodGet, "/healthz", "")
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"ok"`) {
		t.Fatalf("GET /healthz = %d %s, want 200 ok", health.Code, health.Body.String())
	}

	ready := h.do(t, http.MethodGet, "/readyz", "")
	if ready.Code != http.StatusOK || !strings.Contains(ready.Body.String(), `"status":"ready"`) {
		t.Fatalf("GET /readyz = %d %s, want 200 ready", ready.Code, ready.Body.String())
	}
	if ready.Header().Get("X-Request-ID") == "" {
		t.Fatal("GET /readyz did not return X-Request-ID")
	}
}

func TestReadyWithoutRaftLeader(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.New(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ring := cluster.New()
	ring.AddNode(testNodeID, "node1:9001")
	h := NewHTTPServer(":0", st, stubRaft{id: testNodeID, noLeader: true}, ring,
		map[string]kvpb.KVServiceClient{}, &fakeReplicator{}, &metrics.Metrics{}, logger)

	rec := serveHTTP(t, h, http.MethodGet, "/readyz", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz without leader = %d, want 503", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if body["status"] != "not_ready" {
		t.Fatalf("readiness status = %q, want not_ready", body["status"])
	}
}

func TestPrometheusMetrics(t *testing.T) {
	h := newHarness(t)
	rec := h.do(t, http.MethodGet, "/metrics/prometheus", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics/prometheus = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", got)
	}
	for _, want := range []string{
		"# TYPE distrikv_put_total counter",
		"distrikv_put_total 0",
		"# TYPE distrikv_key_count gauge",
		"# TYPE distrikv_raft_term gauge",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("Prometheus response missing %q:\n%s", want, rec.Body.String())
		}
	}
}

func TestRequestIDIsPreserved(t *testing.T) {
	h := newHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "caller-123")
	rec := httptest.NewRecorder()
	h.http.srv.Handler.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "caller-123" {
		t.Fatalf("X-Request-ID = %q, want caller-123", got)
	}
}
