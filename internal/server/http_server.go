package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// HTTPServer wraps net/http.Server and provides the client-facing REST API.
// It routes requests to the local store when this node owns the key, and
// forwards them via gRPC when another node is the ring-primary.
type HTTPServer struct {
	addr        string
	srv         *http.Server
	store       *store.Store
	raft        RaftInterface
	ring        *cluster.Ring
	peers       map[string]kvpb.KVServiceClient // nodeID → gRPC client
	metrics     *metrics.Metrics
	logger      *slog.Logger
}

// NewHTTPServer constructs an HTTPServer and registers all routes. Call Start
// to begin listening.
func NewHTTPServer(
	addr string,
	s *store.Store,
	r RaftInterface,
	ring *cluster.Ring,
	peers map[string]kvpb.KVServiceClient,
	m *metrics.Metrics,
	logger *slog.Logger,
) *HTTPServer {
	h := &HTTPServer{
		addr:    addr,
		store:   s,
		raft:    r,
		ring:    ring,
		peers:   peers,
		metrics: m,
		logger:  logger.With("component", "http"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /keys/{key}", h.handlePut)
	mux.HandleFunc("GET /keys/{key}", h.handleGet)
	mux.HandleFunc("DELETE /keys/{key}", h.handleDelete)
	mux.HandleFunc("GET /status", h.handleStatus)
	mux.HandleFunc("GET /metrics", h.handleMetrics)

	h.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return h
}

// Start begins listening on the configured address. It blocks until ctx is
// cancelled, then performs a graceful shutdown with a 5-second deadline.
func (h *HTTPServer) Start(ctx context.Context) error {
	h.logger.Info("HTTP server listening", "addr", h.addr)

	errCh := make(chan error, 1)
	go func() {
		if err := h.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.srv.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("http: shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("http: serve: %w", err)
	}
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// handlePut implements PUT /keys/{key}
// Body: {"value": "<string>"}
func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %s", err))
		return
	}

	h.metrics.PutTotal.Add(1)

	if !h.isLocalOwner(key) {
		h.metrics.ForwardedRequests.Add(1)
		h.forwardRequest(w, r, key, "PUT", []byte(body.Value))
		return
	}

	if err := h.store.Put(r.Context(), key, []byte(body.Value)); err != nil {
		h.logger.Error("Put failed", "key", key, "error", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleGet implements GET /keys/{key}
func (h *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	h.metrics.GetTotal.Add(1)

	if !h.isLocalOwner(key) {
		h.metrics.ForwardedRequests.Add(1)
		h.forwardRequest(w, r, key, "GET", nil)
		return
	}

	val, err := h.store.Get(r.Context(), key)
	if errors.Is(err, store.ErrNotFound) {
		h.metrics.GetMiss.Add(1)
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"value": string(val)})
}

// handleDelete implements DELETE /keys/{key}
func (h *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing key")
		return
	}

	h.metrics.DeleteTotal.Add(1)

	if !h.isLocalOwner(key) {
		h.metrics.ForwardedRequests.Add(1)
		h.forwardRequest(w, r, key, "DELETE", nil)
		return
	}

	if err := h.store.Delete(r.Context(), key); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

// handleStatus implements GET /status
func (h *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"node_id":   h.raft.ID(),
		"leader":    h.raft.Leader(),
		"term":      h.raft.CurrentTerm(),
		"role":      h.raft.RoleString(),
		"key_count": h.store.KeyCount(),
	})
}

// handleMetrics implements GET /metrics
func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := h.metrics.Snapshot()
	snap["key_count"] = uint64(h.store.KeyCount())
	snap["raft_term"] = h.raft.CurrentTerm()
	writeJSON(w, snap)
}

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------

// isLocalOwner returns true if this node is the ring-primary for key. Falls
// back to true on ring errors (empty ring) so single-node deployments work.
func (h *HTTPServer) isLocalOwner(key string) bool {
	if h.ring == nil || h.ring.NodeCount() == 0 {
		return true
	}
	primary, err := h.ring.Get(key)
	if err != nil {
		return true // degrade gracefully
	}
	return primary.NodeID == h.raft.ID()
}

// forwardRequest sends the operation to the ring-primary via gRPC and writes
// the response back to the HTTP client.
func (h *HTTPServer) forwardRequest(w http.ResponseWriter, r *http.Request, key, method string, value []byte) {
	primary, err := h.ring.Get(key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("ring lookup: %s", err))
		return
	}

	client, ok := h.peers[primary.NodeID]
	if !ok {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("no gRPC client for node %s", primary.NodeID))
		return
	}

	h.logger.Debug("forwarding request", "method", method, "key", key, "to", primary.NodeID)

	resp, err := client.ForwardKey(r.Context(), &kvpb.ForwardKeyRequest{
		Method: method,
		Key:    key,
		Value:  value,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("forward RPC: %s", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(resp.StatusCode))
	if len(resp.Body) > 0 {
		w.Write(resp.Body)
	}
}

// PeerClient returns the gRPC client for the given peer node ID, plus a bool
// indicating whether the peer is known.
func (h *HTTPServer) PeerClient(nodeID string) (kvpb.KVServiceClient, bool) {
	c, ok := h.peers[nodeID]
	return c, ok
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encoding error", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
