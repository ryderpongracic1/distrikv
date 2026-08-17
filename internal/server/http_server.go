package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// defaultForwardTimeout bounds a single forward RPC to the ring-primary, so a
// primary that has vanished costs the client one bounded 502 instead of no
// response at all.
//
// The client's request context cannot supply that bound on its own: it carries
// no deadline, and a host that is simply gone (a stopped container) never sends
// a TCP RST, so the gRPC channel parks in CONNECTING. A fail-fast RPC — the
// default, and what this client uses — only returns immediately once the
// channel reaches TRANSIENT_FAILURE; while it is CONNECTING the RPC blocks.
// The channel therefore does not give up until gRPC's own ~20s connect timeout,
// which is longer than this server's 10s WriteTimeout: the connection gets
// closed with nothing written and curl reports exit code 000.
//
// 2s is far above a healthy forward (sub-millisecond in-cluster, plus the
// primary's replication fan-out, itself bounded by 2×HeartbeatInterval) and far
// below WriteTimeout, so the deadline only ever fires on a real fault.
const defaultForwardTimeout = 2 * time.Second

// HTTPServer wraps net/http.Server and provides the client-facing REST API.
// It routes requests to the local store when this node owns the key, and
// forwards them via gRPC when another node is the ring-primary.
type HTTPServer struct {
	addr    string
	srv     *http.Server
	store   *store.Store
	raft    RaftInterface
	ring    *cluster.Ring
	peers   map[string]kvpb.KVServiceClient // nodeID → gRPC client
	writer  *primaryWriter
	metrics *metrics.Metrics
	logger  *slog.Logger
}

// NewHTTPServer constructs an HTTPServer and registers all routes. Call Start
// to begin listening.
//
// repMgr supplies the replication fan-out used when this node is the
// ring-primary for a mutated key; it must be non-nil for writes to succeed.
func NewHTTPServer(
	addr string,
	s *store.Store,
	r RaftInterface,
	ring *cluster.Ring,
	peers map[string]kvpb.KVServiceClient,
	repMgr ReplicationManager,
	m *metrics.Metrics,
	logger *slog.Logger,
) *HTTPServer {
	log := logger.With("component", "http")
	h := &HTTPServer{
		addr:    addr,
		store:   s,
		raft:    r,
		ring:    ring,
		peers:   peers,
		writer:  newPrimaryWriter(s, repMgr, log),
		metrics: m,
		logger:  log,
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

	if err := h.writer.Put(r.Context(), key, []byte(body.Value)); err != nil {
		code := statusForWriteError(err)
		h.logger.Error("Put failed", "key", key, "status", code, "error", err)
		writeError(w, code, err.Error())
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

	// ?local=true answers from this node's own store and never forwards, which is
	// the only way to ask a *replica* what it holds: an ordinary GET on a
	// non-owning node forwards to the ring-primary and would answer with the
	// primary's value, hiding exactly the divergence a convergence check is
	// looking for.
	//
	// It exposes no data an ordinary GET could not already reach — the same keys
	// are readable through the forwarding path — but it does expose per-replica
	// state, and like the rest of this API it is unauthenticated and intended for
	// a trusted cluster network. Do not expose distrikv's HTTP port publicly.
	if localOnly(r) {
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
		return
	}

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

	// Deletes are blind tombstone writes in the storage engine, so DELETE is
	// idempotent: removing a key that does not exist succeeds and is replicated
	// like any other write. There is no 404 path here — see store.Store.Delete
	// for why the previous Get-then-Delete existence check was racy and expensive.
	if err := h.writer.Delete(r.Context(), key); err != nil {
		code := statusForWriteError(err)
		h.logger.Error("Delete failed", "key", key, "status", code, "error", err)
		writeError(w, code, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

// handleStatus implements GET /status
func (h *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"node_id": h.raft.ID(),
		"leader":  h.raft.Leader(),
		"term":    h.raft.CurrentTerm(),
		"role":    h.raft.RoleString(),
		// key_count is an approximation maintained by the LSM engine; the
		// companion flag makes that explicit to programmatic consumers rather
		// than leaving them to assume an exact count.
		"key_count":             h.store.KeyCount(),
		"key_count_approximate": true,
	})
}

// handleMetrics implements GET /metrics
func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := h.metrics.Snapshot()
	snap["key_count"] = uint64(h.store.KeyCount())
	snap["raft_term"] = h.raft.CurrentTerm()

	puts, gets, dels, walWrites := h.store.Counts()
	snap["put_total"] = puts
	snap["get_total"] = gets
	snap["delete_total"] = dels
	snap["wal_writes"] = walWrites

	writeJSON(w, snap)
}

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------

// localOnly reports whether the request asked to be answered from this node's
// own store without forwarding. Any of ?local, ?local=1, ?local=true and
// ?local=yes count; an explicit false-y value does not.
func localOnly(r *http.Request) bool {
	if !r.URL.Query().Has("local") {
		return false
	}
	switch strings.ToLower(r.URL.Query().Get("local")) {
	case "", "1", "true", "yes":
		return true
	default:
		return false
	}
}

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
//
// The RPC is bounded by defaultForwardTimeout rather than run on the request
// context alone, so an unreachable primary produces a prompt 502 instead of
// hanging until the HTTP WriteTimeout closes the connection unanswered.
//
// Every 502 this function writes carries a "forward_outcome" field saying whether
// the request provably never reached the primary. That judgement is made here
// because this is the last place the gRPC error still has its code, and the
// status message has not yet been flattened into prose — see
// classifyForwardError.
func (h *HTTPServer) forwardRequest(w http.ResponseWriter, r *http.Request, key, method string, value []byte) {
	primary, err := h.ring.Get(key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("ring lookup: %s", err))
		return
	}

	client, ok := h.peers[primary.NodeID]
	if !ok {
		// No RPC is attempted at all, so nothing could have been delivered.
		writeForwardError(w, fmt.Sprintf("no gRPC client for node %s", primary.NodeID),
			forwardNeverSent)
		return
	}

	h.logger.Debug("forwarding request", "method", method, "key", key, "to", primary.NodeID)

	ctx, cancel := context.WithTimeout(r.Context(), defaultForwardTimeout)
	defer cancel()

	resp, err := client.ForwardKey(ctx, &kvpb.ForwardKeyRequest{
		Method: method,
		Key:    key,
		Value:  value,
	})
	if err != nil {
		code := status.Code(err)
		// Classify before flattening to text: this is the last point at which the
		// error still carries its gRPC code, and the message is at its freshest.
		// See classifyForwardError for what is and is not provable.
		outcome := classifyForwardError(err)

		msg := fmt.Sprintf("forward RPC to primary %s: %s", primary.NodeID, err)
		switch {
		case outcome == forwardNeverSent:
			// Do not claim a timeout that did not happen: a refused connection
			// fails in milliseconds, nowhere near defaultForwardTimeout.
			msg = fmt.Sprintf("primary %s unreachable: forward RPC was not delivered", primary.NodeID)
		case code == codes.DeadlineExceeded || code == codes.Unavailable:
			// The primary is unreachable, not misbehaving: say so plainly
			// rather than surfacing a raw gRPC status to the client.
			msg = fmt.Sprintf("primary %s unreachable: forward RPC did not complete within %s",
				primary.NodeID, defaultForwardTimeout)
		}
		h.logger.Warn("forward failed",
			"method", method, "key", key, "to", primary.NodeID,
			"grpc_code", code.String(), "forward_outcome", string(outcome), "error", err)
		writeForwardError(w, msg, outcome)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(resp.StatusCode))
	if len(resp.Body) > 0 {
		w.Write(resp.Body)
	}
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

// writeForwardError writes the 502 a failed forward produces: the same error
// shape every other failure uses, plus the typed outcome.
//
// The outcome travels in the body rather than a header because internal/client
// already captures the body verbatim on a *StatusError, so a consumer gets it
// for free through the existing chain. A field is additive: a reader that does
// not know it still sees the same "error" string it always did.
func writeForwardError(w http.ResponseWriter, msg string, outcome forwardOutcome) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	_ = json.NewEncoder(w).Encode(struct {
		Error          string         `json:"error"`
		ForwardOutcome forwardOutcome `json:"forward_outcome"`
	}{Error: msg, ForwardOutcome: outcome})
}
