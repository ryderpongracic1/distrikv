package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/config"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// fakePeer is a kvpb.KVServiceClient that records Replicate calls. The embedded
// nil interface means any other RPC panics — these tests must only ever reach
// Replicate.
type fakePeer struct {
	kvpb.KVServiceClient

	mu     sync.Mutex
	reqs   []*kvpb.ReplicateRequest
	err    error         // transport-level failure
	refuse bool          // ACK with Success=false
	delay  time.Duration // how long the replica takes to answer
	nodeID string
}

func (f *fakePeer) Replicate(ctx context.Context, in *kvpb.ReplicateRequest, _ ...grpc.CallOption) (*kvpb.ReplicateResponse, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, in)
	delay := f.delay
	f.mu.Unlock()

	// Honour the caller's deadline the way a real gRPC call does, so a test can
	// distinguish "replica was slow" from "deadline was too tight".
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if f.err != nil {
		return nil, f.err
	}
	return &kvpb.ReplicateResponse{Success: !f.refuse, NodeId: f.nodeID}, nil
}

func (f *fakePeer) requests() []*kvpb.ReplicateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*kvpb.ReplicateRequest(nil), f.reqs...)
}

// testNode builds a Node wired for replication tests: a real store, a real
// (idle) Raft node so replication requests carry a genuine term, a ring holding
// self plus the named peers, and a fakePeer client per peer.
func testNode(t *testing.T, replicaCount int, peerIDs ...string) (*Node, map[string]*fakePeer) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()

	st, err := store.New(dir, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	const selfID = "node1"

	cfg := &config.Config{
		NodeID:            selfID,
		DataDir:           dir,
		ReplicaCount:      replicaCount,
		HeartbeatInterval: 75 * time.Millisecond,
	}

	ring := cluster.New()
	ring.AddNode(selfID, "node1:9001")

	m := &metrics.Metrics{}
	peers := make(map[string]*fakePeer, len(peerIDs))
	clients := make(map[string]kvpb.KVServiceClient, len(peerIDs))
	for _, id := range peerIDs {
		ring.AddNode(id, id+":9000")
		p := &fakePeer{nodeID: id}
		peers[id] = p
		clients[id] = p
	}

	raftNode, err := raft.New(raft.Config{
		NodeID:             selfID,
		DataDir:            dir,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  cfg.HeartbeatInterval,
	}, nil, st, &metricsAdapter{m}, logger)
	if err != nil {
		t.Fatalf("init raft: %v", err)
	}

	return &Node{
		cfg:         cfg,
		store:       st,
		raft:        raftNode,
		ring:        ring,
		metrics:     m,
		peerClients: clients,
		logger:      logger,
	}, peers
}

// replicaIDsFor returns the peer node IDs (self excluded) that the ring selects
// as replication targets for key — i.e. the exact set ReplicateWrite must hit.
func replicaIDsFor(t *testing.T, n *Node, key string) []string {
	t.Helper()
	vnodes, err := n.ring.GetN(key, n.cfg.ReplicaCount)
	if err != nil {
		t.Fatalf("ring.GetN(%q): %v", key, err)
	}
	var ids []string
	for _, vn := range vnodes {
		if vn.NodeID != n.cfg.NodeID {
			ids = append(ids, vn.NodeID)
		}
	}
	return ids
}

// TestReplicateWriteFansOutToRingReplicas asserts the fan-out hits exactly the
// R-1 distinct non-self nodes ring.GetN selects — no more, no fewer — and that
// each request carries the op, value, and current Raft term.
func TestReplicateWriteFansOutToRingReplicas(t *testing.T) {
	n, peers := testNode(t, 2, "node2", "node3")

	const key = "alpha"
	want := replicaIDsFor(t, n, key)
	if len(want) != 1 {
		t.Fatalf("precondition: R=2 should select 1 peer replica for %q, got %v", key, want)
	}

	if err := n.ReplicateWrite(context.Background(), server.OpPut, key, []byte("one")); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	for id, p := range peers {
		reqs := p.requests()
		isTarget := id == want[0]

		if !isTarget {
			if len(reqs) != 0 {
				t.Errorf("peer %s is not a replica for %q but received %d request(s)", id, key, len(reqs))
			}
			continue
		}
		if len(reqs) != 1 {
			t.Fatalf("replica %s received %d request(s), want exactly 1", id, len(reqs))
		}
		got := reqs[0]
		if got.Op != server.OpPut || got.Key != key || string(got.Value) != "one" {
			t.Errorf("replica %s got (op=%q key=%q value=%q), want (put, %q, one)", id, got.Op, got.Key, got.Value, key)
		}
		if got.Term != n.raft.CurrentTerm() {
			t.Errorf("replica %s got term %d, want the current Raft term %d", id, got.Term, n.raft.CurrentTerm())
		}
	}
}

// TestReplicateWriteFansOutToAllReplicasAtR3 raises R so both peers are in the
// replica set, proving the fan-out follows ReplicaCount rather than a fixed 1.
func TestReplicateWriteFansOutToAllReplicasAtR3(t *testing.T) {
	n, peers := testNode(t, 3, "node2", "node3")

	if err := n.ReplicateWrite(context.Background(), server.OpPut, "alpha", []byte("one")); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	for id, p := range peers {
		if got := len(p.requests()); got != 1 {
			t.Errorf("replica %s received %d request(s), want exactly 1", id, got)
		}
	}
}

func TestReplicateWriteFansOutDeletes(t *testing.T) {
	n, peers := testNode(t, 3, "node2", "node3")

	if err := n.ReplicateWrite(context.Background(), server.OpDelete, "alpha", nil); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}

	for id, p := range peers {
		reqs := p.requests()
		if len(reqs) != 1 {
			t.Fatalf("replica %s received %d request(s), want exactly 1", id, len(reqs))
		}
		if reqs[0].Op != server.OpDelete {
			t.Errorf("replica %s got op %q, want %q", id, reqs[0].Op, server.OpDelete)
		}
	}
}

// TestReplicateWriteSingleNodeDegradesToLocal covers the single-node
// deployment: R=2 but no peers exist, so the replica set is just this node and
// the write succeeds without contacting anyone.
func TestReplicateWriteSingleNodeDegradesToLocal(t *testing.T) {
	n, peers := testNode(t, 2)
	if len(peers) != 0 {
		t.Fatalf("precondition: expected no peers, got %d", len(peers))
	}

	if err := n.ReplicateWrite(context.Background(), server.OpPut, "alpha", []byte("one")); err != nil {
		t.Fatalf("single-node ReplicateWrite = %v, want nil (degrade to local-only)", err)
	}
	if got := n.metrics.ReplicationErrors.Load(); got != 0 {
		t.Errorf("replication_errors = %d, want 0", got)
	}
}

// TestReplicateWriteReplicaCountOneNeverReplicates pins R=1 as "no replication"
// rather than "replicate to the primary".
func TestReplicateWriteReplicaCountOneNeverReplicates(t *testing.T) {
	n, peers := testNode(t, 1, "node2", "node3")

	if err := n.ReplicateWrite(context.Background(), server.OpPut, "alpha", []byte("one")); err != nil {
		t.Fatalf("ReplicateWrite: %v", err)
	}
	for id, p := range peers {
		if got := len(p.requests()); got != 0 {
			t.Errorf("peer %s received %d request(s) at R=1, want 0", id, got)
		}
	}
}

// TestReplicateWriteTransportFailure is the CP path: an unreachable replica
// fails the caller's write and increments replication_errors.
func TestReplicateWriteTransportFailure(t *testing.T) {
	n, peers := testNode(t, 3, "node2", "node3")
	for _, p := range peers {
		p.err = errors.New("connection refused")
	}

	err := n.ReplicateWrite(context.Background(), server.OpPut, "alpha", []byte("one"))
	if err == nil {
		t.Fatal("ReplicateWrite = nil, want an error when replicas are unreachable")
	}
	if got := n.metrics.ReplicationErrors.Load(); got != 2 {
		t.Errorf("replication_errors = %d, want 2 (one per failed replica)", got)
	}
}

// TestReplicateWriteReplicaRejects covers an ACK that reports failure, which is
// as fatal to the write as a transport error.
func TestReplicateWriteReplicaRejects(t *testing.T) {
	n, peers := testNode(t, 3, "node2", "node3")
	for _, p := range peers {
		p.refuse = true
	}

	if err := n.ReplicateWrite(context.Background(), server.OpPut, "alpha", []byte("one")); err == nil {
		t.Fatal("ReplicateWrite = nil, want an error when a replica rejects the write")
	}
	if got := n.metrics.ReplicationErrors.Load(); got != 2 {
		t.Errorf("replication_errors = %d, want 2", got)
	}
}

// TestReplicateWriteMissingPeerClient covers a ring member this node cannot
// dial — a misconfiguration that must refuse the write, not skip the replica.
func TestReplicateWriteMissingPeerClient(t *testing.T) {
	n, _ := testNode(t, 2)
	n.ring.AddNode("node2", "node2:9002") // in the ring, but no client for it

	// Find a key whose replica set includes node2.
	var key string
	for _, candidate := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
		if ids := replicaIDsFor(t, n, candidate); len(ids) == 1 && ids[0] == "node2" {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Skip("no candidate key maps node2 into the replica set on this ring")
	}

	if err := n.ReplicateWrite(context.Background(), server.OpPut, key, []byte("one")); err == nil {
		t.Fatal("ReplicateWrite = nil, want an error when a replica has no gRPC client")
	}
	if got := n.metrics.ReplicationErrors.Load(); got != 1 {
		t.Errorf("replication_errors = %d, want 1", got)
	}
}

// TestReplicateWriteDeadlineIsIndependentOfHeartbeatInterval pins the decoupling
// of the replication deadline from Raft's heartbeat tuning.
//
// The deadline used to be 2×HeartbeatInterval, so lowering the heartbeat to make
// elections more responsive also shortened the window a replica had to answer a
// write. With the docker-compose value of 150ms that window was 300ms, which a
// replica draining a compaction backlog exceeds routinely — the primary then
// reported the write refused even though it had applied it locally.
//
// A 5ms heartbeat here would give the old code a 10ms deadline; the replica
// takes 120ms, which is well inside the fixed 2s budget and must succeed.
func TestReplicateWriteDeadlineIsIndependentOfHeartbeatInterval(t *testing.T) {
	n, peers := testNode(t, 2, "node2", "node3")
	n.cfg.HeartbeatInterval = 5 * time.Millisecond

	const key = "slow-replica"
	for _, id := range replicaIDsFor(t, n, key) {
		peers[id].mu.Lock()
		peers[id].delay = 120 * time.Millisecond
		peers[id].mu.Unlock()
	}

	start := time.Now()
	if err := n.ReplicateWrite(context.Background(), server.OpPut, key, []byte("v")); err != nil {
		t.Fatalf("ReplicateWrite with a 120ms replica and a 5ms heartbeat: %v "+
			"(the deadline is still coupled to HeartbeatInterval)", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("ReplicateWrite returned in %v; the replica was supposed to take 120ms, "+
			"so the delay was not exercised", elapsed)
	}
	if got := n.metrics.ReplicationErrors.Load(); got != 0 {
		t.Errorf("ReplicationErrors = %d; want 0 — a slow but healthy replica is not an error", got)
	}
}

// TestReplicateWriteBoundsASilentReplica asserts the other half of the contract:
// the deadline still exists. A replica that never answers must not hang the
// primary past defaultReplicateTimeout.
func TestReplicateWriteBoundsASilentReplica(t *testing.T) {
	n, peers := testNode(t, 2, "node2", "node3")

	const key = "silent-replica"
	for _, id := range replicaIDsFor(t, n, key) {
		peers[id].mu.Lock()
		peers[id].delay = time.Hour // never answers within any sane test
		peers[id].mu.Unlock()
	}

	start := time.Now()
	err := n.ReplicateWrite(context.Background(), server.OpPut, key, []byte("v"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("ReplicateWrite succeeded against a replica that never answered")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ReplicateWrite error = %v; want it to wrap context.DeadlineExceeded", err)
	}
	// Generous upper bound: the assertion is that *a* bound exists, not its
	// precise value, so a loaded CI box cannot make this flaky.
	if elapsed > 3*defaultReplicateTimeout {
		t.Errorf("ReplicateWrite took %v; deadline is %v", elapsed, defaultReplicateTimeout)
	}
	t.Logf("silent replica bounded at %v (deadline %v)", elapsed, defaultReplicateTimeout)
}

// TestApplyReplicaWritesLocallyWithoutFanOut is the loop guard at the Node
// level: applying a replicated mutation must touch only the local store.
func TestApplyReplicaWritesLocallyWithoutFanOut(t *testing.T) {
	n, peers := testNode(t, 3, "node2", "node3")
	ctx := context.Background()

	if err := n.ApplyReplica(ctx, server.OpPut, "alpha", []byte("one")); err != nil {
		t.Fatalf("ApplyReplica put: %v", err)
	}
	got, err := n.store.Get(ctx, "alpha")
	if err != nil || string(got) != "one" {
		t.Fatalf("store after ApplyReplica put = (%q, %v), want (\"one\", nil)", got, err)
	}

	if err := n.ApplyReplica(ctx, server.OpDelete, "alpha", nil); err != nil {
		t.Fatalf("ApplyReplica delete: %v", err)
	}
	if _, err := n.store.Get(ctx, "alpha"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("store after ApplyReplica delete: err = %v, want ErrNotFound", err)
	}

	for id, p := range peers {
		if got := len(p.requests()); got != 0 {
			t.Errorf("ApplyReplica fanned out to %s (%d request(s)); this would replicate forever", id, got)
		}
	}
}

// TestApplyReplicaDeleteIsIdempotent covers a replica that missed the original
// write: the tombstone must still be treated as applied, or the primary's
// delete would fail forever.
func TestApplyReplicaDeleteIsIdempotent(t *testing.T) {
	n, _ := testNode(t, 2)

	if err := n.ApplyReplica(context.Background(), server.OpDelete, "never-written", nil); err != nil {
		t.Fatalf("ApplyReplica delete of absent key = %v, want nil", err)
	}
}

func TestApplyReplicaUnknownOp(t *testing.T) {
	n, _ := testNode(t, 2)

	if err := n.ApplyReplica(context.Background(), "upsert", "alpha", []byte("one")); err == nil {
		t.Fatal("ApplyReplica with unknown op = nil, want an error")
	}
}
