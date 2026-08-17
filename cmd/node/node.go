package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/config"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// Node is the top-level component that owns and coordinates all distrikv
// subsystems. There is no global mutable state — everything flows through
// this struct.
type Node struct {
	cfg         *config.Config
	store       *store.Store
	raft        *raft.RaftNode
	ring        *cluster.Ring
	grpcServer  *server.GRPCServer
	httpServer  *server.HTTPServer
	metrics     *metrics.Metrics
	peerClients map[string]kvpb.KVServiceClient // nodeID → gRPC client
	peerConns   []*grpc.ClientConn              // held for Clean shutdown
	cursors     *store.CursorStore              // per-replica WAL catch-up cursors
	health      *cluster.PeerHealth             // peer liveness, drives catch-up
	antiEntropy *antiEntropy                    // replica convergence engine
	logger      *slog.Logger
}

// defaultReplicateTimeout bounds a single replication fan-out to the replica
// set, so a replica that is slow or gone costs the client one bounded 503
// instead of a hung request.
//
// This deadline used to be 2×HeartbeatInterval, which coupled the write path's
// failure threshold to a Raft election-timing knob it has nothing to do with:
// the docker-compose HEARTBEAT_INTERVAL of 150ms made it 300ms, so tuning
// election sensitivity silently changed when writes start failing. 300ms is also
// below the legitimate worst case — a replica draining a compaction backlog
// under write-stall backpressure holds a write for up to the storage engine's
// stall budget (lsm.ErrWriteStalled is returned after 1s) — so a replica that
// was merely busy got reported as failed and the primary refused writes it had
// already applied locally.
//
// 2s is that 1s stall budget plus margin for the RPC itself; a healthy
// in-cluster replicate is sub-millisecond (one WAL fsync and one gRPC hop). It
// stays well below the HTTP server's 10s WriteTimeout, and matches the forward
// deadline (server.defaultForwardTimeout) so the client's worst case is the same
// 2s whichever hop fails.
const defaultReplicateTimeout = 2 * time.Second

// NewNode constructs the Node by initialising all subsystems in dependency
// order and wiring them together. It returns an error if any subsystem fails
// to initialise (e.g. WAL cannot be opened, peer dial times out).
func NewNode(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Node, error) {
	n := &Node{
		cfg:     cfg,
		metrics: &metrics.Metrics{},
		logger:  logger,
	}

	// 1. Ensure DataDir exists.
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("node: create data dir: %w", err)
	}

	// 2. Store (LSM-Tree engine) — wired to node-wide metrics so bloom/WAF
	//    counters surface in /metrics.
	//
	//    The replica catch-up cursors are opened first and handed to the store:
	//    the engine's snapshot-restore path discards the WAL those cursors
	//    address, and a cursor that outlives it makes anti-entropy report a
	//    convergence it never performed (see store.CursorStore.InvalidateAll).
	cursors, err := store.OpenCursorStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("node: open replica cursors: %w", err)
	}
	n.cursors = cursors

	s, err := store.NewWithMetrics(cfg.DataDir, logger, n.metrics, store.WithCursorStore(cursors))
	if err != nil {
		return nil, fmt.Errorf("node: open store: %w", err)
	}
	n.store = s

	// 3. Consistent hash ring — populate with all nodes including self.
	ring := cluster.New()
	ring.AddNode(cfg.NodeID, cfg.GRPCAddr)
	for _, p := range cfg.Peers {
		ring.AddNode(p.ID, p.GRPCAddr)
	}
	n.ring = ring

	// 4. Dial all peer gRPC connections with retry.
	peerClients := make(map[string]kvpb.KVServiceClient, len(cfg.Peers))
	peerConnByID := make(map[string]*grpc.ClientConn, len(cfg.Peers))
	raftPeers := make([]raft.PeerClient, 0, len(cfg.Peers))

	for _, p := range cfg.Peers {
		conn, err := server.DialPeerWithRetry(ctx, p.GRPCAddr, logger)
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("node: dial peer %s: %w", p.ID, err)
		}
		n.peerConns = append(n.peerConns, conn)
		// Keyed here, where the connection and the peer it belongs to are both in
		// hand. Pairing them up afterwards by slice index would work only as long
		// as this loop and that one iterate cfg.Peers identically — a coupling that
		// costs nothing to avoid and would mis-key a peer's health probe if it ever
		// broke.
		peerConnByID[p.ID] = conn
		client := server.NewPeerClient(conn)
		peerClients[p.ID] = client
		raftPeers = append(raftPeers, raft.PeerClient{
			NodeID: p.ID,
			Client: client,
		})
	}
	n.peerClients = peerClients

	// 5. Raft node (election + heartbeat + snapshots).
	raftCfg := raft.Config{
		NodeID:             cfg.NodeID,
		DataDir:            cfg.DataDir,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		SnapshotThreshold:  1000,
	}
	raftNode, err := raft.New(raftCfg, raftPeers, s, &metricsAdapter{n.metrics}, logger)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("node: init raft: %w", err)
	}
	n.raft = raftNode

	// 6. gRPC server (peer communication).
	n.grpcServer = server.NewGRPCServer(
		cfg.GRPCAddr,
		s,
		raftNode,
		ring,
		n, // Node implements server.ReplicationManager
		logger,
	)

	// 7. HTTP server (client-facing REST API). Node is passed as the
	//    ReplicationManager so client writes this node owns are replicated.
	n.httpServer = server.NewHTTPServer(
		cfg.HTTPAddr,
		s,
		raftNode,
		ring,
		peerClients,
		n, // Node implements server.ReplicationManager
		n.metrics,
		logger,
	)

	// 8. Anti-entropy: converge replicas from this node's WAL after a fault.
	//
	//    Health is tracked from three signals — Raft heartbeats (leader only),
	//    replication outcomes (this node's own writes), and a transport probe
	//    over the peer channels, which is the only one that tells a non-leader
	//    that a peer has come back. See cluster.PeerHealth.
	//
	//    The cursor store itself was opened in step 2, so the store could take
	//    ownership of invalidating it across a snapshot restore.
	peerIDs := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peerIDs = append(peerIDs, p.ID)
	}

	n.health = cluster.NewPeerHealth(peerIDs, cluster.HealthConfig{
		Interval: cfg.HeartbeatInterval,
		Probe: func(nodeID string) bool {
			conn, ok := peerConnByID[nodeID]
			if !ok {
				return false
			}
			// A channel that is Ready has a live connection to the peer. Idle is
			// also treated as reachable: gRPC parks a channel with no traffic in
			// Idle, and reporting that as unreachable would demote a peer this
			// node simply has not spoken to recently.
			switch conn.GetState() {
			case connectivity.Ready, connectivity.Idle:
				return true
			default:
				return false
			}
		},
		Logger: logger,
	})
	raftNode.SetPeerHealthObserver(n.health)

	n.antiEntropy = newAntiEntropy(
		cfg.NodeID,
		cfg.ReplicaCount,
		peerIDs,
		s,
		cursors,
		ring,
		peerClients,
		n.health,
		raftNode.CurrentTerm,
		n.metrics,
		logger,
		antiEntropyConfig{},
	)

	return n, nil
}

// Run starts all subsystems and blocks until ctx is cancelled. Subsystems are
// started in dependency order and shut down in reverse order to prevent
// use-after-close errors.
//
// Startup order:  gRPC → HTTP → Raft
// Shutdown order: HTTP → gRPC → Raft → Store
func (n *Node) Run(ctx context.Context) error {
	n.logger.Info("node starting",
		"node_id", n.cfg.NodeID,
		"http_addr", n.cfg.HTTPAddr,
		"grpc_addr", n.cfg.GRPCAddr,
		"peers", len(n.cfg.Peers),
	)

	// Use a cancellable child context so we can stop subsystems independently.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(runCtx)

	g.Go(func() error {
		return n.grpcServer.Start(gCtx)
	})

	g.Go(func() error {
		return n.httpServer.Start(gCtx)
	})

	g.Go(func() error {
		n.raft.Run(gCtx)
		return nil
	})

	// Peer liveness tracking and replica catch-up. Both are pure background
	// convergence work: they never gate a client write, and a failure in either
	// leaves the CP write path exactly as it was.
	if n.health != nil {
		g.Go(func() error {
			n.health.Run(gCtx)
			return nil
		})
	}
	if n.antiEntropy != nil {
		g.Go(func() error {
			n.antiEntropy.Run(gCtx)
			return nil
		})
	}

	// Wait for the parent context to be cancelled (SIGINT/SIGTERM), then
	// trigger a coordinated shutdown by cancelling the run context.
	<-ctx.Done()
	n.logger.Info("shutdown signal received")
	cancel() // stops all subsystem goroutines

	if err := g.Wait(); err != nil {
		n.logger.Error("subsystem error during shutdown", "error", err)
	}

	// Close the store last — subsystems above must have stopped writing.
	if err := n.store.Close(); err != nil {
		return fmt.Errorf("node: close store: %w", err)
	}

	// Close peer connections.
	for _, conn := range n.peerConns {
		_ = conn.Close()
	}

	n.logger.Info("node stopped cleanly")
	return nil
}

// ApplyReplica implements server.ReplicationManager. It writes a replicated
// mutation directly to the local store, deliberately bypassing the replication
// fan-out (because this node IS the replica receiving the write). Fanning out
// from here would make every write replicate forever.
func (n *Node) ApplyReplica(ctx context.Context, op, key string, value []byte) error {
	switch op {
	case server.OpPut:
		return n.store.Put(ctx, key, value)
	case server.OpDelete:
		// A replicated delete is idempotent: if this replica never received
		// the original write (an earlier fan-out to it failed), the key is
		// already absent and the tombstone is satisfied. Reporting ErrNotFound
		// here would fail the client's delete on the primary forever.
		if err := n.store.Delete(ctx, key); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		return nil
	default:
		return fmt.Errorf("node: ApplyReplica: unknown op %q", op)
	}
}

// ReplicateWrite implements server.ReplicationManager. It fans out a mutation
// that has already been applied to this node's local store to the ring's other
// replica nodes for the key (the next R-1 distinct nodes clockwise). Each
// fan-out is bounded by defaultReplicateTimeout so a slow replica cannot stall
// the primary.
//
// A replica whose storage engine is refusing writes under compaction
// backpressure answers with store.ErrWriteStalled well inside that deadline; the
// gRPC layer surfaces it as a rejected write ("replica X rejected write") rather
// than as a deadline, which is what distinguishes an overloaded replica from an
// unreachable one.
//
// Returns an error unless every replica ACKs — with R=2 that means any single
// replica failure fails the client's write. The local write is NOT rolled back
// (no rollback mechanism exists), so a failed fan-out leaves this node ahead of
// its replicas until the next successful write for the key. See the
// "CAP Position" section of docs/architecture.md.
//
// A deployment with no other node in the replica set (single node, or R=1)
// degrades to a local-only write and returns nil: the only member of the
// replica set is this node, which the caller has already written.
func (n *Node) ReplicateWrite(ctx context.Context, op, key string, value []byte) error {
	replicas, err := n.ring.GetN(key, n.cfg.ReplicaCount)
	if err != nil {
		return fmt.Errorf("node: ring lookup for replication: %w", err)
	}

	repCtx, cancel := context.WithTimeout(ctx, defaultReplicateTimeout)
	defer cancel()

	term := n.raft.CurrentTerm()

	var errs []error
	for _, vn := range replicas {
		if vn.NodeID == n.cfg.NodeID {
			continue // skip self — already written by the caller
		}

		client, ok := n.peerClients[vn.NodeID]
		if !ok {
			// No client is a replication failure like any other: the write is
			// refused to the caller and this node is now ahead of that replica for
			// this key. Skipping the bookkeeping here would leave the one hole in
			// "any replication failure marks the replica behind" — and the hole
			// widens rather than closes, because the ring can hand back a member
			// that has no client at all (a peer added to the ring without a client
			// entry), which is exactly the case where nothing else will notice.
			n.metrics.ReplicationErrors.Add(1)
			n.noteReplicationFailure(vn.NodeID)
			errs = append(errs, fmt.Errorf("no client for replica %s", vn.NodeID))
			continue
		}

		resp, err := client.Replicate(repCtx, &kvpb.ReplicateRequest{
			Op:    op,
			Key:   key,
			Value: value,
			Term:  term,
		})
		if err != nil {
			n.metrics.ReplicationErrors.Add(1)
			n.noteReplicationFailure(vn.NodeID)
			errs = append(errs, fmt.Errorf("replicate to %s: %w", vn.NodeID, err))
			continue
		}
		if !resp.Success {
			n.metrics.ReplicationErrors.Add(1)
			n.noteReplicationFailure(vn.NodeID)
			errs = append(errs, fmt.Errorf("replica %s rejected write", vn.NodeID))
			continue
		}
		n.noteReplicationSuccess(vn.NodeID)
	}

	if len(errs) > 0 {
		return fmt.Errorf("node: replication errors: %w", errors.Join(errs...))
	}
	return nil
}

// noteReplicationFailure records that a replica did not take a write, so the
// anti-entropy engine knows this node is now ahead of it and the health tracker
// knows the peer looks unreachable.
//
// It deliberately does not change what the client is told: the write is still
// refused with 503. Convergence happens afterwards, out of the request's way.
// The helpers are nil-safe because the tests construct a Node without the
// convergence subsystems.
func (n *Node) noteReplicationFailure(nodeID string) {
	if n.antiEntropy != nil {
		n.antiEntropy.NoteReplicationFailure(nodeID)
		return
	}
	if n.health != nil {
		n.health.ObserveReplication(nodeID, false)
	}
}

func (n *Node) noteReplicationSuccess(nodeID string) {
	if n.antiEntropy != nil {
		n.antiEntropy.NoteReplicationSuccess(nodeID)
		return
	}
	if n.health != nil {
		n.health.ObserveReplication(nodeID, true)
	}
}

// metricsAdapter bridges metrics.Metrics to raft.metricsInterface.
type metricsAdapter struct{ m *metrics.Metrics }

func (a *metricsAdapter) IncRaftTerms()       { a.m.RaftTerms.Add(1) }
func (a *metricsAdapter) IncLeaderElections() { a.m.LeaderElections.Add(1) }
