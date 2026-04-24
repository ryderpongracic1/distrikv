package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

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
	cfg        *config.Config
	store      *store.Store
	raft       *raft.RaftNode
	ring       *cluster.Ring
	grpcServer *server.GRPCServer
	httpServer *server.HTTPServer
	metrics    *metrics.Metrics
	peerConns  []*grpc.ClientConn // held for Clean shutdown
	logger     *slog.Logger
}

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

	// 2. Store (LSM-Tree engine).
	s, err := store.New(cfg.DataDir, logger)
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
	raftPeers := make([]raft.PeerClient, 0, len(cfg.Peers))

	for _, p := range cfg.Peers {
		conn, err := server.DialPeerWithRetry(ctx, p.GRPCAddr, logger)
		if err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("node: dial peer %s: %w", p.ID, err)
		}
		n.peerConns = append(n.peerConns, conn)
		client := server.NewPeerClient(conn)
		peerClients[p.ID] = client
		raftPeers = append(raftPeers, raft.PeerClient{
			NodeID: p.ID,
			Client: client,
		})
	}

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

	// 7. HTTP server (client-facing REST API).
	n.httpServer = server.NewHTTPServer(
		cfg.HTTPAddr,
		s,
		raftNode,
		ring,
		peerClients,
		n.metrics,
		logger,
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
// mutation directly to the local store, bypassing the replication fan-out
// (because this node IS the replica receiving the write).
func (n *Node) ApplyReplica(ctx context.Context, op, key string, value []byte) error {
	switch op {
	case "put":
		return n.store.Put(ctx, key, value)
	case "delete":
		return n.store.Delete(ctx, key)
	default:
		return fmt.Errorf("node: ApplyReplica: unknown op %q", op)
	}
}

// ReplicateWrite fans out a committed mutation to the ring's replica nodes
// (the next R-1 nodes clockwise from the primary). It uses a deadline of
// 2× heartbeatInterval to prevent slow replicas from stalling the primary.
//
// Returns an error if fewer than all replicas ACK — with R=2 this means any
// single replica failure blocks the write. See the README for the CAP
// discussion.
func (n *Node) ReplicateWrite(ctx context.Context, op, key string, value []byte) error {
	replicas, err := n.ring.GetN(key, n.cfg.ReplicaCount)
	if err != nil {
		return fmt.Errorf("node: ring lookup for replication: %w", err)
	}

	deadline := 2 * n.cfg.HeartbeatInterval
	repCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	var errs []error
	for _, vn := range replicas {
		if vn.NodeID == n.cfg.NodeID {
			continue // skip self — already written by the caller
		}

		client, ok := n.httpServer.PeerClient(vn.NodeID)
		if !ok {
			errs = append(errs, fmt.Errorf("no client for replica %s", vn.NodeID))
			continue
		}

		resp, err := client.Replicate(repCtx, &kvpb.ReplicateRequest{
			Op:    op,
			Key:   key,
			Value: value,
			Term:  n.raft.CurrentTerm(),
		})
		if err != nil {
			n.metrics.ReplicationErrors.Add(1)
			errs = append(errs, fmt.Errorf("replicate to %s: %w", vn.NodeID, err))
			continue
		}
		if !resp.Success {
			n.metrics.ReplicationErrors.Add(1)
			errs = append(errs, fmt.Errorf("replica %s rejected write", vn.NodeID))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("node: replication errors: %v", errs)
	}
	return nil
}

// metricsAdapter bridges metrics.Metrics to raft.metricsInterface.
type metricsAdapter struct{ m *metrics.Metrics }

func (a *metricsAdapter) IncRaftTerms()       { a.m.RaftTerms.Add(1) }
func (a *metricsAdapter) IncLeaderElections() { a.m.LeaderElections.Add(1) }

// dialTimeout is the maximum time to wait for a single peer dial attempt
// before retrying.
const dialTimeout = 500 * time.Millisecond

var _ = dialTimeout // used by DialPeerWithRetry internally
