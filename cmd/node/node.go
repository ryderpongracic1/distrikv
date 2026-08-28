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
	health      *cluster.PeerHealth             // local peer liveness signals
	healthSM    *HealthStateMachine             // committed, cluster-wide health view
	healthAgg   *healthAggregator               // leader-side proposer of health transitions
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
	// The Raft log carries cluster-control entries, never key/value data, so it
	// is given its own state machine rather than the storage engine. That state
	// machine is the committed node-health view: the leader observes peers
	// through its heartbeats and proposes transitions, and every node — leader or
	// not — applies the same committed sequence.
	healthSM := newHealthStateMachine(cfg.NodeID, len(cfg.Peers), n.metrics, logger)
	n.healthSM = healthSM

	raftNode, err := raft.New(raftCfg, raftPeers, healthSM, &metricsAdapter{n.metrics}, logger)
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
	//    Health is tracked from three signals — the committed health view the Raft
	//    log carries, which is the only one a ring-primary that is not the Raft
	//    leader gets for free; this node's own replication outcomes; and the Raft
	//    heartbeat outcomes the leader observes. All three are outcomes of traffic
	//    the cluster was sending anyway: there is no dedicated probing machinery.
	//    See cluster.PeerHealth and HealthStateMachine.
	//
	//    The cursor store itself was opened in step 2, so the store could take
	//    ownership of invalidating it across a snapshot restore.
	peerIDs := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peerIDs = append(peerIDs, p.ID)
	}

	n.health = cluster.NewPeerHealth(peerIDs, cluster.HealthConfig{
		Logger: logger,
	})
	// The aggregator turns this node's heartbeat outcomes into proposed health
	// transitions while it is the leader. It is registered *alongside* the local
	// tracker, not in place of it: raft holds a single observer slot, and the
	// local signals are what cover a leaderless window, when no health entry can
	// be committed at all.
	n.healthAgg = newHealthAggregator(
		raftNode, healthSM, len(cfg.Peers), n.metrics, logger, healthAggregatorConfig{},
	)
	raftNode.SetPeerHealthObserver(multiPeerHealthObserver{n.health, n.healthAgg})

	n.antiEntropy = newAntiEntropy(
		cfg.NodeID,
		cfg.ReplicaCount,
		peerIDs,
		s,
		cursors,
		ring,
		peerClients,
		n.health,
		healthSM,
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

	// Replica catch-up. Pure background convergence work: it never gates a client
	// write, and a failure in it leaves the CP write path exactly as it was.
	// Peer liveness needs no goroutine of its own — cluster.PeerHealth is fed by
	// the heartbeat and replication paths as they happen.
	//
	// The aggregator's own goroutine is where the Raft append for a health
	// transition happens. It is deliberately not the heartbeat goroutine that
	// observed the transition: Propose fsyncs, and the heartbeat path is the one
	// every follower's election timer depends on.
	if n.healthAgg != nil {
		g.Go(func() error {
			n.healthAgg.Run(gCtx)
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
//
// The write is applied only if seq is newer than the version this node already
// holds for the key. Replication is a fan-out of independent RPCs, so two writes
// to one key issued microseconds apart can arrive here in either order, and
// applying them in arrival order would leave this replica holding whichever
// landed last rather than whichever was written last. Nothing downstream would
// notice: this node ACKed both writes, so it is not behind, and an anti-entropy
// pass replays gaps rather than inversions. Comparing sequences is what turns
// that permanent silent divergence into a discarded RPC.
//
// A stale arrival is not an error — the replica is already at or ahead of the
// state the mutation describes, which is exactly what the primary wanted — so it
// ACKs. Reporting a failure instead would refuse the client's write on the
// primary and mark this node behind for a divergence that does not exist.
//
// replay says the mutation came from an anti-entropy catch-up pass rather than
// from the live path. It does not change whether the write is applied. It changes
// how the engine classifies a refusal: a pass covers a range of the primary's log
// pinned when the pass started, so it can legitimately deliver an entry written
// by an earlier incarnation of that primary while this replica already holds a
// newer one, and calling that an epoch regression would raise a latched alarm on
// an ordinary restart. See lsm.LSMTree.noteDiscard.
func (n *Node) ApplyReplica(ctx context.Context, op, key string, value []byte, seq uint64, replay bool) error {
	switch op {
	case server.OpPut:
		applied, err := n.applyPut(ctx, key, value, seq, replay)
		if err != nil {
			return err
		}
		if !applied {
			n.logger.Debug("ApplyReplica: discarded a write older than the version held",
				"key", key, "seq", seq, "replay", replay)
		}
		return nil
	case server.OpDelete:
		// A replicated delete is idempotent: if this replica never received the
		// original write (an earlier fan-out to it failed), the key is already
		// absent and the tombstone is satisfied. The engine's deletes are blind,
		// so an absent key is not an error to begin with — but the tombstone is
		// still written when it is newer, because it must shadow any earlier
		// value this replica holds.
		applied, err := n.applyDelete(ctx, key, seq, replay)
		if err != nil {
			return err
		}
		if !applied {
			n.logger.Debug("ApplyReplica: discarded a delete older than the version held",
				"key", key, "seq", seq, "replay", replay)
		}
		return nil
	default:
		return fmt.Errorf("node: ApplyReplica: unknown op %q", op)
	}
}

// applyPut and applyDelete select the store entry point for the origin of the
// mutation. The selection is a switch on one bit rather than a parameter threaded
// into the engine so that every other caller of the apply-if-newer path keeps
// meaning "live" and only this one, named, path can suppress the
// epoch-regression classification.
func (n *Node) applyPut(ctx context.Context, key string, value []byte, seq uint64, replay bool) (bool, error) {
	if replay {
		return n.store.ReplayPutIfNewer(ctx, key, value, seq)
	}
	return n.store.PutIfNewer(ctx, key, value, seq)
}

func (n *Node) applyDelete(ctx context.Context, key string, seq uint64, replay bool) (bool, error) {
	if replay {
		return n.store.ReplayDeleteIfNewer(ctx, key, seq)
	}
	return n.store.DeleteIfNewer(ctx, key, seq)
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
//
// seq is the sequence number the caller's local write was assigned. Every
// replica receives the same one, which is what lets them agree on the order of
// two writes to a key regardless of the order their RPCs arrive in.
func (n *Node) ReplicateWrite(ctx context.Context, op, key string, value []byte, seq uint64) error {
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
			Seq:   seq,
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

// SetLastAppliedIndex records how far through the Raft log the state machine has
// been fed. A plain Store is correct because Raft's apply loop is serialised and
// advances lastApplied monotonically, so the last write is the highest index.
func (a *metricsAdapter) SetLastAppliedIndex(index uint64) {
	a.m.RaftLastAppliedIndex.Store(index)
}
