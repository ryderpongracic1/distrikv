// Package server provides the gRPC peer server and the HTTP REST API server
// for a distrikv node.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"

	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// RaftInterface is the subset of raft.RaftNode that GRPCServer and HTTPServer
// need. Defining it here, in the server package, breaks the import cycle:
//
//	raft → store → (no server import needed)
//	server → raft (via this interface only)
type RaftInterface interface {
	HandleRequestVote(ctx context.Context, req *kvpb.RequestVoteRequest) (*kvpb.RequestVoteResponse, error)
	HandleAppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error)
	HandlePreVote(ctx context.Context, req *kvpb.PreVoteRequest) (*kvpb.PreVoteResponse, error)
	HandleInstallSnapshot(ctx context.Context, req *kvpb.InstallSnapshotRequest) (*kvpb.InstallSnapshotResponse, error)
	IsLeader() bool
	CurrentTerm() uint64
	Leader() string
	RoleString() string
	ID() string
}

// ReplicationManager replicates a single mutation to the ring's replica nodes.
// It is implemented by the Node struct and injected into GRPCServer so the
// Replicate handler can write the incoming mutation to the local store AND
// forward it onward if this node is the primary.
type ReplicationManager interface {
	// ApplyReplica writes a replicated mutation directly to the local store
	// (bypassing the replication fan-out — this IS the replica receiving the write).
	ApplyReplica(ctx context.Context, op, key string, value []byte) error
}

// GRPCServer wraps a gRPC server and implements kvpb.KVServiceServer. It
// handles all inter-node RPCs: key forwarding, replication, and Raft messages.
type GRPCServer struct {
	kvpb.UnimplementedKVServiceServer

	addr    string
	srv     *grpc.Server
	store   *store.Store
	raft    RaftInterface
	ring    *cluster.Ring
	repMgr  ReplicationManager
	logger  *slog.Logger
}

// NewGRPCServer constructs a GRPCServer. Call Start to begin listening.
func NewGRPCServer(
	addr string,
	s *store.Store,
	r RaftInterface,
	ring *cluster.Ring,
	repMgr ReplicationManager,
	logger *slog.Logger,
) *GRPCServer {
	srv := grpc.NewServer()
	g := &GRPCServer{
		addr:   addr,
		srv:    srv,
		store:  s,
		raft:   r,
		ring:   ring,
		repMgr: repMgr,
		logger: logger.With("component", "grpc"),
	}
	kvpb.RegisterKVServiceServer(srv, g)
	return g
}

// Start begins listening on the configured address. It blocks until ctx is
// cancelled, then performs a graceful shutdown.
func (g *GRPCServer) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("grpc: listen %s: %w", g.addr, err)
	}

	g.logger.Info("gRPC server listening", "addr", g.addr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		g.srv.GracefulStop()
		return nil
	case err := <-errCh:
		return fmt.Errorf("grpc: serve: %w", err)
	}
}

// ---------------------------------------------------------------------------
// RPC implementations
// ---------------------------------------------------------------------------

// ForwardKey handles a key-operation that was forwarded from another node
// because that node is not the ring-primary for the key.
func (g *GRPCServer) ForwardKey(ctx context.Context, req *kvpb.ForwardKeyRequest) (*kvpb.ForwardKeyResponse, error) {
	g.logger.Debug("ForwardKey RPC received", "method", req.Method, "key", req.Key)

	switch req.Method {
	case "PUT":
		if err := g.store.Put(ctx, req.Key, req.Value); err != nil {
			body, _ := json.Marshal(map[string]string{"error": err.Error()})
			return &kvpb.ForwardKeyResponse{StatusCode: 500, Body: body}, nil
		}
		return &kvpb.ForwardKeyResponse{StatusCode: 200}, nil

	case "GET":
		val, err := g.store.Get(ctx, req.Key)
		if err == store.ErrNotFound {
			body, _ := json.Marshal(map[string]string{"error": "not found"})
			return &kvpb.ForwardKeyResponse{StatusCode: 404, Body: body}, nil
		}
		if err != nil {
			body, _ := json.Marshal(map[string]string{"error": err.Error()})
			return &kvpb.ForwardKeyResponse{StatusCode: 500, Body: body}, nil
		}
		body, _ := json.Marshal(map[string]string{"value": string(val)})
		return &kvpb.ForwardKeyResponse{StatusCode: 200, Body: body}, nil

	case "DELETE":
		if err := g.store.Delete(ctx, req.Key); err == store.ErrNotFound {
			body, _ := json.Marshal(map[string]string{"error": "not found"})
			return &kvpb.ForwardKeyResponse{StatusCode: 404, Body: body}, nil
		} else if err != nil {
			body, _ := json.Marshal(map[string]string{"error": err.Error()})
			return &kvpb.ForwardKeyResponse{StatusCode: 500, Body: body}, nil
		}
		return &kvpb.ForwardKeyResponse{StatusCode: 200}, nil

	default:
		body, _ := json.Marshal(map[string]string{"error": "unsupported method"})
		return &kvpb.ForwardKeyResponse{StatusCode: 400, Body: body}, nil
	}
}

// Replicate handles an incoming replication request from a ring-primary node.
// It writes the mutation directly to the local store.
func (g *GRPCServer) Replicate(ctx context.Context, req *kvpb.ReplicateRequest) (*kvpb.ReplicateResponse, error) {
	g.logger.Debug("Replicate RPC received", "op", req.Op, "key", req.Key)

	if err := g.repMgr.ApplyReplica(ctx, req.Op, req.Key, req.Value); err != nil {
		g.logger.Warn("Replicate failed", "op", req.Op, "key", req.Key, "error", err)
		return &kvpb.ReplicateResponse{Success: false, NodeId: g.raft.ID()}, nil
	}
	return &kvpb.ReplicateResponse{Success: true, NodeId: g.raft.ID()}, nil
}

// RequestVote delegates to the Raft state machine.
func (g *GRPCServer) RequestVote(ctx context.Context, req *kvpb.RequestVoteRequest) (*kvpb.RequestVoteResponse, error) {
	return g.raft.HandleRequestVote(ctx, req)
}

// AppendEntries delegates to the Raft state machine.
func (g *GRPCServer) AppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error) {
	return g.raft.HandleAppendEntries(ctx, req)
}

// PreVote delegates to the Raft state machine (dry-run election, §9.6).
func (g *GRPCServer) PreVote(ctx context.Context, req *kvpb.PreVoteRequest) (*kvpb.PreVoteResponse, error) {
	return g.raft.HandlePreVote(ctx, req)
}

// InstallSnapshot delegates to the Raft state machine (log compaction, §7).
func (g *GRPCServer) InstallSnapshot(ctx context.Context, req *kvpb.InstallSnapshotRequest) (*kvpb.InstallSnapshotResponse, error) {
	return g.raft.HandleInstallSnapshot(ctx, req)
}

// ---------------------------------------------------------------------------
// Peer client factory
// ---------------------------------------------------------------------------

// DialPeerWithRetry opens a gRPC connection to addr, retrying with exponential
// backoff until the connection succeeds or ctx is cancelled. This handles the
// Docker Compose startup race where containers start concurrently.
func DialPeerWithRetry(ctx context.Context, addr string, logger *slog.Logger) (*grpc.ClientConn, error) {
	wait := 100

	for {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			return conn, nil
		}
		logger.Warn("peer dial failed, retrying",
			"addr", addr,
			"wait_ms", wait,
			"error", err)

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("grpc: dial %s: %w", addr, ctx.Err())
		case <-waitTimer(wait):
		}

		if wait < 10_000 {
			wait *= 2
		}
	}
}

// NewPeerClient creates a KVServiceClient from an existing connection.
func NewPeerClient(conn *grpc.ClientConn) kvpb.KVServiceClient {
	return kvpb.NewKVServiceClient(conn)
}

// waitTimer returns a channel that fires after ms milliseconds.
func waitTimer(ms int) <-chan time.Time {
	return time.After(time.Duration(ms) * time.Millisecond)
}
