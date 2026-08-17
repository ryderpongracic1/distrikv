package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/ryderpongracic1/distrikv/internal/store"
)

// Replication op codes carried in kvpb.ReplicateRequest.Op. Both the sending
// side (ReplicationManager.ReplicateWrite) and the receiving side
// (ReplicationManager.ApplyReplica) switch on these exact strings, so they are
// declared once here rather than being spelled out at each call site.
const (
	OpPut    = "put"
	OpDelete = "delete"
)

// ErrReplication reports that a mutation was applied to the ring-primary's
// local store but at least one replica failed to acknowledge it.
//
// There is no rollback: the primary keeps the write and is therefore ahead of
// the replicas that did not ACK, until the next successful write for that key
// converges them. The client is told the write was refused (HTTP 503) so it
// does not assume the configured replication factor was achieved. See the
// "CAP Position" section of the README.
var ErrReplication = errors.New("replication to replicas failed")

// primaryWriter executes a mutation for which this node is the ring-primary.
// It is the single place where a primary write happens: local durable write
// first, then a synchronous replication fan-out to the remaining R-1 replicas.
//
// Both client entry points share one instance — the HTTP handlers (this node
// owns the key) and the gRPC ForwardKey handler (a peer forwarded the key to
// this node because it is the primary) — so the two paths cannot drift apart
// on either durability or failure semantics.
type primaryWriter struct {
	store  *store.Store
	repMgr ReplicationManager
	logger *slog.Logger
}

// newPrimaryWriter builds the shared primary-write path. repMgr must be
// non-nil in production; a nil manager makes every mutation fail with
// ErrReplication rather than silently degrading to an unreplicated write.
func newPrimaryWriter(s *store.Store, repMgr ReplicationManager, logger *slog.Logger) *primaryWriter {
	return &primaryWriter{store: s, repMgr: repMgr, logger: logger}
}

// Put writes key locally and then replicates it. On replication failure the
// local write is retained and ErrReplication is returned (see its docs).
//
// The sequence number the local write was assigned travels with the fan-out, so
// every replica can order this mutation against the others for the same key
// rather than trusting the order its RPCs happened to arrive in.
func (p *primaryWriter) Put(ctx context.Context, key string, value []byte) error {
	seq, err := p.store.Put(ctx, key, value)
	if err != nil {
		return fmt.Errorf("primary put %q: %w", key, err)
	}
	return p.replicate(ctx, OpPut, key, value, seq)
}

// Delete writes a tombstone locally and then replicates it.
//
// Deletes are blind tombstone writes: deleting an absent key succeeds and is
// still replicated, because the tombstone itself is a durable local write that
// replicas must also apply (it must shadow any earlier value a replica holds).
// The tombstone carries its own sequence number, so a delete and a put racing on
// one key resolve identically on every replica.
func (p *primaryWriter) Delete(ctx context.Context, key string) error {
	seq, err := p.store.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("primary delete %q: %w", key, err)
	}
	return p.replicate(ctx, OpDelete, key, nil, seq)
}

// replicate fans the mutation out to the replica set. Reads never call this —
// only the primary's PUT and DELETE paths do.
func (p *primaryWriter) replicate(ctx context.Context, op, key string, value []byte, seq uint64) error {
	if p.repMgr == nil {
		return fmt.Errorf("%w: no replication manager configured", ErrReplication)
	}

	if err := p.repMgr.ReplicateWrite(ctx, op, key, value, seq); err != nil {
		p.logger.Warn("write refused: replica did not ACK; primary is now ahead of its replicas for this key",
			"op", op, "key", key, "seq", seq, "error", err)
		return fmt.Errorf("%w: %w", ErrReplication, err)
	}
	return nil
}

// statusForWriteError maps a primaryWriter error onto the HTTP status code
// reported to the client. It is shared by the HTTP handlers and by ForwardKey,
// which returns the same codes inside kvpb.ForwardKeyResponse.
func statusForWriteError(err error) int {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrReplication):
		// The durability contract was not met, so the write is reported as
		// refused rather than as an internal fault.
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
