// Package store provides a thread-safe key-value store backed by an LSM-Tree
// storage engine. The public interface (Get/Put/Delete) is unchanged from the
// prior WAL+map implementation; all callers (gRPC, HTTP, Raft) require no changes.
package store

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store/lsm"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// ErrNotFound is returned by Get when the key does not exist or has been deleted.
var ErrNotFound = lsm.ErrNotFound

// ErrWriteStalled is returned by Put and Delete when the storage engine is
// refusing writes because its L0 compaction backlog has not drained within the
// engine's stall budget. It means "alive, overloaded, retry" — a different fault
// from a deadline (indistinguishable from an unreachable node) and from an
// unbounded wait (indistinguishable from a dead one), which is why callers that
// classify write failures should test for it. See lsm.ErrWriteStalled.
var ErrWriteStalled = lsm.ErrWriteStalled

// Store is a thread-safe key-value store. Create one with New.
type Store struct {
	engine *lsm.LSMTree
	logger *slog.Logger

	// cursors, when attached, is invalidated by RestoreFromSnapshot. It is
	// optional: a store with no replica cursors (a single node, or a test) has
	// nothing to invalidate. Attach it with WithCursorStore.
	cursors *CursorStore

	putCount atomic.Uint64
	getCount atomic.Uint64
	delCount atomic.Uint64
}

// Option configures a Store at construction.
type Option func(*Store)

// WithCursorStore attaches the node's replica cursors so that
// RestoreFromSnapshot can invalidate them. A snapshot restore replaces the whole
// logical store and starts a fresh WAL, which leaves any surviving cursor
// pointing into a log that no longer exists — see CursorStore.InvalidateAll for
// what goes wrong when it is left in place.
func WithCursorStore(cs *CursorStore) Option {
	return func(s *Store) { s.cursors = cs }
}

// New opens or creates the LSM-Tree under dataDir/lsm/.
func New(dataDir string, logger *slog.Logger, opts ...Option) (*Store, error) {
	engine, err := lsm.NewLSMTree(dataDir+"/lsm", logger)
	if err != nil {
		return nil, err
	}
	return newStore(engine, logger, opts), nil
}

// NewWithMetrics opens or creates the LSM-Tree under dataDir/lsm/ and wires it
// to the supplied *metrics.Metrics so engine-internal counters (bloom hits,
// flush/compaction bytes) appear in the node-wide metrics snapshot.
func NewWithMetrics(dataDir string, logger *slog.Logger, m *metrics.Metrics, opts ...Option) (*Store, error) {
	engine, err := lsm.NewLSMTree(dataDir+"/lsm", logger, lsm.WithMetrics(m))
	if err != nil {
		return nil, err
	}
	return newStore(engine, logger, opts), nil
}

func newStore(engine *lsm.LSMTree, logger *slog.Logger, opts []Option) *Store {
	s := &Store{engine: engine, logger: logger}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Get retrieves the value for key. Returns ErrNotFound if absent or deleted.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	s.getCount.Add(1)
	return s.engine.Get(ctx, key)
}

// Put writes key=value durably.
func (s *Store) Put(ctx context.Context, key string, value []byte) error {
	s.putCount.Add(1)
	return s.engine.Put(ctx, key, value)
}

// Delete removes key by writing a tombstone.
//
// Deletes are unconditional (blind) and therefore idempotent: deleting a key
// that does not exist succeeds. The previous implementation did a Get before the
// Delete to synthesise ErrNotFound, which was both racy (another writer could
// insert or remove the key between the two calls, so the answer was never
// authoritative) and expensive — a full read-path traversal through the
// memtable, every L0 SSTable and L1 on every delete. Pushing the check into the
// engine instead would mean holding the engine write lock across those disk
// reads, which is worse. Blind deletes match RocksDB, Cassandra and DynamoDB
// DeleteItem. Callers that must distinguish absence should Get first and accept
// that the result is advisory.
func (s *Store) Delete(ctx context.Context, key string) error {
	s.delCount.Add(1)
	return s.engine.Delete(ctx, key)
}

// KeyCount returns the approximate number of live keys.
//
// It is exact for workloads of distinct keys and drifts up on overwrites, or
// down on deletes, of keys that are no longer resident in the active memtable —
// classifying those correctly would cost a read on the write hot path. The value
// is served as `key_count` on /status alongside `key_count_approximate: true`.
// See lsm.LSMTree.LiveKeys for the full contract and how the count is persisted
// across restarts.
func (s *Store) KeyCount() int { return int(s.engine.LiveKeys()) }

// Counts returns cumulative operation counters since startup. walWrites is the
// number of fsync'd WAL appends performed by the storage engine — one per
// successful Put/Delete; WAL replay during recovery is not counted.
func (s *Store) Counts() (puts, gets, dels, walWrites uint64) {
	return s.putCount.Load(), s.getCount.Load(), s.delCount.Load(), s.engine.WALAppends()
}

// Snapshot returns a point-in-time copy of all live key-value pairs.
// Used by Raft to build an InstallSnapshot payload.
func (s *Store) Snapshot(ctx context.Context) (map[string][]byte, error) {
	return s.engine.Snapshot(ctx)
}

// RestoreFromSnapshot replaces all store contents with the given data.
// Used by Raft followers receiving an InstallSnapshot RPC.
//
// Any attached replica cursors are invalidated first. They describe positions in
// a WAL this call is about to discard, and a restore starts a fresh log that
// reuses the old segment numbers — so a surviving cursor silently reads as
// "replica is up to date" and lets the engine report a convergence it has not
// performed. CursorStore.InvalidateAll documents the three concrete failures and
// why invalidating before the restore (rather than after) is the safe ordering.
//
// Invalidation failing fails the restore: continuing would leave exactly the
// stale-cursor state this guards against, and the caller can retry an
// InstallSnapshot.
func (s *Store) RestoreFromSnapshot(ctx context.Context, data map[string][]byte) error {
	if s.cursors != nil {
		if err := s.cursors.InvalidateAll("store replaced from a snapshot; the WAL no longer describes this node's data"); err != nil {
			return fmt.Errorf("store: invalidate replica cursors for restore: %w", err)
		}
		if s.logger != nil {
			s.logger.Warn("store: replica cursors invalidated for snapshot restore; "+
				"anti-entropy cannot converge replicas from this node's WAL until a full "+
				"sync exists (v1 has none) — see anti_entropy_full_sync_required",
				"keys", len(data))
		}
	}
	return s.engine.Restore(ctx, data)
}

// Close shuts down the LSM-Tree (flushes pending data, closes file handles).
func (s *Store) Close() error {
	return s.engine.Close()
}

// ---------------------------------------------------------------------------
// Replica catch-up (anti-entropy) surface
// ---------------------------------------------------------------------------

// WALTip returns the position one past the last entry durably appended to this
// node's WAL. An anti-entropy pass reads up to it and no further.
func (s *Store) WALTip() storewal.Position { return s.engine.WALTip() }

// WALSegments returns every WAL segment an anti-entropy pass can read: the live
// segments plus any retained past their flush.
func (s *Store) WALSegments() ([]storewal.Segment, error) { return s.engine.WALSegments() }

// RetainWALFrom asks the engine to keep WAL segments numbered at or above seq
// even after they are flushed, so a lagging replica can still be caught up from
// them. Passing 0 releases retention. Retention is bounded — see
// lsm.LSMTree.RetainWALFrom.
func (s *Store) RetainWALFrom(seq uint64) { s.engine.RetainWALFrom(seq) }

// WALRetentionFloor reports the retention request currently in force.
func (s *Store) WALRetentionFloor() uint64 { return s.engine.WALRetentionFloor() }
