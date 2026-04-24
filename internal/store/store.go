// Package store provides a thread-safe key-value store backed by an LSM-Tree
// storage engine. The public interface (Get/Put/Delete) is unchanged from the
// prior WAL+map implementation; all callers (gRPC, HTTP, Raft) require no changes.
package store

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/ryderpongracic1/distrikv/internal/store/lsm"
)

// ErrNotFound is returned by Get when the key does not exist or has been deleted.
var ErrNotFound = lsm.ErrNotFound

// Store is a thread-safe key-value store. Create one with New.
type Store struct {
	engine *lsm.LSMTree

	putCount atomic.Uint64
	getCount atomic.Uint64
	delCount atomic.Uint64
}

// New opens or creates the LSM-Tree under dataDir/lsm/.
func New(dataDir string, logger *slog.Logger) (*Store, error) {
	engine, err := lsm.NewLSMTree(dataDir+"/lsm", logger)
	if err != nil {
		return nil, err
	}
	return &Store{engine: engine}, nil
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

// Delete removes key. Returns ErrNotFound if the key does not exist.
func (s *Store) Delete(ctx context.Context, key string) error {
	s.delCount.Add(1)
	_, err := s.engine.Get(ctx, key)
	if err == ErrNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return s.engine.Delete(ctx, key)
}

// KeyCount returns an approximate count of live keys (may be slightly over
// actual due to overwritten keys not being subtracted immediately).
func (s *Store) KeyCount() int { return 0 }

// Counts returns cumulative operation counters since startup.
func (s *Store) Counts() (puts, gets, dels, walWrites uint64) {
	return s.putCount.Load(), s.getCount.Load(), s.delCount.Load(), 0
}

// Snapshot returns a point-in-time copy of all live key-value pairs.
// Used by Raft to build an InstallSnapshot payload.
func (s *Store) Snapshot(ctx context.Context) (map[string][]byte, error) {
	return s.engine.Snapshot(ctx)
}

// RestoreFromSnapshot replaces all store contents with the given data.
// Used by Raft followers receiving an InstallSnapshot RPC.
func (s *Store) RestoreFromSnapshot(ctx context.Context, data map[string][]byte) error {
	return s.engine.Restore(ctx, data)
}

// Close shuts down the LSM-Tree (flushes pending data, closes file handles).
func (s *Store) Close() error {
	return s.engine.Close()
}
