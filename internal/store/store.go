// Package store provides a thread-safe in-memory key-value store backed by a
// Write-Ahead Log (WAL) for crash recovery. Every mutation is appended to the
// WAL and fsynced before being applied to the in-memory map, so state survives
// restarts through WAL replay.
package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrNotFound is returned by Get when the key does not exist or has been
// deleted.
var ErrNotFound = fmt.Errorf("key not found")

// record is a single in-memory entry.
type record struct {
	value     []byte
	tombstone bool // true after Delete — used during WAL replay
}

// Store is a thread-safe in-memory key-value store. Create one with New.
type Store struct {
	mu   sync.RWMutex
	data map[string]record
	wal  *WAL

	// Counters incremented by the store itself; exposed for metrics.
	putCount atomic.Uint64
	getCount atomic.Uint64
	delCount atomic.Uint64
	walWrites atomic.Uint64
}

// New opens (or creates) the WAL at walPath, replays existing entries into
// memory, and returns a ready-to-use Store.
func New(walPath string) (*Store, error) {
	wal, err := openWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("store.New: %w", err)
	}

	s := &Store{
		data: make(map[string]record),
		wal:  wal,
	}

	if err := s.replay(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("store.New: replay: %w", err)
	}

	return s, nil
}

// replay reads the WAL and reconstructs the in-memory map. Called once during
// New before the store is returned.
func (s *Store) replay() error {
	return s.wal.Replay(func(op opType, key string, value []byte) {
		switch op {
		case opPut:
			s.data[key] = record{value: value}
		case opDelete:
			delete(s.data, key)
		}
	})
}

// Get retrieves the value for key. Returns ErrNotFound if the key does not
// exist.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	s.getCount.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.data[key]
	if !ok || r.tombstone {
		return nil, ErrNotFound
	}
	// Return a copy so callers cannot mutate internal state.
	out := make([]byte, len(r.value))
	copy(out, r.value)
	return out, nil
}

// Put writes key=value. The WAL entry is fsynced before the in-memory map is
// updated. Returns an error if the WAL write fails; in that case the in-memory
// map is not modified.
func (s *Store) Put(_ context.Context, key string, value []byte) error {
	s.putCount.Add(1)

	if err := s.wal.Append(opPut, key, value); err != nil {
		return fmt.Errorf("store.Put %q: %w", key, err)
	}
	s.walWrites.Add(1)

	s.mu.Lock()
	s.data[key] = record{value: value}
	s.mu.Unlock()

	return nil
}

// Delete removes key. A tombstone record is written to the WAL so that replay
// correctly reconstructs the deletion. Returns ErrNotFound if the key does not
// exist (the WAL is not written in that case).
func (s *Store) Delete(_ context.Context, key string) error {
	s.delCount.Add(1)

	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}

	if err := s.wal.Append(opDelete, key, nil); err != nil {
		return fmt.Errorf("store.Delete %q: %w", key, err)
	}
	s.walWrites.Add(1)

	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()

	return nil
}

// KeyCount returns the number of live keys currently held in memory.
func (s *Store) KeyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Counts returns the total put, get, delete, and WAL-write counters since the
// store was created (survives across replays but resets on process restart).
func (s *Store) Counts() (puts, gets, dels, walWrites uint64) {
	return s.putCount.Load(), s.getCount.Load(), s.delCount.Load(), s.walWrites.Load()
}

// Close flushes and closes the underlying WAL. No further calls to the store
// are valid after Close returns.
func (s *Store) Close() error {
	return s.wal.Close()
}
