package raft

import (
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Snapshot is a point-in-time copy of the state machine used for Raft log
// compaction (§7 of the Raft paper). Data is a gob-encoded map[string][]byte
// obtained from store.Snapshot() — not raw SSTable files.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              map[string][]byte
}

// SnapshotStore persists a single Raft snapshot to disk using the atomic
// write-to-tmp + sync + rename pattern for crash safety.
type SnapshotStore struct {
	path   string // e.g. /data/raft_snapshot.bin
	mu     sync.Mutex
	logger *slog.Logger
}

// NewSnapshotStore creates a SnapshotStore whose file lives under dir.
func NewSnapshotStore(dir string, logger *slog.Logger) *SnapshotStore {
	return &SnapshotStore{
		path:   filepath.Join(dir, "raft_snapshot.bin"),
		logger: logger,
	}
}

// Save gob-encodes snap to disk atomically. On crash between write and rename
// the old snapshot file is preserved.
func (ss *SnapshotStore) Save(snap Snapshot) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	tmp := ss.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("snapshot: create tmp: %w", err)
	}

	if err := gob.NewEncoder(f).Encode(snap); err != nil {
		f.Close()
		return fmt.Errorf("snapshot: encode: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("snapshot: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close tmp: %w", err)
	}
	if err := os.Rename(tmp, ss.path); err != nil {
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	return nil
}

// Load reads and decodes the snapshot. Returns (snap, true, nil) if found,
// or (_, false, nil) if no snapshot has been saved yet.
func (ss *SnapshotStore) Load() (Snapshot, bool, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	f, err := os.Open(ss.path)
	if os.IsNotExist(err) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("snapshot: open: %w", err)
	}
	defer f.Close()

	var snap Snapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return Snapshot{}, false, fmt.Errorf("snapshot: decode: %w", err)
	}
	return snap, true, nil
}

// Meta returns only the index and term of the saved snapshot, without loading
// the full data payload. Returns (0, 0, false) if no snapshot exists.
func (ss *SnapshotStore) Meta() (lastIndex, lastTerm uint64, exists bool) {
	snap, ok, err := ss.Load()
	if err != nil || !ok {
		return 0, 0, false
	}
	return snap.LastIncludedIndex, snap.LastIncludedTerm, true
}
