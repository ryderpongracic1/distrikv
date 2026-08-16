package lsm

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/btree"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// Entry is a key-value record in the LSM-Tree. SeqNum is a monotonically
// increasing counter assigned at write time; higher SeqNum = newer version.
// This ordering is used during compaction to resolve duplicate keys across
// SSTable generations.
type Entry struct {
	Key       string
	Value     []byte
	Tombstone bool
	SeqNum    uint64
}

func entryLess(a, b Entry) bool { return a.Key < b.Key }

// Memtable is an in-memory sorted table backed by a WAL for crash recovery.
// It holds exactly one entry per key (overwrites replace prior versions).
// The SeqNum on each entry records when it was written, for SSTable merging.
type Memtable struct {
	tree    *btree.BTreeG[Entry]
	w       *storewal.WAL
	walPath string        // path to this memtable's WAL file; deleted after flush
	size    atomic.Int64  // approximate byte count of live entries
	maxSize int64
	mu      sync.RWMutex
	seqGen  *atomic.Uint64 // global sequence counter; shared with LSMTree

	// liveKeysAtSeal is the engine's approximate live-key count at the instant
	// this memtable was sealed (rotated out of the write path). flushMemtable
	// persists it to the manifest so the count survives restart. It is written
	// once under LSMTree.mu before the memtable is published as l.imm and read
	// after re-acquiring that lock, so it needs no synchronisation of its own.
	liveKeysAtSeal int64
}

// NewMemtable creates a Memtable with the given WAL and flush threshold.
func NewMemtable(w *storewal.WAL, walPath string, seqGen *atomic.Uint64, maxSizeBytes int64) *Memtable {
	return &Memtable{
		tree:    btree.NewG[Entry](32, entryLess),
		w:       w,
		walPath: walPath,
		maxSize: maxSizeBytes,
		seqGen:  seqGen,
	}
}

// livePutDelta returns the change in the live-key count caused by inserting a
// non-tombstone entry over (old, replaced) as reported by ReplaceOrInsert.
//
// The classification is memtable-local: when the key is not resident in this
// memtable (!replaced) the entry is assumed to be a brand-new key, because
// answering "does this key exist in an SSTable?" would require a full read-path
// traversal on every write. See LSMTree.LiveKeys for the resulting drift.
func livePutDelta(old Entry, replaced bool) int {
	if !replaced || old.Tombstone {
		return 1
	}
	return 0
}

// liveDeleteDelta returns the change in the live-key count caused by inserting
// a tombstone over (old, replaced). Same memtable-local caveat as livePutDelta:
// a tombstone for a key not resident here is assumed to have covered a live key.
func liveDeleteDelta(old Entry, replaced bool) int {
	if replaced && old.Tombstone {
		return 0 // already deleted in this memtable — no live key removed
	}
	return -1
}

// Put writes key=value. WAL is fsynced before the tree is updated.
// liveDelta is the (approximate) change in the engine's live-key count: +1 when
// this write created a live key, 0 when it overwrote one.
func (m *Memtable) Put(key string, value []byte) (liveDelta int, err error) {
	seq := m.seqGen.Add(1)
	if err := m.w.Append(storewal.OpPut, key, value); err != nil {
		return 0, fmt.Errorf("memtable: WAL put %q: %w", key, err)
	}
	m.mu.Lock()
	old, replaced := m.tree.ReplaceOrInsert(Entry{Key: key, Value: value, SeqNum: seq})
	if replaced {
		m.size.Add(-int64(len(old.Key) + len(old.Value)))
	}
	m.size.Add(int64(len(key) + len(value)))
	m.mu.Unlock()
	return livePutDelta(old, replaced), nil
}

// Delete writes a tombstone. WAL is fsynced before the tree is updated.
// liveDelta is the (approximate) change in the engine's live-key count: -1 when
// this tombstone covered a live key, 0 when the key was already tombstoned here.
func (m *Memtable) Delete(key string) (liveDelta int, err error) {
	seq := m.seqGen.Add(1)
	if err := m.w.Append(storewal.OpDelete, key, nil); err != nil {
		return 0, fmt.Errorf("memtable: WAL delete %q: %w", key, err)
	}
	m.mu.Lock()
	old, replaced := m.tree.ReplaceOrInsert(Entry{Key: key, Tombstone: true, SeqNum: seq})
	if replaced {
		m.size.Add(-int64(len(old.Key) + len(old.Value)))
	}
	m.size.Add(int64(len(key)))
	m.mu.Unlock()
	return liveDeleteDelta(old, replaced), nil
}

// Get retrieves the entry for key. Tombstones are returned — caller checks.
func (m *Memtable) Get(key string) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Get(Entry{Key: key})
}

// IsFull reports whether the memtable has reached its flush threshold.
func (m *Memtable) IsFull() bool { return m.size.Load() >= m.maxSize }

// SizeBytes returns the approximate byte count of live entries.
func (m *Memtable) SizeBytes() int64 { return m.size.Load() }

// Ascend calls fn for each entry in ascending key order, holding a read lock.
func (m *Memtable) Ascend(fn func(e Entry) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.tree.Ascend(fn)
}

// ReplayWAL replays a WAL file into this memtable's tree without writing to
// the active WAL (used during startup recovery). seqGen is advanced for each
// replayed entry so new writes receive higher sequence numbers.
//
// liveDelta is the net (approximate) change in the engine's live-key count
// implied by the replayed entries, so the caller can rebuild the count for
// writes that were not yet captured in an SSTable at shutdown/crash time.
func (m *Memtable) ReplayWAL(w *storewal.WAL) (liveDelta int64, err error) {
	err = w.Replay(func(op storewal.OpType, key string, value []byte) {
		seq := m.seqGen.Add(1)
		m.mu.Lock()
		switch op {
		case storewal.OpPut:
			old, replaced := m.tree.ReplaceOrInsert(Entry{Key: key, Value: value, SeqNum: seq})
			if replaced {
				m.size.Add(-int64(len(old.Key) + len(old.Value)))
			}
			m.size.Add(int64(len(key) + len(value)))
			liveDelta += int64(livePutDelta(old, replaced))
		case storewal.OpDelete:
			old, replaced := m.tree.ReplaceOrInsert(Entry{Key: key, Tombstone: true, SeqNum: seq})
			if replaced {
				m.size.Add(-int64(len(old.Key) + len(old.Value)))
			}
			m.size.Add(int64(len(key)))
			liveDelta += int64(liveDeleteDelta(old, replaced))
		}
		m.mu.Unlock()
	})
	return liveDelta, err
}
