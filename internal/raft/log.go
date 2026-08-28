package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// LogEntry is a single record in the Raft log.
//
// Op is opaque to Raft: the state machine interprets it (see StateMachine).
// Index and Term are assigned by the leader that created the entry and are
// never rewritten — an entry that must change is truncated and replaced.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// persistedState is the Raft state that must survive a crash: the fields the
// paper calls persistent (currentTerm, votedFor, the log) plus the snapshot
// bounds that anchor the log's virtual indexing.
//
// commitIndex and lastApplied are deliberately absent. Both are volatile in the
// paper: a restarting follower relearns commitIndex from the next AppendEntries
// and a restarting leader from its own majority accounting, so persisting them
// would add a durability cost for state that is reconstructed within one
// heartbeat. The consequence, which the StateMachine contract states, is that
// entries between the snapshot point and the pre-crash lastApplied are applied
// again after a restart — so Apply must be idempotent.
type persistedState struct {
	CurrentTerm   uint64     `json:"current_term"`
	VotedFor      string     `json:"voted_for"`
	SnapLastIndex uint64     `json:"snap_last_index"`
	SnapLastTerm  uint64     `json:"snap_last_term"`
	Log           []LogEntry `json:"log,omitempty"`
}

// PersistentState manages the Raft fields that must survive crashes.
// Uses atomic write: tmp → sync → rename.
//
// # Why a full rewrite per save
//
// Every Save serialises the entire log rather than appending a record. That is
// a deliberate trade for this system's shape, not an oversight: the Raft log
// here carries node-health transitions only — never client data (see the
// package doc) — so it grows by a handful of tiny entries when a node's
// reachability actually changes, and log compaction keeps it bounded. A
// rewrite of a few hundred bytes on an event that rare is cheaper than the
// bookkeeping an append-only segment format would need, and it keeps the
// crash-atomicity argument down to one sentence: the rename is the commit
// point, so a reader sees either the whole prior state or the whole new one.
//
// The honest ceiling: cost is O(entries) per persisted append, so a workload
// that proposed thousands of entries per second would need a segmented,
// append-only log with periodic checkpoints instead. Should the Raft log ever
// carry data, this is the first thing that has to change.
type PersistentState struct {
	path string
	mu   sync.Mutex

	// saves counts completed successful writes. It exists so tests can assert
	// what does *not* touch the disk — a pure heartbeat and a duplicate
	// AppendEntries must both be free of I/O, since persistence on the
	// heartbeat path is the shape of the defect that once cost this cluster a
	// heartbeat every interval.
	saves atomic.Uint64
}

func newPersistentState(path string) *PersistentState {
	return &PersistentState{path: path}
}

// saveCount reports how many successful writes have completed.
func (p *PersistentState) saveCount() uint64 { return p.saves.Load() }

// Save atomically writes all persistent state to disk. It returns only after
// the bytes are durable, so a caller may treat a nil error as "this state
// survives a crash from here" — which is what lets a follower ACK an entry and
// a leader count its own entry toward a majority.
func (p *PersistentState) Save(st persistedState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmp := p.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("raft: persist create tmp: %w", err)
	}
	if err := json.NewEncoder(f).Encode(st); err != nil {
		f.Close()
		return fmt.Errorf("raft: persist encode: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: persist sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: persist close tmp: %w", err)
	}
	if err := os.Rename(tmp, p.path); err != nil {
		return fmt.Errorf("raft: persist rename: %w", err)
	}
	// The rename is the commit point, and it is a directory operation: syncing
	// the file only guarantees its *contents*. Without this the entry can be on
	// disk under the temporary name while the directory entry pointing at it is
	// still in the page cache, so a crash here loses the rename and Load reads
	// the previous state — after Save returned nil and a follower ACKed on the
	// strength of it. That is the promise in this method's doc comment, and Raft
	// safety rests on it.
	if err := syncDir(filepath.Dir(p.path)); err != nil {
		return fmt.Errorf("raft: persist sync dir: %w", err)
	}
	p.saves.Add(1)
	return nil
}

// syncDir fsyncs a directory, making a rename within it durable.
//
// Raft cannot borrow store.writeFileAtomic, which does the same thing one
// package over: this package is deliberately free of any dependency on the data
// path (see the package doc, deviation 1), and that is worth more than the
// duplicated dozen lines.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open %q: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync %q: %w", dir, err)
	}
	return nil
}

// Load reads the last saved state. Returns the zero value on a fresh node.
func (p *PersistentState) Load() (persistedState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.Open(p.path)
	if os.IsNotExist(err) {
		return persistedState{}, nil
	}
	if err != nil {
		return persistedState{}, fmt.Errorf("raft: persist open: %w", err)
	}
	defer f.Close()

	var st persistedState
	if err := json.NewDecoder(f).Decode(&st); err != nil {
		return persistedState{}, fmt.Errorf("raft: persist decode: %w", err)
	}
	return st, nil
}
