package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// LogEntry is a single record in the Raft log.
type LogEntry struct {
	Index uint64
	Term  uint64
	Op    string // "put" or "delete"
	Key   string
	Value []byte
}

// persistedData is the JSON-encoded persistent state for a Raft node.
// Extends the original term+votedFor with snapshot metadata so that after
// log compaction the virtual log offset survives restarts.
//
// Format switch from bespoke binary to JSON: the save frequency (once per
// term/vote change) makes the minor encoding overhead negligible, and JSON
// is far easier to extend without a custom CRC scheme.
type persistedData struct {
	CurrentTerm   uint64 `json:"current_term"`
	VotedFor      string `json:"voted_for"`
	SnapLastIndex uint64 `json:"snap_last_index"`
	SnapLastTerm  uint64 `json:"snap_last_term"`
}

// PersistentState manages the Raft fields that must survive crashes:
// currentTerm, votedFor, and snapshot virtual-log offsets.
// Uses atomic write: tmp → sync → rename.
type PersistentState struct {
	path string
	mu   sync.Mutex
}

func newPersistentState(path string) *PersistentState {
	return &PersistentState{path: path}
}

// Save atomically writes all persistent state to disk.
func (p *PersistentState) Save(term uint64, votedFor string, snapLastIndex, snapLastTerm uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	d := persistedData{
		CurrentTerm:   term,
		VotedFor:      votedFor,
		SnapLastIndex: snapLastIndex,
		SnapLastTerm:  snapLastTerm,
	}

	tmp := p.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("raft: persist create tmp: %w", err)
	}
	if err := json.NewEncoder(f).Encode(d); err != nil {
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
	return nil
}

// Load reads the last saved state. Returns zero values on fresh node.
func (p *PersistentState) Load() (term uint64, votedFor string, snapLastIndex, snapLastTerm uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.Open(p.path)
	if os.IsNotExist(err) {
		return 0, "", 0, 0, nil
	}
	if err != nil {
		return 0, "", 0, 0, fmt.Errorf("raft: persist open: %w", err)
	}
	defer f.Close()

	var d persistedData
	if err := json.NewDecoder(f).Decode(&d); err != nil {
		return 0, "", 0, 0, fmt.Errorf("raft: persist decode: %w", err)
	}
	return d.CurrentTerm, d.VotedFor, d.SnapLastIndex, d.SnapLastTerm, nil
}
