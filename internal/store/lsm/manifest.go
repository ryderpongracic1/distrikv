package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// ManifestEvent is one append-only record in the manifest log.
type ManifestEvent struct {
	Type   string `json:"type"`    // "add" or "remove"
	Path   string `json:"path"`    // SSTable basename, e.g. "sst-00000001.sst"
	SSTSeq uint64 `json:"sst_seq"` // SSTable sequence number for ordering
}

// Manifest is the source of truth for which SSTable files are live.
// It is an append-only log of add/remove events, rewritten atomically on
// every modification (write-to-tmp + sync + rename).
//
// Why not directory scan: directory listing order is not guaranteed across
// OSes. The manifest provides deterministic, sequence-ordered live file sets.
type Manifest struct {
	path   string
	events []ManifestEvent
	mu     sync.Mutex
}

// OpenManifest opens or creates the manifest at path. If the file does not
// exist an empty manifest is returned (fresh cluster).
func OpenManifest(path string) (*Manifest, error) {
	m := &Manifest{path: path}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return m, nil
	}
	if err != nil {
		return nil, fmt.Errorf("manifest: read %q: %w", path, err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var ev ManifestEvent
		if err := dec.Decode(&ev); err != nil {
			return nil, fmt.Errorf("manifest: decode event: %w", err)
		}
		m.events = append(m.events, ev)
	}
	return m, nil
}

// Add records a new SSTable as live and rewrites the manifest atomically.
func (m *Manifest) Add(baseName string, sstSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ManifestEvent{Type: "add", Path: baseName, SSTSeq: sstSeq})
	return m.writeAll()
}

// Remove marks an SSTable as deleted and rewrites the manifest atomically.
func (m *Manifest) Remove(baseName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ManifestEvent{Type: "remove", Path: baseName})
	return m.writeAll()
}

// LiveFiles returns live SSTable events in ascending SSTSeq order (oldest first).
func (m *Manifest) LiveFiles() []ManifestEvent {
	m.mu.Lock()
	defer m.mu.Unlock()

	counts := make(map[string]int)
	seqs := make(map[string]uint64)
	for _, ev := range m.events {
		switch ev.Type {
		case "add":
			counts[ev.Path]++
			seqs[ev.Path] = ev.SSTSeq
		case "remove":
			counts[ev.Path]--
		}
	}

	var live []ManifestEvent
	for path, cnt := range counts {
		if cnt > 0 {
			live = append(live, ManifestEvent{Path: path, SSTSeq: seqs[path]})
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].SSTSeq < live[j].SSTSeq })
	return live
}

// Reset atomically replaces the manifest with an empty state (used by RestoreFromSnapshot).
func (m *Manifest) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = nil
	return m.writeAll()
}

// writeAll rewrites the entire manifest file atomically. Caller must hold m.mu.
func (m *Manifest) writeAll() error {
	tmp := m.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("manifest: create tmp: %w", err)
	}

	enc := json.NewEncoder(f)
	for _, ev := range m.events {
		if err := enc.Encode(ev); err != nil {
			f.Close()
			return fmt.Errorf("manifest: encode event: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("manifest: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: close tmp: %w", err)
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return fmt.Errorf("manifest: rename: %w", err)
	}
	return nil
}
