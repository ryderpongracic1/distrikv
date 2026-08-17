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
	Level  int    `json:"level"`   // LSM level (0 = L0, 1 = L1, …). Defaults to 0 for pre-Phase-3 events.

	// LiveKeys, when non-nil, records the engine's approximate live-key count
	// as of the memtable rotation that produced this SSTable. It is written
	// only by memtable flushes (see LSMTree.flushMemtable), because a flush is
	// exactly the point where the invariant
	//
	//	live keys in SSTable set == count at rotation
	//
	// holds: the flushed memtable's WAL is deleted once the flush lands, and
	// every WAL still on disk contains only writes made after that rotation.
	// Replaying those WALs at open re-applies the remaining deltas.
	//
	// Compactions deliberately do not write this field: they run concurrently
	// with writes, so the live counter at compaction time already includes
	// writes that the active WAL will replay, which would double-count.
	//
	// Pointer + omitempty keeps manifests written before this field existed
	// decodable, and keeps "no count recorded" distinguishable from zero.
	LiveKeys *int64 `json:"live_keys,omitempty"`

	// MaxSeqNum records the highest write sequence number of any entry in this
	// SSTable. Every add — flush, compaction and restore alike — records it,
	// because it is what lets the engine reopen its write-sequence counter above
	// everything already on disk.
	//
	// Sequence numbers are the total write order compaction uses to resolve two
	// versions of a key: the higher one wins. The counter lives in memory, so
	// without this field it restarts at zero on every open, and "newer" comes to
	// mean "written earlier in a longer-lived process". A key written before a
	// restart then outranks the value that replaced it after, and the first
	// compaction to merge the two files keeps the stale one — silently, and
	// durably, since the loser is dropped rather than shadowed. The same
	// inversion resurrects a deleted key, because a tombstone that loses is
	// discarded at the bottom level.
	//
	// Pointer + omitempty for the same reason as LiveKeys: a manifest written
	// before this field must stay decodable, and "not recorded" has to be
	// distinguishable from zero so the engine knows to fall back to a scan.
	MaxSeqNum *uint64 `json:"max_seq_num,omitempty"`
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
// level is the LSM level of the SSTable (0 = L0, 1 = L1, …). maxSeqNum is the
// highest write sequence number in the file — see ManifestEvent.MaxSeqNum for
// why every add must supply it.
func (m *Manifest) Add(baseName string, sstSeq uint64, level int, maxSeqNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := maxSeqNum
	m.events = append(m.events, ManifestEvent{
		Type: "add", Path: baseName, SSTSeq: sstSeq, Level: level, MaxSeqNum: &seq,
	})
	return m.writeAll()
}

// AddWithLiveKeys is Add plus a durable record of the engine's approximate
// live-key count as of the rotation that produced this SSTable. Only memtable
// flushes may use it — see the ManifestEvent.LiveKeys doc for why compactions
// must not. Folding the count into the same event keeps it to one atomic
// manifest rewrite per flush.
func (m *Manifest) AddWithLiveKeys(baseName string, sstSeq uint64, level int, liveKeys int64, maxSeqNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := liveKeys
	seq := maxSeqNum
	m.events = append(m.events, ManifestEvent{
		Type: "add", Path: baseName, SSTSeq: sstSeq, Level: level, LiveKeys: &n, MaxSeqNum: &seq,
	})
	return m.writeAll()
}

// MaxSeqNum returns the highest write sequence number recorded across the live
// SSTables, and whether every live file carried one.
//
// A false second result means the manifest predates the field for at least one
// live file, so this value is not an upper bound on what is on disk and the
// caller must establish one another way. Answering "0, true" for a store with no
// live SSTables is correct rather than incomplete: there is nothing on disk to
// outrank.
func (m *Manifest) MaxSeqNum() (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var max uint64
	complete := true
	for _, ev := range m.liveFilesLocked() {
		if ev.MaxSeqNum == nil {
			complete = false
			continue
		}
		if *ev.MaxSeqNum > max {
			max = *ev.MaxSeqNum
		}
	}
	return max, complete
}

// LastLiveKeys returns the most recently recorded live-key count and whether
// any event carried one. A false result means the manifest predates the field
// or no flush has happened yet, and the caller should seed the count from WAL
// replay alone.
func (m *Manifest) LastLiveKeys() (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.events) - 1; i >= 0; i-- {
		if n := m.events[i].LiveKeys; n != nil {
			return *n, true
		}
	}
	return 0, false
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
	return m.liveFilesLocked()
}

// liveFilesLocked is LiveFiles without the lock. Caller must hold m.mu.
func (m *Manifest) liveFilesLocked() []ManifestEvent {
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

	levels := make(map[string]int)
	maxSeqs := make(map[string]*uint64)
	for _, ev := range m.events {
		if ev.Type == "add" {
			levels[ev.Path] = ev.Level
			maxSeqs[ev.Path] = ev.MaxSeqNum
		}
	}

	var live []ManifestEvent
	for path, cnt := range counts {
		if cnt > 0 {
			live = append(live, ManifestEvent{
				Path: path, SSTSeq: seqs[path], Level: levels[path], MaxSeqNum: maxSeqs[path],
			})
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
