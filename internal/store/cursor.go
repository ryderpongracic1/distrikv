// Package store — replica catch-up cursors.
//
// A cursor (high-water mark) records how far a given replica has been caught up
// in this node's WAL. It is the resume point for anti-entropy: everything before
// it is known to be on that replica, everything after it may not be.
//
// The cursors live in one small JSON file next to the store rather than inside
// the LSM manifest, because they are not part of the logical store: they
// describe what a *peer* has, they are advanced and rewritten far more often
// than the manifest, and losing them is recoverable (a node that loses its
// cursors re-sends from the oldest segment it still has, which converges the
// same replicas at the cost of extra work).
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// cursorFileName is the file the cursors are persisted to, inside the node's
// data directory.
const cursorFileName = "replica-cursors.json"

// CursorStore holds one WAL position per replica node and persists them
// durably. It is safe for concurrent use.
type CursorStore struct {
	path string

	mu      sync.Mutex
	cursors map[string]storewal.Position
	dirty   bool
}

// cursorFile is the on-disk representation. Positions are stored as their
// "segment:offset" string form so the file stays readable by an operator
// debugging a stuck replica.
type cursorFile struct {
	Version int               `json:"version"`
	Cursors map[string]string `json:"cursors"`
}

const cursorFileVersion = 1

// OpenCursorStore loads the cursors persisted under dataDir, or starts empty if
// none have been written yet.
//
// A cursor file that cannot be parsed is an error rather than a silent reset:
// starting from "no cursors" is a valid state, but reaching it by ignoring
// corruption would hide the fact that every replica is about to be re-sent the
// entire retained log.
func OpenCursorStore(dataDir string) (*CursorStore, error) {
	cs := &CursorStore{
		path:    filepath.Join(dataDir, cursorFileName),
		cursors: make(map[string]storewal.Position),
	}

	raw, err := os.ReadFile(cs.path)
	if err != nil {
		if os.IsNotExist(err) {
			return cs, nil
		}
		return nil, fmt.Errorf("store: read replica cursors: %w", err)
	}

	var f cursorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("store: parse replica cursors %q: %w", cs.path, err)
	}
	if f.Version != cursorFileVersion {
		return nil, fmt.Errorf("store: replica cursor file %q has version %d, want %d",
			cs.path, f.Version, cursorFileVersion)
	}
	for node, s := range f.Cursors {
		pos, err := storewal.ParsePosition(s)
		if err != nil {
			return nil, fmt.Errorf("store: replica cursor for %q: %w", node, err)
		}
		cs.cursors[node] = pos
	}
	return cs, nil
}

// Get returns the cursor recorded for nodeID. The zero Position means no cursor
// has ever been recorded, which callers read as "resume from the oldest segment
// still on disk".
func (cs *CursorStore) Get(nodeID string) storewal.Position {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.cursors[nodeID]
}

// All returns a copy of every recorded cursor.
func (cs *CursorStore) All() map[string]storewal.Position {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make(map[string]storewal.Position, len(cs.cursors))
	for k, v := range cs.cursors {
		out[k] = v
	}
	return out
}

// Advance moves nodeID's cursor forward to pos and reports whether it moved.
//
// Cursors are monotonic: an attempt to move one backwards is ignored. A cursor
// that could go backwards would let a node re-send entries it has already
// accounted for — harmless — but it would also let a bug or a stale in-flight
// update *un*-acknowledge progress, and the retention floor derived from these
// cursors would follow it back onto segments that had already been released.
func (cs *CursorStore) Advance(nodeID string, pos storewal.Position) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cur, ok := cs.cursors[nodeID]; ok && !cur.Before(pos) {
		return false
	}
	cs.cursors[nodeID] = pos
	cs.dirty = true
	return true
}

// Forget drops nodeID's cursor. Used when a node leaves the ring, so a departed
// peer cannot pin WAL segments forever.
func (cs *CursorStore) Forget(nodeID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, ok := cs.cursors[nodeID]; !ok {
		return
	}
	delete(cs.cursors, nodeID)
	cs.dirty = true
}

// RetentionFloor returns the lowest WAL segment number any cursor still points
// into, or 0 when no cursors are recorded. It is what the engine must not
// garbage-collect below.
func (cs *CursorStore) RetentionFloor() uint64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	var floor uint64
	for _, pos := range cs.cursors {
		if floor == 0 || pos.Segment < floor {
			floor = pos.Segment
		}
	}
	return floor
}

// Flush persists the cursors if any have changed since the last flush. It is a
// no-op otherwise, so it is cheap to call on a ticker.
//
// The write is atomic (temp file, fsync, rename, fsync of the directory), so a
// crash mid-flush leaves the previous generation intact. A torn cursor file
// would be worse than a stale one: a stale cursor re-sends entries, a corrupt
// one refuses to load at all.
func (cs *CursorStore) Flush() error {
	cs.mu.Lock()
	if !cs.dirty {
		cs.mu.Unlock()
		return nil
	}
	f := cursorFile{Version: cursorFileVersion, Cursors: make(map[string]string, len(cs.cursors))}
	for node, pos := range cs.cursors {
		f.Cursors[node] = pos.String()
	}
	cs.dirty = false
	cs.mu.Unlock()

	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		cs.markDirty()
		return fmt.Errorf("store: encode replica cursors: %w", err)
	}
	raw = append(raw, '\n')

	if err := writeFileAtomic(cs.path, raw); err != nil {
		cs.markDirty()
		return err
	}
	return nil
}

// markDirty re-arms the flush after a failed attempt so the change is not lost.
func (cs *CursorStore) markDirty() {
	cs.mu.Lock()
	cs.dirty = true
	cs.mu.Unlock()
}

// Nodes returns the recorded node IDs in sorted order (stable logging).
func (cs *CursorStore) Nodes() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]string, 0, len(cs.cursors))
	for k := range cs.cursors {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// writeFileAtomic writes data to path via a temp file and a rename, fsyncing
// both the file and its directory so the rename itself is durable.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("store: create temp for %q: %w", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("store: write temp for %q: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("store: sync temp for %q: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("store: close temp for %q: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("store: rename temp onto %q: %w", path, err)
	}

	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("store: open dir %q for sync: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("store: sync dir %q: %w", dir, err)
	}
	return nil
}
