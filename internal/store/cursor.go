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

	// fullSync latches the "this node's WAL cannot converge its replicas"
	// condition set by InvalidateAll. See CursorStore.FullSyncRequired.
	fullSync       bool
	fullSyncReason string
}

// cursorFile is the on-disk representation. Positions are stored as their
// "segment:offset" string form so the file stays readable by an operator
// debugging a stuck replica.
type cursorFile struct {
	Version int               `json:"version"`
	Cursors map[string]string `json:"cursors"`

	// FullSyncRequired records that this node replaced its store from a snapshot,
	// so its WAL no longer describes the data it holds. It is persisted because
	// the condition outlives the process that discovered it: a restart must not
	// forget that the log cannot converge this node's replicas.
	//
	// Both fields are additive and omitted when unset, which is why the file
	// version is NOT bumped for them. A version bump would be a hard failure for
	// every node holding a version-1 file (OpenCursorStore rejects a version it
	// does not recognise), so adding a field that older readers ignore and newer
	// readers default to false is the compatible move.
	FullSyncRequired bool   `json:"full_sync_required,omitempty"`
	FullSyncReason   string `json:"full_sync_reason,omitempty"`
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
	cs.fullSync = f.FullSyncRequired
	cs.fullSyncReason = f.FullSyncReason
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

// InvalidateAll drops every cursor and latches the full-sync-required
// condition, persisting both before it returns. It is called by the snapshot
// restore path, which replaces the whole logical store.
//
// # Why the cursors must go
//
// A restore rewrites the store from a snapshot payload and starts a fresh WAL at
// segment 1, reusing segment numbers the old log had already used. A surviving
// cursor is then not merely stale, it is actively wrong in three ways, all
// verified against this package and the anti-entropy engine:
//
//  1. It orders *after* the new tip (a pre-restore 1:990 against a post-restore
//     1:0), so the engine's "cursor behind tip" check reads the replica as
//     up to date and schedules no catch-up at all.
//  2. Cursors are monotonic, so nothing can move it back: it is frozen until the
//     new log happens to grow past its old offset, and RetentionFloor keeps
//     reporting a segment from a log that no longer exists — which can make the
//     engine delete freshly flushed segments instead of parking them for
//     catch-up.
//  3. Once the new log does grow past that offset, a pass reads from a byte
//     offset that is mid-entry in a different log. The CRC catches it and the
//     reader reports a torn tail, which on the newest segment is a clean stop
//     with no error — so the pass ships nothing, reports no failure, and the
//     engine concludes the replica is caught up.
//
// Note that (3) is a *silent* wrong answer rather than the ErrCursorStale the
// reader raises for a collected segment: staleness is detected by segment
// number, and a restore reuses the number.
//
// # Why zeroing the cursors is not, by itself, convergence
//
// A zero cursor means "no evidence", and the engine reads it as "replay from the
// oldest surviving segment" — but only when something else triggers a pass; on
// its own a zero cursor deliberately does not mark a replica behind. More
// importantly, the restored payload is bulk-loaded into an L0 SSTable and never
// appended to the WAL, so after a restore there is nothing in the log to replay:
// a pass over the fresh WAL converges nothing. Zeroing the cursors removes the
// false state and restores retention sanity; it does not make replicas agree.
// That is what the latched flag is for.
//
// # Ordering
//
// The caller invalidates *before* replacing the store. Losing cursors is
// explicitly recoverable — a node without cursors re-sends from the oldest
// segment it still has — whereas stale cursors surviving a completed restore is
// the defect above. So a crash anywhere in the restore leaves the safe state.
// The consequence is that a restore that then fails still latches the flag: it
// over-reports a problem rather than under-reporting one, which is the direction
// this invariant demands.
func (cs *CursorStore) InvalidateAll(reason string) error {
	cs.mu.Lock()
	cs.cursors = make(map[string]storewal.Position)
	cs.fullSync = true
	cs.fullSyncReason = reason
	cs.dirty = true
	cs.mu.Unlock()
	return cs.Flush()
}

// FullSyncRequired reports whether this node has replaced its store from a
// snapshot without ever full-syncing its replicas, and why.
//
// It latches: v1 has no full-sync mechanism, so nothing clears it. That means it
// keeps reading true even after the affected keys have organically been
// rewritten and the replicas have in fact converged — it over-reports rather
// than going quiet while divergence remains, which is the only safe direction
// for a convergence claim.
//
// TODO(full-sync): the missing mechanism is a key-range scan shipped to the
// replica — walk this node's live keys (the engine can already iterate them for
// Snapshot), filter to the keys this node is ring-primary for with that replica
// in the replica set, send them with the ordinary Replicate RPC in bounded
// batches, and clear this flag per replica once a scan completes without a
// replication failure. It is deliberately not built here: unlike a WAL pass it
// is unbounded in the store's size, so it needs its own throttling, resumability
// and interaction with the write path.
func (cs *CursorStore) FullSyncRequired() (bool, string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.fullSync, cs.fullSyncReason
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
	f.FullSyncRequired = cs.fullSync
	f.FullSyncReason = cs.fullSyncReason
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
