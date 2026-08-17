package store

import (
	"os"
	"path/filepath"
	"testing"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// The cursor store is what makes replica catch-up survive a restart of the
// primary. These tests pin the three properties the anti-entropy engine relies
// on: the cursors come back, they never go backwards, and the retention floor
// they imply is the oldest one.

func TestCursorStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()

	cs, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("OpenCursorStore: %v", err)
	}
	if got := cs.Get("node2"); !got.IsZero() {
		t.Fatalf("a fresh cursor store reported %s for an unknown node, want the zero position", got)
	}

	cs.Advance("node2", storewal.Position{Segment: 3, Offset: 128})
	cs.Advance("node3", storewal.Position{Segment: 7, Offset: 9})
	if err := cs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reopened, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := reopened.Get("node2"); got != (storewal.Position{Segment: 3, Offset: 128}) {
		t.Errorf("node2 cursor after reopen = %s, want 3:128", got)
	}
	if got := reopened.Get("node3"); got != (storewal.Position{Segment: 7, Offset: 9}) {
		t.Errorf("node3 cursor after reopen = %s, want 7:9", got)
	}
	if got := reopened.Nodes(); len(got) != 2 || got[0] != "node2" || got[1] != "node3" {
		t.Errorf("Nodes() = %v, want [node2 node3]", got)
	}
}

// TestCursorsAreMonotonic pins the invariant that keeps WAL retention safe: a
// cursor that could move backwards would drag the retention floor back onto
// segments the engine has already been told it may delete.
func TestCursorsAreMonotonic(t *testing.T) {
	cs, err := OpenCursorStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCursorStore: %v", err)
	}

	if !cs.Advance("node2", storewal.Position{Segment: 4, Offset: 100}) {
		t.Fatal("the first Advance reported no change")
	}
	if cs.Advance("node2", storewal.Position{Segment: 4, Offset: 50}) {
		t.Error("Advance accepted an earlier offset")
	}
	if cs.Advance("node2", storewal.Position{Segment: 2, Offset: 999}) {
		t.Error("Advance accepted an earlier segment")
	}
	if cs.Advance("node2", storewal.Position{Segment: 4, Offset: 100}) {
		t.Error("Advance reported a change for the position it already held")
	}
	if got := cs.Get("node2"); got != (storewal.Position{Segment: 4, Offset: 100}) {
		t.Errorf("cursor = %s after rejected rewinds, want 4:100", got)
	}
	if !cs.Advance("node2", storewal.Position{Segment: 5}) {
		t.Error("Advance rejected a later segment")
	}
}

func TestCursorRetentionFloorIsTheOldest(t *testing.T) {
	cs, err := OpenCursorStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenCursorStore: %v", err)
	}
	if got, ok := cs.RetentionFloor(); ok {
		t.Errorf("floor with no cursors = (%d, true), want ok=false: "+
			"\"nothing is recorded\" must not be reported as a floor value", got)
	}

	cs.Advance("node2", storewal.Position{Segment: 9})
	cs.Advance("node3", storewal.Position{Segment: 4, Offset: 12})
	got, ok := cs.RetentionFloor()
	if !ok || got != 4 {
		t.Errorf("floor = (%d, %t), want (4, true) — the segment the furthest-behind replica still needs", got, ok)
	}

	// A node that leaves the ring must stop pinning segments.
	cs.Forget("node3")
	got, ok = cs.RetentionFloor()
	if !ok || got != 9 {
		t.Errorf("floor after forgetting the lagging replica = (%d, %t), want (9, true)", got, ok)
	}
	if got := cs.Get("node3"); !got.IsZero() {
		t.Errorf("a forgotten node still has cursor %s", got)
	}

	// Segment 1 is a legal floor and must be distinguishable from "no cursors",
	// which is the ambiguity the second result exists to remove.
	cs.Forget("node2")
	cs.Advance("node4", storewal.Position{Segment: 1})
	if got, ok := cs.RetentionFloor(); !ok || got != 1 {
		t.Errorf("floor with a cursor in segment 1 = (%d, %t), want (1, true)", got, ok)
	}
}

// TestCursorFlushIsSkippedWhenNothingChanged keeps the ticker cheap: Flush runs
// every couple of seconds for the life of the process.
func TestCursorFlushIsSkippedWhenNothingChanged(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("OpenCursorStore: %v", err)
	}
	path := filepath.Join(dir, cursorFileName)

	if err := cs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("flushing an unchanged store wrote %q", path)
	}

	cs.Advance("node2", storewal.Position{Segment: 1, Offset: 1})
	if err := cs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	first, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after a real change: %v", err)
	}

	if err := cs.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	second, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !first.ModTime().Equal(second.ModTime()) {
		t.Error("a no-op Flush rewrote the cursor file")
	}
}

// TestCursorStoreRefusesACorruptFile pins the deliberate choice not to silently
// reset: starting from no cursors is valid, but arriving there by ignoring
// corruption would quietly re-send the entire retained log to every replica.
func TestCursorStoreRefusesACorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cursorFileName)

	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenCursorStore(dir); err == nil {
		t.Error("OpenCursorStore accepted an unparsable cursor file")
	}

	if err := os.WriteFile(path, []byte(`{"version":99,"cursors":{}}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenCursorStore(dir); err == nil {
		t.Error("OpenCursorStore accepted an unknown file version")
	}

	if err := os.WriteFile(path, []byte(`{"version":1,"cursors":{"node2":"bogus"}}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := OpenCursorStore(dir); err == nil {
		t.Error("OpenCursorStore accepted a malformed position")
	}
}

// TestMarkFullSyncRequiredLatchesWithoutDroppingCursors covers the second way a
// node loses the ability to converge a replica from its log: retention dropped
// segments the replica still needed. Unlike a snapshot restore this must NOT
// discard the cursors — they are the only record of how far each replica did get,
// and losing them makes every later pass restart from the oldest surviving
// segment.
func TestMarkFullSyncRequiredLatchesWithoutDroppingCursors(t *testing.T) {
	dir := t.TempDir()
	cs, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("OpenCursorStore: %v", err)
	}
	cs.Advance("node2", storewal.Position{Segment: 7, Offset: 24})
	if required, _ := cs.FullSyncRequired(); required {
		t.Fatal("precondition: full sync required before anything asked for it")
	}

	const reason = "replica node2 cursor 3:0 points into a released WAL segment"
	if err := cs.MarkFullSyncRequired(reason); err != nil {
		t.Fatalf("MarkFullSyncRequired: %v", err)
	}

	required, got := cs.FullSyncRequired()
	if !required || got != reason {
		t.Errorf("FullSyncRequired() = (%t, %q), want (true, %q)", required, got, reason)
	}
	if pos := cs.Get("node2"); pos != (storewal.Position{Segment: 7, Offset: 24}) {
		t.Errorf("cursor for node2 = %s after latching, want it untouched at 7:24", pos)
	}

	// A permanent gap is rediscovered on every pass, so a repeat call must be a
	// no-op rather than a flush per pass — and the first reason must survive it.
	if err := cs.MarkFullSyncRequired("a later, less specific reason"); err != nil {
		t.Fatalf("second MarkFullSyncRequired: %v", err)
	}
	if _, got := cs.FullSyncRequired(); got != reason {
		t.Errorf("reason after a repeat call = %q, want the first one %q", got, reason)
	}

	// The segments are gone permanently, so the latch has to outlive the process.
	reopened, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if required, got := reopened.FullSyncRequired(); !required || got != reason {
		t.Errorf("after reopen FullSyncRequired() = (%t, %q), want (true, %q)", required, got, reason)
	}
	if pos := reopened.Get("node2"); pos != (storewal.Position{Segment: 7, Offset: 24}) {
		t.Errorf("cursor for node2 = %s after reopen, want 7:24", pos)
	}
}
