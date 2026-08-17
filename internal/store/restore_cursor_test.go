package store

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

// Restore vs replica cursors.
//
// A snapshot restore replaces the whole logical store and starts a fresh WAL at
// segment 1, reusing segment numbers the discarded log had already used. A
// replica cursor that outlives that is worse than stale — it is a position in a
// different log that happens to look valid, which is why the engine's
// ErrCursorStale guard (keyed on the segment *number*) never fires for it.
//
// The invariant these tests pin: after a restore, no replica cursor references a
// pre-restore WAL position, and the node's recorded convergence state is
// truthful about the fact that its log can no longer converge its replicas.

// storeWithCursors builds a store wired to its own cursor store, as cmd/node
// wires them.
func storeWithCursors(t *testing.T) (*Store, *CursorStore, string) {
	t.Helper()
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cs, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}
	s, err := New(dir, logger, WithCursorStore(cs))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, cs, dir
}

// advanceCursorToTip writes enough to move the WAL and records a cursor at the
// resulting tip, which is the state a healthy cluster's quiet-cursor ticker
// leaves behind.
func advanceCursorToTip(t *testing.T, s *Store, cs *CursorStore, replica string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if _, err := s.Put(ctx, fmt.Sprintf("pre-restore-%d", i), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	tip := s.WALTip()
	if tip.IsZero() {
		t.Fatal("WAL tip is still zero after 50 writes; the test cannot set up a real cursor")
	}
	if !cs.Advance(replica, tip) {
		t.Fatalf("Advance(%s, %s) was rejected; the test needs a recorded cursor", replica, tip)
	}
	if err := cs.Flush(); err != nil {
		t.Fatalf("flush cursors: %v", err)
	}
}

// TestRestoreInvalidatesReplicaCursors is the invariant: a restore leaves no
// cursor addressing the log it discarded.
//
// Without the fix the cursor survives at its pre-restore position, which orders
// *after* the post-restore tip — so the engine reads the replica as up to date,
// monotonicity forbids moving the cursor back, and the retention floor keeps
// naming a segment of a log that no longer exists.
func TestRestoreInvalidatesReplicaCursors(t *testing.T) {
	s, cs, _ := storeWithCursors(t)
	const replica = "node2"

	advanceCursorToTip(t, s, cs, replica)
	preRestore := cs.Get(replica)
	if preRestore.IsZero() {
		t.Fatal("precondition: no cursor recorded before the restore")
	}
	if _, ok := cs.RetentionFloor(); !ok {
		t.Fatal("precondition: no WAL retention floor before the restore")
	}

	if err := s.RestoreFromSnapshot(context.Background(), map[string][]byte{
		"snapshot-a": []byte("1"),
		"snapshot-b": []byte("2"),
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if got := cs.Get(replica); !got.IsZero() {
		t.Errorf("cursor for %s is %s after the restore, want the zero position: it "+
			"addresses a WAL that no longer exists", replica, got)
	}
	if all := cs.All(); len(all) != 0 {
		t.Errorf("cursors still recorded after the restore: %v", all)
	}
	if floor, ok := cs.RetentionFloor(); ok {
		t.Errorf("WAL retention floor is (%d, true) after the restore, want ok=false: a "+
			"floor from the discarded log makes the engine drop freshly flushed "+
			"segments instead of retaining them for catch-up", floor)
	}

	// The specific silent failure this guards: a surviving cursor does not order
	// before the new tip, so "cursor behind tip" reads false and no catch-up is
	// ever scheduled.
	tip := s.WALTip()
	if preRestore.Before(tip) {
		t.Fatalf("precondition drifted: pre-restore cursor %s orders before the "+
			"post-restore tip %s, so this test would pass without the fix", preRestore, tip)
	}
}

// TestRestoreCursorInvalidationIsDurable pins that the invalidation is on disk,
// not just in memory. A process that restored and then restarted must not read
// the pre-restore cursors back.
func TestRestoreCursorInvalidationIsDurable(t *testing.T) {
	s, cs, dir := storeWithCursors(t)
	const replica = "node2"

	advanceCursorToTip(t, s, cs, replica)

	if err := s.RestoreFromSnapshot(context.Background(), map[string][]byte{"k": []byte("v")}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	reopened, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("reopen cursor store: %v", err)
	}
	if got := reopened.Get(replica); !got.IsZero() {
		t.Errorf("cursor for %s read back as %s after a restart, want the zero "+
			"position", replica, got)
	}
	if all := reopened.All(); len(all) != 0 {
		t.Errorf("cursors survived the restore on disk: %v", all)
	}
}

// TestRestoreLatchesFullSyncRequired pins the honesty half of the fix. Zeroing
// the cursors removes the false state but converges nothing: the restored payload
// is bulk-loaded into an SSTable and never appended to the WAL, so there is
// nothing in the log for a catch-up pass to replay. The node has to record that.
func TestRestoreLatchesFullSyncRequired(t *testing.T) {
	s, cs, dir := storeWithCursors(t)

	if required, _ := cs.FullSyncRequired(); required {
		t.Fatal("precondition: full sync already required before any restore")
	}

	if err := s.RestoreFromSnapshot(context.Background(), map[string][]byte{"k": []byte("v")}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	required, reason := cs.FullSyncRequired()
	if !required {
		t.Error("full sync is not marked required after a restore: the node would " +
			"report catch-up over an empty log as convergence")
	}
	if reason == "" {
		t.Error("full-sync-required carries no reason; an operator reading the cursor " +
			"file cannot tell why")
	}

	// Durable for the same reason the cursors are: the condition outlives the
	// process that discovered it.
	reopened, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("reopen cursor store: %v", err)
	}
	if required, _ := reopened.FullSyncRequired(); !required {
		t.Error("full-sync-required did not survive a restart, so a restarted node " +
			"would silently resume claiming convergence")
	}
}

// TestRestoreWithoutCursorStoreStillWorks guards the optional wiring: a store
// with no replica cursors (a single node, or a test) has nothing to invalidate
// and must restore normally.
func TestRestoreWithoutCursorStoreStillWorks(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(dir, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.RestoreFromSnapshot(ctx, map[string][]byte{"only": []byte("value")}); err != nil {
		t.Fatalf("restore without a cursor store: %v", err)
	}
	got, err := s.Get(ctx, "only")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if string(got) != "value" {
		t.Errorf("restored value = %q, want %q", got, "value")
	}
}

// TestCursorFileVersionUnchangedByFullSyncFields pins backward compatibility: a
// cursor file written before the full-sync fields existed must still load, since
// OpenCursorStore rejects a version it does not recognise and a node that cannot
// read its cursor file does not start.
func TestCursorFileVersionUnchangedByFullSyncFields(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"version":1,"cursors":{"node2":"3:120"}}` + "\n"
	if err := writeFileAtomic(dir+"/"+cursorFileName, []byte(legacy)); err != nil {
		t.Fatalf("write legacy cursor file: %v", err)
	}

	cs, err := OpenCursorStore(dir)
	if err != nil {
		t.Fatalf("a pre-existing version-1 cursor file must still load: %v", err)
	}
	if got := cs.Get("node2").String(); got != "3:120" {
		t.Errorf("legacy cursor = %s, want 3:120", got)
	}
	if required, _ := cs.FullSyncRequired(); required {
		t.Error("a legacy cursor file must default to full sync NOT required")
	}
}
