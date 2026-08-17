package lsm

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// WAL segment numbers address replica catch-up cursors, so they must never be
// reused. A cursor is a (segment, offset) pair and wal.ErrCursorStale is keyed on
// the segment *number*: hand a number a second time and every cursor recorded
// against the first log silently addresses the second one, which is a wrong
// convergence claim rather than a detected error.
//
// The reuse was reachable through an ordinary clean shutdown. Close flushes the
// active memtable into an SSTable and releases its segment, so a gracefully
// stopped store can be left with no live segment at all — and the sequence number
// used to be seeded from the live segments only.

// openTreeAt opens a tree on dir with production-sized defaults, so a handful of
// writes stays in one segment the way the chaos workload does.
func openTreeAt(t *testing.T, dir string) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewLSMTree(%q): %v", dir, err)
	}
	return l
}

// TestGracefulRestartDoesNotReuseWALSegmentNumbers is the repro. A store that
// parks its only segment for replica catch-up and is then closed cleanly must not
// reopen numbering its new segment 1, because a segment 1 already exists.
func TestGracefulRestartDoesNotReuseWALSegmentNumbers(t *testing.T) {
	dir := t.TempDir()

	l := openTreeAt(t, dir)
	// A replica is behind and its cursor points into segment 1, so the engine is
	// asked to keep that segment past its flush — the state anti-entropy puts the
	// engine in during every fault window.
	l.RetainWALFrom(1)
	if err := l.Put(context.Background(), "before-restart", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	firstSeq := l.WALTip().Segment
	if firstSeq != 1 {
		t.Fatalf("first active segment = %d, want 1", firstSeq)
	}

	// A clean shutdown: the memtable is flushed to an SSTable and its segment is
	// parked under wal-retained/ rather than deleted.
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	parked, err := storewal.ListSegments(filepath.Join(dir, retainedWALDir))
	if err != nil {
		t.Fatalf("list parked segments: %v", err)
	}
	if len(parked) != 1 || parked[0].Seq != firstSeq {
		t.Fatalf("parked segments = %v, want exactly segment %d — the rest of this test "+
			"depends on the segment surviving its flush", parked, firstSeq)
	}

	reopened := openTreeAt(t, dir)
	defer reopened.Close()

	if got := reopened.WALTip().Segment; got <= firstSeq {
		t.Errorf("after a clean restart the active segment is %d, but segment %d is still "+
			"on disk (parked for replica catch-up). Reusing a segment number makes every "+
			"replica cursor recorded before the restart address a different log: "+
			"ErrCursorStale cannot fire, the parked segment is shadowed by the new one, "+
			"and a pass reads past the end of a shorter log and reports the replica as "+
			"caught up", got, firstSeq)
	}

	// The parked segment must still be reachable at its own number: it holds the
	// entries a lagging replica is owed, and it is the only copy of them in the log.
	segs, err := reopened.WALSegments()
	if err != nil {
		t.Fatalf("WALSegments: %v", err)
	}
	var sawParked bool
	for _, s := range segs {
		if s.Seq == firstSeq {
			sawParked = true
			if filepath.Dir(s.Path) != filepath.Join(dir, retainedWALDir) {
				t.Errorf("segment %d resolves to %q, not the parked copy: the new active "+
					"segment has shadowed the entries a replica is owed", s.Seq, s.Path)
			}
		}
	}
	if !sawParked {
		t.Errorf("segment %d is missing from %v after the restart", firstSeq, segs)
	}
}

// TestCrashRestartKeepsWALSegmentNumbersMonotonic is the control. A SIGKILL leaves
// the live segment on disk, so numbering already continued past it — which is why
// the kill-restart nemesis converged while the graceful stop-restart one did not.
func TestCrashRestartKeepsWALSegmentNumbersMonotonic(t *testing.T) {
	dir := t.TempDir()

	l := openTreeAt(t, dir)
	if err := l.Put(context.Background(), "before-crash", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	crashSeq := l.WALTip().Segment

	// Stop the goroutines without flushing, the way a kill does: the segment stays
	// in the live directory for recovery to replay.
	close(l.stopCh)
	l.wg.Wait()

	reopened := openTreeAt(t, dir)
	defer reopened.Close()

	if got := reopened.WALTip().Segment; got <= crashSeq {
		t.Errorf("after a crash restart the active segment is %d, want greater than the "+
			"surviving segment %d", got, crashSeq)
	}
	if _, err := reopened.Get(context.Background(), "before-crash"); err != nil {
		t.Errorf("pre-crash write did not survive recovery: %v", err)
	}
}
