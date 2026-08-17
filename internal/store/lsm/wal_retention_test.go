package lsm

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// WAL retention is what lets a replica be caught up from a segment whose memtable
// has already been flushed. It has to buy that without breaking the two things
// that depend on a flushed segment being gone: recovery replays every segment it
// finds, and the live-key count assumes each surviving segment holds only writes
// made after the last flush.

func newRetentionTree(t *testing.T, dir string) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(dir, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithMaxMemBytes(256)) // tiny, so a handful of writes forces rotations
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	return l
}

// writeUntilSegments writes keys until at least n segments exist on disk.
func writeUntilSegments(t *testing.T, l *LSMTree, n int, prefix string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 500; i++ {
		if err := l.Put(ctx, prefix+string(rune('a'+i%26))+string(rune('0'+i/26)), []byte("value-padding-to-fill-the-memtable")); err != nil {
			t.Fatalf("put: %v", err)
		}
		if l.walSeq.Load() >= uint64(n) {
			return
		}
	}
	t.Fatalf("only reached segment %d after 500 writes, wanted %d", l.walSeq.Load(), n)
}

// TestWALTipAdvancesWithWrites pins the tip arithmetic the cursor depends on.
func TestWALTipAdvancesWithWrites(t *testing.T) {
	l := newRetentionTree(t, t.TempDir())
	defer l.Close()

	start := l.WALTip()
	if start.Segment == 0 {
		t.Fatalf("tip = %s before any write; the active segment must be numbered", start)
	}
	if err := l.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	after := l.WALTip()
	if !start.Before(after) {
		t.Fatalf("tip did not advance across a write: %s → %s", start, after)
	}
	if want := start.Offset + storewal.EntryWireSize("k", []byte("v")); after.Offset != want {
		t.Errorf("tip offset = %d, want %d (one entry's wire size)", after.Offset, want)
	}
}

// TestFlushedSegmentIsDeletedWithoutRetention is the baseline: retention is opt-in,
// and with none requested the engine behaves exactly as it did before.
func TestFlushedSegmentIsDeletedWithoutRetention(t *testing.T) {
	dir := t.TempDir()
	l := newRetentionTree(t, dir)
	defer l.Close()

	writeUntilSegments(t, l, 3, "nokeep-")

	segs, err := storewal.ListSegments(dir)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	// The engine flushes asynchronously, so allow the active segment plus at most
	// one still-unflushed predecessor; what must not happen is every segment
	// surviving.
	if len(segs) > 2 {
		t.Errorf("%d live segments survived with no retention requested: %v", len(segs), segs)
	}
	if _, err := os.Stat(filepath.Join(dir, retainedWALDir)); err == nil {
		t.Error("a retained-segment directory was created without retention being requested")
	}
}

// TestRetainedSegmentSurvivesItsFlush is the property catch-up needs: a segment a
// replica's cursor still points into remains readable after its memtable has been
// flushed away.
func TestRetainedSegmentSurvivesItsFlush(t *testing.T) {
	dir := t.TempDir()
	l := newRetentionTree(t, dir)
	defer l.Close()

	l.RetainWALFrom(1) // keep everything from the first segment on
	if got := l.WALRetentionFloor(); got != 1 {
		t.Fatalf("retention floor = %d, want 1", got)
	}

	writeUntilSegments(t, l, 4, "keep-")

	segs, err := l.WALSegments()
	if err != nil {
		t.Fatalf("WALSegments: %v", err)
	}
	if len(segs) < 3 {
		t.Fatalf("only %d segments are readable after retention was requested: %v", len(segs), segs)
	}
	// The oldest ones must be the parked copies, not the live directory.
	parkedDir := filepath.Join(dir, retainedWALDir)
	if filepath.Dir(segs[0].Path) != parkedDir {
		t.Errorf("oldest readable segment is %q; a flushed-but-retained segment should be "+
			"parked under %q", segs[0].Path, parkedDir)
	}

	// Every retained entry must still be readable end to end, CRC included.
	r, err := storewal.NewReader(segs, storewal.Position{})
	if err != nil {
		t.Fatalf("NewReader over retained segments: %v", err)
	}
	defer r.Close()
	var read int
	for {
		_, ok, err := r.Next()
		if err != nil {
			t.Fatalf("reading retained segments: %v", err)
		}
		if !ok {
			break
		}
		read++
	}
	if read == 0 {
		t.Error("no entries could be read back from the retained segments")
	}
}

// TestRetainedSegmentsAreInvisibleToRecovery is the reason retained segments are
// parked in a subdirectory rather than simply left in place. Recovery replays
// every wal-NNNN.log in the data directory; a flushed segment left there would be
// replayed on the next open, re-applying writes that are already in an SSTable and
// double-counting them in the live-key estimate.
func TestRetainedSegmentsAreInvisibleToRecovery(t *testing.T) {
	dir := t.TempDir()
	l := newRetentionTree(t, dir)
	l.RetainWALFrom(1)
	writeUntilSegments(t, l, 4, "recov-")
	liveKeysBefore := l.LiveKeys()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	parked, err := storewal.ListSegments(filepath.Join(dir, retainedWALDir))
	if err != nil {
		t.Fatalf("list parked: %v", err)
	}
	if len(parked) == 0 {
		t.Fatal("no segments were parked, so this test proves nothing")
	}

	reopened := newRetentionTree(t, dir)
	defer reopened.Close()

	// Recovery must have replayed only the live segments. The clearest observable
	// is the key count: replaying parked segments as well would count already-
	// flushed writes a second time.
	if got := reopened.LiveKeys(); got != liveKeysBefore {
		t.Errorf("live keys after reopen = %d, want %d: parked segments were replayed", got, liveKeysBefore)
	}
	// And the parked segments must still be there for catch-up to read.
	stillParked, err := storewal.ListSegments(filepath.Join(dir, retainedWALDir))
	if err != nil {
		t.Fatalf("list parked after reopen: %v", err)
	}
	if len(stillParked) < len(parked) {
		t.Errorf("reopening dropped parked segments: %d → %d", len(parked), len(stillParked))
	}
}

// TestRetentionCapDropsTheOldestSegments pins the bound that keeps a permanently
// dead replica from filling the disk, and the loud consequence: the dropped
// segments are exactly the ones a stale cursor would have needed.
func TestRetentionCapDropsTheOldestSegments(t *testing.T) {
	dir := t.TempDir()
	l := newRetentionTree(t, dir)
	defer l.Close()

	parkedDir := filepath.Join(dir, retainedWALDir)
	if err := os.MkdirAll(parkedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Fabricate more parked segments than the cap allows, then let the trim run.
	for seq := uint64(1); seq <= maxRetainedWALSegments+5; seq++ {
		w, err := storewal.Open(filepath.Join(parkedDir, storewal.SegmentName(seq)))
		if err != nil {
			t.Fatalf("open parked segment: %v", err)
		}
		if err := w.Append(storewal.OpPut, "k", []byte("v")); err != nil {
			t.Fatalf("append: %v", err)
		}
		w.Close()
	}

	l.trimRetainedWALSegments(parkedDir)

	left, err := storewal.ListSegments(parkedDir)
	if err != nil {
		t.Fatalf("list parked: %v", err)
	}
	if len(left) != maxRetainedWALSegments {
		t.Fatalf("%d parked segments left, want the cap of %d", len(left), maxRetainedWALSegments)
	}
	if left[0].Seq != 6 {
		t.Errorf("oldest surviving parked segment is %d, want 6: the trim must drop the "+
			"oldest first so the newest history survives", left[0].Seq)
	}
}

// TestRestorePurgesRetainedSegments covers the snapshot path: a restore replaces
// the whole logical store, so a cursor into the pre-restore log addresses writes
// that no longer exist and its segments must not survive.
func TestRestorePurgesRetainedSegments(t *testing.T) {
	dir := t.TempDir()
	l := newRetentionTree(t, dir)
	defer l.Close()

	l.RetainWALFrom(1)
	writeUntilSegments(t, l, 3, "pre-restore-")

	if err := l.Restore(context.Background(), map[string][]byte{"after": []byte("restore")}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, retainedWALDir)); !os.IsNotExist(err) {
		t.Errorf("the retained-segment directory survived a restore (stat err = %v)", err)
	}
	if got, err := l.Get(context.Background(), "after"); err != nil || string(got) != "restore" {
		t.Errorf("restored store: Get(after) = (%q, %v)", got, err)
	}
}
