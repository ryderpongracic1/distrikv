package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPositionOrdering(t *testing.T) {
	cases := []struct {
		a, b Position
		want int
	}{
		{Position{}, Position{}, 0},
		{Position{Segment: 1}, Position{Segment: 2}, -1},
		{Position{Segment: 2}, Position{Segment: 1}, 1},
		{Position{Segment: 1, Offset: 10}, Position{Segment: 1, Offset: 20}, -1},
		{Position{Segment: 1, Offset: 20}, Position{Segment: 1, Offset: 20}, 0},
		// Segment dominates offset: a later segment always sorts after an earlier
		// one, however large the earlier offset. A comparison that got this
		// backwards would rewind a cursor on every rotation.
		{Position{Segment: 1, Offset: 1 << 40}, Position{Segment: 2, Offset: 0}, -1},
	}
	for _, c := range cases {
		if got := c.a.Compare(c.b); got != c.want {
			t.Errorf("%s.Compare(%s) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := c.a.Before(c.b); got != (c.want < 0) {
			t.Errorf("%s.Before(%s) = %v, want %v", c.a, c.b, got, c.want < 0)
		}
	}

	if got := Min(Position{Segment: 3}, Position{Segment: 2, Offset: 9}); got.Segment != 2 {
		t.Errorf("Min picked %s", got)
	}
	if !(Position{}).IsZero() {
		t.Error("the zero Position does not report IsZero")
	}
	if (Position{Segment: 1}).IsZero() {
		t.Error("segment 1 offset 0 reported as the zero Position")
	}
}

func TestPositionRoundTrip(t *testing.T) {
	for _, p := range []Position{{}, {Segment: 1}, {Segment: 12, Offset: 345678}} {
		got, err := ParsePosition(p.String())
		if err != nil {
			t.Fatalf("ParsePosition(%q): %v", p.String(), err)
		}
		if got != p {
			t.Errorf("round trip of %s produced %s", p, got)
		}
	}

	for _, bad := range []string{"", "12", "a:1", "1:b", "1:-4", "1:2:3"} {
		if _, err := ParsePosition(bad); err == nil {
			t.Errorf("ParsePosition(%q) accepted a malformed position", bad)
		}
	}
}

func TestSegmentNameRoundTrip(t *testing.T) {
	for _, seq := range []uint64{1, 9, 10, 4321, 99999} {
		name := SegmentName(seq)
		got, ok := ParseSegmentSeq(name)
		if !ok || got != seq {
			t.Errorf("ParseSegmentSeq(%q) = (%d, %v), want (%d, true)", name, got, ok, seq)
		}
	}
	for _, bad := range []string{"wal.log", "wal-.log", "sst-00000001.sst", "manifest.log", "wal-0001.log.tmp"} {
		if _, ok := ParseSegmentSeq(bad); ok {
			t.Errorf("ParseSegmentSeq(%q) accepted a non-segment filename", bad)
		}
	}
	// Zero-padding must not change the parsed value: a segment written as 0009 and
	// one written as 9 are the same segment.
	if seq, ok := ParseSegmentSeq("wal-9.log"); !ok || seq != 9 {
		t.Errorf("ParseSegmentSeq(\"wal-9.log\") = (%d, %v)", seq, ok)
	}
}

// TestListSegmentsMergesDirectoriesOldestFirst covers the layout retention uses:
// live segments in the data directory, flushed-but-retained ones parked in a
// subdirectory, presented to the reader as one ordered log.
func TestListSegmentsMergesDirectoriesOldestFirst(t *testing.T) {
	live := t.TempDir()
	parked := filepath.Join(live, "wal-retained")
	if err := os.MkdirAll(parked, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	writeSegment(t, parked, 1, Entry{Op: OpPut, Key: "old"})
	writeSegment(t, parked, 2, Entry{Op: OpPut, Key: "older"})
	writeSegment(t, live, 3, Entry{Op: OpPut, Key: "live"})

	segs, err := ListSegments(live, parked)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("ListSegments returned %d segments, want 3: %v", len(segs), segs)
	}
	for i, want := range []uint64{1, 2, 3} {
		if segs[i].Seq != want {
			t.Fatalf("segment %d has seq %d, want %d (segments must be oldest-first)", i, segs[i].Seq, want)
		}
	}

	// A missing directory is normal — nothing has been parked yet.
	if _, err := ListSegments(live, filepath.Join(live, "does-not-exist")); err != nil {
		t.Errorf("ListSegments with an absent directory: %v", err)
	}

	// The first directory wins for a duplicated sequence number, so a segment that
	// was parked and somehow also exists live is read once.
	writeSegment(t, live, 1, Entry{Op: OpPut, Key: "shadow"})
	segs, err = ListSegments(live, parked)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("a duplicated segment number produced %d segments, want 3", len(segs))
	}
	if filepath.Dir(segs[0].Path) != live {
		t.Errorf("segment 1 resolved to %q, want the live directory to win", segs[0].Path)
	}
}
