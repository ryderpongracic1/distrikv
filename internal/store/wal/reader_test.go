package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The reader is the resume mechanism for replica catch-up: a cursor is a
// (segment, offset) pair produced by it and handed back to it later. These tests
// pin the properties that makes safe: the arithmetic that produces an offset, the
// refusal to hand a caller a stream with a hole in it, and clean behaviour at the
// tail, where a crash leaves a partial entry.

// writeSegment writes entries into dir as segment seq and returns the segment.
func writeSegment(t *testing.T, dir string, seq uint64, entries ...Entry) Segment {
	t.Helper()
	path := filepath.Join(dir, SegmentName(seq))
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open segment %d: %v", seq, err)
	}
	defer w.Close()
	for _, e := range entries {
		if err := w.Append(e.Op, e.Key, e.Value); err != nil {
			t.Fatalf("append %q: %v", e.Key, err)
		}
	}
	return Segment{Seq: seq, Path: path}
}

// drain reads every entry a reader will produce.
func drainReader(t *testing.T, r *Reader) []Entry {
	t.Helper()
	var out []Entry
	for {
		e, ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, e)
	}
}

// TestEntryWireSizeMatchesWhatTheWriterWrote is the load-bearing arithmetic: a
// cursor is a byte offset, so if EntryWireSize and Append ever disagree by one
// byte, every cursor addresses the middle of an entry and catch-up reads garbage.
func TestEntryWireSizeMatchesWhatTheWriterWrote(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, SegmentName(1))
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cases := []Entry{
		{Op: OpPut, Key: "k", Value: []byte("v")},
		{Op: OpPut, Key: "", Value: nil},
		{Op: OpDelete, Key: "tombstone", Value: nil},
		{Op: OpPut, Key: "longer-key", Value: make([]byte, 4096)},
	}

	var want int64
	for _, e := range cases {
		if err := w.Append(e.Op, e.Key, e.Value); err != nil {
			t.Fatalf("append: %v", err)
		}
		want += EntryWireSize(e.Key, e.Value)

		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if info.Size() != want {
			t.Fatalf("after %q: file is %d bytes, EntryWireSize sums to %d",
				e.Key, info.Size(), want)
		}
		if w.Size() != want {
			t.Fatalf("after %q: WAL.Size() = %d, file is %d", e.Key, w.Size(), want)
		}
	}
}

// TestReaderResumesFromARecordedPosition is the core round trip: read an entry,
// keep its End, and a later reader starting there sees exactly the rest.
func TestReaderResumesFromARecordedPosition(t *testing.T) {
	dir := t.TempDir()
	seg := writeSegment(t, dir, 1,
		Entry{Op: OpPut, Key: "a", Value: []byte("1")},
		Entry{Op: OpPut, Key: "b", Value: []byte("2")},
		Entry{Op: OpDelete, Key: "c"},
	)

	r, err := NewReader([]Segment{seg}, Position{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	first, ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("first Next: ok=%v err=%v", ok, err)
	}
	if first.Key != "a" || string(first.Value) != "1" {
		t.Fatalf("first entry = (%q, %q), want (a, 1)", first.Key, first.Value)
	}
	if !first.Pos.Before(first.End) {
		t.Fatalf("entry Pos %s is not before End %s", first.Pos, first.End)
	}
	r.Close()

	resumed, err := NewReader([]Segment{seg}, first.End)
	if err != nil {
		t.Fatalf("NewReader at %s: %v", first.End, err)
	}
	defer resumed.Close()

	rest := drainReader(t, resumed)
	if len(rest) != 2 || rest[0].Key != "b" || rest[1].Key != "c" {
		t.Fatalf("resumed stream = %v, want b then c", keysOf(rest))
	}
	if rest[1].Op != OpDelete {
		t.Errorf("tombstone read back as op %d, want OpDelete", rest[1].Op)
	}
	if rest[1].Value != nil {
		t.Errorf("tombstone value = %q, want nil so a delete is distinguishable from an empty put", rest[1].Value)
	}
}

// TestReaderWalksSegmentsAsOneStream covers the rotation case: the log's entries
// span files, and a cursor stays meaningful across them.
func TestReaderWalksSegmentsAsOneStream(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, Entry{Op: OpPut, Key: "a"}, Entry{Op: OpPut, Key: "b"})
	writeSegment(t, dir, 2, Entry{Op: OpPut, Key: "c"})
	writeSegment(t, dir, 3, Entry{Op: OpPut, Key: "d"})

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 3 || segs[0].Seq != 1 || segs[2].Seq != 3 {
		t.Fatalf("ListSegments = %v, want segments 1..3 in order", segs)
	}

	r, err := NewReader(segs, Position{Segment: 1, Offset: EntryWireSize("a", nil)})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	got := keysOf(drainReader(t, r))
	want := []string{"b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("stream = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stream = %v, want %v", got, want)
		}
	}
}

// TestReaderStopsAtItsLimit pins the bound a pass uses so concurrent writes
// cannot chase it forward for ever.
func TestReaderStopsAtItsLimit(t *testing.T) {
	dir := t.TempDir()
	seg := writeSegment(t, dir, 1,
		Entry{Op: OpPut, Key: "a"},
		Entry{Op: OpPut, Key: "b"},
		Entry{Op: OpPut, Key: "c"},
	)

	limit := Position{Segment: 1, Offset: 2 * EntryWireSize("a", nil)}
	r, err := NewReader([]Segment{seg}, Position{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	r.LimitTo(limit)

	if got := keysOf(drainReader(t, r)); len(got) != 2 {
		t.Fatalf("limited stream = %v, want the first two entries only", got)
	}
}

// TestReaderAtTipYieldsNothing is the steady state of a caught-up replica: the
// cursor equals the tip, and that is not an error.
func TestReaderAtTipYieldsNothing(t *testing.T) {
	dir := t.TempDir()
	seg := writeSegment(t, dir, 1, Entry{Op: OpPut, Key: "a"})

	info, err := os.Stat(seg.Path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	r, err := NewReader([]Segment{seg}, Position{Segment: 1, Offset: info.Size()})
	if err != nil {
		t.Fatalf("NewReader at tip: %v", err)
	}
	defer r.Close()
	if got := drainReader(t, r); len(got) != 0 {
		t.Fatalf("reader at the tip produced %d entries, want none", len(got))
	}
}

// TestReaderOnEmptySegmentSet covers a store that has never written anything.
func TestReaderOnEmptySegmentSet(t *testing.T) {
	r, err := NewReader(nil, Position{Segment: 4, Offset: 10})
	if err != nil {
		t.Fatalf("NewReader with no segments: %v", err)
	}
	defer r.Close()
	if got := drainReader(t, r); len(got) != 0 {
		t.Fatalf("got %d entries from an empty segment set", len(got))
	}
}

// TestReaderReportsAStaleCursor covers the v1 bound: the cursor's segment has been
// garbage-collected, so the gap cannot be closed from the log. The caller must be
// told, and must still be able to continue from the oldest surviving segment.
func TestReaderReportsAStaleCursor(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 5, Entry{Op: OpPut, Key: "e"})
	writeSegment(t, dir, 6, Entry{Op: OpPut, Key: "f"})
	segs, _ := ListSegments(dir)

	r, err := NewReader(segs, Position{Segment: 2, Offset: 40})
	if !errors.Is(err, ErrCursorStale) {
		t.Fatalf("NewReader with a collected cursor segment: err = %v, want ErrCursorStale", err)
	}
	if r == nil {
		t.Fatal("NewReader returned no reader alongside ErrCursorStale; the caller cannot " +
			"fall back to the oldest surviving segment")
	}
	defer r.Close()
	if got := keysOf(drainReader(t, r)); len(got) != 2 || got[0] != "e" {
		t.Fatalf("fallback stream = %v, want every entry from segment 5 on", got)
	}
}

// TestReaderCursorNewerThanEverySegment covers the other direction: everything up
// to the cursor was flushed and collected, so there is nothing to replay — and
// that is not an error either.
func TestReaderCursorNewerThanEverySegment(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 2, Entry{Op: OpPut, Key: "b"})
	segs, _ := ListSegments(dir)

	r, err := NewReader(segs, Position{Segment: 9, Offset: 0})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	if got := drainReader(t, r); len(got) != 0 {
		t.Fatalf("got %d entries for a cursor past every segment", len(got))
	}
}

// TestReaderStopsCleanlyAtATornTail is the crash signature: the newest segment
// ends mid-entry. Recovery treats that as the end of the log, and so must this
// reader.
func TestReaderStopsCleanlyAtATornTail(t *testing.T) {
	dir := t.TempDir()
	seg := writeSegment(t, dir, 1,
		Entry{Op: OpPut, Key: "a", Value: []byte("1")},
		Entry{Op: OpPut, Key: "b", Value: []byte("2")},
	)

	info, _ := os.Stat(seg.Path)
	if err := os.Truncate(seg.Path, info.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	r, err := NewReader([]Segment{seg}, Position{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	if got := keysOf(drainReader(t, r)); len(got) != 1 || got[0] != "a" {
		t.Fatalf("torn tail stream = %v, want just the intact entry", got)
	}
}

// TestReaderRejectsAHoleInAnOlderSegment is the case a stream must not paper
// over. A torn entry in the *newest* segment is a crash at the tail; the same
// damage in an older one means bytes were lost from the middle of the log, and
// skipping to the next segment would hand the caller a stream that silently
// omits writes a replica is owed.
func TestReaderRejectsAHoleInAnOlderSegment(t *testing.T) {
	dir := t.TempDir()
	older := writeSegment(t, dir, 1,
		Entry{Op: OpPut, Key: "a", Value: []byte("1")},
		Entry{Op: OpPut, Key: "b", Value: []byte("2")},
	)
	writeSegment(t, dir, 2, Entry{Op: OpPut, Key: "c"})

	info, _ := os.Stat(older.Path)
	if err := os.Truncate(older.Path, info.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	segs, _ := ListSegments(dir)
	r, err := NewReader(segs, Position{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	if _, ok, err := r.Next(); !ok || err != nil {
		t.Fatalf("first Next: ok=%v err=%v", ok, err)
	}
	_, ok, err := r.Next()
	if ok {
		t.Fatal("the reader carried on past a hole in an older segment")
	}
	if !errors.Is(err, ErrCursorStale) {
		t.Fatalf("err = %v, want ErrCursorStale so the caller knows the stream is incomplete", err)
	}
}

// TestReaderRejectsACorruptEntryViaCRC proves the CRC is actually checked on the
// replay path, not just on recovery.
func TestReaderRejectsACorruptEntryViaCRC(t *testing.T) {
	dir := t.TempDir()
	seg := writeSegment(t, dir, 1,
		Entry{Op: OpPut, Key: "a", Value: []byte("original")},
		Entry{Op: OpPut, Key: "b", Value: []byte("second")},
	)

	// Flip a byte inside the first entry's value, leaving every length prefix
	// intact — only the CRC can catch this.
	f, err := os.OpenFile(seg.Path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	valueOffset := int64(1 + 4 + len("a") + 4) // op + key-len + key + val-len
	if _, err := f.WriteAt([]byte("X"), valueOffset); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	f.Close()

	r, err := NewReader([]Segment{seg}, Position{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	entry, ok, err := r.Next()
	if ok {
		t.Fatalf("the reader returned a CRC-invalid entry (%q = %q)", entry.Key, entry.Value)
	}
	if err != nil {
		// A CRC failure on the last segment is a torn tail: clean stop, no error.
		t.Fatalf("err = %v, want a clean stop for a torn entry in the newest segment", err)
	}
}

func keysOf(entries []Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Key)
	}
	return out
}
