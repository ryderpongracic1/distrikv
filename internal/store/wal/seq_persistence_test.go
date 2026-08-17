package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// The log carries a sequence number per entry so that a node recovering from it
// restores the ordering each write was given rather than inventing a new one.
// That matters for replicated writes: the sequence a replica stores for a key
// comes from its ring-primary, and re-deriving it locally at recovery time makes
// it incomparable with the number the primary will send next.
//
// These tests pin the format and its compatibility in both directions.

func openSeqWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seq.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

type replayed struct {
	op    OpType
	key   string
	value []byte
	seq   uint64
}

// replaySeqEntries reopens the log at path and returns every entry with the
// sequence it was stored with. It reopens rather than reusing the writer so the
// bytes are read back from disk, which is the only thing that proves the
// sequence was persisted rather than remembered.
func replaySeqEntries(t *testing.T, path string) []replayed {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()

	var got []replayed
	if err := r.Replay(func(op OpType, key string, value []byte, seq uint64) {
		got = append(got, replayed{op: op, key: key, value: value, seq: seq})
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return got
}

// TestAppendSeqRoundTripsThroughReplay is the core of the format: what went in
// comes back out, with the op reported as the logical one the caller named rather
// than the seq-carrying wire code.
func TestAppendSeqRoundTripsThroughReplay(t *testing.T) {
	w, path := openSeqWAL(t)

	if err := w.AppendSeq(OpPut, "a", []byte("1"), 7); err != nil {
		t.Fatalf("append put: %v", err)
	}
	if err := w.AppendSeq(OpDelete, "a", nil, 99); err != nil {
		t.Fatalf("append delete: %v", err)
	}

	got := replaySeqEntries(t, path)
	if len(got) != 2 {
		t.Fatalf("replayed %d entries, want 2", len(got))
	}
	if got[0].op != OpPut || got[0].key != "a" || string(got[0].value) != "1" || got[0].seq != 7 {
		t.Errorf("first entry = %+v, want put a=1 seq=7", got[0])
	}
	if got[1].op != OpDelete || got[1].key != "a" || got[1].value != nil || got[1].seq != 99 {
		t.Errorf("second entry = %+v, want delete a seq=99", got[1])
	}
}

// TestAppendSeqRejectsAnUnorderableOp pins that only the two logical ops have a
// seq-carrying encoding, so a new op cannot silently be written in a format no
// reader knows how to frame.
func TestAppendSeqRejectsAnUnorderableOp(t *testing.T) {
	w, _ := openSeqWAL(t)
	if err := w.AppendSeq(OpType(9), "a", []byte("1"), 1); err == nil {
		t.Fatal("AppendSeq accepted an unknown op; the record would be unframeable")
	}
}

// TestLegacyRecordsReplayWithoutASequence is the backward half of compatibility:
// a segment written before the format existed still replays, reporting seq 0 for
// "this record does not know its ordering".
func TestLegacyRecordsReplayWithoutASequence(t *testing.T) {
	w, path := openSeqWAL(t)
	if err := w.Append(OpPut, "old", []byte("v")); err != nil {
		t.Fatalf("append v1: %v", err)
	}

	got := replaySeqEntries(t, path)
	if len(got) != 1 {
		t.Fatalf("replayed %d entries, want 1", len(got))
	}
	if got[0].seq != 0 {
		t.Errorf("a v1 record reported seq %d, want 0", got[0].seq)
	}
	if got[0].op != OpPut || got[0].key != "old" || string(got[0].value) != "v" {
		t.Errorf("v1 record mis-decoded: %+v", got[0])
	}
}

// TestMixedFormatSegmentReplaysInOrder is what a binary upgrade actually leaves
// on disk: a segment whose head is v1 and whose tail is v2. Both must decode, in
// log order, from one pass.
func TestMixedFormatSegmentReplaysInOrder(t *testing.T) {
	w, path := openSeqWAL(t)

	if err := w.Append(OpPut, "before", []byte("1")); err != nil {
		t.Fatalf("append v1: %v", err)
	}
	if err := w.AppendSeq(OpPut, "after", []byte("2"), 42); err != nil {
		t.Fatalf("append v2: %v", err)
	}
	if err := w.Append(OpDelete, "before", nil); err != nil {
		t.Fatalf("append v1 delete: %v", err)
	}

	got := replaySeqEntries(t, path)
	if len(got) != 3 {
		t.Fatalf("replayed %d entries, want 3", len(got))
	}
	want := []replayed{
		{op: OpPut, key: "before", seq: 0},
		{op: OpPut, key: "after", seq: 42},
		{op: OpDelete, key: "before", seq: 0},
	}
	for i, w := range want {
		if got[i].op != w.op || got[i].key != w.key || got[i].seq != w.seq {
			t.Errorf("entry %d = {op:%d key:%q seq:%d}, want {op:%d key:%q seq:%d}",
				i, got[i].op, got[i].key, got[i].seq, w.op, w.key, w.seq)
		}
	}
}

// TestReaderReportsSequenceAndSize covers the anti-entropy read path, which is a
// second decoder over the same bytes: it must agree with Replay on the sequence,
// and its byte accounting must include the sequence field or every cursor after
// the first entry lands mid-record.
func TestReaderReportsSequenceAndSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal-000001.log")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.AppendSeq(OpPut, "k1", []byte("v1"), 11); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.AppendSeq(OpDelete, "k2", nil, 12); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	r, err := NewReader(segs, Position{})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer r.Close()

	first, ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("first Next: ok=%v err=%v", ok, err)
	}
	if first.Seq != 11 {
		t.Errorf("first entry seq = %d, want 11", first.Seq)
	}
	if want := EntrySeqWireSize("k1", []byte("v1")); first.End.Offset != want {
		t.Errorf("first entry ends at offset %d, want %d — the reader is not counting the sequence field",
			first.End.Offset, want)
	}

	second, ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("second Next: ok=%v err=%v", ok, err)
	}
	if second.Seq != 12 || second.Op != OpDelete || second.Key != "k2" {
		t.Errorf("second entry = %+v, want delete k2 seq=12", second)
	}
	if _, ok, err := r.Next(); ok || err != nil {
		t.Errorf("expected clean end of log, got ok=%v err=%v", ok, err)
	}
}

// TestSequenceIsCoveredByTheCRC pins that the sequence is protected like every
// other byte of the record. A sequence that can be corrupted without failing the
// checksum is worse than no sequence at all: it would silently reorder writes
// rather than being detected as a torn record.
func TestSequenceIsCoveredByTheCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crc.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.AppendSeq(OpPut, "k", []byte("v"), 0x0102030405060708); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip a bit inside the sequence field (offset 1..8; the op byte is at 0).
	raw[4] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := replaySeqEntries(t, path)
	if len(got) != 0 {
		t.Errorf("a record with a corrupted sequence replayed as %+v; the CRC must reject it", got)
	}
}
