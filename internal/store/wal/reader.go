package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// ErrCursorStale reports that the segment a cursor points into is no longer on
// disk: it was garbage-collected after its memtable was flushed, and the entries
// between the cursor and the oldest surviving segment are gone from the log.
//
// A caller that gets this error cannot catch a replica up from the WAL alone.
// The v1 answer is to log it, count it, and resume from the oldest segment that
// does survive — which converges every key written since then, and leaves any
// key whose only write fell in the lost range divergent until it is written
// again. See docs/replication-and-anti-entropy.md for why full-keyspace repair
// is deliberately out of scope for v1.
var ErrCursorStale = errors.New("wal: cursor points into a garbage-collected segment")

// Entry is one decoded WAL record together with its address in the log.
//
// Pos is the offset of the entry's first byte; End is the offset one past its
// last byte, which is the cursor value to persist once the entry has been
// applied elsewhere. Both are absolute positions (segment + offset), so they
// remain meaningful after segment rotation.
//
// Seq is the sequence number the write was assigned by the node that accepted
// it, or 0 for a v1 record written before the log carried one. It is what lets
// an anti-entropy pass replay an entry with its original ordering instead of
// its arrival ordering — see WAL for the two record formats.
type Entry struct {
	Op    OpType
	Key   string
	Value []byte
	Seq   uint64
	Pos   Position
	End   Position
}

// Reader iterates WAL entries forward across segments, starting at a Position.
//
// It is the read side of the anti-entropy path: the writer appends, and this
// reader replays what a replica missed. Unlike WAL.Replay it (a) starts at an
// arbitrary offset instead of the beginning, (b) walks a whole ordered set of
// segments as one logical stream, and (c) reports each entry's address so the
// caller can record progress entry by entry.
//
// Torn-write handling matches WAL.Replay: a truncated entry or a CRC mismatch
// ends the current segment cleanly rather than erroring, because that is the
// expected shape of a crash at the tail of a log. The difference is that only
// the *last* segment can legitimately be torn — an earlier segment that stops
// short means the log is damaged, and Reader reports that as an error rather
// than silently skipping to the next segment and handing the caller a stream
// with a hole in it.
//
// A Reader is not safe for concurrent use.
type Reader struct {
	segments []Segment
	idx      int // index into segments of the currently open segment

	f      *os.File
	br     *bufio.Reader
	offset int64  // absolute offset within the open segment
	seq    uint64 // sequence number of the open segment

	// limit, when non-zero, stops iteration at this position. It lets a caller
	// pin a pass to the tip observed when the pass started, so a pass cannot be
	// chased forward indefinitely by concurrent writes.
	limit Position

	done bool
}

// NewReader opens a reader over segs (which must be sorted by ascending Seq, as
// ListSegments returns them) positioned at from.
//
// A zero `from` starts at the oldest segment. A `from` whose segment is absent
// from segs, while a newer segment is present, returns ErrCursorStale together
// with a Reader positioned at the oldest surviving segment, so the caller can
// choose to continue from there after recording the gap.
//
// A `from` at or past the end of the newest segment yields a reader that
// immediately reports no entries; that is the normal steady state of a
// fully-caught-up replica, not an error.
func NewReader(segs []Segment, from Position) (*Reader, error) {
	r := &Reader{segments: segs}

	if len(segs) == 0 {
		r.done = true
		return r, nil
	}

	if from.IsZero() {
		r.idx = 0
		r.offset = 0
		return r, nil
	}

	// Locate the segment the cursor points into.
	for i, s := range segs {
		if s.Seq == from.Segment {
			r.idx = i
			r.offset = from.Offset
			return r, nil
		}
		if s.Seq > from.Segment {
			// The cursor's segment is gone; entries were lost.
			r.idx = i
			r.offset = 0
			return r, fmt.Errorf("%w: cursor %s, oldest surviving segment %d",
				ErrCursorStale, from, s.Seq)
		}
	}

	// The cursor is newer than every segment on disk. This happens when the
	// cursor names the active segment and that segment has since been rotated
	// and flushed away, i.e. everything up to the cursor is already applied and
	// nothing survives after it. There is nothing to replay.
	r.done = true
	return r, nil
}

// LimitTo stops iteration once the reader reaches pos. Passing the zero
// Position removes the limit. Call before the first Next.
func (r *Reader) LimitTo(pos Position) { r.limit = pos }

// Position returns the reader's current position — the address of the next
// entry it would read. It is a diagnostic: progress is recorded from
// Entry.End, which is the position of a specific applied entry.
func (r *Reader) Position() Position {
	seq := r.seq
	if seq == 0 && r.idx < len(r.segments) {
		seq = r.segments[r.idx].Seq
	}
	return Position{Segment: seq, Offset: r.offset}
}

// Next returns the next entry in the log. ok is false when the stream is
// exhausted (or the limit was reached), which is not an error.
//
// The returned Entry owns its Key and Value: the caller may retain them.
func (r *Reader) Next() (Entry, bool, error) {
	for {
		if r.done {
			return Entry{}, false, nil
		}

		if r.f == nil {
			if r.idx >= len(r.segments) {
				r.done = true
				return Entry{}, false, nil
			}
			seg := r.segments[r.idx]
			if !r.limit.IsZero() && seg.Seq > r.limit.Segment {
				r.done = true
				return Entry{}, false, nil
			}
			f, err := os.Open(seg.Path)
			if err != nil {
				if os.IsNotExist(err) {
					// Raced with garbage collection of this segment. Treat it
					// the same way as a cursor into a collected segment: the
					// stream has a hole, so stop rather than skip.
					r.done = true
					return Entry{}, false, fmt.Errorf("%w: segment %d disappeared during replay",
						ErrCursorStale, seg.Seq)
				}
				return Entry{}, false, fmt.Errorf("wal: open segment %q: %w", seg.Path, err)
			}
			if r.offset > 0 {
				if _, err := f.Seek(r.offset, io.SeekStart); err != nil {
					f.Close()
					return Entry{}, false, fmt.Errorf("wal: seek segment %q to %d: %w",
						seg.Path, r.offset, err)
				}
			}
			r.f = f
			r.br = bufio.NewReaderSize(f, 64*1024)
			r.seq = seg.Seq
		}

		start := Position{Segment: r.seq, Offset: r.offset}
		if !r.limit.IsZero() && !start.Before(r.limit) {
			r.done = true
			return Entry{}, false, nil
		}

		entry, n, err := r.readEntry(start)
		if err != nil {
			// Torn tail. Only the newest segment may legitimately end mid-entry;
			// a short read anywhere else means the log lost bytes it had
			// already handed to a caller.
			if errors.Is(err, errTornEntry) {
				lastSegment := r.idx == len(r.segments)-1
				if err := r.closeSegment(); err != nil {
					return Entry{}, false, err
				}
				if !lastSegment {
					return Entry{}, false, fmt.Errorf(
						"%w: segment %d ends mid-entry at offset %d but is not the newest segment",
						ErrCursorStale, start.Segment, start.Offset)
				}
				r.done = true
				return Entry{}, false, nil
			}
			return Entry{}, false, err
		}
		if n == 0 {
			// Clean end of this segment; advance to the next one.
			if err := r.closeSegment(); err != nil {
				return Entry{}, false, err
			}
			continue
		}

		r.offset += n
		entry.End = Position{Segment: r.seq, Offset: r.offset}
		return entry, true, nil
	}
}

// closeSegment closes the open segment and advances to the next one, resetting
// the offset to the start of that segment.
func (r *Reader) closeSegment() error {
	if r.f != nil {
		if err := r.f.Close(); err != nil {
			return fmt.Errorf("wal: close segment: %w", err)
		}
		r.f = nil
		r.br = nil
	}
	r.idx++
	r.offset = 0
	if r.idx < len(r.segments) {
		r.seq = r.segments[r.idx].Seq
	}
	return nil
}

// errTornEntry marks a truncated or CRC-invalid entry — the signature of a
// crash while appending. It never escapes Reader.
var errTornEntry = errors.New("wal: torn entry")

// readEntry decodes one entry at the reader's current offset. n is the number of
// bytes consumed; n == 0 with a nil error means a clean end of segment.
func (r *Reader) readEntry(pos Position) (Entry, int64, error) {
	opByte, err := r.br.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Entry{}, 0, nil // clean end of segment
		}
		return Entry{}, 0, fmt.Errorf("wal: read op at %s: %w", pos, err)
	}

	// A v2 record carries its sequence number between the op byte and the key
	// length; a v1 record reports seq 0, meaning "this record does not know".
	logicalOp, hasSeq := decodeWireOp(OpType(opByte))
	var seq uint64
	var seqBuf [8]byte
	if hasSeq {
		if err := r.readFull(seqBuf[:], pos, "seq"); err != nil {
			return Entry{}, 0, err
		}
		seq = binary.BigEndian.Uint64(seqBuf[:])
	}

	var lenBuf [4]byte
	if err := r.readFull(lenBuf[:], pos, "key-len"); err != nil {
		return Entry{}, 0, err
	}
	keyLen := binary.BigEndian.Uint32(lenBuf[:])
	if keyLen > maxFieldBytes {
		return Entry{}, 0, fmt.Errorf("%w: implausible key length %d at %s", errTornEntry, keyLen, pos)
	}
	keyBuf := make([]byte, keyLen)
	if err := r.readFull(keyBuf, pos, "key"); err != nil {
		return Entry{}, 0, err
	}

	if err := r.readFull(lenBuf[:], pos, "val-len"); err != nil {
		return Entry{}, 0, err
	}
	valLen := binary.BigEndian.Uint32(lenBuf[:])
	if valLen > maxFieldBytes {
		return Entry{}, 0, fmt.Errorf("%w: implausible value length %d at %s", errTornEntry, valLen, pos)
	}
	var valBuf []byte
	if valLen > 0 {
		valBuf = make([]byte, valLen)
		if err := r.readFull(valBuf, pos, "value"); err != nil {
			return Entry{}, 0, err
		}
	}

	var crcBuf [4]byte
	if err := r.readFull(crcBuf[:], pos, "crc"); err != nil {
		return Entry{}, 0, err
	}

	var computed uint32
	computed = crc32.Update(computed, crc32Table, []byte{opByte})
	if hasSeq {
		computed = crc32.Update(computed, crc32Table, seqBuf[:])
	}
	binary.BigEndian.PutUint32(lenBuf[:], keyLen)
	computed = crc32.Update(computed, crc32Table, lenBuf[:])
	computed = crc32.Update(computed, crc32Table, keyBuf)
	binary.BigEndian.PutUint32(lenBuf[:], valLen)
	computed = crc32.Update(computed, crc32Table, lenBuf[:])
	computed = crc32.Update(computed, crc32Table, valBuf)

	if computed != binary.BigEndian.Uint32(crcBuf[:]) {
		return Entry{}, 0, fmt.Errorf("%w: CRC mismatch at %s", errTornEntry, pos)
	}

	size := int64(entryHeaderBytes) + int64(keyLen) + int64(valLen)
	if hasSeq {
		size += seqFieldBytes
	}

	return Entry{
		Op:    logicalOp,
		Key:   string(keyBuf),
		Value: valBuf,
		Seq:   seq,
		Pos:   pos,
	}, size, nil
}

// readFull reads len(buf) bytes, mapping a short read onto errTornEntry.
func (r *Reader) readFull(buf []byte, pos Position, field string) error {
	if _, err := io.ReadFull(r.br, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: truncated %s at %s", errTornEntry, field, pos)
		}
		return fmt.Errorf("wal: read %s at %s: %w", field, pos, err)
	}
	return nil
}

// Close releases the open segment, if any.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	f := r.f
	r.f, r.br = nil, nil
	return f.Close()
}

// maxFieldBytes bounds a key or value length read from the log before it is
// used to size an allocation. A torn write can leave arbitrary bytes where a
// length prefix belongs; without this check a garbage prefix would be trusted
// far enough to attempt a multi-gigabyte allocation before the CRC could reject
// it. 64 MiB is far above any legitimate distrikv value.
const maxFieldBytes = 64 << 20
