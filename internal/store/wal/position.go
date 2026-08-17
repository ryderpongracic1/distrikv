package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// segmentFileRE matches a WAL segment filename produced by SegmentName.
var segmentFileRE = regexp.MustCompile(`^wal-(\d+)\.log$`)

// SegmentName returns the filename of the WAL segment with the given sequence
// number. The storage engine opens one segment per memtable generation, so a
// segment number plus a byte offset addresses any entry the engine has written.
//
// The format lives here rather than at the call sites because two independent
// readers now depend on it: recovery (which replays every segment on disk) and
// the anti-entropy replay reader (which resumes from a saved offset inside a
// specific segment). A format spelled out in two places is a format that can
// drift.
func SegmentName(seq uint64) string { return fmt.Sprintf("wal-%04d.log", seq) }

// ParseSegmentSeq extracts the sequence number from a WAL segment filename.
// ok is false when name is not a WAL segment.
func ParseSegmentSeq(name string) (seq uint64, ok bool) {
	m := segmentFileRE.FindStringSubmatch(filepath.Base(name))
	if m == nil {
		return 0, false
	}
	seq, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// Segment is one WAL segment file on disk.
type Segment struct {
	Seq  uint64
	Path string
}

// ListSegments returns every WAL segment found under the given directories,
// sorted by ascending sequence number. Directories are searched in order and
// the first occurrence of a sequence number wins, so a caller may pass the live
// directory first and an archive directory second.
//
// Missing directories are not an error: a node that has never parked a segment
// has no archive directory.
func ListSegments(dirs ...string) ([]Segment, error) {
	var out []Segment
	seen := make(map[uint64]struct{})

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("wal: list segments in %q: %w", dir, err)
		}
		for _, de := range entries {
			if de.IsDir() {
				continue
			}
			seq, ok := ParseSegmentSeq(de.Name())
			if !ok {
				continue
			}
			if _, dup := seen[seq]; dup {
				continue
			}
			seen[seq] = struct{}{}
			out = append(out, Segment{Seq: seq, Path: filepath.Join(dir, de.Name())})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// Position addresses a byte offset within a numbered WAL segment. It is the
// cursor type used to record how far a replica has been caught up: ordering is
// lexicographic on (Segment, Offset), which matches the order in which the
// engine wrote the entries because segment numbers only ever increase and each
// segment is append-only.
//
// The zero Position sorts before every real position and means "no cursor
// recorded"; a reader given the zero Position starts at the oldest segment
// still on disk.
type Position struct {
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
}

// IsZero reports whether p is the zero Position ("no cursor recorded").
func (p Position) IsZero() bool { return p.Segment == 0 && p.Offset == 0 }

// Compare returns -1 if p is before q, +1 if after, 0 if equal.
func (p Position) Compare(q Position) int {
	switch {
	case p.Segment != q.Segment:
		if p.Segment < q.Segment {
			return -1
		}
		return 1
	case p.Offset != q.Offset:
		if p.Offset < q.Offset {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// Before reports whether p orders strictly before q.
func (p Position) Before(q Position) bool { return p.Compare(q) < 0 }

// Min returns whichever of p and q orders first.
func Min(p, q Position) Position {
	if p.Before(q) {
		return p
	}
	return q
}

// String renders the position as "segment:offset", the form used in logs and in
// the persisted cursor file.
func (p Position) String() string { return fmt.Sprintf("%d:%d", p.Segment, p.Offset) }

// ParsePosition parses the "segment:offset" form produced by Position.String.
func ParsePosition(s string) (Position, error) {
	seg, off, ok := strings.Cut(s, ":")
	if !ok {
		return Position{}, fmt.Errorf("wal: malformed position %q (want segment:offset)", s)
	}
	segment, err := strconv.ParseUint(seg, 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("wal: malformed position %q: segment: %w", s, err)
	}
	offset, err := strconv.ParseInt(off, 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("wal: malformed position %q: offset: %w", s, err)
	}
	if offset < 0 {
		return Position{}, fmt.Errorf("wal: malformed position %q: negative offset", s)
	}
	return Position{Segment: segment, Offset: offset}, nil
}

// EntryWireSize returns the number of bytes one v1 entry occupies on disk. It is
// the sum of the fixed framing (op byte, two length prefixes, CRC trailer) and
// the payload, and is what advances a Position past an entry.
func EntryWireSize(key string, value []byte) int64 {
	return int64(entryHeaderBytes + len(key) + len(value))
}

// EntrySeqWireSize returns the number of bytes one v2 entry occupies on disk —
// EntryWireSize plus the 8-byte sequence number. See WAL for both formats.
func EntrySeqWireSize(key string, value []byte) int64 {
	return EntryWireSize(key, value) + seqFieldBytes
}

// entryHeaderBytes is the fixed framing overhead of one v1 WAL entry:
// 1 op byte + 4 key-len + 4 val-len + 4 CRC.
const entryHeaderBytes = 13

// seqFieldBytes is the width of the sequence number a v2 entry carries between
// its op byte and its key length.
const seqFieldBytes = 8
