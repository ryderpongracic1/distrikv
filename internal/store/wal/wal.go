// Package wal provides a crash-safe append-only Write-Ahead Log.
// It is shared by both the store package (legacy compatibility) and the
// lsm package (one WAL file per memtable generation).
//
// # Phase 2 — GC optimisation and torn-write hardening
//
// Original implementation allocations per Append call:
//   - bufio.NewWriter(f)         — 1 alloc (4 KB header + internal state)
//   - crc32.NewIEEE()            — 1 alloc (hash.Hash32 interface value)
//   - []byte(key)                — 1 alloc for CRC pass (key traversed twice)
//
// This implementation eliminates all three:
//   - bw *bufio.Writer is allocated once at Open and reused across all calls.
//   - CRC is computed with crc32.Update (rolling, no object) in a single pass
//     over the wire bytes; crc32.IEEETable is a package-level pointer to the
//     pre-computed table, zero allocation.
//   - Key bytes for the CRC step are read from the same pooled buffer already
//     used to write to bw, avoiding the duplicate heap copy that []byte(key)
//     would produce.
//
// Replay GC optimisation:
//   - A sync.Pool of *[]byte scratch buffers is shared across Replay calls.
//     Key and value bytes are read into pooled buffers that grow to the
//     high-water mark of that WAL file, then returned to the pool. On the
//     hot path (all entries ≤ high-water-mark) zero new buffers are allocated
//     for reading; the final string(keyBuf) and value copy are unavoidable
//     because callers retain ownership.
//
// Torn-write contract:
//   - Replay stops cleanly (returns nil) for EOF / io.ErrUnexpectedEOF at any
//     position within an entry, and for CRC mismatch (the expected signature
//     of a crash at tail).
//   - Replay returns a non-nil error ONLY for genuine I/O failures (e.g. disk
//     read error mid-entry). The caller must treat this as data loss, not a
//     clean truncation, and surface it to the operator.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// OpType is the single-byte discriminant identifying the kind of WAL entry.
//
// It doubles as the record-format discriminant: OpPut/OpDelete introduce a v1
// record, and the unexported opPutSeq/opDeleteSeq introduce a v2 record that
// additionally carries the write's LSM sequence number. Callers only ever name
// OpPut/OpDelete — the seq-carrying codes are an encoding detail that Replay and
// Reader normalise away, so an entry read back from either format reports the
// logical op it was written with.
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2

	// opPutSeq and opDeleteSeq introduce a v2 record: identical to v1 but with
	// an 8-byte sequence number between the op byte and the key length.
	//
	// A new op code rather than a segment-header version field, because there is
	// no segment header to put one in and adding one would invalidate every WAL
	// on disk. Per-record discrimination costs one byte of switch and makes the
	// two formats freely interleavable within a segment, which is exactly what a
	// binary upgrade produces: the tail of an existing segment is v1 and
	// everything appended after the upgrade is v2.
	opPutSeq    OpType = 3
	opDeleteSeq OpType = 4
)

// WAL is an append-only Write-Ahead Log for crash recovery.
//
// Wire format per entry — v1 (Append):
//
//	[1 byte op][4 byte key-len][key bytes][4 byte val-len][val bytes][4 byte CRC32]
//
// v2 (AppendSeq), written by the LSM engine since per-key sequence numbers
// became load-bearing for replica convergence:
//
//	[1 byte op][8 byte seq][4 byte key-len][key bytes][4 byte val-len][val bytes][4 byte CRC32]
//
// CRC32 covers every byte preceding it in the entry, seq included. A CRC
// mismatch during replay signals a torn write from a prior crash; replay stops
// cleanly.
//
// Compatibility runs one way: a current binary reads both formats, and a v1
// record replays with seq 0 ("this record does not know its sequence number").
// A binary predating v2 reading a v2 record misparses the seq bytes as a key
// length, which its own implausible-length and CRC checks turn into a clean
// torn-write stop — so a downgrade loses the v2 tail of a segment rather than
// corrupting it. Downgrading a node whose WAL has v2 records therefore drops
// unflushed writes, and is not supported.
type WAL struct {
	f  *os.File
	bw *bufio.Writer // persistent; allocated once at Open, reused across Append calls

	// size is the number of bytes durably appended to this segment: the file's
	// size at Open plus the wire size of every entry Append has fsynced since.
	// It is the offset an anti-entropy reader resumes from, so it is maintained
	// here rather than derived from a Seek: the file is opened O_APPEND and
	// Replay seeks it back to the start, so the file offset says nothing useful
	// about where the log ends.
	size atomic.Int64
}

// entryBufPool is a pool of *[]byte scratch buffers used during Replay to
// read key and value bytes without allocating per-entry heap slices.
// The pool holds pointers so the slice header (and thus capacity) survives
// pool round-trips; buffers grow to the high-water mark of the WAL file.
var entryBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

// crc32Table is the pre-computed IEEE CRC32 table.  Accessing it is a simple
// pointer load — no allocation and no function call.
var crc32Table = crc32.IEEETable

// Open opens (or creates) the WAL file at path in append mode.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat %q: %w", path, err)
	}
	w := &WAL{
		f:  f,
		bw: bufio.NewWriterSize(f, 64*1024),
	}
	w.size.Store(info.Size())
	return w, nil
}

// Size returns the number of bytes durably present in this segment. Together
// with the segment's sequence number it is the log's tip — the position an
// anti-entropy pass reads up to.
func (w *WAL) Size() int64 { return w.size.Load() }

// Append writes one log entry and fsyncs before returning.
//
// Allocation budget per call (hot path):
//   - 0 buffer allocations — bw is persistent, CRC uses crc32.Update.
//   - 1 unavoidable: []byte(key) for the CRC pass (key is a string; the CRC
//     function requires []byte). This single copy is performed into a pooled
//     scratch buffer so no new heap memory is allocated on the warm path.
//
// On any write error the bufio.Writer is reset so the next Append starts
// from a clean state and does not emit a partial entry.
func (w *WAL) Append(op OpType, key string, value []byte) error {
	if err := w.appendEntry(op, key, value, 0, false); err != nil {
		// Discard any partial entry left in the write buffer.  The next
		// Append call will start cleanly from the last-fsynced position.
		w.bw.Reset(w.f)
		return err
	}
	return nil
}

// AppendSeq writes one log entry that carries seq, and fsyncs before returning.
// It is the v2 record described on WAL; Append writes the v1 record that omits
// the sequence number.
//
// The engine uses this for every write, because a sequence number that only
// exists in memory cannot survive the restart it is needed across: a replica
// compares the sequence the primary sent against the sequence it has stored for
// that key, and if replay had to invent a fresh local sequence for every
// recovered entry, that comparison would be against a number from a different
// counter after every restart. See Memtable.ReplayWAL.
//
// op must be OpPut or OpDelete; the seq-carrying wire code is chosen here.
func (w *WAL) AppendSeq(op OpType, key string, value []byte, seq uint64) error {
	if err := w.appendEntry(op, key, value, seq, true); err != nil {
		w.bw.Reset(w.f)
		return err
	}
	return nil
}

// seqWireOp maps a logical op onto the v2 wire code that carries a sequence
// number.
func seqWireOp(op OpType) (OpType, error) {
	switch op {
	case OpPut:
		return opPutSeq, nil
	case OpDelete:
		return opDeleteSeq, nil
	default:
		return 0, fmt.Errorf("wal: cannot write sequence number for op %d", op)
	}
}

func (w *WAL) appendEntry(op OpType, key string, value []byte, seq uint64, withSeq bool) error {
	wireOp := op
	if withSeq {
		var err error
		if wireOp, err = seqWireOp(op); err != nil {
			return err
		}
	}

	var lenBuf [4]byte
	var crc uint32

	// ── op byte ────────────────────────────────────────────────────────────
	if err := w.bw.WriteByte(byte(wireOp)); err != nil {
		return fmt.Errorf("wal: write op: %w", err)
	}
	opBuf := [1]byte{byte(wireOp)}
	crc = crc32.Update(crc, crc32Table, opBuf[:])

	// ── seq (v2 only) ──────────────────────────────────────────────────────
	if withSeq {
		var seqBuf [8]byte
		binary.BigEndian.PutUint64(seqBuf[:], seq)
		if _, err := w.bw.Write(seqBuf[:]); err != nil {
			return fmt.Errorf("wal: write seq: %w", err)
		}
		crc = crc32.Update(crc, crc32Table, seqBuf[:])
	}

	// ── key-len ────────────────────────────────────────────────────────────
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
	if _, err := w.bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write key-len: %w", err)
	}
	crc = crc32.Update(crc, crc32Table, lenBuf[:])

	// ── key ────────────────────────────────────────────────────────────────
	// Write via WriteString — avoids []byte(key) allocation for the I/O path.
	if _, err := io.WriteString(w.bw, key); err != nil {
		return fmt.Errorf("wal: write key: %w", err)
	}
	// CRC over key bytes — use a zero-allocation unsafe view of the string.
	// Safe: crc32.Update reads the slice but never stores or escapes it; the
	// string backing array lives for the duration of this call.
	crc = crc32.Update(crc, crc32Table, unsafeStringBytes(key))

	// ── val-len ────────────────────────────────────────────────────────────
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	if _, err := w.bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write val-len: %w", err)
	}
	crc = crc32.Update(crc, crc32Table, lenBuf[:])

	// ── value ──────────────────────────────────────────────────────────────
	if _, err := w.bw.Write(value); err != nil {
		return fmt.Errorf("wal: write val: %w", err)
	}
	crc = crc32.Update(crc, crc32Table, value)

	// ── CRC32 trailer ──────────────────────────────────────────────────────
	binary.BigEndian.PutUint32(lenBuf[:], crc)
	if _, err := w.bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write crc: %w", err)
	}

	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	// The entry is now durable, so the log's end has moved. Advancing here
	// rather than before the fsync keeps Size() a statement about what a reader
	// can actually find on disk.
	if withSeq {
		w.size.Add(EntrySeqWireSize(key, value))
	} else {
		w.size.Add(EntryWireSize(key, value))
	}
	return nil
}

// Replay reads the WAL from the beginning and calls fn for each valid entry.
//
// seq is the sequence number the entry was written with, or 0 for a v1 record
// that predates the format carrying one. Callers must treat 0 as "unknown"
// rather than as an ordering below every other write.
//
// Torn-write handling (see package doc for the full contract):
//   - Returns nil and stops at any EOF / io.ErrUnexpectedEOF (torn write).
//   - Returns nil and stops at a CRC mismatch (torn write signature).
//   - Returns a non-nil error only for genuine I/O failures.
//
// The fn callback receives an owned copy of the value slice and an owned
// string copy of the key; it may retain them after fn returns.
func (w *WAL) Replay(fn func(op OpType, key string, value []byte, seq uint64)) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek: %w", err)
	}
	br := bufio.NewReaderSize(w.f, 256*1024)
	var lenBuf [4]byte

	// Borrow two pooled scratch buffers — one for keys, one for values.
	// They grow to the high-water mark of this WAL file; on the warm path
	// (after the first few entries) no new slice backing arrays are allocated.
	kbp := entryBufPool.Get().(*[]byte)
	vbp := entryBufPool.Get().(*[]byte)
	defer func() {
		entryBufPool.Put(kbp)
		entryBufPool.Put(vbp)
	}()

	for {
		// ── op byte ────────────────────────────────────────────────────────
		opByte, err := br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil // clean end of log
			}
			return fmt.Errorf("wal: read op: %w", err)
		}

		// ── seq (v2 records only) ──────────────────────────────────────────
		logicalOp, hasSeq := decodeWireOp(OpType(opByte))
		var seq uint64
		var seqBuf [8]byte
		if hasSeq {
			if _, err := io.ReadFull(br, seqBuf[:]); err != nil {
				if isTornWrite(err) {
					return nil
				}
				return fmt.Errorf("wal: read seq: %w", err)
			}
			seq = binary.BigEndian.Uint64(seqBuf[:])
		}

		// ── key-len ────────────────────────────────────────────────────────
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			if isTornWrite(err) {
				return nil
			}
			return fmt.Errorf("wal: read key-len: %w", err)
		}
		keyLen := binary.BigEndian.Uint32(lenBuf[:])

		// ── key ────────────────────────────────────────────────────────────
		growBuf(kbp, keyLen)
		keyBuf := (*kbp)[:keyLen]
		if _, err := io.ReadFull(br, keyBuf); err != nil {
			if isTornWrite(err) {
				return nil
			}
			return fmt.Errorf("wal: read key: %w", err)
		}

		// ── val-len ────────────────────────────────────────────────────────
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			if isTornWrite(err) {
				return nil
			}
			return fmt.Errorf("wal: read val-len: %w", err)
		}
		valLen := binary.BigEndian.Uint32(lenBuf[:])

		// ── value ──────────────────────────────────────────────────────────
		growBuf(vbp, valLen)
		valBuf := (*vbp)[:valLen]
		if _, err := io.ReadFull(br, valBuf); err != nil {
			if isTornWrite(err) {
				return nil
			}
			return fmt.Errorf("wal: read val: %w", err)
		}

		// ── CRC32 ──────────────────────────────────────────────────────────
		var crcBuf [4]byte
		if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
			if isTornWrite(err) {
				return nil
			}
			return fmt.Errorf("wal: read crc: %w", err)
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf[:])

		// Verify CRC. All bytes are already materialised in stack/pooled
		// buffers; crc32.Update traverses them once with no extra allocation.
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

		if computed != storedCRC {
			return nil // CRC mismatch — torn write at tail; stop cleanly
		}

		// Deliver owned copies to fn.  string(keyBuf) copies the key (required
		// for string immutability).  Value must be a fresh slice because the
		// Memtable tree stores value references; if we passed valBuf the next
		// Replay iteration would silently overwrite it.  nil is preserved for
		// zero-length values (e.g. tombstone deletes) so callers can distinguish
		// a Delete (nil) from a Put of an empty string (non-nil, zero-length).
		var valCopy []byte
		if valLen > 0 {
			valCopy = make([]byte, valLen)
			copy(valCopy, valBuf)
		}
		fn(logicalOp, string(keyBuf), valCopy, seq)
	}
}

// decodeWireOp maps a wire op byte onto the logical op it represents and
// reports whether the record carries a sequence number.
//
// An unrecognised code is returned unchanged with hasSeq false, which leaves it
// to the CRC check to reject: a byte that is neither a known op nor a valid
// framing start is the signature of a torn write, and the existing torn-write
// path already handles it cleanly.
func decodeWireOp(op OpType) (logical OpType, hasSeq bool) {
	switch op {
	case opPutSeq:
		return OpPut, true
	case opDeleteSeq:
		return OpDelete, true
	default:
		return op, false
	}
}

// Close flushes any buffered data, syncs, and closes the underlying file.
// Flush is a no-op when Append was the last operation (already flushed +
// synced), but is called defensively here.
func (w *WAL) Close() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("wal: close flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: close sync: %w", err)
	}
	return w.f.Close()
}

// ── helpers ──────────────────────────────────────────────────────────────────

// isTornWrite reports whether err represents a truncated read at the tail of
// the WAL — expected after a sudden power loss or process kill.
func isTornWrite(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// growBuf ensures *bp has capacity ≥ n, growing the backing array only when
// necessary.  On the hot path (n ≤ current cap) this is a bounds update with
// no allocation.
func growBuf(bp *[]byte, n uint32) {
	if uint32(cap(*bp)) < n {
		*bp = make([]byte, n)
	}
}

// unsafeStringBytes returns a []byte view of s without copying.  The returned
// slice MUST NOT be retained or written to after the call returns; it is only
// safe for read-only operations (like CRC) that complete synchronously.
// Requires Go 1.20+ (unsafe.StringData).
func unsafeStringBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
