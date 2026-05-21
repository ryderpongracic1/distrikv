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
	"unsafe"
)

// OpType is the single-byte discriminant identifying the kind of WAL entry.
type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// WAL is an append-only Write-Ahead Log for crash recovery.
//
// Wire format per entry:
//
//	[1 byte op][4 byte key-len][key bytes][4 byte val-len][val bytes][4 byte CRC32]
//
// CRC32 covers every byte preceding it in the entry. A CRC mismatch during
// replay signals a torn write from a prior crash; replay stops cleanly.
type WAL struct {
	f  *os.File
	bw *bufio.Writer // persistent; allocated once at Open, reused across Append calls
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
	return &WAL{
		f:  f,
		bw: bufio.NewWriterSize(f, 64*1024),
	}, nil
}

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
	if err := w.appendEntry(op, key, value); err != nil {
		// Discard any partial entry left in the write buffer.  The next
		// Append call will start cleanly from the last-fsynced position.
		w.bw.Reset(w.f)
		return err
	}
	return nil
}

func (w *WAL) appendEntry(op OpType, key string, value []byte) error {
	var lenBuf [4]byte
	var crc uint32

	// ── op byte ────────────────────────────────────────────────────────────
	if err := w.bw.WriteByte(byte(op)); err != nil {
		return fmt.Errorf("wal: write op: %w", err)
	}
	opBuf := [1]byte{byte(op)}
	crc = crc32.Update(crc, crc32Table, opBuf[:])

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
	return nil
}

// Replay reads the WAL from the beginning and calls fn for each valid entry.
//
// Torn-write handling (see package doc for the full contract):
//   - Returns nil and stops at any EOF / io.ErrUnexpectedEOF (torn write).
//   - Returns nil and stops at a CRC mismatch (torn write signature).
//   - Returns a non-nil error only for genuine I/O failures.
//
// The fn callback receives an owned copy of the value slice and an owned
// string copy of the key; it may retain them after fn returns.
func (w *WAL) Replay(fn func(op OpType, key string, value []byte)) error {
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
		fn(OpType(opByte), string(keyBuf), valCopy)
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
