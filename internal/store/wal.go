package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// opType is a single-byte discriminant that identifies the kind of WAL entry.
type opType uint8

const (
	opPut    opType = 1
	opDelete opType = 2
)

// WAL is an append-only Write-Ahead Log for crash recovery.
//
// Wire format per entry:
//
//	[1 byte op][4 byte key-len][key bytes][4 byte val-len][val bytes][4 byte CRC32]
//
// The CRC32 covers every byte that precedes it in the entry. On replay,
// an entry whose CRC does not match is treated as a torn (incomplete) write
// caused by a crash, and replay stops at that point without returning an
// error. This correctly handles the common crash-at-tail scenario.
type WAL struct {
	f *os.File
}

// openWAL opens the WAL file at path in append mode, creating it if it does
// not exist. The caller is responsible for calling Close.
func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	return &WAL{f: f}, nil
}

// Append writes one log entry and calls Sync before returning, guaranteeing
// that the entry is durable on the storage device before the write is
// reported as successful.
func (w *WAL) Append(op opType, key string, value []byte) error {
	// Compute the entry into a temporary buffer so we can CRC the whole thing.
	bw := bufio.NewWriter(w.f)

	keyBytes := []byte(key)
	var lenBuf [4]byte

	// op byte
	if err := bw.WriteByte(byte(op)); err != nil {
		return fmt.Errorf("wal: write op: %w", err)
	}

	// key length + key
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(keyBytes)))
	if _, err := bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write key-len: %w", err)
	}
	if _, err := bw.Write(keyBytes); err != nil {
		return fmt.Errorf("wal: write key: %w", err)
	}

	// value length + value
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	if _, err := bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write val-len: %w", err)
	}
	if _, err := bw.Write(value); err != nil {
		return fmt.Errorf("wal: write val: %w", err)
	}

	// Compute CRC over: op || key-len || key || val-len || val
	h := crc32.NewIEEE()
	h.Write([]byte{byte(op)})
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(keyBytes)))
	h.Write(lenBuf[:])
	h.Write(keyBytes)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	h.Write(lenBuf[:])
	h.Write(value)

	binary.BigEndian.PutUint32(lenBuf[:], h.Sum32())
	if _, err := bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write crc: %w", err)
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}

	// fsync — must reach the storage device, not just the OS page cache.
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	return nil
}

// Replay reads the WAL from the beginning and calls fn for each valid entry.
// It stops at the first CRC mismatch without returning an error, because a
// CRC mismatch indicates a partial (torn) write from a prior crash — the
// only valid scenario for a correctly maintained append-only log.
func (w *WAL) Replay(fn func(op opType, key string, value []byte)) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek to start: %w", err)
	}

	br := bufio.NewReader(w.f)
	var lenBuf [4]byte

	for {
		// op byte
		opByte, err := br.ReadByte()
		if err == io.EOF {
			return nil // clean end of log
		}
		if err != nil {
			return fmt.Errorf("wal: read op: %w", err)
		}

		// key length
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return nil // torn write
		}
		keyLen := binary.BigEndian.Uint32(lenBuf[:])

		// key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(br, keyBytes); err != nil {
			return nil // torn write
		}

		// value length
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return nil
		}
		valLen := binary.BigEndian.Uint32(lenBuf[:])

		// value
		value := make([]byte, valLen)
		if _, err := io.ReadFull(br, value); err != nil {
			return nil
		}

		// stored CRC
		var crcBuf [4]byte
		if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
			return nil
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf[:])

		// verify CRC
		h := crc32.NewIEEE()
		h.Write([]byte{opByte})
		binary.BigEndian.PutUint32(lenBuf[:], keyLen)
		h.Write(lenBuf[:])
		h.Write(keyBytes)
		binary.BigEndian.PutUint32(lenBuf[:], valLen)
		h.Write(lenBuf[:])
		h.Write(value)

		if h.Sum32() != storedCRC {
			return nil // CRC mismatch = torn write, stop cleanly
		}

		fn(opType(opByte), string(keyBytes), value)
	}
}

// Close flushes any buffered data and closes the underlying file.
func (w *WAL) Close() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: close sync: %w", err)
	}
	return w.f.Close()
}
