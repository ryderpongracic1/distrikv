// Package wal provides a crash-safe append-only Write-Ahead Log.
// It is shared by both the store package (legacy compatibility) and the
// lsm package (one WAL file per memtable generation).
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
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
	f *os.File
}

// Open opens (or creates) the WAL file at path in append mode.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	return &WAL{f: f}, nil
}

// Append writes one log entry and fsyncs before returning.
func (w *WAL) Append(op OpType, key string, value []byte) error {
	bw := bufio.NewWriter(w.f)
	keyBytes := []byte(key)
	var lenBuf [4]byte

	if err := bw.WriteByte(byte(op)); err != nil {
		return fmt.Errorf("wal: write op: %w", err)
	}
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(keyBytes)))
	if _, err := bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write key-len: %w", err)
	}
	if _, err := bw.Write(keyBytes); err != nil {
		return fmt.Errorf("wal: write key: %w", err)
	}
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	if _, err := bw.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("wal: write val-len: %w", err)
	}
	if _, err := bw.Write(value); err != nil {
		return fmt.Errorf("wal: write val: %w", err)
	}

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
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// Replay reads the WAL from the beginning and calls fn for each valid entry.
// It stops at the first CRC mismatch without returning an error.
func (w *WAL) Replay(fn func(op OpType, key string, value []byte)) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek: %w", err)
	}
	br := bufio.NewReader(w.f)
	var lenBuf [4]byte

	for {
		opByte, err := br.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("wal: read op: %w", err)
		}
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return nil // torn write
		}
		keyLen := binary.BigEndian.Uint32(lenBuf[:])
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(br, keyBytes); err != nil {
			return nil
		}
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return nil
		}
		valLen := binary.BigEndian.Uint32(lenBuf[:])
		value := make([]byte, valLen)
		if _, err := io.ReadFull(br, value); err != nil {
			return nil
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
			return nil
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf[:])

		h := crc32.NewIEEE()
		h.Write([]byte{opByte})
		binary.BigEndian.PutUint32(lenBuf[:], keyLen)
		h.Write(lenBuf[:])
		h.Write(keyBytes)
		binary.BigEndian.PutUint32(lenBuf[:], valLen)
		h.Write(lenBuf[:])
		h.Write(value)
		if h.Sum32() != storedCRC {
			return nil // CRC mismatch = torn write
		}
		fn(OpType(opByte), string(keyBytes), value)
	}
}

// Close syncs and closes the underlying file.
func (w *WAL) Close() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: close sync: %w", err)
	}
	return w.f.Close()
}
