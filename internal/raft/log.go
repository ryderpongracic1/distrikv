package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// LogEntry is a single record in the Raft log. Because data writes in
// distrikv are replicated through the consistent-hash ring rather than
// through Raft consensus, the log is primarily used to track the leader's
// term-indexed operation history for up-to-date comparisons during elections.
type LogEntry struct {
	Index uint64
	Term  uint64
	Op    string // "put" or "delete"
	Key   string
	Value []byte
}

// PersistentState manages the two Raft fields that must survive crashes:
// currentTerm and votedFor. It uses an atomic write pattern:
//
//  1. Write new state to a temporary file.
//  2. fsync the temporary file.
//  3. Rename the temp file over the real file.
//
// On POSIX systems, rename(2) is atomic with respect to the filesystem, so a
// crash between steps 2 and 3 leaves the old file intact and readable.
type PersistentState struct {
	path string
	mu   sync.Mutex
}

// newPersistentState creates a PersistentState that stores to path.
func newPersistentState(path string) *PersistentState {
	return &PersistentState{path: path}
}

// Save atomically writes currentTerm and votedFor to disk. It must be called
// (and must return nil) before any RPC response that depends on the updated
// state is sent to a peer.
//
// Wire format:
//
//	[8 byte term][2 byte votedFor-len][votedFor bytes][4 byte CRC32]
func (p *PersistentState) Save(term uint64, votedFor string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmp := p.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("raft: persistent state create tmp: %w", err)
	}

	vfBytes := []byte(votedFor)

	var buf [8 + 2]byte
	binary.BigEndian.PutUint64(buf[:8], term)
	binary.BigEndian.PutUint16(buf[8:], uint16(len(vfBytes)))

	h := crc32.NewIEEE()
	h.Write(buf[:])
	h.Write(vfBytes)

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], h.Sum32())

	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("raft: persistent state write header: %w", err)
	}
	if _, err := f.Write(vfBytes); err != nil {
		f.Close()
		return fmt.Errorf("raft: persistent state write votedFor: %w", err)
	}
	if _, err := f.Write(crcBuf[:]); err != nil {
		f.Close()
		return fmt.Errorf("raft: persistent state write crc: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: persistent state sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: persistent state close tmp: %w", err)
	}

	if err := os.Rename(tmp, p.path); err != nil {
		return fmt.Errorf("raft: persistent state rename: %w", err)
	}

	return nil
}

// Load reads the last saved persistent state. Returns zero values if the file
// does not exist (fresh node).
func (p *PersistentState) Load() (term uint64, votedFor string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.Open(p.path)
	if os.IsNotExist(err) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("raft: persistent state open: %w", err)
	}
	defer f.Close()

	var header [10]byte // 8 term + 2 votedFor-len
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return 0, "", fmt.Errorf("raft: persistent state read header: %w", err)
	}

	term = binary.BigEndian.Uint64(header[:8])
	vfLen := binary.BigEndian.Uint16(header[8:])

	vfBytes := make([]byte, vfLen)
	if _, err := io.ReadFull(f, vfBytes); err != nil {
		return 0, "", fmt.Errorf("raft: persistent state read votedFor: %w", err)
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
		return 0, "", fmt.Errorf("raft: persistent state read crc: %w", err)
	}
	storedCRC := binary.BigEndian.Uint32(crcBuf[:])

	h := crc32.NewIEEE()
	h.Write(header[:])
	h.Write(vfBytes)
	if h.Sum32() != storedCRC {
		return 0, "", fmt.Errorf("raft: persistent state CRC mismatch — file corrupted")
	}

	return term, string(vfBytes), nil
}
