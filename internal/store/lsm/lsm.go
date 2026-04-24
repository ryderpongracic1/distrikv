// Package lsm provides an LSM-Tree (Log-Structured Merge-Tree) storage engine.
//
// Architecture:
//
//	Writes  → WAL (crash safety) → Memtable (in-memory sorted tree)
//	         → SSTable (disk, on memtable flush)
//	Reads   → Memtable → Immutable Memtable → SSTables (newest first)
//	Compact → background goroutine merges SSTables when count ≥ threshold
package lsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

const (
	defaultMaxMemBytes = 4 << 20 // 4 MB memtable flush threshold
	defaultCompactN    = 4       // compact when SSTable count ≥ this
)

// LSMTree is a Log-Structured Merge-Tree key-value engine.
//
// Goroutine model:
//   - runFlush:   sole writer of l.readers (prepend) and l.imm (set nil)
//   - runCompact: sole remover of entries from l.readers
//   - Put/Delete: hold l.mu write lock; call mem.Put/Delete then rotate if full
//   - Get:        snapshots mem/imm/readers under l.mu.RLock; reads without lock
type LSMTree struct {
	// --- Protected by mu ---
	mu         sync.RWMutex
	mem        *Memtable
	imm        *Memtable         // non-nil only while a flush is in progress
	readers    []*SSTableReader  // index 0 = newest; appended by flush, pruned by compact
	immFlushed *sync.Cond        // signaled when imm transitions to nil

	manifest *Manifest
	compact  *Compactor

	// --- Atomic counters (no lock needed) ---
	seqNum  atomic.Uint64 // global write sequence; shared with Memtables
	nextSST atomic.Uint64 // SSTable file sequence number
	walSeq  atomic.Uint64 // WAL file sequence number

	dataDir string
	logger  *slog.Logger
	maxMem  int64
	nCompact int // SSTable count threshold

	// --- Background goroutine control ---
	flushCh   chan struct{} // capacity 1; triggers flush goroutine
	compactCh chan struct{} // capacity 1; triggers compaction goroutine
	stopCh    chan struct{} // closed by Close()
	wg        sync.WaitGroup
}

// NewLSMTree opens or creates an LSM-Tree in dataDir.
func NewLSMTree(dataDir string, logger *slog.Logger) (*LSMTree, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir %q: %w", dataDir, err)
	}

	// Check for an in-progress restore (crash during RestoreFromSnapshot).
	sentinelPath := filepath.Join(dataDir, "restore-in-progress")
	if _, err := os.Stat(sentinelPath); err == nil {
		logger.Warn("lsm: detected incomplete restore; wiping data dir")
		if err := wipeLSMDir(dataDir); err != nil {
			return nil, fmt.Errorf("lsm: wipe after incomplete restore: %w", err)
		}
	}

	manifest, err := OpenManifest(filepath.Join(dataDir, "manifest.log"))
	if err != nil {
		return nil, fmt.Errorf("lsm: open manifest: %w", err)
	}

	l := &LSMTree{
		manifest: manifest,
		dataDir:  dataDir,
		logger:   logger,
		maxMem:   defaultMaxMemBytes,
		nCompact: defaultCompactN,
		flushCh:  make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	l.compact = NewCompactor(dataDir, manifest, logger, defaultCompactN, func() uint64 {
		return l.nextSST.Add(1)
	})
	l.immFlushed = sync.NewCond(&l.mu)

	// Open live SSTables from manifest (oldest → newest order from LiveFiles).
	liveFiles := manifest.LiveFiles()
	readers := make([]*SSTableReader, 0, len(liveFiles))
	var maxSSTSeq uint64
	for _, ev := range liveFiles {
		path := filepath.Join(dataDir, ev.Path)
		r, err := OpenSSTableReader(path, ev.SSTSeq)
		if err != nil {
			return nil, fmt.Errorf("lsm: open SSTable %q: %w", ev.Path, err)
		}
		readers = append(readers, r)
		if ev.SSTSeq > maxSSTSeq {
			maxSSTSeq = ev.SSTSeq
		}
	}
	l.nextSST.Store(maxSSTSeq)

	// Reverse readers so index 0 = newest (highest SSTSeq).
	for i, j := 0, len(readers)-1; i < j; i, j = i+1, j-1 {
		readers[i], readers[j] = readers[j], readers[i]
	}
	l.readers = readers

	// Find and seed walSeq from existing WAL files.
	walFiles, maxWalSeq, err := findWALFiles(dataDir)
	if err != nil {
		return nil, fmt.Errorf("lsm: scan WAL files: %w", err)
	}
	l.walSeq.Store(maxWalSeq)

	// Replay existing WAL files into a fresh Memtable.
	// The active WAL for new writes gets the next sequence number.
	activeWalSeq := l.walSeq.Add(1)
	activeWalPath := filepath.Join(dataDir, fmt.Sprintf("wal-%04d.log", activeWalSeq))
	activeWAL, err := storewal.Open(activeWalPath)
	if err != nil {
		return nil, fmt.Errorf("lsm: open active WAL: %w", err)
	}
	l.mem = NewMemtable(activeWAL, activeWalPath, &l.seqNum, l.maxMem)

	for _, wf := range walFiles {
		w, err := storewal.Open(wf)
		if err != nil {
			return nil, fmt.Errorf("lsm: open WAL %q for replay: %w", wf, err)
		}
		if err := l.mem.ReplayWAL(w); err != nil {
			w.Close()
			return nil, fmt.Errorf("lsm: replay WAL %q: %w", wf, err)
		}
		w.Close()
	}

	// Start background goroutines.
	l.wg.Add(2)
	go l.runFlush(context.Background())
	go l.runCompact(context.Background())

	logger.Info("lsm: opened",
		"sstables", len(readers),
		"wal_replayed", len(walFiles),
		"mem_size", l.mem.SizeBytes(),
	)
	return l, nil
}

// Get retrieves the value for key. Returns ErrNotFound if absent or deleted.
// Checks mem → imm → SSTables (newest first).
func (l *LSMTree) Get(_ context.Context, key string) ([]byte, error) {
	// Snapshot all state atomically under RLock.
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	readers := l.readers
	l.mu.RUnlock()

	// 1. Active memtable (newest writes).
	if e, ok := mem.Get(key); ok {
		if e.Tombstone {
			return nil, ErrNotFound
		}
		return append([]byte(nil), e.Value...), nil
	}

	// 2. Immutable memtable — non-nil during a flush, NOT yet in readers.
	//    Critical: without this check, keys written just before a rotation
	//    would be invisible until the flush goroutine updates l.readers.
	if imm != nil {
		if e, ok := imm.Get(key); ok {
			if e.Tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e.Value...), nil
		}
	}

	// 3. SSTables, newest to oldest.
	for _, r := range readers {
		e, found, err := r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("lsm: get from SSTable: %w", err)
		}
		if found {
			if e.Tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e.Value...), nil
		}
	}

	return nil, ErrNotFound
}

// Put writes key=value.
func (l *LSMTree) Put(_ context.Context, key string, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.mem.Put(key, value); err != nil {
		return err
	}
	if l.mem.IsFull() {
		return l.rotateMemtable()
	}
	return nil
}

// Delete writes a tombstone for key.
func (l *LSMTree) Delete(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.mem.Delete(key); err != nil {
		return err
	}
	if l.mem.IsFull() {
		return l.rotateMemtable()
	}
	return nil
}

// rotateMemtable promotes l.mem to l.imm and starts a fresh active memtable.
// Caller must hold l.mu write lock.
//
// Lock ordering: l.mu (write) is always acquired before mem.mu (inside
// Memtable.Put/Delete). rotateMemtable() does NOT acquire mem.mu; it only
// swaps pointers. This ordering is never reversed, preventing deadlocks.
//
// Backpressure: if a previous flush is still in progress (l.imm != nil),
// immFlushed.Wait() atomically releases l.mu and parks the goroutine until
// the flush goroutine signals completion. This prevents unbounded accumulation.
func (l *LSMTree) rotateMemtable() error {
	// Wait for the previous flush to complete.
	for l.imm != nil {
		l.immFlushed.Wait() // atomically releases l.mu; re-acquires on wake
	}

	// Create the new active WAL before swapping (crash here = orphan WAL,
	// which is replayed harmlessly on the next startup).
	seq := l.walSeq.Add(1)
	walPath := filepath.Join(l.dataDir, fmt.Sprintf("wal-%04d.log", seq))
	newWAL, err := storewal.Open(walPath)
	if err != nil {
		return fmt.Errorf("lsm: open new WAL: %w", err)
	}

	l.imm = l.mem
	l.mem = NewMemtable(newWAL, walPath, &l.seqNum, l.maxMem)

	select {
	case l.flushCh <- struct{}{}:
	default:
	}
	return nil
}

// runFlush is the sole goroutine that writes SSTables and clears l.imm.
func (l *LSMTree) runFlush(ctx context.Context) {
	defer l.wg.Done()
	for {
		select {
		case <-l.stopCh:
			return
		case <-l.flushCh:
			l.mu.RLock()
			imm := l.imm
			l.mu.RUnlock()

			if imm == nil {
				continue
			}
			if err := l.flushMemtable(imm); err != nil {
				l.logger.Error("lsm: flush failed", "err", err)
			}
		}
	}
}

// flushMemtable writes imm to a new SSTable and updates l.readers atomically.
func (l *LSMTree) flushMemtable(imm *Memtable) error {
	sstSeq := l.nextSST.Add(1)
	outName := fmt.Sprintf("sst-%08d.sst", sstSeq)
	outPath := filepath.Join(l.dataDir, outName)

	// Count approximate entries for Bloom filter sizing.
	approxKeys := int(imm.SizeBytes()/64) + 1
	writer, err := NewSSTableWriter(outPath, approxKeys)
	if err != nil {
		return fmt.Errorf("lsm: flush create writer: %w", err)
	}

	var writeErr error
	imm.Ascend(func(e Entry) bool {
		if err := writer.Write(e); err != nil {
			writeErr = err
			return false
		}
		return true
	})
	if writeErr != nil {
		writer.Close()
		os.Remove(outPath)
		return fmt.Errorf("lsm: flush write entry: %w", writeErr)
	}
	if err := writer.Close(); err != nil {
		os.Remove(outPath)
		return fmt.Errorf("lsm: flush close writer: %w", err)
	}

	if err := l.manifest.Add(outName, sstSeq); err != nil {
		os.Remove(outPath)
		return fmt.Errorf("lsm: flush manifest add: %w", err)
	}

	newReader, err := OpenSSTableReader(outPath, sstSeq)
	if err != nil {
		return fmt.Errorf("lsm: flush open new reader: %w", err)
	}

	// Atomically swap: prepend newest reader, clear imm, wake blocked writers.
	l.mu.Lock()
	l.readers = append([]*SSTableReader{newReader}, l.readers...)
	l.imm = nil
	l.immFlushed.Broadcast()
	l.mu.Unlock()

	// Delete the flushed WAL file.
	if err := os.Remove(imm.walPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove flushed WAL", "path", imm.walPath, "err", err)
	}

	l.logger.Info("lsm: memtable flushed", "sst", outName, "size", imm.SizeBytes())

	select {
	case l.compactCh <- struct{}{}:
	default:
	}
	return nil
}

// runCompact is the sole goroutine that removes entries from l.readers.
func (l *LSMTree) runCompact(ctx context.Context) {
	defer l.wg.Done()
	for {
		select {
		case <-l.stopCh:
			return
		case <-l.compactCh:
			l.mu.RLock()
			readers := l.readers
			l.mu.RUnlock()

			if !l.compact.ShouldCompact(len(readers)) {
				continue
			}

			newReader, err := l.compact.Compact(ctx, readers)
			if err != nil {
				l.logger.Error("lsm: compaction failed", "err", err)
				continue
			}

			// Replace the entire readers slice with just the new (merged) reader.
			l.mu.Lock()
			if newReader != nil {
				l.readers = []*SSTableReader{newReader}
			} else {
				l.readers = nil
			}
			l.mu.Unlock()
		}
	}
}

// Snapshot returns a full copy of all live (non-tombstone) key-value pairs.
// Used by Raft to capture state machine state for InstallSnapshot.
func (l *LSMTree) Snapshot(_ context.Context) (map[string][]byte, error) {
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	readers := l.readers
	l.mu.RUnlock()

	out := make(map[string][]byte)

	// Collect from SSTables first (oldest to newest), so newer overwrites older.
	for i := len(readers) - 1; i >= 0; i-- {
		it := readers[i].Iterator()
		for {
			e, ok := it.Next()
			if !ok {
				break
			}
			if e.Tombstone {
				delete(out, e.Key)
			} else {
				out[e.Key] = append([]byte(nil), e.Value...)
			}
		}
		if it.Err() != nil {
			return nil, fmt.Errorf("lsm: snapshot SSTable iter: %w", it.Err())
		}
	}

	// imm overwrites SSTable data.
	if imm != nil {
		imm.Ascend(func(e Entry) bool {
			if e.Tombstone {
				delete(out, e.Key)
			} else {
				out[e.Key] = append([]byte(nil), e.Value...)
			}
			return true
		})
	}

	// mem overwrites everything.
	mem.Ascend(func(e Entry) bool {
		if e.Tombstone {
			delete(out, e.Key)
		} else {
			out[e.Key] = append([]byte(nil), e.Value...)
		}
		return true
	})

	return out, nil
}

// Restore replaces the entire store with the contents of data (from a Raft snapshot).
// Stops background goroutines, wipes all LSM data, then replays the snapshot.
func (l *LSMTree) Restore(ctx context.Context, data map[string][]byte) error {
	// Write sentinel before wiping — crash between here and Done() → wipe on restart.
	sentinelPath := filepath.Join(l.dataDir, "restore-in-progress")
	if err := os.WriteFile(sentinelPath, []byte("1"), 0o644); err != nil {
		return fmt.Errorf("lsm: write restore sentinel: %w", err)
	}

	// Stop background goroutines.
	close(l.stopCh)
	l.wg.Wait()

	// Close all existing readers and the active WAL.
	l.mu.Lock()
	for _, r := range l.readers {
		r.Close()
	}
	l.readers = nil
	l.imm = nil
	if l.mem != nil && l.mem.w != nil {
		l.mem.w.Close()
	}
	l.mu.Unlock()

	// Wipe all LSM data files (SSTables, WALs, manifest).
	if err := wipeLSMDir(l.dataDir); err != nil {
		return fmt.Errorf("lsm: wipe for restore: %w", err)
	}

	// Create fresh manifest.
	manifest, err := OpenManifest(filepath.Join(l.dataDir, "manifest.log"))
	if err != nil {
		return fmt.Errorf("lsm: open fresh manifest: %w", err)
	}
	l.manifest = manifest
	l.compact = NewCompactor(l.dataDir, manifest, l.logger, l.nCompact, func() uint64 {
		return l.nextSST.Add(1)
	})

	// Reset all counters.
	l.seqNum.Store(0)
	l.nextSST.Store(0)
	l.walSeq.Store(0)

	// Create fresh WAL and memtable.
	activeWalSeq := l.walSeq.Add(1)
	activeWalPath := filepath.Join(l.dataDir, fmt.Sprintf("wal-%04d.log", activeWalSeq))
	activeWAL, err := storewal.Open(activeWalPath)
	if err != nil {
		return fmt.Errorf("lsm: open restore WAL: %w", err)
	}

	l.mu.Lock()
	l.mem = NewMemtable(activeWAL, activeWalPath, &l.seqNum, l.maxMem)
	l.mu.Unlock()

	// Write snapshot data into the fresh memtable (may trigger inline flushes).
	for k, v := range data {
		if err := l.Put(ctx, k, v); err != nil {
			return fmt.Errorf("lsm: restore write %q: %w", k, err)
		}
	}

	// Restart background goroutines.
	l.stopCh = make(chan struct{})
	l.flushCh = make(chan struct{}, 1)
	l.compactCh = make(chan struct{}, 1)
	l.wg.Add(2)
	go l.runFlush(ctx)
	go l.runCompact(ctx)

	// Remove sentinel — restore is complete.
	if err := os.Remove(sentinelPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove restore sentinel", "err", err)
	}

	l.logger.Info("lsm: snapshot restored", "keys", len(data))
	return nil
}

// Close flushes any pending memtable, stops background goroutines, and closes
// all file handles. No further operations are valid after Close.
func (l *LSMTree) Close() error {
	close(l.stopCh)
	l.wg.Wait()

	// Flush remaining mem to disk (synchronous, no goroutine).
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	l.mu.RUnlock()

	if imm != nil {
		if err := l.flushMemtable(imm); err != nil {
			l.logger.Warn("lsm: close flush imm failed", "err", err)
		}
	}
	if mem != nil && mem.SizeBytes() > 0 {
		if err := l.flushMemtableDirect(mem); err != nil {
			l.logger.Warn("lsm: close flush mem failed", "err", err)
		}
	} else if mem != nil && mem.w != nil {
		if err := mem.w.Close(); err != nil {
			l.logger.Warn("lsm: close active WAL", "err", err)
		}
	}

	l.mu.RLock()
	readers := l.readers
	l.mu.RUnlock()
	for _, r := range readers {
		if err := r.Close(); err != nil {
			l.logger.Warn("lsm: close SSTable reader", "path", r.path, "err", err)
		}
	}

	return nil
}

// flushMemtableDirect flushes mem synchronously (used during Close and Restore).
// Does NOT update l.imm; caller ensures no concurrent writers.
func (l *LSMTree) flushMemtableDirect(mem *Memtable) error {
	defer func() {
		if mem.w != nil {
			mem.w.Close()
		}
	}()

	sstSeq := l.nextSST.Add(1)
	outName := fmt.Sprintf("sst-%08d.sst", sstSeq)
	outPath := filepath.Join(l.dataDir, outName)

	approxKeys := int(mem.SizeBytes()/64) + 1
	writer, err := NewSSTableWriter(outPath, approxKeys)
	if err != nil {
		return err
	}
	var writeErr error
	mem.Ascend(func(e Entry) bool {
		if err := writer.Write(e); err != nil {
			writeErr = err
			return false
		}
		return true
	})
	if writeErr != nil {
		writer.Close()
		os.Remove(outPath)
		return writeErr
	}
	if err := writer.Close(); err != nil {
		os.Remove(outPath)
		return err
	}
	if err := l.manifest.Add(outName, sstSeq); err != nil {
		os.Remove(outPath)
		return err
	}
	r, err := OpenSSTableReader(outPath, sstSeq)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.readers = append([]*SSTableReader{r}, l.readers...)
	l.mu.Unlock()

	if err := os.Remove(mem.walPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove WAL after direct flush", "path", mem.walPath, "err", err)
	}
	return nil
}

// ---- Helpers ---------------------------------------------------------------

var walFileRE = regexp.MustCompile(`^wal-(\d+)\.log$`)

// findWALFiles returns all WAL files in dataDir sorted ascending by sequence
// number, and the maximum sequence number found.
func findWALFiles(dataDir string) (paths []string, maxSeq uint64, err error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, 0, fmt.Errorf("lsm: read dir: %w", err)
	}

	type walFile struct {
		path string
		seq  uint64
	}
	var wals []walFile

	for _, de := range entries {
		m := walFileRE.FindStringSubmatch(de.Name())
		if m == nil {
			continue
		}
		seq, _ := strconv.ParseUint(m[1], 10, 64)
		wals = append(wals, walFile{
			path: filepath.Join(dataDir, de.Name()),
			seq:  seq,
		})
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	sort.Slice(wals, func(i, j int) bool { return wals[i].seq < wals[j].seq })
	for _, w := range wals {
		paths = append(paths, w.path)
	}
	return paths, maxSeq, nil
}

// wipeLSMDir removes all SSTable, WAL, and manifest files from dataDir.
func wipeLSMDir(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("lsm: read dir for wipe: %w", err)
	}
	for _, de := range entries {
		name := de.Name()
		if walFileRE.MatchString(name) ||
			len(name) > 4 && name[len(name)-4:] == ".sst" ||
			name == "manifest.log" ||
			name == "manifest.log.tmp" {
			if err := os.Remove(filepath.Join(dataDir, name)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("lsm: wipe %q: %w", name, err)
			}
		}
	}
	return nil
}
