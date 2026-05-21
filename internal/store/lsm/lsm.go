// Package lsm provides an LSM-Tree (Log-Structured Merge-Tree) storage engine.
//
// # Phase 3 — Leveled Compaction Strategy (LCS) + Write-Stall Backpressure
//
// Architecture:
//
//	Writes  → WAL (crash safety) → Memtable (in-memory sorted tree)
//	         → L0 SSTable (disk, on memtable flush)
//	         → L1 SSTable (disk, after L0→L1 compaction)
//	Reads   → Memtable → Immutable Memtable → L0 SSTables → L1 SSTables
//	Compact → background goroutine: all L0 + all L1 → single new L1 SSTable
//	          when len(l0) ≥ compactThreshold
//
// # Level semantics
//
// L0 files are produced directly by memtable flushes and may have overlapping
// key ranges. L1 files are the output of compaction and have non-overlapping
// ranges (enforced by the merge-sort). The Get path checks all L0 files before
// all L1 files, ensuring the newest version of any key is returned even when
// L0 is not yet compacted.
//
// Tombstones are dropped during L0→L1 compaction. This is safe because L1 is
// treated as the bottom level (there is no L2+). When future phases add L2+,
// tombstone preservation for non-bottom levels will need to be added to the
// MergeIterator.
//
// # Write-stall backpressure
//
// If memtable flushes produce L0 files faster than background compaction can
// drain them, incoming writes are throttled before they take the write lock:
//
//   - len(l0) ≥ l0SlowThreshold (default 8): soft stall — proportional sleep,
//     capped at 50 ms, retried until L0 falls below the threshold.
//   - len(l0) ≥ l0StopThreshold (default 12): hard stop — blocks on l0Drained
//     condition variable until runCompact signals that L0 has drained.
//
// Stall events and cumulative delay are recorded in metrics.WriteStallCount
// and metrics.WriteStallMicros, visible via the /metrics HTTP endpoint.
//
// # Goroutine model
//
//   - runFlush:   sole writer of l.l0 (prepend) and l.imm (set nil)
//   - runCompact: sole remover of entries from l.l0; sole writer of l.l1
//   - Put/Delete: call maybeStallWrite without lock, then hold l.mu write lock
//   - Get:        snapshots l0/imm/mem under l.mu.RLock; reads without lock
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
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

const (
	defaultMaxMemBytes    = 4 << 20 // 4 MB memtable flush threshold
	defaultCompactN       = 4       // compact when len(l0) ≥ this
	defaultL0SlowThreshold = 8      // 2× defaultCompactN: begin soft stall
	defaultL0StopThreshold = 12     // 3× defaultCompactN: hard stop writes
)

// LSMTree is a Log-Structured Merge-Tree key-value engine.
//
// Goroutine model (see package doc for full description):
//   - runFlush:   sole writer of l.l0 and l.imm
//   - runCompact: sole mover of files from l.l0 to l.l1
//   - Put/Delete: maybeStallWrite (no lock) then hold l.mu write lock
//   - Get:        snapshot l0/imm/mem under l.mu.RLock; reads without lock
type LSMTree struct {
	// --- Protected by mu ---
	mu  sync.RWMutex
	mem *Memtable
	imm *Memtable // non-nil only while a flush is in progress

	// l0 holds SSTables freshly flushed from the memtable (may overlap).
	// l0[0] = newest. Solely written by runFlush; solely pruned by runCompact.
	l0 []*SSTableReader

	// l1 holds the single output of the most recent L0→L1 compaction.
	// l1[0] = newest. Solely replaced by runCompact.
	l1 []*SSTableReader

	// l0Drained is broadcast by runCompact when l0Count drops below
	// l0StopThreshold, unblocking writers in hard-stop stall.
	l0Drained *sync.Cond

	// immFlushed is broadcast when imm transitions to nil.
	immFlushed *sync.Cond

	manifest *Manifest
	compact  *Compactor

	// --- Atomic counters (no lock needed) ---
	seqNum  atomic.Uint64 // global write sequence; shared with Memtables
	nextSST atomic.Uint64 // SSTable file sequence number
	walSeq  atomic.Uint64 // WAL file sequence number

	// l0Count mirrors len(l.l0) as an atomic so maybeStallWrite can read it
	// without taking the mutex. It is always updated under mu before Unlock.
	l0Count atomic.Int32

	dataDir          string
	logger           *slog.Logger
	maxMem           int64
	nCompact         int // L0 file count threshold for compaction
	l0SlowThreshold  int // soft-stall threshold
	l0StopThreshold  int // hard-stop threshold
	metrics          *metrics.Metrics // may be nil

	// --- Background goroutine control ---
	flushCh   chan struct{} // capacity 1; triggers flush goroutine
	compactCh chan struct{} // capacity 1; triggers compaction goroutine
	stopCh    chan struct{} // closed by Close()
	wg        sync.WaitGroup
}

// Option configures an LSMTree at construction time.
type Option func(*LSMTree)

// WithMetrics injects a *metrics.Metrics so the LSM can publish bloom-filter
// hit/miss/false-positive counts and flush/compaction byte totals. Passing nil
// is allowed and disables instrumentation.
func WithMetrics(m *metrics.Metrics) Option {
	return func(l *LSMTree) { l.metrics = m }
}

// WithMaxMemBytes overrides the memtable flush threshold. Used by tests to
// trigger flushes without writing megabytes of data.
func WithMaxMemBytes(n int64) Option {
	return func(l *LSMTree) {
		if n > 0 {
			l.maxMem = n
		}
	}
}

// WithL0StallConfig sets the L0 file-count thresholds for write-stall
// backpressure. slowThreshold is the file count at which soft stall begins;
// stopThreshold is the count at which writes block entirely until compaction
// catches up. Both must be > 0 and stopThreshold > slowThreshold.
func WithL0StallConfig(slowThreshold, stopThreshold int) Option {
	return func(l *LSMTree) {
		if slowThreshold > 0 {
			l.l0SlowThreshold = slowThreshold
		}
		if stopThreshold > slowThreshold {
			l.l0StopThreshold = stopThreshold
		}
	}
}

// NewLSMTree opens or creates an LSM-Tree in dataDir.
func NewLSMTree(dataDir string, logger *slog.Logger, opts ...Option) (*LSMTree, error) {
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
		// Remove the sentinel so subsequent opens don't re-wipe.
		if err := os.Remove(sentinelPath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("lsm: remove restore sentinel: %w", err)
		}
	}

	manifest, err := OpenManifest(filepath.Join(dataDir, "manifest.log"))
	if err != nil {
		return nil, fmt.Errorf("lsm: open manifest: %w", err)
	}

	l := &LSMTree{
		manifest:         manifest,
		dataDir:          dataDir,
		logger:           logger,
		maxMem:           defaultMaxMemBytes,
		nCompact:         defaultCompactN,
		l0SlowThreshold:  defaultL0SlowThreshold,
		l0StopThreshold:  defaultL0StopThreshold,
		flushCh:          make(chan struct{}, 1),
		compactCh:        make(chan struct{}, 1),
		stopCh:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.compact = NewCompactor(dataDir, manifest, logger, l.nCompact, func() uint64 {
		return l.nextSST.Add(1)
	})
	l.compact.metrics = l.metrics
	l.immFlushed = sync.NewCond(&l.mu)
	l.l0Drained = sync.NewCond(&l.mu)

	// Open live SSTables from manifest, split into l0 and l1 slices.
	liveFiles := manifest.LiveFiles()
	var l0readers, l1readers []*SSTableReader
	var maxSSTSeq uint64

	for _, ev := range liveFiles {
		path := filepath.Join(dataDir, ev.Path)
		r, err := OpenSSTableReader(path, ev.SSTSeq)
		if err != nil {
			return nil, fmt.Errorf("lsm: open SSTable %q: %w", ev.Path, err)
		}
		r.Level = ev.Level
		r.metrics = l.metrics
		if ev.SSTSeq > maxSSTSeq {
			maxSSTSeq = ev.SSTSeq
		}
		if ev.Level == 0 {
			l0readers = append(l0readers, r)
		} else {
			l1readers = append(l1readers, r)
		}
	}
	l.nextSST.Store(maxSSTSeq)

	// Both slices are newest-first (LiveFiles returns ascending SSTSeq →
	// reverse each to get descending / newest-first order).
	reverseReaders(l0readers)
	reverseReaders(l1readers)
	l.l0 = l0readers
	l.l1 = l1readers
	l.l0Count.Store(int32(len(l.l0)))
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(int64(len(l.l0)))
	}

	// Find and seed walSeq from existing WAL files.
	walFiles, maxWalSeq, err := findWALFiles(dataDir)
	if err != nil {
		return nil, fmt.Errorf("lsm: scan WAL files: %w", err)
	}
	l.walSeq.Store(maxWalSeq)

	// Replay existing WAL files into a fresh Memtable.
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
		"l0_sstables", len(l.l0),
		"l1_sstables", len(l.l1),
		"wal_replayed", len(walFiles),
		"mem_size", l.mem.SizeBytes(),
	)
	return l, nil
}

// Get retrieves the value for key. Returns ErrNotFound if absent or deleted.
// Read path: mem → imm → L0 (newest-first) → L1 (newest-first).
func (l *LSMTree) Get(_ context.Context, key string) ([]byte, error) {
	// Snapshot all state atomically under RLock.
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	l0 := l.l0
	l1 := l.l1
	l.mu.RUnlock()

	// 1. Active memtable.
	if e, ok := mem.Get(key); ok {
		if e.Tombstone {
			return nil, ErrNotFound
		}
		return append([]byte(nil), e.Value...), nil
	}

	// 2. Immutable memtable — non-nil only during a flush.
	if imm != nil {
		if e, ok := imm.Get(key); ok {
			if e.Tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e.Value...), nil
		}
	}

	// 3. L0 SSTables — may overlap; must check all, newest-first.
	for _, r := range l0 {
		e, found, err := r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("lsm: get from L0 SSTable: %w", err)
		}
		if found {
			if e.Tombstone {
				return nil, ErrNotFound
			}
			return append([]byte(nil), e.Value...), nil
		}
	}

	// 4. L1 SSTables — non-overlapping after compaction; check newest-first.
	for _, r := range l1 {
		e, found, err := r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("lsm: get from L1 SSTable: %w", err)
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
func (l *LSMTree) Put(ctx context.Context, key string, value []byte) error {
	if err := l.maybeStallWrite(ctx); err != nil {
		return err
	}
	return l.putInternal(ctx, key, value)
}

// putInternal is the lock-holding write path, shared by Put and Restore.
// Stall check is NOT applied here; callers are responsible.
func (l *LSMTree) putInternal(_ context.Context, key string, value []byte) error {
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
func (l *LSMTree) Delete(ctx context.Context, key string) error {
	if err := l.maybeStallWrite(ctx); err != nil {
		return err
	}
	return l.deleteInternal(ctx, key)
}

// deleteInternal is the lock-holding delete path, shared by Delete and Restore.
func (l *LSMTree) deleteInternal(_ context.Context, key string) error {
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

// maybeStallWrite applies write-stall backpressure when L0 is filling up.
// Must be called WITHOUT l.mu held.
//
//   - No stall:                  l0Count < l0SlowThreshold
//   - Soft stall (sleep+retry):  l0SlowThreshold ≤ l0Count < l0StopThreshold
//   - Hard stop (wait on cond):  l0Count ≥ l0StopThreshold
//
// Each stall event increments metrics.WriteStallCount and adds elapsed
// microseconds to metrics.WriteStallMicros.
func (l *LSMTree) maybeStallWrite(ctx context.Context) error {
	for {
		n := l.l0Count.Load()
		if n < int32(l.l0SlowThreshold) {
			return nil
		}

		// Context already cancelled — propagate immediately without stalling.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		start := time.Now()

		if n >= int32(l.l0StopThreshold) {
			// Hard stop: block until runCompact signals l0 has drained.
			l.mu.Lock()
			for l.l0Count.Load() >= int32(l.l0StopThreshold) {
				l.l0Drained.Wait() // releases l.mu; reacquires on wake
			}
			l.mu.Unlock()
		} else {
			// Soft stall: proportional sleep, max 50 ms.
			overage := int(n) - l.l0SlowThreshold + 1
			delay := time.Duration(overage) * 5 * time.Millisecond
			if delay > 50*time.Millisecond {
				delay = 50 * time.Millisecond
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if l.metrics != nil {
			l.metrics.WriteStallCount.Add(1)
			l.metrics.WriteStallMicros.Add(uint64(time.Since(start).Microseconds()))
		}
		// Re-check after waking — L0 may still be above the slow threshold.
	}
}

// rotateMemtable promotes l.mem to l.imm and starts a fresh active memtable.
// Caller must hold l.mu write lock.
func (l *LSMTree) rotateMemtable() error {
	// Wait for any previous flush to complete.
	for l.imm != nil {
		l.immFlushed.Wait()
	}

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

// runFlush is the sole goroutine that writes L0 SSTables and clears l.imm.
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

// flushMemtable writes imm to a new L0 SSTable and prepends it to l.l0.
func (l *LSMTree) flushMemtable(imm *Memtable) error {
	sstSeq := l.nextSST.Add(1)
	outName := fmt.Sprintf("sst-%08d.sst", sstSeq)
	outPath := filepath.Join(l.dataDir, outName)

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

	if err := l.manifest.Add(outName, sstSeq, 0 /*level=L0*/); err != nil {
		os.Remove(outPath)
		return fmt.Errorf("lsm: flush manifest add: %w", err)
	}

	if l.metrics != nil {
		if info, statErr := os.Stat(outPath); statErr == nil {
			l.metrics.FlushBytes.Add(uint64(info.Size()))
		}
	}

	newReader, err := OpenSSTableReader(outPath, sstSeq)
	if err != nil {
		return fmt.Errorf("lsm: flush open new reader: %w", err)
	}
	newReader.Level = 0
	newReader.metrics = l.metrics

	// Atomically: prepend to l0, update l0Count, clear imm, wake writers.
	l.mu.Lock()
	l.l0 = append([]*SSTableReader{newReader}, l.l0...)
	l.l0Count.Store(int32(len(l.l0)))
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(int64(len(l.l0)))
	}
	l.imm = nil
	l.immFlushed.Broadcast()
	l.mu.Unlock()

	if err := os.Remove(imm.walPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove flushed WAL", "path", imm.walPath, "err", err)
	}

	l.logger.Info("lsm: memtable flushed to L0", "sst", outName,
		"size", imm.SizeBytes(), "l0_depth", len(l.l0)+1)

	select {
	case l.compactCh <- struct{}{}:
	default:
	}
	return nil
}

// runCompact is the sole goroutine that moves data from L0 to L1.
func (l *LSMTree) runCompact(ctx context.Context) {
	defer l.wg.Done()
	for {
		select {
		case <-l.stopCh:
			return
		case <-l.compactCh:
			l.mu.RLock()
			l0snap := l.l0
			l1snap := l.l1
			l.mu.RUnlock()

			if !l.compact.ShouldCompact(len(l0snap)) {
				continue
			}

			// Compact all L0 + all L1 into a single new L1 SSTable.
			// Pass them oldest-first: l1 reversed + l0 reversed.
			allReaders := compactInputOrder(l0snap, l1snap)

			newReader, err := l.compact.Compact(ctx, allReaders)
			if err != nil {
				l.logger.Error("lsm: L0→L1 compaction failed", "err", err)
				continue
			}

			if newReader != nil {
				newReader.Level = 1
			}

			// Replace l0 and l1 atomically, decrement l0Count, wake stalled writers.
			l.mu.Lock()
			prevL0 := len(l.l0)
			l.l0 = nil
			if newReader != nil {
				l.l1 = []*SSTableReader{newReader}
			} else {
				l.l1 = nil
			}
			l.l0Count.Store(0)
			if l.metrics != nil {
				l.metrics.L0FileCount.Store(0)
			}
			l.l0Drained.Broadcast() // wake any hard-stopped writers
			l.mu.Unlock()

			l.logger.Info("lsm: L0→L1 compaction complete",
				"l0_inputs", len(l0snap),
				"l1_inputs", len(l1snap),
				"l0_cleared", prevL0,
			)
		}
	}
}

// compactInputOrder builds the oldest-first reader slice for the merge:
// L1 files (oldest) reversed + L0 files (oldest in l0 slice = highest index) reversed.
func compactInputOrder(l0, l1 []*SSTableReader) []*SSTableReader {
	out := make([]*SSTableReader, 0, len(l0)+len(l1))
	// L1 oldest first (l1 is newest-first → reverse)
	for i := len(l1) - 1; i >= 0; i-- {
		out = append(out, l1[i])
	}
	// L0 oldest first (l0 is newest-first → reverse)
	for i := len(l0) - 1; i >= 0; i-- {
		out = append(out, l0[i])
	}
	return out
}

// Snapshot returns a full copy of all live (non-tombstone) key-value pairs.
// Used by Raft to capture state machine state for InstallSnapshot.
func (l *LSMTree) Snapshot(_ context.Context) (map[string][]byte, error) {
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	l0 := l.l0
	l1 := l.l1
	l.mu.RUnlock()

	out := make(map[string][]byte)

	// Apply oldest-first so newer writes overwrite older:
	// L1 (oldest, reverse order) → L0 (older, reverse order) → imm → mem.
	for i := len(l1) - 1; i >= 0; i-- {
		it := l1[i].Iterator()
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
			return nil, fmt.Errorf("lsm: snapshot L1 SSTable iter: %w", it.Err())
		}
	}

	for i := len(l0) - 1; i >= 0; i-- {
		it := l0[i].Iterator()
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
			return nil, fmt.Errorf("lsm: snapshot L0 SSTable iter: %w", it.Err())
		}
	}

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
func (l *LSMTree) Restore(ctx context.Context, data map[string][]byte) error {
	sentinelPath := filepath.Join(l.dataDir, "restore-in-progress")
	if err := os.WriteFile(sentinelPath, []byte("1"), 0o644); err != nil {
		return fmt.Errorf("lsm: write restore sentinel: %w", err)
	}

	close(l.stopCh)
	l.wg.Wait()

	l.mu.Lock()
	for _, r := range l.l0 {
		r.Close()
	}
	for _, r := range l.l1 {
		r.Close()
	}
	l.l0 = nil
	l.l1 = nil
	l.l0Count.Store(0)
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(0)
	}
	l.imm = nil
	if l.mem != nil && l.mem.w != nil {
		l.mem.w.Close()
	}
	l.mu.Unlock()

	if err := wipeLSMDir(l.dataDir); err != nil {
		return fmt.Errorf("lsm: wipe for restore: %w", err)
	}

	manifest, err := OpenManifest(filepath.Join(l.dataDir, "manifest.log"))
	if err != nil {
		return fmt.Errorf("lsm: open fresh manifest: %w", err)
	}
	l.manifest = manifest
	l.compact = NewCompactor(l.dataDir, manifest, l.logger, l.nCompact, func() uint64 {
		return l.nextSST.Add(1)
	})
	l.compact.metrics = l.metrics

	l.seqNum.Store(0)
	l.nextSST.Store(0)
	l.walSeq.Store(0)

	activeWalSeq := l.walSeq.Add(1)
	activeWalPath := filepath.Join(l.dataDir, fmt.Sprintf("wal-%04d.log", activeWalSeq))
	activeWAL, err := storewal.Open(activeWalPath)
	if err != nil {
		return fmt.Errorf("lsm: open restore WAL: %w", err)
	}

	l.mu.Lock()
	l.mem = NewMemtable(activeWAL, activeWalPath, &l.seqNum, l.maxMem)
	l.mu.Unlock()

	// Restart background goroutines before writing data so flushes are serviced.
	l.stopCh = make(chan struct{})
	l.flushCh = make(chan struct{}, 1)
	l.compactCh = make(chan struct{}, 1)
	l.wg.Add(2)
	go l.runFlush(ctx)
	go l.runCompact(ctx)

	// Write snapshot data via putInternal (no stall — this is a recovery path).
	for k, v := range data {
		if err := l.putInternal(ctx, k, v); err != nil {
			return fmt.Errorf("lsm: restore write %q: %w", k, err)
		}
	}

	if err := os.Remove(sentinelPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove restore sentinel", "err", err)
	}

	l.logger.Info("lsm: snapshot restored", "keys", len(data))
	return nil
}

// Close flushes any pending memtable, stops background goroutines, and closes
// all file handles.
func (l *LSMTree) Close() error {
	close(l.stopCh)
	l.wg.Wait()

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
	l0 := l.l0
	l1 := l.l1
	l.mu.RUnlock()

	for _, r := range l0 {
		if err := r.Close(); err != nil {
			l.logger.Warn("lsm: close L0 SSTable", "path", r.path, "err", err)
		}
	}
	for _, r := range l1 {
		if err := r.Close(); err != nil {
			l.logger.Warn("lsm: close L1 SSTable", "path", r.path, "err", err)
		}
	}
	return nil
}

// flushMemtableDirect flushes mem synchronously (used during Close).
// Writes to L0 and updates manifest accordingly.
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
	if err := l.manifest.Add(outName, sstSeq, 0 /*L0*/); err != nil {
		os.Remove(outPath)
		return err
	}
	r, err := OpenSSTableReader(outPath, sstSeq)
	if err != nil {
		return err
	}
	r.Level = 0
	r.metrics = l.metrics

	l.mu.Lock()
	l.l0 = append([]*SSTableReader{r}, l.l0...)
	l.l0Count.Store(int32(len(l.l0)))
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(int64(len(l.l0)))
	}
	l.mu.Unlock()

	if err := os.Remove(mem.walPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove WAL after direct flush", "path", mem.walPath, "err", err)
	}
	return nil
}

// ---- Helpers ---------------------------------------------------------------

var walFileRE = regexp.MustCompile(`^wal-(\d+)\.log$`)

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

func reverseReaders(rs []*SSTableReader) {
	for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
		rs[i], rs[j] = rs[j], rs[i]
	}
}
