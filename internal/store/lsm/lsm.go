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
//	Compact → background goroutine: the L0 files present when the merge starts,
//	          plus all L1 → single new L1 SSTable, when len(l0) ≥ compactThreshold.
//	          L0 files flushed while the merge runs are retained, not merged.
//
// # Level semantics
//
// L0 files are produced directly by memtable flushes and may have overlapping
// key ranges. L1 files are the output of compaction and have non-overlapping
// ranges (enforced by the merge-sort). The Get path checks all L0 files before
// all L1 files, ensuring the newest version of any key is returned even when
// L0 is not yet compacted.
//
// A compaction snapshots its input set and then merges without holding the lock,
// so a memtable flush can publish a new L0 file while the merge is in flight.
// That file is strictly newer than every merge input and its entries are not in
// the merge output, so installCompactionResult removes only the files it
// actually merged and retains the rest at L0 — ahead of the new L1 file in the
// read path, which is their correct precedence.
//
// Tombstones are dropped during L0→L1 compaction. This is safe because L1 is
// treated as the bottom level (there is no L2+), and because every file the
// merge does not consume is newer than every file it does: a dropped tombstone
// can never resurrect a value from a retained file. When future phases add L2+,
// tombstone preservation for non-bottom levels will need to be added to the
// MergeIterator.
//
// # Write-stall backpressure
//
// If memtable flushes produce L0 files faster than background compaction can
// drain them, incoming writes are throttled before they take the write lock:
//
//   - len(l0) ≥ l0SlowThreshold (default 8): soft stall — proportional sleep,
//     capped at 50 ms, retried until L0 falls below the threshold or the
//     caller's context is done. Soft stall is a throttle, so it is bounded only
//     by the caller.
//   - len(l0) ≥ l0StopThreshold (default 12): hard stop — waits for compaction
//     to signal that L0 has drained, and gives up with ErrWriteStalled after
//     maxStallWait (default 1 s). Hard stop means writes are being refused, so
//     it reports that rather than waiting indefinitely: an unbounded wait makes
//     an overloaded node look identical to a dead one.
//
// The thresholds key off the live L0 file count, which is restored from the
// manifest at open — so a store closed with an L0 backlog reopens stalled.
// NewLSMTree therefore arms compaction at open when the restored L0 set is
// already over the compaction threshold. Without that, the stall could never
// clear: writes stall, so no memtable fills, so nothing flushes, and a flush is
// otherwise the only thing that signals compaction.
//
// Stall events and cumulative delay are recorded in metrics.WriteStallCount
// and metrics.WriteStallMicros, visible via the /metrics HTTP endpoint.
//
// # Engine counters
//
// Two counters back observability fields the node serves over HTTP:
//
//   - LiveKeys — approximate live (non-tombstoned) key count, served as
//     /status key_count. Maintained incrementally from the memtable's
//     replaced/tombstone state so the write path stays read-free and
//     allocation-free; persisted at each flush via the manifest and rebuilt
//     from WAL replay at open. See LSMTree.LiveKeys for the accuracy contract.
//   - WALAppends — fsync'd WAL appends since open, served as /metrics
//     wal_writes. Mirrored into metrics.WALWrites when metrics are wired.
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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

// ErrWriteStalled is returned by Put and Delete when write-stall backpressure
// held the write for longer than the engine's stall budget
// (defaultMaxStallWait) without L0 draining.
//
// It is a distinct sentinel because "alive but shedding load" is a different
// fault from the two errors it would otherwise be confused with: an unbounded
// wait (which makes the node indistinguishable from one that is down) and
// context.DeadlineExceeded (which a caller cannot tell from an unreachable
// peer). A replica that answers its primary with this error is reporting a
// compaction backlog, which converges on its own; the operator response is
// different from that for a dead node.
var ErrWriteStalled = errors.New("write stalled: L0 compaction backlog")

const (
	defaultMaxMemBytes     = 4 << 20  // 4 MB memtable flush threshold
	defaultCompactN        = 4        // compact when len(l0) ≥ this
	defaultL0SlowThreshold = 8        // 2× defaultCompactN: begin soft stall
	defaultL0StopThreshold = 12       // 3× defaultCompactN: hard stop writes
	defaultBlockCacheBytes = 64 << 20 // 64 MB in-process LRU block cache

	// defaultMaxStallWait bounds how long one write may sit in write-stall
	// backpressure before it gives up with ErrWriteStalled.
	//
	// The value is chosen against the replication deadline that a stalled
	// replica is answering into (cmd/node.defaultReplicateTimeout, 2s): a
	// replica must report its own overload *before* the primary's deadline
	// fires, because a deadline-exceeded reply is indistinguishable from an
	// unreachable node while ErrWriteStalled is not. 1s leaves the primary a
	// full second of margin and is an order of magnitude above a healthy stall
	// (one compaction pass — tens to low hundreds of milliseconds), so a write
	// only fails here when the backlog genuinely is not clearing.
	//
	// A caller whose own context expires sooner still wins: the stall loop
	// selects on both.
	defaultMaxStallWait = 1 * time.Second
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

	// l0DrainedCh is closed (and replaced with a fresh channel) whenever
	// l0Count falls back below l0StopThreshold, waking every writer parked in
	// hard stop. It is a channel rather than a sync.Cond because a stalled
	// writer must be able to give up as well as be woken: select can wait on
	// this and on the caller's context at the same time, which Cond.Wait
	// cannot. Read and replaced only under mu.
	l0DrainedCh chan struct{}

	// immFlushed is broadcast when imm transitions to nil.
	immFlushed *sync.Cond

	manifest *Manifest
	compact  *Compactor

	// --- Atomic counters (no lock needed) ---
	seqNum  atomic.Uint64 // global write sequence; shared with Memtables
	nextSST atomic.Uint64 // SSTable file sequence number
	walSeq  atomic.Uint64 // WAL file sequence number

	// WAL cursor/retention state for replica catch-up (see wal_retention.go).
	walRetentionState

	// liveKeys is the approximate number of live (non-tombstoned) keys in the
	// engine. See LiveKeys for the accuracy contract and how it is persisted.
	// May go negative transiently; LiveKeys clamps on read.
	liveKeys atomic.Int64

	// walAppends counts fsync'd WAL appends since this engine was opened. One
	// append per successful Put/Delete (including snapshot-restore writes);
	// WAL replay is not an append and is not counted.
	walAppends atomic.Uint64

	// l0Count mirrors len(l.l0) as an atomic so maybeStallWrite can read it
	// without taking the mutex. It is always updated under mu before Unlock.
	l0Count atomic.Int32

	// closeOnce makes Close idempotent; see Close.
	closeOnce sync.Once

	dataDir         string
	logger          *slog.Logger
	maxMem          int64
	nCompact        int              // L0 file count threshold for compaction
	l0SlowThreshold int              // soft-stall threshold
	l0StopThreshold int              // hard-stop threshold
	maxStallWait    time.Duration    // per-write stall budget before ErrWriteStalled
	metrics         *metrics.Metrics // may be nil
	cache           *BlockCache      // may be nil (disabled when maxBytes=0)

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

// WithBlockCacheBytes configures the in-process LRU block cache capacity.
// The cache holds raw SSTable block bytes so that repeated reads of the same
// block skip the f.ReadAt syscall. The capacity is divided evenly across
// numCacheShards shards.
//
// Pass 0 (or omit this option) to disable caching entirely.
// The default when this option is absent is 64 MB.
func WithBlockCacheBytes(n int64) Option {
	return func(l *LSMTree) { l.cache = NewBlockCache(n) }
}

// WithCompactThreshold overrides the L0 file count at which background
// compaction is triggered (default 4). Lower values keep read amplification
// down at the cost of more merging; higher values do the opposite.
//
// Tests use it to build a deliberate L0 backlog: setting it above the number of
// files they create keeps compaction from racing the build.
func WithCompactThreshold(n int) Option {
	return func(l *LSMTree) {
		if n > 0 {
			l.nCompact = n
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
		manifest:        manifest,
		dataDir:         dataDir,
		logger:          logger,
		maxMem:          defaultMaxMemBytes,
		nCompact:        defaultCompactN,
		l0SlowThreshold: defaultL0SlowThreshold,
		l0StopThreshold: defaultL0StopThreshold,
		maxStallWait:    defaultMaxStallWait,
		cache:           NewBlockCache(defaultBlockCacheBytes),
		flushCh:         make(chan struct{}, 1),
		compactCh:       make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.compact = NewCompactor(dataDir, manifest, logger, l.nCompact, func() uint64 {
		return l.nextSST.Add(1)
	})
	l.compact.metrics = l.metrics
	l.immFlushed = sync.NewCond(&l.mu)
	l.l0DrainedCh = make(chan struct{})

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
		r.cache = l.cache
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

	// Find the live WAL segments recovery must replay.
	walFiles, err := findWALFiles(dataDir)
	if err != nil {
		return nil, fmt.Errorf("lsm: scan WAL files: %w", err)
	}

	// Seed the sequence number from every segment this node has on disk —
	// including the ones parked under retainedWALDir for replica catch-up, which
	// findWALFiles deliberately excludes because they must not be replayed.
	//
	// Excluding them here too is what made segment numbers repeat. A clean
	// shutdown flushes the active memtable and releases its segment, so a store
	// that closes gracefully can be left with no live segment at all; seeding from
	// the live set alone then restarts numbering at 1 while wal-retained/ already
	// holds a segment 1. Positions are (segment, offset) pairs, so every replica
	// cursor recorded before the restart silently addresses the *new* log:
	// wal.ErrCursorStale is keyed on the segment number and cannot fire, the old
	// parked segment is shadowed by the new one of the same number (ListSegments
	// prefers the live directory), and a pass reads from a byte offset that is past
	// the end of a different log — which is a clean stop with no error, so the pass
	// ships nothing and the engine concludes the replica is caught up. That is a
	// silently wrong convergence claim, and it is the same failure the README
	// documents for snapshot restore, reached by an ordinary restart.
	//
	// Monotonic numbering removes the ambiguity outright: a cursor into a released
	// segment now names a segment number that no longer exists, which is exactly
	// the condition ErrCursorStale reports and the anti-entropy engine already
	// handles by withholding the convergence claim.
	maxWalSeq, err := highestWALSeq(dataDir)
	if err != nil {
		return nil, fmt.Errorf("lsm: scan WAL segments: %w", err)
	}
	l.walSeq.Store(maxWalSeq)

	// Seed the write-sequence counter above every entry already on disk.
	//
	// Sequence numbers are the total write order compaction uses to resolve two
	// versions of a key — higher wins, and the loser is dropped rather than
	// shadowed. The counter is in memory, so left at zero on open it makes every
	// write in this process rank *below* the data it replaces: the first
	// compaction to merge a pre-restart file with a post-restart one keeps the
	// pre-restart value, and a tombstone that loses is discarded at the bottom
	// level, so a deleted key comes back. Neither is visible until that
	// compaction runs, and by then the acknowledged write is gone from disk.
	//
	// This must happen before the active memtable is created, because WAL replay
	// draws from the same counter and its entries are newer than anything the
	// SSTables hold.
	if err := l.seedSeqNum(); err != nil {
		return nil, err
	}

	// Replay existing WAL files into a fresh Memtable.
	//
	// Live-key count recovery: seed from the count the last flush recorded in
	// the manifest (which describes exactly the SSTable set on disk), then let
	// each replayed WAL apply the deltas for writes that never reached an
	// SSTable. Manifests written before the field existed report no count, in
	// which case the WAL deltas alone are the best available estimate.
	if seeded, ok := manifest.LastLiveKeys(); ok {
		l.liveKeys.Store(seeded)
	}

	activeWalSeq := l.walSeq.Add(1)
	activeWalPath := filepath.Join(dataDir, storewal.SegmentName(activeWalSeq))
	activeWAL, err := storewal.Open(activeWalPath)
	if err != nil {
		return nil, fmt.Errorf("lsm: open active WAL: %w", err)
	}
	l.mem = NewMemtable(activeWAL, activeWalPath, &l.seqNum, l.maxMem)
	l.activeWalSeq.Store(activeWalSeq)

	for _, wf := range walFiles {
		w, err := storewal.Open(wf)
		if err != nil {
			return nil, fmt.Errorf("lsm: open WAL %q for replay: %w", wf, err)
		}
		delta, err := l.mem.ReplayWAL(w)
		if err != nil {
			w.Close()
			return nil, fmt.Errorf("lsm: replay WAL %q: %w", wf, err)
		}
		l.liveKeys.Add(delta)
		w.Close()
	}

	// Start background goroutines.
	l.wg.Add(2)
	go l.runFlush(context.Background())
	go l.runCompact(context.Background())

	// Arm compaction if the L0 set restored from the manifest already needs
	// merging. Nothing else will do it: compaction is otherwise only ever
	// signalled by a memtable flush or by a compaction that left work behind,
	// and write-stall backpressure keys off this same restored L0 count. A
	// store closed with L0 at or above l0StopThreshold would therefore reopen
	// with every write parked in hard stop, waiting for a compaction that
	// cannot be triggered until a write gets through — writes stall, so no
	// memtable fills, so nothing flushes, so nothing signals compaction. That
	// deadlock is what made a node inherited from a busy data volume serve
	// almost no writes indefinitely while reporting healthy.
	if l.compact.ShouldCompact(len(l.l0)) {
		select {
		case l.compactCh <- struct{}{}:
		default:
		}
		logger.Info("lsm: armed compaction at open", "l0_sstables", len(l.l0),
			"compact_threshold", l.nCompact)
	}

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
	// Snapshot all state and acquire SSTableReader references atomically under
	// RLock.  Acquiring refs before releasing the lock guarantees that
	// runCompact cannot call Release() and close the underlying file descriptor
	// while we are still mid-ReadAt.  See SSTableReader.Release for details.
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	l0 := l.l0
	l1 := l.l1
	for _, r := range l0 {
		r.refs.Add(1)
	}
	for _, r := range l1 {
		r.refs.Add(1)
	}
	l.mu.RUnlock()
	defer func() {
		for _, r := range l0 {
			r.Release()
		}
		for _, r := range l1 {
			r.Release()
		}
	}()

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
	delta, err := l.mem.Put(key, value)
	if err != nil {
		return err
	}
	l.recordWrite(delta)
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
//
// Deletes are blind tombstone writes: the engine does not check whether the key
// exists first. Doing so would require a full read-path traversal (memtable →
// immutable memtable → every L0 file → L1) per delete, and doing it atomically
// would mean holding the write lock across those disk reads. Callers that need
// existence semantics must Get first and accept the race.
func (l *LSMTree) deleteInternal(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delta, err := l.mem.Delete(key)
	if err != nil {
		return err
	}
	l.recordWrite(delta)
	if l.mem.IsFull() {
		return l.rotateMemtable()
	}
	return nil
}

// recordWrite folds one successful memtable write into the engine-wide
// counters: a fsync'd WAL append, plus that write's live-key delta.
//
// The caller holds l.mu for the memtable write itself, not for these counters —
// they are atomic so that Get-path callers and the HTTP handlers can read them
// without taking the lock.
func (l *LSMTree) recordWrite(liveDelta int) {
	l.walAppends.Add(1)
	if l.metrics != nil {
		l.metrics.WALWrites.Add(1)
	}
	if liveDelta != 0 {
		l.liveKeys.Add(int64(liveDelta))
	}
}

// LiveKeys returns the approximate number of live (non-tombstoned) keys.
//
// Accuracy contract — the count is exact for a workload of distinct keys, and
// drifts in exactly two cases, both of which need a read to classify:
//
//   - Overwriting a key that is no longer in the active memtable (it has been
//     flushed to an SSTable) counts as a new key: the count drifts UP.
//   - Deleting a key that does not exist, or is not in the active memtable,
//     counts as removing a live key: the count drifts DOWN.
//
// Classifying either case correctly would require a full read-path traversal
// on the write hot path, which costs more than the field is worth. The value is
// therefore published as approximate (see /status key_count) rather than made
// exact.
//
// Durability: each memtable flush records the count in the manifest, and any
// WAL still on disk at open replays its deltas on top, so the count survives
// close/reopen and crash recovery without a startup scan of the SSTables. Data
// directories written before the manifest field existed start from their WAL
// contents alone.
//
// The internal counter is signed and may go negative under a delete-heavy
// approximation; the returned value is clamped at zero.
func (l *LSMTree) LiveKeys() int64 {
	if n := l.liveKeys.Load(); n > 0 {
		return n
	}
	return 0
}

// WALAppends returns the number of fsync'd WAL appends since this engine was
// opened — one per successful Put/Delete. WAL replay is not counted.
func (l *LSMTree) WALAppends() uint64 { return l.walAppends.Load() }

// maybeStallWrite applies write-stall backpressure when L0 is filling up.
// Must be called WITHOUT l.mu held.
//
//   - No stall:                  l0Count < l0SlowThreshold
//   - Soft stall (sleep+retry):  l0SlowThreshold ≤ l0Count < l0StopThreshold
//   - Hard stop (wait to drain): l0Count ≥ l0StopThreshold
//
// The two branches bound differently, on purpose. Soft stall is a throttle: the
// write is going to be accepted, just later, so it waits as long as its caller
// is willing to (ctx) and no longer. Hard stop means the engine is refusing
// writes, so it also gives up after maxStallWait with ErrWriteStalled rather
// than blocking indefinitely — an unbounded wait makes an overloaded node
// indistinguishable from one that is down, which is exactly how a replica ends
// up eating its primary's whole replication deadline and being reported as
// unreachable.
//
// Each stall cycle increments metrics.WriteStallCount and adds elapsed
// microseconds to metrics.WriteStallMicros.
func (l *LSMTree) maybeStallWrite(ctx context.Context) error {
	var stalledFor time.Duration

	for {
		// Take the drain channel BEFORE reading the count. Both are touched
		// under l.mu by the compaction that drains L0, so this ordering cannot
		// miss a wake-up: either we read the channel the drainer is about to
		// close, or we read its replacement — in which case the count we then
		// load is already the post-drain value.
		l.mu.RLock()
		drained := l.l0DrainedCh
		l.mu.RUnlock()

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
			remaining := l.maxStallWait - stalledFor
			if remaining <= 0 {
				return fmt.Errorf("%w: %d L0 SSTables (hard stop at %d) did not drain within %v",
					ErrWriteStalled, n, l.l0StopThreshold, l.maxStallWait)
			}
			timer := time.NewTimer(remaining)
			select {
			case <-drained:
			case <-ctx.Done():
				timer.Stop()
				l.recordStall(start)
				return ctx.Err()
			case <-timer.C:
			}
			timer.Stop()
		} else {
			// Soft stall: proportional sleep, max 50 ms.
			overage := int(n) - l.l0SlowThreshold + 1
			delay := time.Duration(overage) * 5 * time.Millisecond
			if delay > 50*time.Millisecond {
				delay = 50 * time.Millisecond
			}
			select {
			case <-ctx.Done():
				l.recordStall(start)
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		waited := time.Since(start)
		stalledFor += waited
		l.recordStall(start)
		// Re-check after waking — L0 may still be above the slow threshold.
	}
}

// recordStall folds one stall cycle that began at start into the stall metrics.
func (l *LSMTree) recordStall(start time.Time) {
	if l.metrics == nil {
		return
	}
	l.metrics.WriteStallCount.Add(1)
	l.metrics.WriteStallMicros.Add(uint64(time.Since(start).Microseconds()))
}

// signalL0DrainedLocked wakes every writer parked in hard stop, once L0 has
// fallen back below the hard-stop threshold. Closing the current channel is the
// broadcast; a fresh one takes its place for the next round.
//
// Caller must hold l.mu (write lock) and must have already published the new
// l0Count.
func (l *LSMTree) signalL0DrainedLocked() {
	if int(l.l0Count.Load()) >= l.l0StopThreshold {
		return // still stopped; nothing to wake for
	}
	close(l.l0DrainedCh)
	l.l0DrainedCh = make(chan struct{})
}

// rotateMemtable promotes l.mem to l.imm and starts a fresh active memtable.
// Caller must hold l.mu write lock.
func (l *LSMTree) rotateMemtable() error {
	// Wait for any previous flush to complete.
	for l.imm != nil {
		l.immFlushed.Wait()
	}

	seq := l.walSeq.Add(1)
	walPath := filepath.Join(l.dataDir, storewal.SegmentName(seq))
	newWAL, err := storewal.Open(walPath)
	if err != nil {
		return fmt.Errorf("lsm: open new WAL: %w", err)
	}

	l.imm = l.mem
	// Seal the live-key count for the memtable leaving the write path. Its
	// flush will persist this value to the manifest; every write from here on
	// lands in the new WAL and is re-applied by replay at open.
	l.imm.liveKeysAtSeal = l.liveKeys.Load()
	l.mem = NewMemtable(newWAL, walPath, &l.seqNum, l.maxMem)
	// Published with the new memtable, under the same lock, so WALTip never
	// pairs one segment's number with another's offset.
	l.activeWalSeq.Store(seq)

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

	if err := l.manifest.AddWithLiveKeys(outName, sstSeq, 0 /*level=L0*/, imm.liveKeysAtSeal, writer.MaxSeqNum()); err != nil {
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
	newReader.cache = l.cache

	// Atomically: prepend to l0, update l0Count, clear imm, wake writers.
	l.mu.Lock()
	l.l0 = append([]*SSTableReader{newReader}, l.l0...)
	l0Depth := len(l.l0)
	l.l0Count.Store(int32(len(l.l0)))
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(int64(len(l.l0)))
	}
	l.imm = nil
	l.immFlushed.Broadcast()
	l.mu.Unlock()

	// The segment is either deleted or parked for a replica that still needs to
	// be caught up from it — see releaseWALSegment.
	l.releaseWALSegment(imm.walPath)

	// l0Depth is captured under the lock above: reading len(l.l0) here would
	// race with runCompact clearing l.l0, and would also be off by one because
	// l.l0 already includes the reader appended above.
	l.logger.Info("lsm: memtable flushed to L0", "sst", outName,
		"size", imm.SizeBytes(), "l0_depth", l0Depth)

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

			// Compact the L0 files snapshotted above plus all L1 into a single
			// new L1 SSTable. Files flushed after this snapshot are retained by
			// installCompactionResult rather than merged.
			// Pass them oldest-first: l1 reversed + l0 reversed.
			allReaders := compactInputOrder(l0snap, l1snap)

			newReader, err := l.compact.Compact(ctx, allReaders)
			if err != nil {
				l.logger.Error("lsm: L0→L1 compaction failed", "err", err)
				continue
			}

			if newReader != nil {
				newReader.Level = 1
				newReader.metrics = l.metrics
				newReader.cache = l.cache
			}

			// Replace l0 and l1 atomically, decrement l0Count, wake stalled writers.
			l.installCompactionResult(l0snap, l1snap, newReader)

			// Proactively evict cache entries for the now-deleted input SSTables.
			for _, r := range l0snap {
				l.cache.Evict(r.path)
			}
			for _, r := range l1snap {
				l.cache.Evict(r.path)
			}

			// Release the ownership reference for each old reader. After the
			// swap above, no new Get/Snapshot can snapshot these readers.
			// Any concurrent Get that acquired a reference before the swap
			// still holds it and will call Release when it finishes, safely
			// closing the fd only after the last reference is gone.
			for _, r := range l0snap {
				r.Release()
			}
			for _, r := range l1snap {
				r.Release()
			}

			l.logger.Info("lsm: L0→L1 compaction complete",
				"l0_inputs", len(l0snap),
				"l1_inputs", len(l1snap),
			)
		}
	}
}

// installCompactionResult publishes a finished compaction into the level state:
// the merged output becomes L1, and the merged inputs leave L0.
//
// l0Inputs and l1Inputs are the reader sets the compaction actually merged,
// snapshotted before the merge ran without the lock. l.l0 may have grown in the
// meantime: runFlush prepends every newly flushed SSTable, and a flush that
// lands while the merge is in flight produces a table whose entries are NOT in
// newReader. Such a table must be retained — dropping it makes every key whose
// newest version lives there unreadable for the rest of the process lifetime,
// even though the file is still on disk and still live in the manifest. Only
// readers that were merge inputs are removed from L0.
//
// Retained tables stay ahead of the compaction output in the read path, which is
// the correct precedence: they are newer than everything the merge consumed, and
// Get checks all of L0 before any of L1.
func (l *LSMTree) installCompactionResult(l0Inputs, l1Inputs []*SSTableReader, newReader *SSTableReader) {
	merged := make(map[*SSTableReader]struct{}, len(l0Inputs)+len(l1Inputs))
	for _, r := range l0Inputs {
		merged[r] = struct{}{}
	}
	for _, r := range l1Inputs {
		merged[r] = struct{}{}
	}

	l.mu.Lock()
	retained := make([]*SSTableReader, 0, len(l.l0))
	for _, r := range l.l0 { // newest-first; filtering preserves that order
		if _, wasInput := merged[r]; !wasInput {
			retained = append(retained, r)
		}
	}
	l.l0 = retained
	if newReader != nil {
		l.l1 = []*SSTableReader{newReader}
	} else {
		l.l1 = nil
	}
	l.l0Count.Store(int32(len(l.l0)))
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(int64(len(l.l0)))
	}
	l.signalL0DrainedLocked() // wake any hard-stopped writers
	needMore := l.compact.ShouldCompact(len(l.l0))
	l.mu.Unlock()

	if len(retained) > 0 {
		l.logger.Info("lsm: retained L0 SSTables flushed during compaction",
			"retained", len(retained))
	}

	// Tables retained above were never merged, so re-arm compaction rather than
	// waiting for the next flush to signal it.
	if needMore {
		select {
		case l.compactCh <- struct{}{}:
		default:
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
	// Acquire SSTableReader references before releasing the lock for the same
	// reason as Get: prevents runCompact from closing fds while we iterate.
	l.mu.RLock()
	mem := l.mem
	imm := l.imm
	l0 := l.l0
	l1 := l.l1
	for _, r := range l0 {
		r.refs.Add(1)
	}
	for _, r := range l1 {
		r.refs.Add(1)
	}
	l.mu.RUnlock()
	defer func() {
		for _, r := range l0 {
			r.Release()
		}
		for _, r := range l1 {
			r.Release()
		}
	}()

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

// Restore replaces the entire store with the contents of data (from a Raft
// snapshot).
//
// The payload is written as a single L0 SSTable rather than replayed through the
// write path. The write path fsyncs the WAL once per key, which at snapshot
// scale (200k keys) is 200k fsyncs and minutes of total write unavailability on
// a container volume — for data that is already durable in the snapshot file the
// caller restored it from, so the WAL adds nothing. The bulk path pays one
// SSTable write plus one manifest rewrite regardless of key count.
//
// Crash atomicity is unchanged and comes from two mechanisms the engine already
// uses. The restore-in-progress sentinel is written before anything is touched
// and removed only on success, so an open that finds it wipes the directory
// rather than exposing a half-restored store (see NewLSMTree). Within the bulk
// load, the manifest add is the commit point: an SSTable that was written but
// never recorded is invisible to every reader, exactly as for a memtable flush
// that dies before its manifest add.
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
	// Fresh drain channel with the rest of the level state. No writer can be
	// parked on the old one (the goroutines are stopped and a restore is not
	// concurrent with serving), so this is defensive rather than load-bearing —
	// but leaving a stale channel behind is the kind of thing that stops being
	// harmless the moment restore gains a concurrent caller.
	l.l0DrainedCh = make(chan struct{})
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
	// The store is now empty; bulkLoadL0 sets the count from the payload it
	// writes, every entry of which is live (Snapshot omits tombstones).
	l.liveKeys.Store(0)
	// walAppends is deliberately NOT reset: it counts fsync'd WAL appends made
	// by this process, and is a process-lifetime IO counter rather than a
	// property of the logical store. The bulk load performs no WAL appends, so
	// a restore leaves it where it was.

	activeWalSeq := l.walSeq.Add(1)
	activeWalPath := filepath.Join(l.dataDir, storewal.SegmentName(activeWalSeq))
	activeWAL, err := storewal.Open(activeWalPath)
	if err != nil {
		return fmt.Errorf("lsm: open restore WAL: %w", err)
	}

	l.mu.Lock()
	l.mem = NewMemtable(activeWAL, activeWalPath, &l.seqNum, l.maxMem)
	l.activeWalSeq.Store(activeWalSeq)
	l.mu.Unlock()

	// Write the payload straight to L0. Nothing is served from the memtable or
	// the WAL after a restore, so no background goroutine is needed for this.
	if err := l.bulkLoadL0(data); err != nil {
		return err
	}

	// Restart background goroutines now that the restored state is published.
	l.stopCh = make(chan struct{})
	l.flushCh = make(chan struct{}, 1)
	l.compactCh = make(chan struct{}, 1)
	l.wg.Add(2)
	go l.runFlush(ctx)
	go l.runCompact(ctx)

	if err := os.Remove(sentinelPath); err != nil && !os.IsNotExist(err) {
		l.logger.Warn("lsm: remove restore sentinel", "err", err)
	}

	l.logger.Info("lsm: snapshot restored", "keys", len(data))
	return nil
}

// bulkLoadL0 writes data as one L0 SSTable and publishes it, without touching
// the WAL or the memtable. Used by Restore; see its doc comment for why the
// write path is bypassed and where the commit point is.
//
// A single output file (rather than one per memtable-sized chunk) keeps the
// restored store below every compaction and write-stall threshold, so a node
// that has just restored is immediately writable.
func (l *LSMTree) bulkLoadL0(data map[string][]byte) error {
	if len(data) == 0 {
		return nil
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys) // SSTableWriter requires strictly ascending keys

	sstSeq := l.nextSST.Add(1)
	outName := fmt.Sprintf("sst-%08d.sst", sstSeq)
	outPath := filepath.Join(l.dataDir, outName)

	writer, err := NewSSTableWriter(outPath, len(keys))
	if err != nil {
		return fmt.Errorf("lsm: restore create writer: %w", err)
	}
	for _, k := range keys {
		e := Entry{Key: k, Value: data[k], SeqNum: l.seqNum.Add(1)}
		if err := writer.Write(e); err != nil {
			writer.Close()
			os.Remove(outPath)
			return fmt.Errorf("lsm: restore write %q: %w", k, err)
		}
	}
	if err := writer.Close(); err != nil {
		os.Remove(outPath)
		return fmt.Errorf("lsm: restore close writer: %w", err)
	}

	// Commit point: before this the file is invisible, after it the restored
	// state is the store. The live-key count goes in the same record so a
	// restart straight after a restore recovers it without a scan.
	if err := l.manifest.AddWithLiveKeys(outName, sstSeq, 0 /*level=L0*/, int64(len(keys)), writer.MaxSeqNum()); err != nil {
		os.Remove(outPath)
		return fmt.Errorf("lsm: restore manifest add: %w", err)
	}

	reader, err := OpenSSTableReader(outPath, sstSeq)
	if err != nil {
		return fmt.Errorf("lsm: restore open reader: %w", err)
	}
	reader.Level = 0
	reader.metrics = l.metrics
	reader.cache = l.cache

	l.mu.Lock()
	l.l0 = []*SSTableReader{reader}
	l.l0Count.Store(1)
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(1)
	}
	l.liveKeys.Store(int64(len(keys)))
	l.mu.Unlock()

	l.logger.Info("lsm: restore bulk-loaded to L0", "sst", outName, "keys", len(keys))
	return nil
}

// Close flushes any pending memtable, stops background goroutines, and closes
// all file handles.
//
// It is idempotent. A second call used to panic on `close` of an already-closed
// channel, which made "close it twice" a crash rather than a no-op — an easy
// mistake for any caller with more than one shutdown path, and one that turns an
// orderly stop into a lost flush.
func (l *LSMTree) Close() error {
	var err error
	l.closeOnce.Do(func() { err = l.doClose() })
	return err
}

func (l *LSMTree) doClose() error {
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
	// Close is the one flush of a memtable that was never sealed by a rotation,
	// so the count to persist is simply the current one: every write is either
	// already in an SSTable or in the memtable being flushed here, and this
	// memtable's WAL is removed below.
	if err := l.manifest.AddWithLiveKeys(outName, sstSeq, 0 /*L0*/, l.liveKeys.Load(), writer.MaxSeqNum()); err != nil {
		os.Remove(outPath)
		return err
	}

	r, err := OpenSSTableReader(outPath, sstSeq)
	if err != nil {
		return err
	}
	r.Level = 0
	r.metrics = l.metrics
	r.cache = l.cache

	l.mu.Lock()
	l.l0 = append([]*SSTableReader{r}, l.l0...)
	l.l0Count.Store(int32(len(l.l0)))
	if l.metrics != nil {
		l.metrics.L0FileCount.Store(int64(len(l.l0)))
	}
	l.mu.Unlock()

	l.releaseWALSegment(mem.walPath)
	return nil
}

// ---- Helpers ---------------------------------------------------------------

// seedSeqNum sets the write-sequence counter above every entry on disk, so that
// writes made after a restart outrank the data they replace. See the call site in
// NewLSMTree for what an unseeded counter costs.
//
// The manifest records each SSTable's highest sequence number, which makes the
// common case free. A manifest written before that field existed cannot answer,
// and there is no cheap upper bound to substitute — the numbers live in the
// entries themselves — so those files are scanned once. It is a one-time cost per
// data directory: every file this engine writes records the field, and the first
// compaction replaces the whole live set with one file that has it.
func (l *LSMTree) seedSeqNum() error {
	recorded, complete := l.manifest.MaxSeqNum()
	if complete {
		l.seqNum.Store(recorded)
		return nil
	}

	start := time.Now()
	scanned, entries, err := l.scanMaxSeqNum()
	if err != nil {
		return err
	}
	if scanned < recorded {
		scanned = recorded
	}
	l.seqNum.Store(scanned)
	l.logger.Warn("lsm: manifest predates per-SSTable sequence numbers; scanned to recover the write order",
		"max_seq_num", scanned,
		"entries_scanned", entries,
		"took", time.Since(start).String())
	return nil
}

// scanMaxSeqNum reads every live SSTable and returns the highest sequence number
// it finds, together with the number of entries read.
//
// The lock is defensive rather than load-bearing: the only caller runs during
// NewLSMTree, before the flush and compaction goroutines exist, so nothing can
// swap the reader set underneath it yet. It is taken anyway so this stays correct
// if it is ever called from anywhere else.
func (l *LSMTree) scanMaxSeqNum() (uint64, int, error) {
	l.mu.RLock()
	readers := make([]*SSTableReader, 0, len(l.l0)+len(l.l1))
	readers = append(readers, l.l0...)
	readers = append(readers, l.l1...)
	l.mu.RUnlock()

	var max uint64
	entries := 0
	for _, r := range readers {
		it := r.Iterator()
		for {
			e, ok := it.Next()
			if !ok {
				break
			}
			entries++
			if e.SeqNum > max {
				max = e.SeqNum
			}
		}
		// The iterator stops on error the same way it stops at EOF, so a read
		// failure would otherwise read as "this file holds no sequence numbers"
		// — which would seed the counter too low and reintroduce the inversion
		// this scan exists to prevent.
		if err := it.Err(); err != nil {
			return 0, entries, fmt.Errorf("lsm: scan %q for write order: %w", r.path, err)
		}
	}
	return max, entries, nil
}

// findWALFiles returns the live WAL segments in dataDir — the ones recovery must
// replay — sorted by ascending sequence number.
//
// Segments parked for replica catch-up live in a subdirectory and are
// deliberately not returned: they have already been flushed into an SSTable, so
// replaying them would re-apply committed writes. Their sequence numbers are
// still reserved, though, which is what highestWALSeq is for — this function must
// not be used to decide the next segment number.
//
// The filename format is owned by the wal package (wal.SegmentName /
// wal.ParseSegmentSeq); this function must not restate it.
func findWALFiles(dataDir string) (paths []string, err error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("lsm: read dir: %w", err)
	}

	type walFile struct {
		path string
		seq  uint64
	}
	var wals []walFile

	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		seq, ok := storewal.ParseSegmentSeq(de.Name())
		if !ok {
			continue
		}
		wals = append(wals, walFile{
			path: filepath.Join(dataDir, de.Name()),
			seq:  seq,
		})
	}

	sort.Slice(wals, func(i, j int) bool { return wals[i].seq < wals[j].seq })
	for _, w := range wals {
		paths = append(paths, w.path)
	}
	return paths, nil
}

func wipeLSMDir(dataDir string) error {
	// Parked WAL segments describe writes in the log being replaced, so a wipe
	// must take them with it: a cursor into them would address entries that no
	// longer belong to this store.
	if err := purgeRetainedWALSegments(dataDir); err != nil {
		return err
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("lsm: read dir for wipe: %w", err)
	}
	for _, de := range entries {
		name := de.Name()
		_, isWAL := storewal.ParseSegmentSeq(name)
		if isWAL ||
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
