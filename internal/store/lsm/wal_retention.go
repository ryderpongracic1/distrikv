package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// retainedWALDir is the subdirectory a flushed-but-still-referenced WAL segment
// is moved into.
//
// Parking rather than keeping the segment in place is what makes retention safe:
// recovery replays every wal-NNNN.log it finds in the data directory, and a
// flushed segment left there would be replayed on the next open, re-applying
// writes that are already in an SSTable and double-counting them in the live-key
// estimate the manifest carries. Moving the file one directory down keeps it
// readable by the anti-entropy reader and invisible to recovery, with no new
// bookkeeping to keep in sync.
const retainedWALDir = "wal-retained"

// maxRetainedWALSegments bounds how many flushed segments may be parked at once.
//
// Retention exists so a replica that was briefly unreachable can be caught up
// from the log; it must not turn a replica that is gone for good into unbounded
// disk growth. Once the cap is reached the oldest parked segments are deleted
// and any cursor pointing into them goes stale — reported as such by
// wal.ErrCursorStale, which is the v1 bound on WAL-based recovery.
const maxRetainedWALSegments = 128

// WALTip returns the position one byte past the last entry the engine has
// durably appended: the point an anti-entropy pass reads up to.
//
// It is a snapshot, not a reservation — a concurrent write may advance the tip
// the instant it is returned, and the offset is read after the lock is dropped,
// so the value can already be slightly generous by the time the caller sees it.
// That direction is the safe one: callers use the tip as an upper bound, and a
// bound a few entries too high makes a pass ship a little more than it had to,
// while a bound too low would leave entries unshipped and the cursor claiming
// them.
func (l *LSMTree) WALTip() storewal.Position {
	l.mu.RLock()
	mem := l.mem
	// Read the active segment number inside the lock: rotateMemtable publishes
	// the new memtable and the new segment number together under it, so reading
	// them apart could pair a fresh memtable's offset with the previous
	// segment's number — a position that never existed.
	seq := l.activeWalSeq.Load()
	l.mu.RUnlock()
	if mem == nil {
		return storewal.Position{Segment: seq}
	}
	return storewal.Position{Segment: seq, Offset: mem.WALSize()}
}

// WALSegments returns every WAL segment readable by an anti-entropy pass —
// the live segments plus any that have been flushed but are still retained —
// sorted by ascending sequence number.
func (l *LSMTree) WALSegments() ([]storewal.Segment, error) {
	return storewal.ListSegments(l.dataDir, filepath.Join(l.dataDir, retainedWALDir))
}

// RetainWALFrom asks the engine to keep every WAL segment numbered at or above
// seq, even after the segment's memtable has been flushed. Passing 0 releases
// retention entirely.
//
// The caller is the anti-entropy engine: seq is the lowest segment any replica's
// cursor still points into. Retention is best-effort and bounded by
// maxRetainedWALSegments; when the bound is hit the oldest parked segments are
// dropped and the affected cursors go stale.
func (l *LSMTree) RetainWALFrom(seq uint64) { l.walRetainFrom.Store(seq) }

// WALRetentionFloor reports the current retention request set by RetainWALFrom.
func (l *LSMTree) WALRetentionFloor() uint64 { return l.walRetainFrom.Load() }

// highestWALSeq returns the largest segment sequence number this node has on
// disk, counting both the live segments and the ones parked for replica catch-up.
//
// It exists so segment numbers are never reused. A parked segment is not replayed
// at open (its contents are already in an SSTable), but its *number* is still
// spoken for: replica cursors are (segment, offset) pairs, so handing that number
// to a new segment makes every cursor recorded before the restart address a
// different log. See the seeding comment in NewLSMTree for the three concrete
// failures that follow.
//
// Zero means this node has no WAL segment at all, so the first segment it opens
// is 1.
func highestWALSeq(dataDir string) (uint64, error) {
	segs, err := storewal.ListSegments(dataDir, filepath.Join(dataDir, retainedWALDir))
	if err != nil {
		return 0, err
	}
	var max uint64
	for _, s := range segs {
		if s.Seq > max {
			max = s.Seq
		}
	}
	return max, nil
}

// releaseWALSegment disposes of the WAL segment belonging to a memtable that has
// just been flushed: either deleting it, or parking it under retainedWALDir when
// a replica cursor still needs it.
//
// Errors are logged rather than returned: the flush itself has already
// committed, and a segment that could not be cleaned up costs disk space and a
// stale cursor, not correctness.
func (l *LSMTree) releaseWALSegment(walPath string) {
	seq, ok := storewal.ParseSegmentSeq(walPath)
	floor := l.walRetainFrom.Load()

	if !ok || floor == 0 || seq < floor {
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			l.logger.Warn("lsm: remove flushed WAL", "path", walPath, "err", err)
		}
		return
	}

	dir := filepath.Join(l.dataDir, retainedWALDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		l.logger.Warn("lsm: create retained WAL dir; deleting segment instead",
			"dir", dir, "err", err)
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			l.logger.Warn("lsm: remove flushed WAL", "path", walPath, "err", err)
		}
		return
	}

	dest := filepath.Join(dir, storewal.SegmentName(seq))
	if err := os.Rename(walPath, dest); err != nil {
		l.logger.Warn("lsm: park flushed WAL for replica catch-up; deleting instead",
			"path", walPath, "err", err)
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			l.logger.Warn("lsm: remove flushed WAL", "path", walPath, "err", err)
		}
		return
	}
	l.logger.Info("lsm: parked flushed WAL for replica catch-up",
		"segment", seq, "retain_from", floor)

	l.trimRetainedWALSegments(dir)
}

// trimRetainedWALSegments enforces maxRetainedWALSegments, deleting the oldest
// parked segments first and warning loudly: dropping them is what makes a
// replica cursor go stale.
func (l *LSMTree) trimRetainedWALSegments(dir string) {
	parked, err := storewal.ListSegments(dir)
	if err != nil {
		l.logger.Warn("lsm: list retained WAL segments", "dir", dir, "err", err)
		return
	}
	excess := len(parked) - maxRetainedWALSegments
	for i := 0; i < excess; i++ {
		if err := os.Remove(parked[i].Path); err != nil && !os.IsNotExist(err) {
			l.logger.Warn("lsm: trim retained WAL segment", "path", parked[i].Path, "err", err)
			continue
		}
		l.logger.Warn("lsm: dropped retained WAL segment at retention cap; "+
			"a replica cursor pointing into it can no longer be resumed from the log",
			"segment", parked[i].Seq, "cap", maxRetainedWALSegments)
	}
}

// purgeRetainedWALSegments removes the parked-segment directory outright. Used by
// Restore, which replaces the entire logical store: a cursor into the pre-restore
// log addresses writes that no longer exist.
func purgeRetainedWALSegments(dataDir string) error {
	dir := filepath.Join(dataDir, retainedWALDir)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("lsm: purge retained WAL dir %q: %w", dir, err)
	}
	return nil
}

// walRetentionState is embedded in LSMTree; declared here so the retention
// fields live next to the code that uses them.
type walRetentionState struct {
	// activeWalSeq is the sequence number of the segment the active memtable is
	// appending to. Together with the memtable's WAL size it forms the tip.
	activeWalSeq atomic.Uint64

	// walRetainFrom is the lowest segment number that must survive its
	// memtable's flush; 0 means no retention.
	walRetainFrom atomic.Uint64
}
