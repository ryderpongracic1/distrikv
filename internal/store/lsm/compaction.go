package lsm

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// ---- Min-heap for k-way merge ----------------------------------------------

type heapItem struct {
	entry   Entry
	iter    *SSTableIterator
	iterIdx int // tiebreaker: lower idx = older SSTable
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	a, b := h[i].entry, h[j].entry
	if a.Key != b.Key {
		return a.Key < b.Key
	}
	if a.SeqNum != b.SeqNum {
		return a.SeqNum > b.SeqNum // higher SeqNum = newer = "smaller" in heap
	}
	return h[i].iterIdx > h[j].iterIdx // newer SSTable (higher idx) wins
}
func (h *mergeHeap) Push(x any) { *h = append(*h, x.(heapItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// MergeIterator performs a k-way merge over multiple SSTableIterators,
// deduplicating by key (highest SeqNum wins) and dropping tombstones.
type MergeIterator struct {
	h       mergeHeap
	prevKey string
	started bool
}

// newMergeIterator seeds the heap with the first entry from each iterator.
// readers are ordered oldest-first (index 0 = oldest); newer SSTables have
// higher iterIdx, which the heap uses as a tiebreaker.
func newMergeIterator(iters []*SSTableIterator) (*MergeIterator, error) {
	m := &MergeIterator{}
	heap.Init(&m.h)
	for i, it := range iters {
		e, ok := it.Next()
		if it.Err() != nil {
			return nil, it.Err()
		}
		if ok {
			heap.Push(&m.h, heapItem{entry: e, iter: it, iterIdx: i})
		}
	}
	return m, nil
}

// Next returns the next non-duplicate, non-tombstone entry in merged order.
// Returns (_, false) when exhausted.
func (m *MergeIterator) Next() (Entry, bool) {
	for m.h.Len() > 0 {
		item := heap.Pop(&m.h).(heapItem)
		e := item.entry

		// Advance this iterator and re-push its next entry.
		if next, ok := item.iter.Next(); ok {
			heap.Push(&m.h, heapItem{entry: next, iter: item.iter, iterIdx: item.iterIdx})
		}

		// Skip duplicate keys (lower SeqNum = older version, already seen).
		if m.started && e.Key == m.prevKey {
			continue
		}
		m.started = true
		m.prevKey = e.Key

		// Drop tombstones — we treat every compaction as bottom-level.
		// This is safe because we always compact ALL SSTables together.
		if e.Tombstone {
			continue
		}
		return e, true
	}
	return Entry{}, false
}

// ---- Compactor -------------------------------------------------------------

// Compactor merges a set of SSTables into one, updating the manifest.
type Compactor struct {
	dataDir    string
	manifest   *Manifest
	logger     *slog.Logger
	threshold  int            // compact when len(readers) >= threshold
	nextSSTSeq func() uint64 // callback into LSMTree.nextSST.Add(1)
}

// NewCompactor creates a Compactor.
func NewCompactor(dataDir string, manifest *Manifest, logger *slog.Logger, threshold int, nextSSTSeq func() uint64) *Compactor {
	return &Compactor{
		dataDir:    dataDir,
		manifest:   manifest,
		logger:     logger,
		threshold:  threshold,
		nextSSTSeq: nextSSTSeq,
	}
}

// ShouldCompact reports whether compaction is warranted.
func (c *Compactor) ShouldCompact(numSSTables int) bool {
	return numSSTables >= c.threshold
}

// Compact merges readers (oldest-first) into a single new SSTable.
// On success the old files are removed from the manifest and deleted from disk.
// Returns the new SSTableReader, or nil if the compacted output is empty.
func (c *Compactor) Compact(ctx context.Context, readers []*SSTableReader) (*SSTableReader, error) {
	if len(readers) == 0 {
		return nil, nil
	}

	// Open iterators (oldest-first order).
	iters := make([]*SSTableIterator, len(readers))
	for i, r := range readers {
		iters[i] = r.Iterator()
	}

	merger, err := newMergeIterator(iters)
	if err != nil {
		return nil, fmt.Errorf("compaction: create merge iterator: %w", err)
	}

	// Estimate expected keys for the Bloom filter.
	expectedKeys := 0
	for _, r := range readers {
		expectedKeys += len(r.index) * (blockTargetSize / 64) // rough estimate
	}
	if expectedKeys < 1 {
		expectedKeys = 1024
	}

	// Write merged output.
	sstSeq := c.nextSSTSeq()
	outName := fmt.Sprintf("sst-%08d.sst", sstSeq)
	outPath := filepath.Join(c.dataDir, outName)

	writer, err := NewSSTableWriter(outPath, expectedKeys)
	if err != nil {
		return nil, fmt.Errorf("compaction: open writer: %w", err)
	}

	wrote := 0
	for {
		e, ok := merger.Next()
		if !ok {
			break
		}
		if err := writer.Write(e); err != nil {
			writer.Close()
			os.Remove(outPath)
			return nil, fmt.Errorf("compaction: write entry: %w", err)
		}
		wrote++
	}

	if err := writer.Close(); err != nil {
		os.Remove(outPath)
		return nil, fmt.Errorf("compaction: close writer: %w", err)
	}

	// If nothing was written (all entries were tombstones), remove the empty file.
	if wrote == 0 {
		os.Remove(outPath)
		c.logger.Info("compaction produced empty output; no new SSTable")
	} else {
		if err := c.manifest.Add(outName, sstSeq); err != nil {
			os.Remove(outPath)
			return nil, fmt.Errorf("compaction: manifest add: %w", err)
		}
	}

	// Remove old SSTables from manifest and disk.
	for _, r := range readers {
		baseName := filepath.Base(r.path)
		if err := c.manifest.Remove(baseName); err != nil {
			c.logger.Error("compaction: manifest remove failed", "file", baseName, "err", err)
		}
		if err := r.Close(); err != nil {
			c.logger.Warn("compaction: close old reader", "err", err)
		}
		if err := os.Remove(r.path); err != nil && !os.IsNotExist(err) {
			c.logger.Warn("compaction: remove old SSTable", "path", r.path, "err", err)
		}
	}

	c.logger.Info("compaction complete",
		"inputs", len(readers),
		"output_entries", wrote,
		"output_sst", outName,
	)

	if wrote == 0 {
		return nil, nil
	}

	return OpenSSTableReader(outPath, sstSeq)
}
