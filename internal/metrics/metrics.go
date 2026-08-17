// Package metrics provides a lightweight, allocation-free metrics collector
// built on sync/atomic counters. All fields are safe for concurrent use
// without additional locking.
//
// Usage: allocate one *Metrics on the Node struct and pass it by pointer to
// subsystems that need to increment counters. Never use a global instance.
package metrics

import (
	"sync/atomic"
)

// Metrics holds observable counters for a single distrikv node.
// Every field is an atomic.Uint64 so any goroutine can increment it without
// acquiring a lock.
type Metrics struct {
	// Client-facing operation totals.
	PutTotal    atomic.Uint64
	GetTotal    atomic.Uint64
	DeleteTotal atomic.Uint64

	// GetMiss counts Get calls that returned ErrNotFound.
	GetMiss atomic.Uint64

	// WALWrites counts the number of fsync'd WAL appends. Maintained by the LSM
	// engine (one per successful Put/Delete); recovery replay is not counted.
	WALWrites atomic.Uint64

	// RaftTerms counts the number of term increments (candidate elections
	// started), giving a rough proxy for election churn.
	RaftTerms atomic.Uint64

	// LeaderElections counts the number of times this node became leader.
	LeaderElections atomic.Uint64

	// ForwardedRequests counts client requests this node forwarded to another
	// node because it was not the ring-primary for the requested key.
	ForwardedRequests atomic.Uint64

	// ReplicationErrors counts failures when replicating a write to a replica.
	ReplicationErrors atomic.Uint64

	// --- Anti-entropy (replica catch-up) ------------------------------------

	// AntiEntropyPasses counts completed catch-up passes over this node's WAL,
	// successful or not. One recovery of one replica normally costs two: the
	// pass that ships the missed writes and the pass that confirms there is
	// nothing left to ship.
	AntiEntropyPasses atomic.Uint64

	// AntiEntropyEntriesSent counts individual mutations replayed from the WAL
	// to a replica. It is the direct measure of how much divergence the
	// refused-but-applied writes actually left behind.
	AntiEntropyEntriesSent atomic.Uint64

	// AntiEntropyPassErrors counts passes that ended early because a replica
	// stopped accepting entries — the replica died mid-sync, or went away again.
	AntiEntropyPassErrors atomic.Uint64

	// AntiEntropyStaleCursors counts times a catch-up pass could not cover a
	// replica's gap from the log, because the segments it needed had already been
	// garbage-collected — either a recorded cursor pointed into one of them, or the
	// replica had no cursor at all and the log no longer starts at segment 1. A
	// non-zero value means some keys may stay divergent until they are written
	// again — the documented v1 bound.
	AntiEntropyStaleCursors atomic.Uint64

	// AntiEntropyFullSyncRequired is a 0/1 gauge: 1 means this node's WAL is not a
	// complete record of the data it holds, so anti-entropy cannot converge its
	// replicas from the log. Two causes set it — the store was replaced from a
	// snapshot whose payload was never appended to the log, or WAL retention
	// dropped segments a replica still needed. The node's logs carry which.
	//
	// It is a gauge rather than a counter because it describes a standing
	// condition, and it latches: v1 has no full-sync mechanism, so nothing
	// clears it. While it reads 1, treat any "replica caught up" signal from
	// this node as covering only the writes its log can still account for.
	AntiEntropyFullSyncRequired atomic.Uint64

	// --- LSM engine counters (Phase 1 benchmark harness) -------------------

	// BloomHits counts SSTable lookups where the Bloom filter returned true
	// (the key might be present).
	BloomHits atomic.Uint64

	// BloomMisses counts SSTable lookups where the Bloom filter returned false
	// (the key is definitely absent — the filter saved a block read).
	BloomMisses atomic.Uint64

	// BloomFalsePositives counts SSTable lookups where the Bloom filter
	// returned true but the block lookup confirmed the key was absent.
	// (false_positive_rate = BloomFalsePositives / BloomHits)
	BloomFalsePositives atomic.Uint64

	// FlushBytes is the total bytes written by memtable→L0 flushes. This is
	// the "user write" denominator for the Write Amplification Factor.
	FlushBytes atomic.Uint64

	// CompactionBytesWritten is the total bytes written by compaction output
	// SSTables (numerator side of WAF).
	CompactionBytesWritten atomic.Uint64

	// CompactionBytesRead is the total bytes consumed from input SSTables
	// during compaction.
	CompactionBytesRead atomic.Uint64

	// CompactionsTotal counts completed compactions.
	CompactionsTotal atomic.Uint64

	// --- Block cache counters (Phase 5 in-process LRU block cache) ----------

	// BlockCacheHits counts readBlock calls satisfied from the in-memory LRU
	// cache (i.e., f.ReadAt was skipped).
	BlockCacheHits atomic.Uint64

	// BlockCacheMisses counts readBlock calls that required a disk read
	// (cache miss) and subsequently populated the cache.
	BlockCacheMisses atomic.Uint64

	// --- Write-stall counters (Phase 3 leveled compaction) ------------------

	// WriteStallCount counts the number of write-stall events triggered by
	// L0 file accumulation (both soft-stall delays and hard-stop waits).
	WriteStallCount atomic.Uint64

	// WriteStallMicros is the cumulative wall-clock time spent stalling
	// incoming writes, in microseconds. Divide by WriteStallCount for the
	// mean stall duration.
	WriteStallMicros atomic.Uint64

	// L0FileCount is a point-in-time gauge of live L0 SSTable files.
	// Updated atomically by flushMemtable (+1) and runCompact (-n).
	// Useful for dashboards — not a cumulative counter.
	L0FileCount atomic.Int64
}

// Snapshot returns a point-in-time copy of all counters as a map suitable for
// JSON serialisation. A derived write_amp_factor field is computed at snapshot
// time so callers don't need to do the math.
func (m *Metrics) Snapshot() map[string]uint64 {
	flush := m.FlushBytes.Load()
	compWritten := m.CompactionBytesWritten.Load()

	// WAF = (flush_bytes + compaction_bytes_written) / max(flush_bytes, 1)
	// Reported as a milli-factor (e.g. 2520 = 2.52x) to fit the uint64 map.
	var wafMilli uint64
	if flush > 0 {
		wafMilli = ((flush + compWritten) * 1000) / flush
	}

	return map[string]uint64{
		"put_total":                       m.PutTotal.Load(),
		"get_total":                       m.GetTotal.Load(),
		"delete_total":                    m.DeleteTotal.Load(),
		"get_miss":                        m.GetMiss.Load(),
		"wal_writes":                      m.WALWrites.Load(),
		"raft_terms":                      m.RaftTerms.Load(),
		"leader_elections":                m.LeaderElections.Load(),
		"forwarded_requests":              m.ForwardedRequests.Load(),
		"replication_errors":              m.ReplicationErrors.Load(),
		"anti_entropy_passes":             m.AntiEntropyPasses.Load(),
		"anti_entropy_entries":            m.AntiEntropyEntriesSent.Load(),
		"anti_entropy_errors":             m.AntiEntropyPassErrors.Load(),
		"anti_entropy_stale":              m.AntiEntropyStaleCursors.Load(),
		"anti_entropy_full_sync_required": m.AntiEntropyFullSyncRequired.Load(),
		"bloom_hits":                      m.BloomHits.Load(),
		"bloom_misses":                    m.BloomMisses.Load(),
		"bloom_false_positives":           m.BloomFalsePositives.Load(),
		"flush_bytes":                     flush,
		"compaction_bytes_written":        compWritten,
		"compaction_bytes_read":           m.CompactionBytesRead.Load(),
		"compactions_total":               m.CompactionsTotal.Load(),
		"write_amp_factor_milli":          wafMilli,
		"write_stall_count":               m.WriteStallCount.Load(),
		"write_stall_micros":              m.WriteStallMicros.Load(),
		"l0_file_count":                   uint64(m.L0FileCount.Load()),
		"block_cache_hits":                m.BlockCacheHits.Load(),
		"block_cache_misses":              m.BlockCacheMisses.Load(),
	}
}
