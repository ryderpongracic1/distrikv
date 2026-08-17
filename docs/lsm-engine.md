# LSM-Tree Storage Engine

The write path, the read path, and the three phases of measured work that shaped
them: WAL allocation (P2), leveled compaction with write-stall backpressure (P3),
and the sharded block cache (P5).

See also [defect-log.md](defect-log.md) for the defects this subsystem produced —
two of them data-loss class — and [benchmarks.md](benchmarks.md) for the
cluster-level read-path measurements that exercise the Bloom filters, the block
cache and compaction under load.

---

## LSM-Tree Storage Engine (`internal/store/lsm`)

Replaces the original in-memory map. Writes land in a **MemTable** (a sorted `btree.BTreeG` protected by a read-write mutex). When the MemTable hits its size threshold it is frozen as an immutable buffer and a new active MemTable opens. A background goroutine flushes immutable MemTables to **SSTables** on disk.

SSTables are organised into levels. A background compaction goroutine merges overlapping level-0 files and moves data to level-1+, bounding read amplification. Each SSTable carries a **Bloom filter** (FNV-1a, configurable false-positive rate) so point reads skip files that cannot contain a key.

All writes also append to a **WAL** before touching the MemTable. On restart, unflushed WAL entries replay into a fresh MemTable before the node begins serving traffic.

**Write sequence numbers are the total write order, and they must survive a
restart (fixed).** Every entry carries a sequence number, and that number is how
compaction decides between two versions of a key: the higher one wins and the
loser is **dropped**, not shadowed. The counter lived only in memory, so it
restarted at zero on every open while the SSTables on disk still carried the
previous process's numbers. "Newer" therefore came to mean "written earlier in a
longer-lived process": a value written before a restart outranked the value that
replaced it after, and the first compaction to merge the two files kept the stale
one. The same inversion resurrects a deleted key, because a tombstone that loses
is discarded outright at the bottom level.

Nothing is visible until that compaction runs, which is what makes it a
*consistency* bug rather than a crash: reads are served newest-file-first, so the
correct value is returned right up until the merge silently replaces it on disk.
The engine now records each SSTable's highest sequence number in the manifest
(`ManifestEvent.MaxSeqNum`) and seeds the counter above all of them at open. A
data directory written before that field existed cannot answer, and the numbers
live in the entries themselves, so those files are scanned once — logged as
`lsm: manifest predates per-SSTable sequence numbers`, and self-limiting, since
the first compaction replaces the whole live set with a file that records it.

This was found while investigating a chaos run that reported `converged: true`
with `linearizable: FAIL` — data consistent at rest, history illegal — and it is
sufficient to produce exactly that pair: a resurrected value fails a read
mid-run, and the next write to the key converges every copy again.

## Write-Ahead Log (`internal/store/wal`)

Every `Put` and `Delete` is appended to a binary WAL file **before** the MemTable is updated. There are two record formats, discriminated by the op byte, and they interleave freely within a segment:

```
v1 (legacy):  [1B op][4B key-len][key][4B val-len][val][4B CRC32]
v2 (current): [1B op][8B seq][4B key-len][key][4B val-len][val][4B CRC32]
```

The engine writes v2, because a write's sequence number has to survive recovery with the same meaning it had before — a replica compares the sequence its ring-primary sent against the one it has stored for that key, and a sequence re-derived from the local counter at replay time would be a number from a different counter. See [Per-key write ordering](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not). A v1 record replays with sequence 0 ("this record does not know its ordering"), which applies unconditionally.

`Append` calls `f.Sync()` (not just `bufio.Flush`) before returning. The CRC covers the sequence like every other byte, so a corrupted sequence is rejected as a torn record rather than silently reordering writes. On restart, `Replay` reads entries sequentially and stops cleanly at a CRC mismatch — the expected signature of a crash-at-tail.

## Phase 2 WAL allocation profile (2026-05-21, Apple M1 Max)

`go test -bench=BenchmarkWAL_Append -benchmem` after the Phase 2 refactor:

| Benchmark | ns/op | B/op | allocs/op | vs. original |
| --- | ---: | ---: | ---: | --- |
| `Append` (256B value) | 4,643–4,687 | 8 | 2 | −3 allocs (bufio.Writer, CRC hasher, []byte(key) eliminated) |
| `Append` (64KB value) | 4,844–4,960 | 8 | 2 | same — value is passed by pointer, not copied |

The 2 remaining allocs/op are baseline OS-call overhead (error interface values
from `f.Sync`), not WAL logic. The dominant hot-path allocations from the
original implementation are gone:

| Source | Original | Phase 2 |
| --- | --- | --- |
| `bufio.NewWriter(f)` per Append | 1 alloc (~4 KB) | 0 — persistent `bw` on struct |
| `crc32.NewIEEE()` per Append | 1 alloc (hash object) | 0 — `crc32.Update` rolling accumulator |
| `[]byte(key)` for CRC pass | 1 alloc (key size) | 0 — `unsafe.Slice` zero-copy view |
| `bufio.NewReader(f)` per Replay | 1 alloc (read buffer) | 0 — pooled `*[]byte` from `sync.Pool` |
| `make([]byte, keyLen)` per entry | 1 alloc per WAL entry | 0 — pool buffer reused across entries |

Replay delivers owned copies of values (1 alloc per entry, unavoidable since
the Memtable tree retains value references); keys produce 1 `string(buf)` copy
per entry (unavoidable for string immutability). All other Replay allocations
are eliminated on the warm path.

**Torn-write contract (hardened in Phase 2):**
- `Replay` stops cleanly (`nil` return) for `io.EOF`, `io.ErrUnexpectedEOF`,
  or CRC mismatch at any byte offset within an entry — the expected signature
  of a sudden power loss or `kill -9`.
- `Replay` returns a **non-nil error** only for genuine I/O failures (e.g.
  disk read error mid-entry). Previously, all errors were silently swallowed
  as clean truncations — a disk failure would have been misread as a torn
  write, masking data loss from the operator.

## Phase 3 — Leveled Compaction Strategy (LCS) + Write-Stall Backpressure

### What changed

The LSM storage engine was restructured from a flat `readers []*SSTableReader`
slice to a two-level model:

| Level | Produced by | Key-range overlap | Read order |
| --- | --- | --- | --- |
| **L0** | Memtable flushes | May overlap (newest-first wins) | All L0 files checked, newest first |
| **L1** | L0→L1 compaction | Non-overlapping | Single merged output, newest first |

A compaction triggers when `len(l0) ≥ 4` (configurable). The compaction job
merges all L0 files **plus** the existing L1 file into a single new L1 file
using the existing k-way `MergeIterator`, which deduplicates by sequence number
and drops tombstones (safe because L1 is always the bottom level — no L2+
exists yet).

The Manifest tracks each SSTable's level so the engine reconstructs the correct
L0/L1 split on restart. Pre-Phase-3 manifest events default to `level=0`.

### Write-stall backpressure

Unthrottled writes can produce L0 files faster than the background compaction
goroutine can drain them. Left unchecked, this degrades read performance (more
L0 files to scan) and eventually exhausts disk space. The new
`maybeStallWrite` gate applies two tiers of backpressure **before** the write
lock is taken, avoiding any lock-ordering issue:

| L0 file count | Behaviour | Default threshold |
| --- | --- | --- |
| < `l0SlowThreshold` | No stall | — |
| `l0SlowThreshold` ≤ n < `l0StopThreshold` | **Soft stall** — proportional sleep 5–50 ms, repeated until L0 drains or the caller's context is done | 8 files (2× compaction trigger) |
| ≥ `l0StopThreshold` | **Hard stop** — waits for compaction to signal that L0 drained, giving up with `ErrWriteStalled` after `maxStallWait` | 12 files (3× compaction trigger), 1 s budget |

The two tiers bound differently on purpose. Soft stall is a throttle — the write
is going to be accepted, just later — so it waits exactly as long as its caller
is willing to. Hard stop means the engine is *refusing* writes, so it says so:
after `maxStallWait` (1 s) the write returns `lsm.ErrWriteStalled`, re-exported as
`store.ErrWriteStalled`. An unbounded wait there is worse than an error, because
it makes an overloaded node indistinguishable from a dead one — which is exactly
how a replica ends up consuming its primary's entire replication deadline and
being reported as unreachable. `ErrWriteStalled` says "alive, overloaded, retry".

Every stall event increments `metrics.WriteStallCount` and adds elapsed
microseconds to `metrics.WriteStallMicros` — both visible via `/metrics`.
The `l0_file_count` gauge tracks the current L0 depth in real time.

Both thresholds are tunable via `WithL0StallConfig(slowThreshold, stopThreshold)`
for tests or custom deployments; `WithCompactThreshold(n)` moves the compaction
trigger itself.

### Reopening on an accumulated store (write-availability)

Backpressure keys off the **live L0 file count**, which is restored from the
manifest at open — so a store that was closed with a compaction backlog reopens
already stalled. Compaction is otherwise only ever armed by a memtable flush, or
by a compaction that left work behind. Neither happens at open, which produced a
deadlock rather than a delay:

```
writes stall (L0 ≥ 12 from the manifest)
   → no write reaches the memtable
   → no memtable fills
   → nothing flushes
   → nothing signals compaction
   → L0 never drains → writes stall …
```

`NewLSMTree` now arms compaction at open whenever the restored L0 set is already
over the compaction threshold, and logs `lsm: armed compaction at open`.

This is what made a three-node cluster inherited from a busy data volume serve KV
traffic terribly while every container reported healthy: a chaos baseline with no
faults injected managed **287 ops / 30 s with 164 errors**, and stayed that way
for 80+ minutes, where a clean-slate cluster on the identical binary served
**115,170 ops / 30 s with 0 errors**. The failure surfaced as replication and
forward errors (503s and 502s) because the writes never returned — the stalled
node was a black hole, not a slow node.

**Validated on dirty volumes (2026-08-16, commit e59a545).** The same accumulated
data volumes (bench + chaos residue) that produced 287 ops / 164 errors before the
fix now produce **122,551 ops / 30 s with 0 errors**, PASS in 58 ms. The
compaction-arm-at-open fix converts a permanent write outage into a sub-100 ms
recovery window.

Measured on Linux with `internal/store/lsm/recovery_availability_test.go`, a store
closed with 13 live L0 SSTables and reopened with production defaults:

| | Before | After |
| --- | --- | --- |
| Time until the first write is accepted | never (no write accepted in 30 s, `compactions=0`) | **63 ms** |
| L0 depth after that first write | 13 (unchanged) | 0 (backlog merged) |
| A hard-stopped write whose context expires | never returns — the wait ignored `ctx` entirely | returns `context.DeadlineExceeded` at the deadline |
| A hard-stopped write with no deadline of its own | never returns | `ErrWriteStalled` after the stall budget |

The bench-scale end of the same file (`TestRecovery_BenchScaleReopen`, gated on
`DISTRIKV_RECOVERY_REPRO=1`) builds the ~200k × 256 B state the field cluster
held and reports time-to-first-accepted-write plus write p50/p99 through the
recovery window. At **production settings on a local ext4 disk it does not
reproduce**, and that is the informative part: 200k keys took 9 m 20 s to write
(357 writes/s, WAL-fsynced), compaction kept up (3 compactions, **1 live L0 file**
at close), and the reopen was healthy — `NewLSMTree` returned in **1.7 ms**, the
first write was accepted in **2.1 ms**, and the following 2000 writes ran
p50 = 2.7 ms / p99 = 3.2 ms with zero stalls. Key *count* is not what breaks
recovery; the L0 *backlog* is, and whether one accumulates depends on how
expensive compaction is on the volume — on Colima/virtiofs each compaction pays
six manifest fsyncs and falls behind the flush rate, where ext4 keeps up.
`DISTRIKV_RECOVERY_COMPACT_THRESHOLD` raises the compaction trigger to emulate
that, and the harness always prints the L0 depth it actually achieved so a run
is never read as more than it is.

With the trigger raised (`DISTRIKV_RECOVERY_COMPACT_THRESHOLD=1000`), the same
200k × 256 B build closes with **13 live L0 SSTables** — the field condition at
field scale — and the reopen behaves as the deterministic test predicts: open in
1.7 ms, one stall of 268 ms while a single compaction merges the whole ~50 MB
backlog, **first write accepted at 270 ms**, L0 drained to 0, and the following
2000 writes at p50 = 2.7 ms / p99 = 3.1 ms — indistinguishable from a store that
never had a backlog. The recovery window is therefore bounded by one compaction
pass over the accumulated data, not open-ended. On the unfixed engine the same
directory accepts no write at all.

That 1.7 ms reopen also settles startup ordering as a factor: a node opens its
store in `NewNode` and only starts its gRPC/HTTP listeners in `Run`, so a slow
recovery would be indistinguishable from a down node — but recovery itself is
cheap (WAL replay loads a single memtable with no per-entry fsync), so the
black-hole window came from the stall above, not from recovery time, and the
ordering was left alone.

### Read path after Phase 3

```
Get(key)
 └─ mem (active Memtable)
 └─ imm (immutable Memtable, non-nil only during a flush)
 └─ l0[0], l0[1], … l0[n-1]   ← newest-first, ALL files checked (keys may overlap)
 └─ l1[0]                      ← single merged output, non-overlapping
```

The overhead of scanning all L0 files is bounded by the compaction trigger
threshold (≤ 4 files in steady state), so read amplification stays predictable.

### Phase 3 test coverage

| Test | What it verifies |
| --- | --- |
| `TestLCS_ReadCorrectness` | All keys readable after L0→L1 compaction; last writer wins across rounds |
| `TestLCS_TombstoneDroppedAfterCompaction` | Deleted keys return `ErrNotFound`; no tombstone entries in L1 SSTables |
| `TestRecovery_ReopenWithL0Backlog_AcceptsWrites` | A store closed with L0 at the hard-stop threshold accepts a write after reopening, and the backlog drains (fails by hanging on the unfixed engine) |
| `TestRecovery_HardStopRespectsContext` | A hard-stopped write returns when its caller's context expires |
| `TestRecovery_HardStopReturnsStallError` | A hard-stopped write with no deadline of its own returns `ErrWriteStalled` within the stall budget |
| `TestRecovery_BenchScaleReopen` | Env-gated harness: ~200k × 256 B state, reopen, time-to-first-accepted-write and write p50/p99 through recovery |
| `TestRestore_BulkLoadPerformsNoWALAppends` | Snapshot restore performs zero WAL appends and lands as one L0 SSTable |
| `TestRestore_SurvivesReopen` | Restored state, live-key count included, survives close/open; sentinel removed |
| `TestRestore_EmptySnapshotEmptiesStore` | An empty payload empties the store and leaves it writable |
| `TestLCS_WriteStallMetrics` | `WriteStallCount` and `WriteStallMicros` increment correctly during soft-stall loops |
| `TestLCS_L0CountTracking` | `l0Count` atomic, `L0FileCount` metric, and `len(l.l0)` stay consistent before and after compaction |
| `TestCompactionKeepsTheNewerWriteAcrossARestart` | A write acknowledged after a restart survives the compaction that merges it with a pre-restart file — the sequence counter is seeded from disk, so "newer" cannot mean "written in a longer-lived process" |
| `TestCompactionKeepsATombstoneAcrossARestart` | The same defect on the delete path, which is the worse half: a tombstone that loses is dropped at the bottom level, so the deleted key comes back |
| `TestLegacyManifestRecoversTheWriteOrderByScanning` | A data directory whose manifest predates the recorded sequence numbers still reopens above everything on disk, by scanning its live SSTables once |
| `TestSequenceNumbersAreMonotonicAcrossARestart` | The invariant on its own, so a future change that zeroes the counter fails with a readable reason |

## Phase 5 — In-process Sharded LRU Block Cache

### What changed

The read path previously issued a `f.ReadAt` syscall for every SSTable data block
that passed the Bloom filter.  Under a hot-key workload this means the same
blocks are fetched from the OS page cache on every request — wasting a syscall
and a kernel context switch per block even when the data is already in RAM.

Phase 5 introduces an **in-process sharded LRU block cache** that intercepts
`readBlock` before it touches the file:

```
Get(key)
 └─ Bloom filter: MightContain?  no → return (no disk I/O, no cache)
 └─ blockIndexFor(key)
 └─ readBlock(idx):
     ├─ cache.Get(path, offset)?  yes → BlockCacheHits++, decode, return
     └─ no → f.ReadAt + cache.Put(path, offset, raw) + BlockCacheMisses++
```

The Bloom filter still runs first — the cache only sees keys that pass the
filter.  On a cache miss the block is read from disk and inserted; on subsequent
reads (same key, same block, or adjacent keys sharing a block) the `ReadAt` is
skipped entirely.

### Design

| Property | Value |
| --- | --- |
| Shards | 32 (`numCacheShards`) — one `sync.Mutex` per shard, zero cross-shard contention |
| Default capacity | 64 MB (`defaultBlockCacheBytes`) — configurable via `WithBlockCacheBytes(n)` |
| Eviction policy | LRU per shard — `container/list`, O(1) promote / evict |
| Cache key | `(sstable-path, block-offset)` — unique across all live SSTables |
| Stored value | Raw block bytes (pre-decode) — avoids the `ReadAt` syscall; decoding is CPU-cheap |
| Shard routing | `FNV-1a(path + offset) % 32` — balances entries from the same SSTable across shards |
| Disabled | `WithBlockCacheBytes(0)` — returns `nil *BlockCache`; all methods are no-ops |

Entries for compacted-away SSTables are proactively evicted via `cache.Evict(path)`
right after the compaction swap, so stale memory is freed without waiting for LRU pressure.

### Metrics

Two new counters are visible via `/metrics`:

| Counter | Meaning |
| --- | --- |
| `block_cache_hits` | `readBlock` calls served from the in-memory cache (syscall skipped) |
| `block_cache_misses` | `readBlock` calls that required a disk read and subsequently populated the cache |

`cmd/bench` now reports a `hit_rate` line in the engine-side section:

```
engine-side:   bloom_hits=240118  bloom_misses=119  bloom_fp_rate=0.04%
               flush_bytes=42MB  compaction_bytes_written=64MB  WAF=2.52
               compactions=6  forwarded_requests=99882  replication_errors=0
               block_cache_hits=198432  block_cache_misses=1680  hit_rate=99.2%
```

A hit rate above 95% on a hot-key workload (`--keyspace 20`) is expected; a
workload larger than the cache (`--keyspace 100000 --valuesize 4096`) will produce
a lower hit rate due to natural working-set pressure.

### Phase 5 test coverage

| Test | What it verifies |
| --- | --- |
| `TestBlockCache_HitRateAfterWarmup` | Cold pass: `BlockCacheMisses > 0`; warm pass: `BlockCacheHits ≥ cold misses`, zero misses |
| `TestBlockCache_NilSafe` | `WithBlockCacheBytes(0)` produces no panics and all keys are still readable |
| `TestBlockCache_EvictOnCompaction` | Post-compaction keys remain readable; cache eviction does not corrupt data |
| `BenchmarkBlockCache_ColdVsWarm` | Microbenchmark isolating cold `f.ReadAt` vs warm in-process cache path |
| `TestBlockCache_ZipfHitRate` | Zipfian α=1.1 hit-rate sweep across 4 cache sizes (0, 8, 32, 64 MB); 100k-key dataset, 768 B values, reverse-scan warm + 10k measurement pass |

### Phase 5 latency profile (2026-05-21, Apple M1 Max)

`go test -bench=BenchmarkBlockCache_ColdVsWarm -benchtime=3s -count=3 -benchmem`

| Path | ns/op | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| **Cold** (no cache, `ReadAt`) | 8,013 | 24,576 | 227 | OS page cache warm; SSD not involved |
| **Warm** (in-process LRU cache) | 7,480 | 24,576 | 227 | ~7% faster; identical allocation profile |

**Why the gap is small here:** the test SSTable is tiny (~6 KB) and was just written, so it is fully resident in the OS page cache. A page-cached `ReadAt` on M1 costs ~500 ns — competitive with a sharded map lookup. The 227 allocs/op are dominated by `string(...)` copies during block decoding, which happen equally on both paths. Both paths also allocate a fresh `[]byte` for the block data (cold: `make([]byte, blockLen)` inside `ReadAt`; warm: the copy returned from `cache.Get`).

**When the cache earns its keep:**

| Scenario | Benefit |
| --- | --- |
| Working set > OS page cache | Blocks are in Go heap instead of requiring kernel I/O + context switch; warm reads stay in user space |
| High concurrency (many goroutines, same hot blocks) | Eliminates serialization on the kernel page-cache lock; sharded in-process cache has finer-grained locking |
| Large SSTables with cold blocks (post-restart) | First access warms the in-process cache; subsequent accesses skip the syscall and kernel→user copy |
| Mixed read/write (compaction pressure) | Compaction evicts old SSTables from both the in-process cache and triggers OS cache pressure; in-process cache survives the swap since it evicts by path, not by fd |

The Phase 1 benchmark baseline used a 100k-key uniform workload where the full dataset is many times larger than any reasonable in-process cache — that workload will show minimal cache benefit. The cache targets hot-key / Zipfian workloads (`--keydist zipf`) where a small fraction of keys accounts for most reads. The `block_cache_hits` / `hit_rate` fields in `cmd/bench` output make this visible without guesswork.

### Phase 5 Zipfian hit-rate baseline (2026-05-22, Apple M1 Max)

`go test ./internal/store/lsm/ -run TestBlockCache_ZipfHitRate -v -timeout 120s`

**Setup:** 100,000 keys × 768-byte values (≈ 76 MB) across 4 L1 SSTable files (~19 MB each, simulating 4 compaction rounds at the default 4 MB memtable threshold). **Distribution:** Zipfian α = 1.1 — the YCSB/RocksDB canonical exponent, where the top 1% of keys account for ~54% of reads and the top 10% account for ~88%. **Warm pass:** reverse-rank sequential scan (rank 99,999 → 0) so the LRU eviction leaves the hottest blocks resident. **Measurement:** 10,000 seeded-random (seed=42) Zipfian reads.

| Cache size | Hits | Misses | Hit rate |
| ---: | ---: | ---: | ---: |
| 0 MB (disabled) | 0 | 10,000 | 0.0% |
| 8 MB | 8,633 | 1,367 | **86.3%** |
| 32 MB | 9,579 | 421 | **95.8%** |
| 64 MB (default) | 9,920 | 80 | **99.2%** |

The 86.3% figure for 8 MB is analytically consistent: 8 MB holds ~2,048 blocks, covering the top ~10,240 keys, which at α = 1.1 capture ≈ 88% of reads (harmonic sum H ≈ 6.84). The cache earns disproportionate returns on the first few megabytes — 8 MB captures 86% of the benefit that 64 MB achieves.

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
