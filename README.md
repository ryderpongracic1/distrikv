# distrikv

A production-quality distributed key-value store written in Go from scratch.

```
                ┌──────────────────────────────────────────────────────┐
                │         Client (distrikv-cli / curl / SDK)           │
                └──────────┬──────────────────────────┬────────────────┘
                           │ HTTP REST                 │ HTTP REST
                   ┌───────▼──────┐             ┌──────▼───────┐
                   │    node1     │             │    node2     │  ...
                   │  :8001/:9001 │◄───────────►│  :8002/:9002 │
                   └──────────────┘    gRPC     └──────────────┘
                          │
          ┌───────────────┼───────────────────┐
          │               │                   │
   ┌──────▼──────┐ ┌──────▼──────┐ ┌──────────▼──────┐
   │ Consistent  │ │ Raft Leader │ │   LSM-Tree +    │
   │ Hash Ring   │ │  Election   │ │   WAL (fsync)   │
   └─────────────┘ └─────────────┘ └─────────────────┘
```

**Stack:** Go 1.25 · gRPC · `net/http` REST · LSM-Tree storage · Raft consensus · Docker Compose

---

## Engineering Deep Dive

An audio overview of distrikv's distributed systems design — generated with [Google NotebookLM](https://notebooklm.google/). Thanks Google!

[![distrikv NotebookLM overview](https://drive.google.com/thumbnail?id=12ibGA01jQrEr-3HEpLdM_G-kAlQxX379&sz=w1280)](https://drive.google.com/file/d/12ibGA01jQrEr-3HEpLdM_G-kAlQxX379/view?usp=sharing)

---

## Status

distrikv was built across two tracks. **Phases 1–7** below are the original
developmental phases that took the project from a single-node prototype to a
fully-distributed, CLI-equipped cluster. **Production-grade upgrade phases**
(P1–P7) are the ongoing engineering uplift — benchmarking, storage
optimisation, chaos testing, and operational hardening.

### Original developmental phases

| Phase | Description | Status |
|---|---|---|
| 1 | Single-node KV + WAL + HTTP REST | ✅ Done |
| 2 | Consistent hash ring + gRPC request forwarding | ✅ Done |
| 3 | Write replication to R=2 replicas via gRPC | ✅ Done |
| 4 | Raft leader election + heartbeats | ✅ Done |
| 5 | Docker Compose cluster + demo script | ✅ Done |
| 6 | LSM-Tree storage engine + complete Raft (snapshots, pre-vote) | ✅ Done |
| 7 | `distrikv-cli` — first-class CLI tool | ✅ Done |

### Production-grade upgrade phases

| Phase | Description | Status |
|---|---|---|
| P1 | Quantified benchmarking & metrics harness (`cmd/bench`, HDR histograms, bloom + WAF counters) | ✅ Done |
| P2 | WAL GC optimisation & zero-copy parsing (`sync.Pool` buffers, single-pass CRC, torn-write hardening) | ✅ Done |
| P3 | Leveled compaction strategy + write-stall backpressure | ✅ Done |
| P4 | Deterministic fault injection & Jepsen-style linearisability verification | ✅ Done |
| P5 | In-process sharded LRU block cache (64 MB default, configurable) | ✅ Done |
| P6 | TBD | 🔲 Planned |
| P7 | TBD | 🔲 Planned |

---

## Architecture

### Consistent Hash Ring (`internal/cluster`)

Each physical node is assigned 150 virtual positions on a `uint32` ring. Keys are placed by taking the first 4 bytes of `MD5(key)` as a big-endian uint32 and walking clockwise to the next virtual node.

`GetN(key, R)` returns `R` **distinct physical nodes** starting from the primary — these are the replication targets. The naive approach of returning the next N vnodes can return duplicates for the same physical node; `GetN` skips those.

### LSM-Tree Storage Engine (`internal/store/lsm`)

Replaces the original in-memory map. Writes land in a **MemTable** (a sorted `btree.BTreeG` protected by a read-write mutex). When the MemTable hits its size threshold it is frozen as an immutable buffer and a new active MemTable opens. A background goroutine flushes immutable MemTables to **SSTables** on disk.

SSTables are organised into levels. A background compaction goroutine merges overlapping level-0 files and moves data to level-1+, bounding read amplification. Each SSTable carries a **Bloom filter** (FNV-1a, configurable false-positive rate) so point reads skip files that cannot contain a key.

All writes also append to a **WAL** before touching the MemTable. On restart, unflushed WAL entries replay into a fresh MemTable before the node begins serving traffic.

### Write-Ahead Log (`internal/store/wal`)

Every `Put` and `Delete` is appended to a binary WAL file **before** the MemTable is updated. Each entry is framed as:

```
[1B op][4B key-len][key][4B val-len][val][4B CRC32]
```

`Append` calls `f.Sync()` (not just `bufio.Flush`) before returning. On restart, `Replay` reads entries sequentially and stops cleanly at a CRC mismatch — the expected signature of a crash-at-tail.

### Raft (`internal/raft`)

Full implementation covering:

- Randomised election timeouts (150–300 ms) with **pre-vote phase** — a candidate first checks it can win a real election before incrementing its term, preventing a rejoining partitioned node from disrupting a stable leader.
- `RequestVote` with log-up-to-date check, majority-vote election, term-based split-brain prevention.
- `AppendEntries` heartbeats (75 ms) and log replication.
- `InstallSnapshot` — the leader can ship a binary state snapshot to a lagging follower that has fallen too far behind, replacing full log replay.
- Atomic persistence of `currentTerm`/`votedFor` via write-temp-then-`os.Rename`.

**Intentional deviations from the paper (important for reviewers):**

1. **Data writes bypass Raft consensus.** Writes flow through the consistent-hash ring's `ReplicationManager`, not through the Raft log. Raft here is a leader-election and failure-detection mechanism only. This means Raft's "if committed, all future leaders have it" guarantee does **not** apply to data. Under partition the ring-primary and its replicas can diverge.

2. **No log truncation on leader change.** A new leader doesn't truncate uncommitted follower entries. Mitigation: all reads route to the ring-primary, so stale reads are bounded to the in-flight crash window.

3. **Static membership.** No membership-change protocol. Adding/removing a node requires a cluster restart.

### CAP Position

With R=2 and both-replicas-must-ACK writes, the system is **CP**: it refuses writes when any replica is unreachable. Under network partition, nodes on the minority side will accept reads from their local store but reject writes. This is the correct trade-off for a store where stale reads are more tolerable than split-brain writes.

---

## HTTP REST API

```
PUT    /keys/{key}   body: {"value": "..."}
GET    /keys/{key}
DELETE /keys/{key}
GET    /status       → {node_id, leader, term, role, key_count}
GET    /metrics      → atomic counters (put_total, get_miss, raft_terms, …)
```

All error responses: `{"error": "..."}`.

### Quick start (single node)

```bash
export NODE_ID=node1
export HTTP_ADDR=:8001
export GRPC_ADDR=:9001
export DATA_DIR=/tmp/distrikv
export PEERS=""

mkdir -p /tmp/distrikv
go run ./cmd/node
```

---

## distrikv-cli

A first-class CLI tool that wraps the HTTP API — no direct gRPC, no internal imports. Feels like `redis-cli` or `psql`.

### Install

```bash
go install github.com/ryderpongracic1/distrikv/cmd/cli@latest
# binary is named distrikv-cli
```

Or build locally:

```bash
make build-cli       # → bin/distrikv-cli
```

### Commands

```
distrikv-cli get <key>
distrikv-cli put <key> <value>
distrikv-cli put <key>                        # read value from stdin pipe
distrikv-cli delete <key>                     # prompts for confirmation
distrikv-cli delete <key> --confirm           # skip prompt
distrikv-cli status                           # single node
distrikv-cli status --all                     # all nodes concurrently
distrikv-cli metrics
distrikv-cli metrics --watch                  # live-clearing table, Ctrl-C to stop
distrikv-cli watch <key>                      # poll for changes, print diffs
distrikv-cli config show
distrikv-cli config set host localhost:8002
distrikv-cli version
```

Every command supports `--help`.

### Configuration

Target node resolves in priority order:

1. `--host localhost:8002` flag
2. `DISTRIKV_HOST` environment variable
3. `.distrikv.yaml` in the current directory
4. `.distrikv.yaml` in `$HOME`
5. Built-in default: `localhost:8001`

```yaml
# .distrikv.yaml
host: localhost:8001
timeout: 5s
output: table        # "table" or "json"
peers:
  - localhost:8002
  - localhost:8003
```

### Output modes

Every command supports `-o json` for machine-readable output, keeping stdout clean for piping:

```bash
distrikv-cli get foo -o json | jq .value
distrikv-cli status --all -o json | jq '.[] | select(.role == "leader")'
```

Errors always go to stderr — stdout is never polluted regardless of output mode.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Key not found |
| 2 | Node unreachable |
| 3 | Bad arguments |
| 4 | Server error (5xx) |
| 5 | Operation cancelled |

---

## Running the Cluster

```bash
docker compose -f docker/docker-compose.yml up

# Using distrikv-cli (recommended):
distrikv-cli put hello world
distrikv-cli get hello
distrikv-cli status --all --peers localhost:8002,localhost:8003
distrikv-cli metrics --watch

# Or raw curl:
curl -X PUT  localhost:8001/keys/hello -d '{"value":"world"}'
curl         localhost:8001/keys/hello
curl         localhost:8001/status
curl         localhost:8001/metrics
```

---

## Benchmarking (`cmd/bench`)

`cmd/bench` is a standalone open-loop load generator that drives a running
cluster over HTTP and reports HDR-histogram latency percentiles plus
engine-side counters scraped from `/metrics`.

```bash
# 1. Start the cluster
docker compose -f docker/docker-compose.yml up -d

# 2. Run the bench (60s @ 1000 QPS, 80% PUT / 20% GET)
go run ./cmd/bench \
    --target   localhost:8001 \
    --qps      1000 \
    --duration 60s \
    --warmup   5s \
    --mix      80:20:0 \
    --workers  128 \
    --valuesize 1024
```

### Why open-loop?

A naive closed-loop bench (N workers each `for { client.Put(...) }`) hides
tail latency under saturation: when the cluster slows down, workers
automatically slow down their request rate, so the recorded p99 reflects
*how fast workers happened to issue requests*, not *how slow the system
actually got*. This is called **coordinated omission**.

`cmd/bench` instead generates Poisson arrivals at the configured QPS and
records latency from each request's *scheduled* time (not its dispatch
time). Queue wait counts as latency. If the cluster falls behind, the
report flags `saturation: TRUE` and `max_queue_depth` climbs to the cap.

### Interpreting the report

```
== distrikv bench: 1m0s @ target 1000 qps, mix=80:20:0, keyspace=100000 uniform ==
ops:           59937      achieved_qps: 998.6    errors: 0
latency (us):  p50=1734    p90=4403    p99=26767   p999=81151   max=108863
engine-side:   bloom_hits=673  bloom_misses=4939  bloom_fp_rate=0.00%
               flush_bytes=16.7MB  compaction_bytes_written=14.0MB  WAF=1.83
               compactions=1  forwarded_requests=40107  replication_errors=0
saturation:    false   max_queue_depth=30
```

| Metric | Meaning |
| --- | --- |
| `bloom_misses` | Bloom said *definitely absent* — saved a block read. Higher is better for negative lookups. |
| `bloom_hits` | Bloom said *might be present* — block was read. Includes true hits and false positives. |
| `bloom_fp_rate` | `false_positives / bloom_hits`. A correctly sized Bloom filter stays well under 1%. |
| `WAF` (Write Amplification Factor) | `(flush_bytes + compaction_bytes_written) / flush_bytes`. 1.0× means no compaction overhead; 2-3× is normal for size-tiered compaction; Phase 3 (leveled) will trade higher WAF for better read amp. |
| `forwarded_requests` | Ring routed the key to a peer instead of handling it locally. |
| `saturation` | `TRUE` if the arrival queue reached its cap → the cluster couldn't keep up; tail latencies include queue wait. |

### Phase 1 baseline (2026-05-21, 3-node docker-compose on M-series laptop)

| Workload | QPS achieved | p50 | p99 | p999 | WAF | Bloom FP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 80:20 PUT/GET, 1KB values, 60s @ 1000 QPS | 998.6 | 1.7 ms | 26.8 ms | 81.2 ms | 1.83× | 0.00% |

These numbers establish the baseline for Phases 2 (WAL GC optimisation), 3
(leveled compaction), and 4 (chaos). Any regression in p99 or WAF in a
later phase has to be justified.

### Phase 2 WAL allocation profile (2026-05-21, Apple M1 Max)

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

### Phase 3 — Leveled Compaction Strategy (LCS) + Write-Stall Backpressure (2026-05-21)

#### What changed

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

#### Write-stall backpressure

Unthrottled writes can produce L0 files faster than the background compaction
goroutine can drain them. Left unchecked, this degrades read performance (more
L0 files to scan) and eventually exhausts disk space. The new
`maybeStallWrite` gate applies two tiers of backpressure **before** the write
lock is taken, avoiding any lock-ordering issue:

| L0 file count | Behaviour | Default threshold |
| --- | --- | --- |
| < `l0SlowThreshold` | No stall | — |
| `l0SlowThreshold` ≤ n < `l0StopThreshold` | **Soft stall** — proportional sleep 5–50 ms, repeated until L0 drains | 8 files (2× compaction trigger) |
| ≥ `l0StopThreshold` | **Hard stop** — blocks on `l0Drained` cond var until `runCompact` broadcasts | 12 files (3× compaction trigger) |

Every stall event increments `metrics.WriteStallCount` and adds elapsed
microseconds to `metrics.WriteStallMicros` — both visible via `/metrics`.
The `l0_file_count` gauge tracks the current L0 depth in real time.

Both thresholds are tunable via `WithL0StallConfig(slowThreshold, stopThreshold)`
for tests or custom deployments.

#### Read path after Phase 3

```
Get(key)
 └─ mem (active Memtable)
 └─ imm (immutable Memtable, non-nil only during a flush)
 └─ l0[0], l0[1], … l0[n-1]   ← newest-first, ALL files checked (keys may overlap)
 └─ l1[0]                      ← single merged output, non-overlapping
```

The overhead of scanning all L0 files is bounded by the compaction trigger
threshold (≤ 4 files in steady state), so read amplification stays predictable.

#### Phase 3 test coverage

| Test | What it verifies |
| --- | --- |
| `TestLCS_ReadCorrectness` | All keys readable after L0→L1 compaction; last writer wins across rounds |
| `TestLCS_TombstoneDroppedAfterCompaction` | Deleted keys return `ErrNotFound`; no tombstone entries in L1 SSTables |
| `TestLCS_WriteStallMetrics` | `WriteStallCount` and `WriteStallMicros` increment correctly during soft-stall loops |
| `TestLCS_L0CountTracking` | `l0Count` atomic, `L0FileCount` metric, and `len(l.l0)` stay consistent before and after compaction |

### Phase 4 — Deterministic Fault Injection & Jepsen-style Linearisability Verification (2026-05-21)

#### What changed

Phase 4 adds a complete correctness verification layer consisting of three components:

**1. Porcupine linearisability harness (`internal/linearizability`)**

Every concurrent put/get/delete operation is bracketed with `rec.Begin(input)` /
`rec.End(id, output)` calls that record a [Porcupine](https://github.com/anishathalye/porcupine)
event timeline. After all goroutines finish, `rec.CheckTimeout(d)` verifies that
the observed history is linearisable — i.e., every read returns a value consistent
with *some* sequential ordering of the concurrent writes.

The KV model treats each key as an independent register
(`State = map[string]string`). The `PartitionEvent` hook splits the history by key
before checking, reducing complexity from O(exp(N)) to O(K × exp(N/K)) where K is
the keyspace size — essential for large histories.

**Failed operations** (`Output.Err = true`) are modelled as no-ops: the checker
allows the state to remain unchanged. This is exact for in-process failures
(a panicking Put never reaches the memtable) and conservative for network-level
errors (the server *may* have committed the write).

**2. Crash recovery tests (`internal/store/lsm/crash_test.go`)**

Six deterministic crash scenarios, each pairing a specific failure mode with an
expected recovery guarantee:

| Test | Failure injected | Recovery guarantee |
| --- | --- | --- |
| `TestCrash_WalReplayDurability` | Clean close + reopen | All 200 pre-close keys readable |
| `TestCrash_TornWALEntry` | WAL file truncated mid-entry (simulates power-loss byte tear) | Pre-tear keys present; post-tear key absent |
| `TestCrash_FlushedDataSurvivesTruncation` | Post-flush WAL removed | L0 SSTable survives; keys readable |
| `TestCrash_ConcurrentWriteDurability` | 6-goroutine write storm + clean close | All successfully-acknowledged keys durable |
| `TestCrash_RestoreSentinelRecovery` | Manually planted "restore-in-progress" sentinel | Data dir wiped on reopen; sentinel removed |
| `TestCrash_NoDataLossUnderFlushedAndUnflushed` | Both flushed (L0 SSTable) and unflushed (WAL-only) data present | Both batches readable after close/reopen |

Torn-write simulation stops goroutines directly (`close(tree.stopCh); tree.wg.Wait()`) without calling `tree.Close()`, preserving the WAL on disk for truncation. `os.Truncate` removes trailing bytes before reopening. The test is in `package lsm` (white-box) so it can reach unexported fields.

The sentinel-recovery test exposed a bug: `wipeLSMDir()` removed WAL/SST/manifest files but not the sentinel itself, causing an infinite wipe loop on every subsequent open. Fixed by adding `os.Remove(sentinelPath)` after `wipeLSMDir()` succeeds in `NewLSMTree`.

**3. Distributed chaos runner (`cmd/chaos`)**

A standalone `cmd/chaos` binary runs a Jepsen-style chaos test against a live
cluster over HTTP:

```bash
docker compose -f docker/docker-compose.yml up -d
go run ./cmd/chaos \
  --target    localhost:8001 \
  --duration  30s \
  --workers   8 \
  --keyspace  20 \
  --put       50 \
  --delete    5
```

The binary runs a **warmup phase** (ops issued but not recorded), then a
**measurement phase** (all ops recorded as Porcupine events), and finally checks
the full history for linearisability. Exit codes:

| Code | Meaning |
| --- | --- |
| 0 | PASS — history is linearisable |
| 1 | FAIL — non-linearisable anomaly detected |
| 2 | UNKNOWN — check timed out (`--check-timeout`) |
| 3 | Bad flags / startup error |

The chaos runner uses an explicit `http.Transport` with
`MaxIdleConnsPerHost = workers + 64` — the same TCP-pool fix applied in
`cmd/bench` — so high worker counts don't generate `TIME_WAIT` storms.

#### Phase 4 test coverage

| Test | What it verifies |
| --- | --- |
| `TestLinearizability_ConcurrentOps` | 5 goroutines × 80 ops on a 5-key space; full Porcupine history is linearisable |
| `TestLinearizability_WithLevels` | Same check after forcing L0→L1 compaction; reads exercise the full mem→L0→L1 path |
| `TestCrash_WalReplayDurability` | 200-key WAL round-trip through close/reopen |
| `TestCrash_TornWALEntry` | Byte-level WAL truncation; pre-tear durability, post-tear absence |
| `TestCrash_FlushedDataSurvivesTruncation` | Flushed SSTable data outlives WAL removal |
| `TestCrash_ConcurrentWriteDurability` | 6-writer concurrent storm; all ACK'd keys survive |
| `TestCrash_RestoreSentinelRecovery` | Sentinel wipe + sentinel self-removal |
| `TestCrash_NoDataLossUnderFlushedAndUnflushed` | Mixed flushed + in-flight WAL data both survive |
| `BenchmarkCrash_RecoveryThroughput` | WAL replay throughput for 1000-key history |

### Phase 5 — In-process Sharded LRU Block Cache (2026-05-21)

#### What changed

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

#### Design

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

#### Metrics

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

#### Phase 5 test coverage

| Test | What it verifies |
| --- | --- |
| `TestBlockCache_HitRateAfterWarmup` | Cold pass: `BlockCacheMisses > 0`; warm pass: `BlockCacheHits ≥ cold misses`, zero misses |
| `TestBlockCache_NilSafe` | `WithBlockCacheBytes(0)` produces no panics and all keys are still readable |
| `TestBlockCache_EvictOnCompaction` | Post-compaction keys remain readable; cache eviction does not corrupt data |

### Operator notes

- `--workers` sizes the worker pool *and* the HTTP connection pool
  (`MaxIdleConnsPerHost = workers + 64`). Go's defaults cap idle conns/host
  at 2, which causes TCP `TIME_WAIT` exhaustion under high concurrency;
  the bench overrides this.
- For runs above ~10k QPS on macOS, you may need to raise
  `kern.ipc.somaxconn` and the ephemeral port range
  (`net.inet.ip.portrange.first`).
- Pass `--output json` for a machine-readable report (suitable for
  piping into diff tools across runs).

---

## Running Tests

```bash
go test ./... -race
```

Tests cover:
- **Hash ring distribution** — 10k keys across 3 nodes, no node owns >40% or <20%
- **WAL replay correctness** — put/delete/truncated-entry scenarios
- **Raft vote-granting logic** — stale term, duplicate vote, outdated log, idempotent re-vote
- **PersistentState round-trip** — atomic write/read of `currentTerm` + `votedFor`
- **Concurrent store writes** — 100 goroutines, verified under `-race`
- **LSM-Tree correctness** — MemTable flush, SSTable read, compaction, Bloom filter false-positive rate, leveled-compaction read correctness, tombstone removal, write-stall metrics, L0-count tracking
- **Crash recovery** — WAL replay, torn-write truncation, flushed-data survival, concurrent-write durability, restore-sentinel self-removal, mixed flushed+unflushed data
- **Linearisability** — Porcupine-verified concurrent put/get/delete histories; both in-memory and with L0→L1 compaction in progress
- **Block cache** — hit/miss accounting, nil-safe disabled mode, eviction correctness after compaction
- **HTTP client** — all five endpoints, 404/5xx/unreachable/context-cancel via `httptest.NewServer`
- **CLI commands** — all commands via mock client + buffer-backed formatters; no real HTTP servers

---

## Project Structure

```
distrikv/
├── cmd/
│   ├── node/
│   │   ├── main.go          # Entrypoint: config → Node → Run
│   │   └── node.go          # Node struct: wires all subsystems, owns shutdown order
│   ├── cli/
│   │   └── main.go          # distrikv-cli entrypoint (ldflags version injection)
│   ├── bench/
│   │   └── main.go          # Open-loop load generator; HDR histograms + engine counters
│   └── chaos/
│       └── main.go          # Jepsen-style chaos runner; Porcupine linearisability check
├── cli/                     # Cobra command definitions (no internal imports)
│   ├── root.go              # CLI struct, AppContext, Viper config loading
│   ├── get.go / put.go / delete.go
│   ├── status.go            # --all concurrent fan-out via sync.WaitGroup
│   ├── metrics.go           # --watch with ANSI in-place reprint
│   ├── watch.go             # change-detection state machine
│   ├── config.go            # config show / set
│   ├── formatter.go         # Formatter interface: TableFormatter + JSONFormatter
│   └── errors.go            # CLIError, HandleErr, exit code constants
├── internal/
│   ├── client/
│   │   ├── client.go        # Pure HTTP client; only layer that knows the REST API
│   │   └── client_test.go
│   ├── cluster/
│   │   ├── ring.go          # Consistent hash ring (MD5, 150 vnodes/node)
│   │   └── ring_test.go
│   ├── raft/
│   │   ├── raft.go          # Leader election, pre-vote, heartbeats, stepDown
│   │   ├── log.go           # LogEntry, PersistentState (atomic write-rename)
│   │   ├── snapshot.go      # InstallSnapshot: binary state transfer to lagging followers
│   │   └── raft_test.go
│   ├── linearizability/
│   │   └── kv_model.go      # Porcupine KV model, PartitionEvent per-key split, Recorder
│   ├── store/
│   │   ├── store.go         # High-level KV API wrapping LSM engine
│   │   ├── wal.go           # WAL wrapper (CRC32 framing, fsync, replay-on-truncation)
│   │   ├── store_test.go
│   │   └── lsm/
│   │       ├── lsm.go       # Engine: MemTable → immutable → SSTable flush cycle
│   │       ├── memtable.go  # Sorted btree.BTreeG; thread-safe via RWMutex
│   │       ├── sstable.go   # On-disk sorted file; binary search over block index
│   │       ├── compaction.go# Background level merging; bounds read amplification
│   │       ├── bloom.go     # Bloom filter (FNV-1a); skips SSTables on point reads
│   │       ├── manifest.go  # SSTable metadata and level membership
│   │       ├── linearizability_test.go  # Porcupine concurrent-op correctness tests
│   │       └── crash_test.go            # Deterministic WAL tear + restart recovery tests
│   ├── server/
│   │   ├── grpc_server.go   # KVService: ForwardKey, Replicate, RequestVote, AppendEntries, InstallSnapshot
│   │   └── http_server.go   # REST handlers, ring-based routing, gRPC forwarding
│   ├── metrics/
│   │   └── metrics.go       # Atomic counters, Snapshot() for /metrics
│   └── config/
│       └── config.go        # LoadFromEnv(): typed config from env vars
├── proto/
│   └── kv.proto             # KVService gRPC definitions
├── Makefile                 # build-cli, build-node, test, install-cli
└── docker/
    ├── Dockerfile
    └── docker-compose.yml
```

---

## Code Quality Notes

- **No global mutable state** — all state lives on a `Node` struct; subsystems receive dependencies through constructors.
- **`context.Context` everywhere** — every I/O function takes a context as its first argument.
- **Goroutine ownership** — every goroutine has a named owner and a clean shutdown path. `runLeader` exits via a per-term `leaderStop` channel; no goroutine leaks.
- **Error wrapping** — `fmt.Errorf("component.Op %q: %w", key, err)` throughout; `errors.Is` contract preserved.
- **Atomic WAL writes** — `PersistentState.Save` uses write-to-temp + `Sync` + `os.Rename`; `WAL.Append` calls `f.Sync()` before returning.
- **Clean CLI separation** — `internal/client` has no Cobra dependency and is tested independently; `cli/` commands receive an injected `AppContext` and never reach into Viper or `os.Args` directly.
