# distrikv

[![CI](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml/badge.svg)](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml)

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

> **Phase 3 note — honest disclosure.** The replication fan-out
> (`Node.ReplicateWrite`) was written during Phase 3, but nothing ever called
> it: no caller existed in any commit from the initial one onward. Until it was
> wired into the ring-primary's write path, every key lived on exactly one node
> and the receive side (`Replicate` → `ApplyReplica`) was never exercised in
> production. Any benchmark table or `replication_errors=0` reading produced
> before that wiring therefore describes a single-copy sharded store; those are
> labelled where they appear.

### Production-grade upgrade phases

| Phase | Description | Status |
|---|---|---|
| P1 | Quantified benchmarking & metrics harness (`cmd/bench`, HDR histograms, bloom + WAF counters) | ✅ Done |
| P2 | WAL GC optimisation & zero-copy parsing (`sync.Pool` buffers, single-pass CRC, torn-write hardening) | ✅ Done |
| P3 | Leveled compaction strategy + write-stall backpressure | ✅ Done |
| P4 | Deterministic fault injection & Jepsen-style linearizability verification | ✅ Done |
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

**Raft here is a leader-election and failure-detection mechanism, not a data replication mechanism.** Read that before the feature list below — it is the single most important thing to know about this package, and the feature list is easy to misread without it.

**Live in production traffic:**

- Randomised election timeouts (150–300 ms) with **pre-vote phase** — a candidate first checks it can win a real election before incrementing its term, preventing a rejoining partitioned node from disrupting a stable leader.
- `RequestVote` with log-up-to-date check, majority-vote election, term-based split-brain prevention.
- `AppendEntries` **heartbeats only** (75 ms send period). Heartbeats carry no entries, so they serve purely as a liveness signal and a leader-authority assertion. Each heartbeat RPC carries its own deadline, deliberately decoupled from the send period and bounded by the minimum election timeout: a heartbeat is useful to a follower right up until that follower's election timer expires, and pointless after it.
- Atomic persistence of `currentTerm`/`votedFor` via write-temp-then-`os.Rename`.

**Implemented but dormant — exercised by unit tests, never by a running cluster:**

- **Log replication.** `AppendEntries` entry handling, `applyEntryLocked`, and the `nextIndex`/`matchIndex` bookkeeping are all implemented, but nothing in the system ever proposes an entry to the Raft log. Client writes go to the storage engine directly (see deviation 1). Consequence: `r.log` stays empty for the lifetime of the process.
- **Snapshot delivery.** `InstallSnapshot` and the snapshot codec work, and `internal/raft/snapshot_test.go` covers them. But snapshots are triggered by log growth past `snapshotThreshold: 1000`, and since the log never grows, that trigger never fires. No `InstallSnapshot` RPC is ever sent outside tests.

Both paths are kept rather than deleted because they are the natural landing spot if writes are ever moved onto the Raft log. They are listed here so that "distrikv implements InstallSnapshot" is not mistaken for "distrikv ships snapshots between nodes at runtime" — it doesn't.

**Correction — the leader-election storm (fixed).** Until recently the two claims above were not true of a *running* cluster, and the gap was a chronic election storm: a live 3-node Docker cluster burned roughly 1.7 terms per second indefinitely, passing term 900 within nine minutes of startup, alternating leadership every ~500 ms. Two defects combined:

1. `broadcastHeartbeat` classified a peer as needing a snapshot when `nextIndex-1 <= snapLastIndex` — but a fully caught-up peer sits at exactly `nextIndex == snapLastIndex+1` and satisfies that test. Since the Raft log never grows, *every* peer matched on *every* tick, and the snapshot was sent **instead of** the heartbeat. With no snapshot on disk that send returned early, so no heartbeat ever left the leader at all, and every follower elected the moment its timer expired. A heartbeat now goes to every peer unconditionally; snapshot delivery (still dormant, for the reason above) rides alongside it rather than replacing it.
2. Each heartbeat RPC was given a deadline of exactly one send interval, so any RPC delayed past 150 ms by gRPC connection setup or container scheduling was cancelled by its own deadline and never reached the follower.

Because the data path is ring-based and never consults Raft leadership, the storm cost nothing but log volume and wasted CPU — which is why it went unnoticed until the container logs were read directly. `internal/raft/cluster_test.go` now stands an in-process 3-node cluster up and fails if the term advances more than once over three seconds, both on an idle cluster and with per-RPC latency injected above the send interval.

**Intentional deviations from the paper (important for reviewers):**

1. **Data writes bypass Raft consensus.** Writes flow through the consistent-hash ring, not through the Raft log. Raft here is a leader-election and failure-detection mechanism only. This means Raft's "if committed, all future leaders have it" guarantee does **not** apply to data. Under partition the ring-primary and its replicas can diverge.

2. **The Raft log is never written, so log-based guarantees are vacuous.** There is no log truncation on leader change because there are no entries to truncate, and no commit index advancing because nothing is proposed. Mitigation for data: all reads route to the ring-primary, so stale reads are bounded to the in-flight crash window.

3. **Static membership.** No membership-change protocol. Adding/removing a node requires a cluster restart.

The package doc comment at the top of `internal/raft/raft.go` states the same deviations for anyone reading the code first.

### CAP Position

With R=2 and both-replicas-must-ACK writes, distrikv is **CP**. The ring-primary
applies a mutation to its own store and then synchronously replicates it over
gRPC to the other R−1 replicas that `ring.GetN(key, R)` selects, and it returns
success to the client only if **every** replica acknowledges. If any replica is
unreachable or rejects the write, the client receives `503 Service Unavailable`
and must treat the write as refused. Both client entry points behave identically
— a request that lands on the ring-primary directly and one forwarded to it by a
peer via gRPC `ForwardKey` share a single primary-write path, so they cannot
drift apart on durability or on failure semantics.

If the ring-primary itself is unreachable, the forwarding node answers
`502 Bad Gateway` within ~2s: the forward RPC carries its own deadline, so a
primary whose host has vanished (which never sends a TCP RST, leaving the gRPC
channel stuck in `CONNECTING`) can no longer outlast the HTTP `WriteTimeout` and
leave the client with no response at all. `503` still means "the primary took the
write but a replica did not ACK"; `502` means "the primary could not be reached".

Whether a 502 means anything was written is a separate question, and the response
answers it rather than leaving it to be inferred: every 502 carries a
`forward_outcome` field, `"never-sent"` when the request provably never left this
node and `"unknown"` when the RPC failed in a way that may have been applied
before the response was lost. The distinction is not cosmetic — the
linearizability model needs it, and cannot derive it downstream. Phase 4's
*How failed operations are modelled* below carries the decision table and the
proof behind each row.

Reads are served from the ring-primary's local store and are never replicated,
so a node on the minority side of a partition still answers reads while
refusing writes. That is the right trade-off for a store where a stale read is
more tolerable than a split-brain write.

**A refused write is not an undone write.** There is no rollback. By the time
replication is attempted the mutation is already durable on the primary
(WAL-fsynced, then applied to the memtable), so a failed fan-out leaves the
primary **ahead of** the replicas that did not ACK: the client sees a 5xx for a
write that is in fact present on the primary and that subsequent reads will
return. The divergence persists until the next successful write for that key
converges the replica set — there is no anti-entropy, hinted handoff, or read
repair. Closing that hole properly means either two-phase commit across the
replica set or routing data writes through the Raft log; both are out of scope
for the current design (see "Intentional deviations from the paper" above).

This is not a theoretical caveat: it is what the first real fault-injection runs
detected. The linearizability checker rejected both a SIGKILL and a SIGTERM run
purely because refused-but-applied writes were being read back afterwards — see
Phase 4's "How failed operations are modelled" for the A/B evidence and the model
change it forced.

Three smaller guarantees, stated because they are load-bearing:

- **Replicas do not re-replicate.** A replicated mutation is applied straight to
  the receiving node's local store and is never fanned out again. A second
  fan-out from a replica would replicate forever.
- **Replicated deletes are idempotent.** A `delete` for a key a replica never
  received is treated as already applied, so a replica that missed an earlier
  write cannot block the primary's deletes indefinitely.
- **A single-node deployment still works.** With no other node in the replica
  set (one node, or R=1) the fan-out has no targets and the write completes
  local-only.

---

## HTTP REST API

```
PUT    /keys/{key}   body: {"value": "..."}
GET    /keys/{key}
DELETE /keys/{key}
GET    /status       → {node_id, leader, term, role, key_count, key_count_approximate}
GET    /metrics      → atomic counters (put_total, get_miss, wal_writes, raft_terms, …)
```

All error responses: `{"error": "..."}`.

**`DELETE` is idempotent and never returns 404.** The storage engine writes tombstones blindly, so deleting a key that does not exist returns `200 {"status":"ok"}`. The previous behaviour — a `Get` before the `Delete` to synthesise a 404 — was removed because it was racy (another writer could insert or remove the key between the two calls, so the answer was never authoritative) and cost a full read-path traversal through the memtable, every L0 SSTable and L1 on every delete. This matches RocksDB, Cassandra and DynamoDB `DeleteItem`. `distrikv-cli delete <key>` therefore reports success for keys that were never there; use `get` first if you need to know.

**`key_count` is approximate**, which is why `/status` also returns `key_count_approximate: true`. The LSM engine maintains the count incrementally: it is exact for a workload of distinct keys, drifts up when a key that has already been flushed to an SSTable is overwritten, and drifts down when a key that does not exist is deleted. Classifying either case correctly would require a read on the write hot path. The count is recorded in the manifest at each memtable flush and re-applied from the WAL at startup, so it survives restarts and crash recovery without scanning the SSTables. `wal_writes` is the engine's real count of fsync'd WAL appends — one per successful `PUT`/`DELETE`.

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
| 1 | Key not found (`get` and `watch` only — `delete` is idempotent, see the HTTP API notes) |
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
| `replication_errors` | Replica fan-out failures — a replica that did not ACK a primary's PUT/DELETE (unreachable, or an explicit rejection). Each one also fails the client's write with `503`, so a non-zero value means writes were **refused**, not merely under-replicated. Reports produced before replication was wired read `0` by construction; see the Phase 3 note under Status. |
| `saturation` | `TRUE` if the arrival queue reached its cap → the cluster couldn't keep up; tail latencies include queue wait. |

### Phase 1 baseline (2026-05-21, 3-node docker-compose on M-series laptop)

| Workload | QPS achieved | p50 | p99 | p999 | WAF | Bloom FP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 80:20 PUT/GET, 1KB values, 60s @ 1000 QPS | 998.6 | 1.7 ms | 26.8 ms | 81.2 ms | 1.83× | 0.00% |

These numbers establish the baseline for Phases 2 (WAL GC optimisation), 3
(leveled compaction), and 4 (chaos). Any regression in p99 or WAF in a
later phase has to be justified.

### Throughput baseline with replication (2026-08-16, 3-node docker-compose, Apple M4 Pro, Colima VM 8 CPU / 8 GB)

Measured with `cmd/bench` open-loop Poisson arrivals against a healthy 3-node
cluster; Zipfian α=1.1, 100k-key space, 256 B values, 128 workers. "Not
saturated" means `max_queue_depth ≪ cap` and achieved QPS ≈ target QPS.
**Every PUT here pays the full replicated write path**: WAL fsync on the
ring-primary plus a synchronous gRPC ACK from each of the R−1 replicas.

**Methodology note:** the bench binary runs *inside* the cluster's Docker
network (`docker run --network docker_default … bench-linux --target
node1:8001`), not through the host's port forward. Measuring through a
localhost forwarder (Docker Desktop or Colima/Lima) adds a proxy hop to every
request and, on macOS, caps concurrent connections at the host's ephemeral
port range — both of which measure the plumbing rather than the store.

| Workload | Target QPS | Achieved | p50 | p90 | p99 | p999 | Errors | Saturated? |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100% writes (PUT) | 1,200 | **1,199 /s** | 1.7 ms | 2.7 ms | 4.6 ms | 13.6 ms | 0 | No |
| 100% reads (GET) | 6,000 | **6,017 /s** | 0.54 ms | 1.3 ms | 1.7 ms | 2.0 ms | 0 | No |
| 20% write / 80% read | 3,000 | **3,000 /s** | 0.70 ms | 1.5 ms | 2.5 ms | 3.7 ms | 0 | No |

Engine-side counters for these runs show `flush_bytes=0`, `bloom_*=0` and
`block_cache_*=0` — that is expected, not broken: a Zipfian workload at 256 B
accumulates *unique*-key bytes slowly (overwrites replace in place in the
memtable), so each node stays under the 4 MB flush threshold and every read is
served from the memtable. The Bloom filter and block cache are only consulted
on the SSTable path; exercising them requires a larger value size or keyspace
(see the Zipfian hit-rate baseline below, which builds its SSTables explicitly).

#### Why these numbers replaced the earlier table

The previous baseline (2026-05-22, Apple M1 Max) reported 1,191 /s writes at
p99 = 70 ms and 5,977 /s reads at p99 = 11 ms, measured **without replication**
(the fan-out was never invoked — see the Phase 3 note under Status) and through
a client bug that has since been fixed: `internal/client` never drained HTTP
response bodies, so Go's transport discarded every connection instead of
returning it to the idle pool, and **every request paid a fresh TCP dial**.
Under load this exhausted ephemeral ports (`cannot assign requested address`)
and inflated tail latencies with handshake and TIME_WAIT stalls — it is also
the real cause behind the TIME_WAIT warning that used to live in the operator
notes. The fix is a drain-before-close helper pinned by a test asserting 60
sequential calls arrive over one connection.

The two tables are therefore not comparable (different hardware, fixed client,
replication on), but the direction is telling: **writes now do strictly more
work per request — a synchronous replica ACK — and still post a 15× better
p99**, because the connection churn cost far more than replication does.

Reads are never replicated; read throughput is bounded by ring lookup,
memtable/LSM reads, and HTTP round-trip overhead. Roughly half of all requests
are forwarded to the ring-primary via gRPC (`forwarded_requests` ≈ ops × 2/3
per node on a 3-node ring), so forwarding overhead is already included in
every percentile above.

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

### Phase 3 — Leveled Compaction Strategy (LCS) + Write-Stall Backpressure

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

### Phase 4 — Deterministic Fault Injection & Jepsen-style Linearizability Verification (2026-05-21)

#### What changed

Phase 4 adds a complete correctness verification layer consisting of three components:

**1. Porcupine linearizability harness (`internal/linearizability`)**

Every concurrent put/get/delete operation is bracketed with `rec.Begin(input)` /
`rec.End(id, output)` calls that record a [Porcupine](https://github.com/anishathalye/porcupine)
event timeline. After all goroutines finish, `rec.CheckTimeout(d)` verifies that
the observed history is linearisable — i.e., every read returns a value consistent
with *some* sequential ordering of the concurrent writes.

The KV model treats each key as an independent register
(`State = map[string]string`). The `PartitionEvent` hook splits the history by key
before checking, reducing complexity from O(exp(N)) to O(K × exp(N/K)) where K is
the keyspace size — essential for large histories.

**Failed operations** are classified rather than assumed. A failed read
constrains nothing. A failed *write* is modelled as a no-op only when it provably
never reached the store; a write distrikv refused with 503 after applying it
locally is modelled as **applied** (`Output.Applied`), and a write whose effect is
unknown becomes a **pending operation** (`Recorder.EndUnknown`) the checker may
place anywhere or nowhere. Recording every failure as a no-op — the original
model — is exact for in-process failures but reports a correct store as broken
under replication; see "How failed operations are modelled" below for the
evidence that forced the change.

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
the full history for linearizability.

*Fault injection (nemesis).* Load alone does not make a chaos test. With
`--nemesis` the runner kills and restarts cluster members *during the
measurement phase*, so the history is checked against a cluster that is actually
losing and regaining nodes:

```bash
go run ./cmd/chaos \
  --target            localhost:8001 \
  --duration          60s \
  --nemesis           kill-restart \
  --nemesis-services  node2,node3 \
  --nemesis-interval  10s \
  --nemesis-downtime  5s
```

The nemesis loop picks a random victim from `--nemesis-services`, takes it down,
leaves it down for `--nemesis-downtime`, brings it back, waits
`--nemesis-interval`, and repeats. The first strike lands immediately so short
runs still see a fault. It never runs during warmup — warmup exists to fill
connection pools, not to be measured — and it heals its victim on **every path
the runner controls**: a completed cycle, the run deadline, SIGINT/SIGTERM, a
failed disrupt, or a panic in the nemesis loop. It cannot cover what it never
runs through: a `SIGKILL` of the runner itself, or a `Heal` that fails, which is
reported as `heal error` on the window.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--nemesis` | `none` | `none` \| `kill-restart` \| `stop-restart` |
| `--nemesis-services` | *(empty)* | Comma-separated compose service names to draw victims from. Required unless `--nemesis=none` |
| `--nemesis-interval` | `10s` | Delay between the end of one outage and the start of the next |
| `--nemesis-downtime` | `5s` | How long a victim stays down |
| `--nemesis-compose-file` | `docker/docker-compose.yml` | Compose file the nemesis operates on |

`kill-restart` runs `docker compose kill` (SIGKILL) — a real crash, no graceful
drain, recovery driven entirely by what reached disk. `stop-restart` runs
`docker compose stop` (SIGTERM) and is a strictly weaker fault, useful for
A/B-ing whether an anomaly needs a hard crash to reproduce. Both restart with
`docker compose start`, so the victim keeps its named volume and recovers from
its own WAL rather than starting empty. Compose *service* names are used rather
than container names because container names are project- and
compose-version-dependent (`docker-node2-1`, `docker_node2_1`, …).

Before any load is issued, the nemesis is **preflighted**: the docker daemon must
answer and every named victim must be a service the compose file actually
defines. A misspelled service or a missing compose plugin exits 3 rather than
silently degrading the run into a no-fault run that passes for the wrong reason.

*Fault windows.* Every outage is recorded as a `(victim, down-at, up-at)` window
and printed with offsets relative to the start of measurement, so a failure can
be correlated with the fault that produced it. Verbatim from the `stop-restart`
run tabulated below:

```
────────────────────────────────────────────────────────────
  distrikv chaos  PASS  9s @ 4 workers, 5-key space
────────────────────────────────────────────────────────────
  ops:                     89649
  errors:                  85775
  indeterminate writes:    0
  events:                  179298
  nemesis:                 stop-restart on [node1] interval=2s downtime=1s
  faults injected:         3 of 3 attempted
  check_duration:          238ms
  linearizable:            PASS
────────────────────────────────────────────────────────────
  fault windows (offsets from measurement start):
    #1   node1        down +0s      up +1.1s    (1.1s)
    #2   node1        down +3.1s    up +4.1s    (1.1s)
    #3   node1        down +6.1s    up +7.2s    (1.1s)
────────────────────────────────────────────────────────────
```

A window measures the *observed* outage — down-at is stamped before the disrupt
command and up-at after the heal returns — so its span is `--nemesis-downtime`
plus the two commands, which is why 1s of configured downtime reads as 1.1s here
and why strikes are 3.1s apart rather than 3s. The 86k errors against 90k ops are
the expected shape, not breakage: the runner talks only to `--target`, and this
run's only victim *is* the target, so every request during 3.3s of a 9s run is a
refused connection.

A strike counts as *injected* only if its `Disrupt` returned success, and a
strike is never started once the run has ended — the disrupt and heal commands
each run on a context detached from the run deadline, so a command is never
killed halfway and `injected` never has to mean "unknown". `--output=json` emits
the same windows as a `fault_windows` array with millisecond offsets, RFC3339
timestamps, and `up_at`/`up_at_offset_ms`/`down_ms` all `null` together for a
window whose victim never came back. An interrupted run reports `interrupted` and
the truncated duration its verdict actually covers.

*How failed operations are modelled.* A failed operation is **classified, not
assumed**. A read that fails constrains nothing — the value it returned is not
asserted — but a *write* that fails has three possible effects on the store, and
collapsing them into one is how a correct store gets reported as broken:

| Outcome | When | Encoding | What the checker may conclude |
| --- | --- | --- | --- |
| **Never applied** | The transport delivered nothing: a connection refused before the request could be written, an address that was never dialed. Either observed by the client directly, or reported by the forwarding node as `forward_outcome: never-sent` | `Output{Err: true}` — no-op | The value cannot appear. A read that returns it is an anomaly |
| **Applied anyway** | HTTP 503: the ring-primary wrote to its own store and then failed to replicate. There is no rollback, and reads are served by that primary | `Output{Err: true, Applied: true}` — the write happened | The value is present. A read that *misses* it is an anomaly |
| **Unknown** | The connection died mid-request, a deadline expired, or a forwarding hop failed in a way that may already have been applied — `forward_outcome: unknown` | `Recorder.EndUnknown` — a **pending operation** | Either. It may be linearized anywhere, including after the whole history |

Pending is Porcupine's treatment of an unfinished operation and Jepsen's `:info`.
Porcupine's event API cannot express it by dropping the return — an unmatched call
is a dead end for the checker, pinned by `TestKVModelRequiresAReturnForEveryCall`
— so `EndUnknown` synthesizes a return placed after every other event. Because
that API uses each event's index as its timestamp, that is the equivalent of
`Return = +∞`: the operation's interval extends past everything observed, so
"anywhere or nowhere" is exactly what the checker gets to choose from.

The classification reads the **error chain**, not the message, wherever a chain
exists. `internal/client` returns a typed `*StatusError`, so 503 (applied), 502
(the forward hop failed) and other 5xx (unknown) are separated by `errors.As` on
the status code; transport failures the client observes itself are separated by
`errors.Is` down to the `syscall.Errno`.
`TestClassifyWriteEffectDecidesFromTheChain` proves it by wrapping each error in a
shell whose text names nothing, so a correct answer can only come from the chain.

Within the 502 class there is no chain to read — a gRPC failure keeps neither —
so the *forwarding node* classifies it and sends a typed `forward_outcome` field,
which the runner reads in preference to the prose. That is the subject of *How a
502 is separated, and where* below, including why the decision cannot be made
anywhere else and what makes `"never-sent"` a proof rather than a guess.

*What the first real fault-injection runs found.* Measured 2026-08-16 against a
live 3-node docker-compose cluster on an Apple M4 Pro (Colima VM, 8 CPU / 8 GB),
60s, 8 workers, 20-key space, `--put 50 --delete 5`, victims `node2,node3`,
`--nemesis-interval 10s --nemesis-downtime 5s`, 4 of 4 strikes landing — the first
runs with write replication actually wired:

| Nemesis | Ops | Errors | Indeterminate writes | Verdict |
| --- | ---: | ---: | ---: | --- |
| `kill-restart` (SIGKILL) | 242,476 | 47,266 | 13 | **FAIL** |
| `stop-restart` (SIGTERM) | 288,377 | 65,932 | **0** | **FAIL** |

```
  fault windows (kill-restart)          fault windows (stop-restart)
    #1  node2  down +0s     up +5.4s      #1  node2  down +0s     up +5.4s
    #2  node2  down +15.4s  up +20.8s     #2  node3  down +15.4s  up +20.8s
    #3  node2  down +30.8s  up +36.2s     #3  node3  down +30.8s  up +36.2s
    #4  node3  down +46.2s  up +51.7s     #4  node2  down +46.2s  up +51.6s
```

The graceful nemesis is the ablation. SIGTERM drains in-flight requests, so it
produced **zero** unknown-outcome writes — and the history was still rejected.
That eliminates the torn-connection artefact as the cause and leaves exactly one
mechanism: while a replica was down, the ring-primary answered 503 for writes it
had **already applied to its own store**, the model recorded each of them as a
no-op, and the next read of that key — served by the same primary — returned the
refused value. With a 20-key space and tens of thousands of refused writes per
run, that is not a probabilistic artefact; it happens in every fault window.

The checker was right. It detected precisely the caveat the CAP section above
states in bold — *a refused write is not an undone write* — and the model was the
thing that did not encode it. The fix is the three-outcome table above:
`internal/linearizability` learned `Applied` and `EndUnknown`, and the runner
learned to tell the classes apart.

*What re-running them then found.* The refused-but-applied encoding worked — 18,623
and 20,474 writes classified, and **no FAIL**, so the false anomalies were gone.
But both runs came back `UNKNOWN (timeout)`, and for an instructive reason: the
same release bounded the forward RPC with its own deadline, and its rewritten 502
body no longer quoted the underlying transport failure. The runner's text scan was
the only thing separating never-sent from ambiguous, so *every* forward to a downed
primary became a pending operation — 27,356 and 20,519 of them, each overlapping
every later operation on its key.

| Nemesis | Ops | Errors | Refused-but-applied | Indeterminate writes | Verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| `kill-restart` (SIGKILL) | 282,708 | 68,119 | 18,623 | 27,356 | `UNKNOWN` (60.1s) |
| `stop-restart` (SIGTERM) | 259,793 | 57,641 | 20,474 | 20,519 | `UNKNOWN` (60.1s) |

No `--check-timeout` fixes that shape of failure: the search space is combinatorial
in the number of pending operations, not linear in time. Two fixes composed into a
regression, and each was individually correct — which is what made the response a
typed `forward_outcome` field rather than a bigger budget or a restored substring.
With never-sent forwards classified as no-ops again, the pending count falls to the
genuinely ambiguous handful and both runs should reach a verdict.

The standing property is unchanged and is the point: **a FAIL is now a real
consistency bug**, and the printed note says so instead of telling the operator to
discount anomalies near a fault window.

For the record, the pre-fix artefact this replaces was also measured, on a single
local node (Linux x86-64, Intel Xeon 6975P-C, 4 cores), 3 runs per nemesis,
`--duration 9s --warmup 1s --workers 4 --keyspace 5`, with a test stub standing in
for `docker compose`:

| Nemesis | Indeterminate writes (3 runs) | Verdicts (pre-fix) |
| --- | --- | --- |
| `kill-restart` (SIGKILL) | 9, 9, 12 | PASS, **FAIL**, **FAIL** |
| `stop-restart` (SIGTERM) | 0, 0, 1 | PASS, PASS, PASS |

~10 durable-but-unacknowledged writes per run were enough to reject an otherwise
correct history, 2 runs in 3. Those are now pending operations and cannot reject
anything on their own.

*How a 502 is separated, and where.* A 502 from `forwardRequest` hides two causes
behind one code: a hop that was never made, and a `ForwardKey` RPC that failed
*after* the primary may have applied the mutation. The status code cannot separate
them, and the second is genuinely ambiguous — so 502 alone would have to be
unknown. That default is expensive: a pending operation overlaps every later
operation on its key, and a fault window produces thousands of forwarded writes to
a node that is down, which pushes the checker into a timeout instead of a verdict.
Both nemesis runs did exactly that, at 20,519 and 27,356 pending operations.

So the forwarding node decides, and says so: every 502 carries a
`forward_outcome` field, `"never-sent"` or `"unknown"`, and the runner reads it
in preference to the prose. The decision belongs there because that is the last
point at which the error still has its gRPC code, and the message has not yet
been flattened into a sentence.

It is also, measurably, the last point at which the error has *any* identity. A
grpc-go RPC failure is a `*status.Error` carrying a code and a string and nothing
else: `errors.Unwrap` returns nil, and `errors.Is` against `syscall.ECONNREFUSED`
and `errors.As` against `*net.OpError` and `*net.DNSError` all fail.
`TestForwardErrorsCarryNoTypedCause` asserts that, so a future grpc-go that starts
preserving the cause will fail the test and invite the stronger implementation.
There is therefore no chain to reach for on either side of the HTTP boundary — the
code plus the message is the whole of the available evidence, and what changed is
not the kind of evidence but where it is read and what crosses the wire.

What the code buys is the proof. A gRPC stream is only created once the HTTP/2
transport is `READY`, so an error raised *inside* transport creation cannot have
carried any part of the request — and grpc-go frames exactly those errors
distinctively:

```
code = Unavailable  desc = connection error: desc = "transport: Error while dialing:
                             dial tcp 127.0.0.1:45983: connect: connection refused"
```

Requiring that framing *and* a delivery-impossible cause is what makes
`"never-sent"` a claim rather than a guess. A connection that broke after the
request went out reads differently — `transport is closing`, `error reading from
server: EOF`, `connection reset by peer` — and stays unknown, correctly, because
those may have been applied. The framing is also what defuses the trap that
`codes.Unavailable` is a legal *application* code: a primary that could not reach
a replica can produce a message quoting `connection refused` from its own fan-out,
and without the framing requirement that would read as never-sent — the same
mistake, one layer up, that made the runner classify refused-but-applied writes as
no-ops.

| gRPC code | message signature | outcome |
| --- | --- | --- |
| `Unavailable` | dial framing **and** `connection refused` / `no such host` / `no route to host` / `network is unreachable` | **never-sent** |
| `Unavailable` | `name resolver error` — no address was ever dialed | **never-sent** |
| `Unavailable` | anything else: broken stream, draining connection, remote-generated status | unknown |
| `DeadlineExceeded` | any — the 2s bound can fire after the primary applied the write | unknown |
| `Canceled` | any — the caller gave up; the server may not have | unknown |
| anything else | any — a code only the remote can produce implies delivery | unknown |

Two cases are left unknown deliberately even though they are *probably*
never-sent. A dial that fails with `i/o timeout` also could not have created a
transport, but "probably" is not the bar for a never-sent claim. And a blackholed
address — a host that completes the TCP handshake but never finishes the HTTP/2
one, which is what a stopped container looks like — surfaces as
`DeadlineExceeded` with a message about waiting for a load-balancer update that
names no transport failure at all. The asymmetry justifies the caution: a wrong
`"never-sent"` tells the model a write did not happen when it may have, which can
invent an anomaly out of correct behaviour, whereas a wrong `"unknown"` costs only
checker time.

The runner's side of the contract is three-valued rather than two, and the
distinction between the last two is the point. A recognised value is trusted. A
value present but *unrecognised* is unknown, and the text is **not** consulted — a
server that speaks this field is authoritative even when its answer is
unintelligible, and substituting a weaker signal that might contradict it would be
worse than declining to answer. Only an *absent* field falls back to scanning the
message, for a server predating the field.

That fallback is retained, and its limits are the epilogue to this argument. Two
of its four markers could never have fired on this path: gRPC reports an
unresolvable target as `name resolver error: produced zero addresses`, not "no
such host", and an unroutable address as a plain `DeadlineExceeded` naming no
route at all. Matching wording chosen by a library two hops away, for an audience
of humans, is what the typed field replaces. It stays bounded in the safe
direction regardless — an unrecognised body is unknown — so a rewording costs
checker time, never a verdict.

Ordering inside the classifier matters for the same reason. A 503's body quotes
the replication failure underneath it, which during an outage reads
`…connect: connection refused` from the fan-out to the dead replica. The status
code therefore has to be consulted *first*: the message-text path used to see that
body and declare a refused-but-applied write "provably never sent", which is why
the `stop-restart` run above reported 0 indeterminate writes while failing.

The report accounts for both classes separately — `refused-but-applied` for writes
the primary kept, `indeterminate writes` for pending ones — so an operator can see
which mechanism a run exercised. (The first pair of runs above predates the row and
reports no `refused-but-applied` count; the re-run pair has it.)

*Why keys carry a per-run nonce.* `KVModel.Init` is an empty map, so a recorded
history has to start against an empty keyspace. Keys are therefore prefixed with
a per-run nonce, and warmup writes to a disjoint set from measurement — otherwise
the first measured read of a key that warmup already wrote (or that an earlier run
left in a persistent volume) is a value the model believes cannot exist, and the
run reports FAIL on a completely healthy cluster. It did: the pre-nemesis runner
failed 4 out of 4 default-flag runs against a healthy single node with zero
errors, and passes 5 out of 5 with the nonce, on the same populated store. Two
costs, both accepted deliberately: each run leaves 2 × `--keyspace` keys behind,
and the measured keys start cold in the block cache and Bloom filters because
warmup no longer touches them.

*What this test can and cannot detect today.* A kill-restart nemesis is the test
that can actually catch replica divergence — two copies of a key disagreeing after
a crash and recovery — and with write replication wired, more than one copy now
exists, so that is live. Killing a node exercises three things at once: crash
recovery under concurrent load, WAL replay correctness (each restart logs a
`wal_replayed` open), and whether a replica that missed writes while it was down
can serve a stale value afterwards.

What it still cannot detect is bounded by where reads go. Reads are served by the
ring-primary only, so a replica that fell behind is invisible to the checker until
it becomes the primary for that key — the divergence the CAP section documents
(no anti-entropy, no hinted handoff, no read repair) is real but unobserved by this
history. Killing the *primary* of a key makes that key unavailable rather than
inconsistent, which the model records as a no-op or a pending operation depending
on how the request failed. Closing that gap means reading from replicas, which the
current design deliberately does not do.

Exit codes:

| Code | Meaning |
| --- | --- |
| 0 | PASS — history is linearisable |
| 1 | FAIL — non-linearisable anomaly detected |
| 2 | UNKNOWN — check timed out (`--check-timeout`) |
| 3 | Bad flags / startup error (including nemesis preflight failure) |

The chaos runner uses an explicit `http.Transport` with
`MaxIdleConnsPerHost = workers + 64` — the same TCP-pool fix applied in
`cmd/bench` — so high worker counts don't generate `TIME_WAIT` storms.

#### Phase 4 test coverage

In-process linearizability and crash-recovery tests (`internal/store`):

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

Nemesis suite (`cmd/chaos`) — the fault injector and the accounting a verdict
rests on. Docker is never invoked: the scheduler is driven against a fake
nemesis, and the compose driver's command execution is injected, so the
guarantees below are asserted deterministically rather than by observing a live
cluster:

| Test | What it verifies |
| --- | --- |
| `TestSchedulerAlternatesDisruptAndHeal` | Strike loop runs disrupt → outage → heal in order, one recorded window per strike |
| `TestSchedulerHealsWhenCancelledMidOutage` | Cancellation during an outage still heals — a run cannot exit leaving a node down |
| `TestSchedulerHealsAfterPanic` | A panicking disrupt heals on the way out and does not take the run down |
| `TestSchedulerHealsEvenWhenDisruptFails` | A failed disrupt is still healed, since it may have landed partially |
| `TestSchedulerRecordsHealFailure` | A heal that fails is recorded in the window, not swallowed |
| `TestSchedulerDoesNotStrikeAfterCancellation` | No new strike begins once the run context is done |
| `TestSchedulerDisruptIsDetachedFromTheRunContext` | An in-flight disrupt is not interrupted by shutdown, so `disrupt_error` means "failed", never "interrupted" |
| `TestSchedulerVictimsComeOnlyFromTheConfiguredSet` | Victims are drawn only from `--nemesis-services` |
| `TestSchedulerNoopWithoutVictimsOrNemesis` | Absent a nemesis or targets the scheduler is inert; default invocations are unchanged |
| `TestFaultWindowReports` | Fault-window accounting: down/up offsets and duration; an unhealed window reports `up_at`, `up_at_offset_ms` and `down_ms` as null together |
| `TestCountInjectedExcludesFailedStrikes` | `faults_injected` counts landed outages only — a failed disrupt is not an outage |
| `TestFormatFaultWindows` | Human-readable window lines carry index, victim, offsets and duration |
| `TestComposeNemesisPreflight` | Preflight validation: targets must be services the compose file defines, and an unreachable daemon fails the run at startup (exit 3) rather than mid-measurement |
| `TestComposeNemesisBuildsExpectedArgv` | `kill-restart` and `stop-restart` emit the expected `docker compose` argv, healing with `start` |
| `TestComposeNemesisWrapsFailuresWithTheCommand` | A docker failure names the command that produced it |
| `TestParseNemesisFlags` | Flag parsing and rejection of unusable nemesis configurations |
| `TestKVModelTreatsFailedOpsAsNoOps` | Porcupine model contract: a write that never reached the store is a no-op, and one known applied is not |
| `TestKVModelRequiresAReturnForEveryCall` | An unmatched call is a dead end for the checker, which is why an unknown outcome goes through `EndUnknown` rather than dropping the return |
| `TestMakeKeysAreRunScoped` | Warmup and measured keyspaces are separated by a run nonce, so unrecorded warmup writes cannot make a recorded history look illegal |
| `TestProvablyNeverSentClassifiesRealClientErrors` | A refused write from `internal/client` is classified through `errors.Is`/`errors.As` on the preserved chain, not by message text |
| `TestClassifyWriteEffect` | One case per error class: 503 applied, 502 never-sent vs ambiguous, other 5xx unknown, refused/unresolvable never sent, reset/timeout/cancelled unknown, unrecognised shapes unknown |
| `TestClassifyWriteEffectDecidesFromTheChain` | Each error wrapped in a shell whose message names nothing still classifies correctly, so the status code and syscall are read from the chain |
| `TestNeverSentTextIsBoundedToDeliveryFailures` | The one remaining message path admits only failures that delivered nothing; anything ambiguous falls through to unknown |
| `TestClassifyDeleteErr` | A 404 delete is recorded as applied; every other failure survives |
| `TestFinishWriteEncodesEachOutcome` | End-to-end encoding, asserted through the checker: for each failure class, whether a later read may see the value and whether it may miss it |
| `TestFinishWriteToleratesANilRecorder` | Statistics are still counted when history recording is off |
| `TestVerdictNotesMatchTheEncoding` | The printed guidance matches the current encoding — a FAIL is reported as real, and the superseded "failed writes are no-ops" explanation is gone |

Model semantics (`internal/linearizability`) — the three outcomes a failed write
can have, each asserted by running a hand-built history through the checker:

| Test | What it verifies |
| --- | --- |
| `TestRefusedButAppliedWriteIsNotAnAnomaly` | The observed failure shape: a 503-refused put or delete followed by a read of its effect is legal, including a concurrent read of the pre-refusal value |
| `TestNeverSentWriteStaysANoOp` | A write that provably never reached the store did not happen — a read returning its value is still illegal |
| `TestLostAcknowledgedWriteStillFails` | The relaxation does not leak: an acknowledged write that disappears, and a stale read after a known-applied write, both still FAIL |
| `TestPendingWriteIsUnconstrained` | A pending write is legal whether or not any read observes it, may linearize between two reads, and does not license a lost write on another key |
| `TestPendingReturnsComeAfterEveryRecordedEvent` | The mechanism: a pending operation recorded first is still placeable after 20 later reads that never observed it |
| `TestDescribeOperationLabelsEachOutcome` | A failing history distinguishes "the store confirmed this" from "we never found out" |

### Phase 5 — In-process Sharded LRU Block Cache

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
| `BenchmarkBlockCache_ColdVsWarm` | Microbenchmark isolating cold `f.ReadAt` vs warm in-process cache path |
| `TestBlockCache_ZipfHitRate` | Zipfian α=1.1 hit-rate sweep across 4 cache sizes (0, 8, 32, 64 MB); 100k-key dataset, 768 B values, reverse-scan warm + 10k measurement pass |

#### Phase 5 latency profile (2026-05-21, Apple M1 Max)

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

#### Phase 5 Zipfian hit-rate baseline (2026-05-22, Apple M1 Max)

`go test ./internal/store/lsm/ -run TestBlockCache_ZipfHitRate -v -timeout 120s`

**Setup:** 100,000 keys × 768-byte values (≈ 76 MB) across 4 L1 SSTable files (~19 MB each, simulating 4 compaction rounds at the default 4 MB memtable threshold). **Distribution:** Zipfian α = 1.1 — the YCSB/RocksDB canonical exponent, where the top 1% of keys account for ~54% of reads and the top 10% account for ~88%. **Warm pass:** reverse-rank sequential scan (rank 99,999 → 0) so the LRU eviction leaves the hottest blocks resident. **Measurement:** 10,000 seeded-random (seed=42) Zipfian reads.

| Cache size | Hits | Misses | Hit rate |
| ---: | ---: | ---: | ---: |
| 0 MB (disabled) | 0 | 10,000 | 0.0% |
| 8 MB | 8,633 | 1,367 | **86.3%** |
| 32 MB | 9,579 | 421 | **95.8%** |
| 64 MB (default) | 9,920 | 80 | **99.2%** |

The 86.3% figure for 8 MB is analytically consistent: 8 MB holds ~2,048 blocks, covering the top ~10,240 keys, which at α = 1.1 capture ≈ 88% of reads (harmonic sum H ≈ 6.84). The cache earns disproportionate returns on the first few megabytes — 8 MB captures 86% of the benefit that 64 MB achieves.

### Operator notes

- `--workers` sizes the worker pool *and* the HTTP connection pool
  (`MaxIdleConnsPerHost = workers + 64`). Go's defaults cap idle conns/host
  at 2; the bench overrides this so the pool can actually hold a connection
  per worker. Connection reuse also depends on the client draining response
  bodies — see the throughput baseline notes for the bug where it didn't.
- Prefer running the bench inside the cluster's Docker network
  (`docker run --network docker_default …`) rather than through a localhost
  port forward: forwarders (Docker Desktop, Colima/Lima) add a proxy hop to
  every request, and macOS additionally caps concurrency at its ephemeral
  port range. If you must bench through the host, raise
  `kern.ipc.somaxconn` and widen `net.inet.ip.portrange.first`.
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
- **Raft leadership stability** — in-process 3-node cluster asserting the term stays flat on an idle cluster, under injected per-RPC latency above the heartbeat send interval, and with one peer partitioned off; plus the snapshot-vs-heartbeat dispatch boundary and the heartbeat deadline contract
- **PersistentState round-trip** — atomic write/read of `currentTerm` + `votedFor`
- **Concurrent store writes** — 100 goroutines, verified under `-race`
- **LSM-Tree correctness** — MemTable flush, SSTable read, compaction, Bloom filter false-positive rate, leveled-compaction read correctness, tombstone removal, write-stall metrics, L0-count tracking
- **Crash recovery** — WAL replay, torn-write truncation, flushed-data survival, concurrent-write durability, restore-sentinel self-removal, mixed flushed+unflushed data
- **Linearizability** — Porcupine-verified concurrent put/get/delete histories; both in-memory and with L0→L1 compaction in progress
- **Block cache** — hit/miss accounting, nil-safe disabled mode, eviction correctness after compaction, Zipfian α=1.1 hit-rate sweep (0→8→32→64 MB)
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
│       └── main.go          # Jepsen-style chaos runner; Porcupine linearizability check
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
