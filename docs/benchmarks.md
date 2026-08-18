# Benchmarks

Every measured table in one place: the open-loop harness and how to read its
report, the throughput baselines, the etcd ceiling comparison with its durability
confound stated rather than corrected, the `--prefill` read-path measurements, and
the per-phase engine profiles.

Two conventions apply throughout. Any figure meant for a table is measured from
**inside** the cluster's Docker network, never through a host port forward. And
where a number cannot be cleanly attributed — the etcd write comparison being the
clearest case — the confound is disclosed in place instead of the number being
quoted without it.

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

That form goes through the host's port forward, which is fine for a smoke run but
**not** for a number worth publishing: on macOS it adds a proxy hop and caps
concurrent connections at the ephemeral port range. Any figure meant for a table
must be measured from inside the cluster's Docker network — see the methodology
note under
[Throughput baseline with replication](#throughput-baseline-with-replication-2026-08-16-3-node-docker-compose-apple-m4-pro-colima-vm-8-cpu--8-gb).

To measure the **LSM read path** (Bloom filters, block cache, compaction) rather
than the memtable, add `--prefill` and a keyspace larger than the memtable can
hold — see [Read-path engine metrics](#read-path-engine-metrics---prefill) for
why this is required and what the numbers look like.

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
| `bloom_fp_rate` | `false_positives / bloom_hits`. Both terms are exact `atomic.Uint64` counters incremented in `SSTableReader.Get`, so this is a measured rate and not an estimate — only its *display* is rounded, to two decimals. A correctly sized Bloom filter stays well under 1%. |
| `block_cache_hits` / `block_cache_misses` / `hit_rate` | Whether an SSTable block read was served from the in-process LRU cache or cost a `ReadAt`. Zero on a memtable-resident workload — nothing reached an SSTable. |
| `prefill` | Present only with `--prefill`: keys written, achieved keys/s, retries, and failures. A non-zero `failed` aborts the run, because a read-path ratio measured over a partly-written keyspace is not a measurement. |
| `during prefill` | Write-path counters (`flush_bytes`, `compactions`, `write_stalls`) attributed to the prefill phase, which is where they are earned — a read-only measurement window cannot produce a flush. |
| `WAF` (Write Amplification Factor) | `(flush_bytes + compaction_bytes_written) / flush_bytes`. 1.0× means no compaction overhead; 2-3× is normal for size-tiered compaction; Phase 3 (leveled) will trade higher WAF for better read amp. |
| `forwarded_requests` | Ring routed the key to a peer instead of handling it locally. |
| `replication_errors` | Replica fan-out failures — a replica that did not ACK a primary's PUT/DELETE (unreachable, or an explicit rejection). Each one also fails the client's write with `503`, so a non-zero value means writes were **refused**, not merely under-replicated. Reports produced before replication was wired read `0` by construction; see the Phase 3 note under [Status](architecture.md#status). |
| `saturation` | `TRUE` if the arrival queue reached its cap → the cluster couldn't keep up; tail latencies include queue wait. |

### Phase 1 baseline (2026-05-21, 3-node docker-compose on M-series laptop)

| Workload | QPS achieved | p50 | p99 | p999 | WAF | Bloom FP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 80:20 PUT/GET, 1KB values, 60s @ 1000 QPS | 998.6 | 1.7 ms | 26.8 ms | 81.2 ms | 1.83× | 0.00% |

These numbers establish the baseline for Phases 2 (WAL GC optimisation), 3
(leveled compaction), and 4 (chaos). Any regression in p99 or WAF in a
later phase has to be justified.

### Throughput baseline with replication (2026-08-16, 3-node docker-compose, Apple M4 Pro, Colima VM 8 CPU / 8 GB)

> This table is the **pre-H2 baseline** — measured before replicated writes carried
> a per-key sequence. The post-H2 re-measurement of the same workloads, plus the
> flushed-key configuration that discriminates the apply-if-newer comparison's cost,
> is [below](#post-h2-re-measurement-2026-08-17-same-cluster-and-method).

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
on the SSTable path. Reaching that path from a cluster bench requires writing
each key once, which `--prefill` does — see
[Read-path engine metrics](#read-path-engine-metrics---prefill) for the measured
counters and the mechanism behind the zeros.

#### Post-H2 re-measurement (2026-08-17, same cluster and method)

The table above is the **pre-H2 baseline**: it was measured before replicated
writes carried a per-key sequence, so a replica applied them blind. Apply-if-newer
adds a comparison against the version the replica already stores, which for a key
that is not memtable-resident is a Bloom-filtered SSTable lookup taken *inside* the
engine write lock. The cost of that was argued rather than measured, so both
configurations were re-run. Same cluster, same method, same flags.

The heading's date is when this section opened; the flushed-key table below also
carries the n=3-per-build series run on 2026-08-18, and the anchor is left alone so
links from other documents keep resolving.

**Memtable-resident (the baseline's own configuration — Zipf α=1.1, 100k keys):**

| Workload | Target QPS | p50 | p90 | p99 | p999 | Errors | Pre-H2 p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100% writes (PUT) | 1,200 | 1.5 ms | 2.3 ms | **3.8 ms** | 8.1 ms | 0 | 4.6 ms |
| 100% reads (GET) | 6,000 | 0.58 ms | 1.4 ms | **1.7 ms** | 2.6 ms | 0 | 1.7 ms |
| 20% write / 80% read | 3,000 | 0.70 ms | 1.6 ms | **2.5 ms** | 5.3 ms | 0 | 2.5 ms |

No regression on any workload; the write improvement is within run variance. But
this configuration cannot settle the question, and the reason is the same one the
zeros above explain: with `flush_bytes=0` there is no SSTable to read through, so
the comparison answers from the memtable and the flagged path is barely exercised.

**Flushed-key writes (the discriminating configuration — 500k-key prefill, then
writes at 1,200 qps Zipf, so overwrites land on keys that have been flushed):**

Every run below is a fresh cluster with its own prefill. The first pair was the
single run per build that opened the question; the six that follow are the n=3 per
build that was outstanding. **Read the saturation column before the p99 column** —
it is what the rest of this section turns on.

| Date | Build | Run | p99 | Max queue depth | Saturated? |
| --- | --- | ---: | ---: | ---: | --- |
| 08-17 | Post-H2 (`28d410e`) | 1 | **40.5 ms** | 36 | No |
| 08-17 | Pre-H2 (`5c7aa92`) | 1 | **53.9 ms** | 57 | No |
| 08-18 | Post-H2 (`738ece7`) | 1 | **34.8 ms** | 20 | No |
| 08-18 | Post-H2 (`738ece7`) | 2 | 1,119 ms | 513 | **TRUE** |
| 08-18 | Post-H2 (`738ece7`) | 3 | 1,366 ms | 512 | **TRUE** |
| 08-18 | Pre-H2 (`5c7aa92`) | 1 | 1,055 ms | — | **TRUE** |
| 08-18 | Pre-H2 (`5c7aa92`) | 2 | 1,254 ms | — | **TRUE** |
| 08-18 | Pre-H2 (`5c7aa92`) | 3 | 1,260 ms | — | **TRUE** |

A dash in the queue column means the depth was not recorded for that run, not that
it was zero; the saturation flag is what was captured.

Full percentiles for the two unsaturated 08-17 runs, which are the only pair
measured with the whole distribution recorded: post-H2 `28d410e` p50 1.8 ms, p90
4.7 ms, p99 40.5 ms, p999 114.9 ms, 0 errors; pre-H2 `5c7aa92` p50 1.7 ms, p90
4.6 ms, p99 53.9 ms, p999 130.6 ms, 0 errors. Note that p50 and p90 are within
~0.1 ms of each other across builds — the divergence is confined to the tail, which
is the part this regime measures worst.

**The bimodality is the finding, and it is not a per-build one.** Five of the six
n=3 runs tripped the harness's saturation flag (`max_queue_depth` at its 512 cap,
achieved QPS behind target), which means their ~1.2 s p99 figures are **queue wait,
not service latency** — the cluster was not keeping up, so the tail measures how
long requests sat in the arrival queue rather than how long the store took. Across
all eight runs nothing landed in between: each one either held in the tens of
milliseconds or tipped over past a second, and both builds did both — the pre-H2
build held only in its 08-17 run, while the post-H2 build held once on each day.

The reading that fits is that this operating point — 1,200 qps of writes against a
flushed 500k-key store — sits at or above the cluster's **capacity knee**. That is
an interpretation of eight observations at a single offered load, not a measurement
of the knee: nothing here locates it, and the series cannot say whether the two
builds' knees differ, only that 1,200 qps is above both.

That also means **no per-build p99 comparison can be drawn from this series**, and
none is drawn here. Ranking two builds on a metric that is measuring queue depth in
5 of 6 observations would be worse than the n=1 claim it was meant to replace.

**No evidence of regression, from three independent directions — and no stronger
claim than that:**

1. **The unsaturated observations do not separate the builds.** Post-H2 40.5 ms and
   34.8 ms; pre-H2 53.9 ms. Three observations of a metric this bursty, unbalanced
   two-to-one across builds and drawn from two different days, cannot rank anything.
   What they can do is fail to show a cost, and they do fail to show one: nothing
   here puts post-H2 above pre-H2.
2. **Saturation behaviour does not separate them either.** A meaningful per-write
   cost should show up as the post-H2 build tipping over at a *lower* offered load
   than the control. At the one load tested there is no such separation — both
   builds saturate at 1,200 qps. This rules out a cost large enough to move the knee
   past that load; it says nothing about smaller ones, because only one load was
   tested.
3. **The counter evidence, which is independent of latency altogether — and the
   strongest of the three.** The post-H2 run reports `bloom_hits=5917`,
   `bloom_misses=7460` and block cache at 22.9% over 23,273 misses; the pre-H2
   control reports `bloom_hits=0` and no block-cache activity at all, because a
   pre-H2 replica write reads nothing before writing. The read path this change adds
   is demonstrably live in one build and demonstrably absent in the other — so the
   comparison is definitely executing, and the two latency directions above are
   measuring a build that really does perform it.

**Why this regime resists fine-grained comparison, and what would fix that.** Two
confounds sit on top of the saturation. Both builds' *unsaturated* p99 lands near
the 50 ms soft-stall sleep cap
([write-stall backpressure](lsm-engine.md#write-stall-backpressure)), so even there
p99 is largely counting stall *incidence*, which is bursty by construction. And
these were back-to-back heavy runs — each one a 500k-key prefill followed by a 60 s
warmup and a 60 s measurement — on a laptop VM, so thermal and VM drift are in
play; the only unsaturated run of the evening was the first one.

The methodology that would produce a stable per-build comparison is therefore a
**lower offered load** — 800 qps rather than 1,200 — chosen to sit clear of wherever
the knee actually is, so that every run measures service latency instead of queue
wait, with the cluster given time to settle between runs. The 800 figure is a
proposal derived from the one load that failed, not a measured safe point; the run
that establishes it has not been done, and this table does not contain it.

A compaction (97.9 MB written, WAF 4.35) also overlapped the 08-17 post-H2
measurement, which is the confound the control was run to address in the first
place — there was no baseline for this configuration anywhere before it. The 08-16
write table is memtable-resident and the earlier prefilled runs were reads-only.

Reproducing the discriminating run (the prefill is what puts the overwritten keys
on disk; everything after it is the ordinary write workload):

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --prefill \
    --qps 1200 --duration 60s --warmup 60s --mix 100:0:0 \
    --keyspace 500000 --keydist zipf --workers 128 --valuesize 256
```

The one thing this configuration needs that the memtable-resident one does not is
a **fresh cluster per build** (`docker compose … down -v` between runs): the
prefill is what establishes the on-disk state, so comparing two builds means
prefilling for each rather than reusing a volume one of them wrote.

And if the goal is a per-build *latency* comparison rather than a capacity
observation, drop the offered load until every run reports `saturation: false` —
`--qps 800` is the proposed starting point, not a verified one — so that each p99 is
service latency instead of queue wait. Discard, or re-run, any run whose
`saturation` reads `TRUE`: at that point the number describes the arrival queue, not
the store, and averaging it with an unsaturated run mixes two different quantities.

Note for anyone re-running this after the incarnation-epoch change: **both builds
measured above predate it** (`28d410e` and `5c7aa92`). The epoch adds work to the
same critical section — two shifts, plus a handful of atomic counter increments on a
*refused* write only (see
[replication-and-anti-entropy.md → observables](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not)).
No additional disk read, and nothing on the accepted-write path, so these tables are
not invalidated by it; they were simply not measured with it, and this note is here
so the next reader does not have to infer that from the commit dates.

#### Why these numbers replaced the earlier table

The previous baseline (2026-05-22, Apple M1 Max) reported 1,191 /s writes at
p99 = 70 ms and 5,977 /s reads at p99 = 11 ms, measured **without replication**
(the fan-out was never invoked — see the Phase 3 note under
[Status](architecture.md#status)) and through
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
memtable/LSM reads, and HTTP round-trip overhead. A request that lands on a node
which is not the key's ring-primary is forwarded there over gRPC, and that
forwarding overhead is already inside every percentile above. The measured
fraction is **distribution-dependent, not the 2/3 a uniformly-owned 3-node ring
would predict**: the read run above forwarded 167,334 of 361,026 ops (**46%**),
while the uniform read-path runs in
[Read-path engine metrics](#read-path-engine-metrics---prefill) forwarded 160,486
of 239,923 (**67%**, i.e. 2/3 as predicted). The Zipfian shortfall is consistent
with node1 owning more than a third of the *hot* keys — under skew the forward
fraction tracks ownership of the small hot set, not of the whole key space.

### Ceiling — vs etcd (SOTA CP key-value store)

The tables above say what distrikv does. They do not say whether that is fast,
because they have nothing to sit next to. [etcd](https://etcd.io) is the
reference implementation of a consistent distributed key-value store — the
thing Kubernetes stores its entire cluster state in, a decade of production
hardening, Raft consensus done properly — so it is the honest ceiling to
measure against.

**These are different classes of system, and the comparison is a reference
point rather than a competition.** Both are CP, but they buy consistency in
structurally different ways:

- **etcd** commits every write through a single Raft leader. The leader appends
  to its log, replicates to followers, and applies once a majority has
  persisted the entry. Every key in the cluster is serialised through that one
  log. Reads are linearizable by default, which also costs a leader round-trip.
- **distrikv** never puts data on a Raft log (see *Intentional deviations from
  the paper* in [raft.md](raft.md)). A key's ring-primary applies the write
  locally and synchronously
  replicates to its R−1 replicas, returning success only if **every** replica
  ACKs and `503` otherwise. Different keys have different primaries, so writes
  to different parts of the key space proceed in parallel. Reads go to the
  ring-primary directly, with no quorum step.

So the interesting output is not a winner. It is the shape of the gap, and what
the shape says about the trade each design made.

`bench/etcd/` is a separate Go module containing a harness able to run **the same
workload** against etcd: the same key format, the same value bytes, the same
open-loop Poisson arrivals, and the same Zipfian generator (α=1.1, seeded
identically) when asked for it. It launches its own 3-member etcd cluster on
loopback and reports exact latency percentiles per operation type. Keeping it in
its own module keeps etcd's dependency tree out of distrikv's `go.mod`.

The published run below did **not** use every one of those knobs: it ran the
harness at its defaults for key distribution (uniform) and worker count (256),
against a distrikv baseline that used Zipf α=1.1 and 128 workers. Both
asymmetries are disclosed under the table; the runbook reproduces the run as it
actually happened, and notes the matched-parameter variant alongside it.

#### Results (2026-08-17, Apple M4 Pro, etcd 3.7.1 native vs distrikv 3-node Colima)

etcd: 3-member cluster on loopback, native APFS, uniform 100k-key distribution,
256 B values, 60 s runs after a 5 s warmup, **256 workers** (harness default),
0 errors across all three workloads. Offered load: 2,000 / 6,000 / 3,000 QPS.
distrikv: 3-node docker-compose inside Colima VM (8 CPU / 8 GB), Zipfian α=1.1,
**128 workers**, same value size and duration, offered 1,200 / 6,000 / 3,000 QPS
— reproduced from the validated baseline above.

| Workload (60s, 256 B values) | distrikv offered | distrikv achieved | etcd offered | etcd achieved | distrikv p99 | etcd p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100% writes (PUT) | 1,200 /s | **1,199 /s** | 2,000 /s | **1,995 /s** | 4.6 ms | 73.7 ms |
| 100% reads (GET) | 6,000 /s | **6,017 /s** | 6,000 /s | **6,009 /s** | 1.7 ms | 0.79 ms |
| 20% write / 80% read | 3,000 /s | **3,000 /s** | 3,000 /s | **2,993 /s** | 2.5 ms | 80.9 ms† |

† Mixed p99 is the worse of the two op types; etcd's gets queue behind puts.

**Both write rows are achieved-equals-offered, so neither is a throughput
ceiling** — and the two rows were not offered the same load. See mechanism 1
below before reading anything about write throughput out of this table.

| Per-op latency, mixed workload (20:80) | distrikv p50‡ | etcd p50 | distrikv p99‡ | etcd p99 |
| --- | ---: | ---: | ---: | ---: |
| PUT | 0.70 ms | 34.1 ms | 2.5 ms | 70.3 ms |
| GET | 0.70 ms | 36.2 ms | 2.5 ms | 80.9 ms |

‡ `cmd/bench` reports one merged latency distribution, not a per-op split, so
both distrikv rows repeat the overall figure — its PUT and GET tails are not
measured separately. Only the etcd columns are genuinely per-op (that split is
one of the harness's deliberate divergences; see `bench/etcd/README.md`).

**The honest headline is the read comparison, not the write comparison.** At
matched throughput (~6,000 reads/s), etcd's p99 is **0.79 ms vs distrikv's
1.7 ms** — etcd reads are ~2× faster at the tail despite paying a linearizable
quorum round-trip. distrikv's extra latency is consistent with the HTTP hop plus
the internal gRPC forward that a non-primary node pays — measured at 46% of
requests on this Zipfian baseline (167,334 forwards over 361,026 reads). The
read path has no fsync on either side, so this comparison is clean.

The write comparison is **not clean** — see the durability confound below.

> **Parameter mismatches (disclosed).** Two knobs differed between the columns,
> both because the etcd run took the value both harnesses default to while
> distrikv's baseline had overridden it on the command line:
>
> - **Key distribution.** The etcd harness used uniform random over 100k keys
>   (its default generator); distrikv's baseline used Zipfian α=1.1 over 100k
>   keys. For the read and mixed rows this means etcd paid uniform cache
>   pressure while distrikv benefited from hot-key locality. For writes the
>   difference is smaller (both distributions produce distinct keys at similar
>   rates over 60 s at these QPS levels), but it is not zero.
> - **Worker count.** etcd ran with 256 concurrent workers, distrikv with 128.
>   Neither run saturated (`saturation: false`, `max_queue_depth ≪ cap` on both
>   sides), and latency is measured from the *scheduled* arrival time on both
>   harnesses, so the concurrency difference does not inflate either tail
>   through coordinated omission — but it does mean the two stores were held at
>   different in-flight depths, and etcd's deeper pool is a mild handicap on its
>   fsync-bound write path.
>
> The runbook below reproduces the run as it happened; matching both knobs is
> listed there as the variant that removes these two asymmetries.

#### Why the gap has the shape it does

Three mechanisms account for it, and they push in different directions — which
is why a single "distrikv is N× faster/slower" number would be meaningless.

1. **Write throughput: Raft log serialisation vs ring fan-out.** etcd's leader
   is a single serialisation point for the whole key space: every PUT is an
   append to one log, in one order, on one node, whose disk and CPU are shared
   by all keys. distrikv has three concurrent write paths, one per ring
   primary, each with its own WAL and its own fsync stream. A 3-node distrikv
   ring can therefore absorb roughly 3× the independent write streams before
   any single node's disk becomes the bottleneck, and that advantage grows with
   node count, where etcd's write throughput is flat in node count (and mildly
   *decreasing*, since a larger majority means more replication work per
   commit).

   **This claim is supported by structure only — nothing on this page measures
   it.** distrikv's write row was offered 1,200 QPS and achieved 1,199; etcd's
   was offered ~2,000 and achieved 1,995. Both systems delivered the load they
   were asked for, unsaturated (`saturation: false` on both), so **neither
   number is a throughput ceiling** — they are two different offered rates, and
   the 1,199-vs-1,995 gap in the table is a gap in what was *requested*, not in
   what either store could do. Nothing here establishes that ring fan-out has a
   higher write ceiling than a Raft log; it establishes only that both stores
   were comfortable below theirs.

   The measurement that would settle it is a **saturation sweep**: raise offered
   write QPS on each store in steps until achieved QPS stops tracking target or
   `saturation: true` trips, and report the knee. Run against distrikv at 1, 2
   and 3 nodes, that also tests the scaling half of the claim — the ceiling
   should move with node count for the ring and stay flat for etcd. Until that
   exists, treat this mechanism as a design argument, not a result. (The
   durability confound below would have to be neutralised first — a ceiling
   measured against fsyncs that may be absorbed by a VM page cache is not a
   ceiling.)

2. **Read latency: linearizable quorum read vs primary-local read.** An etcd
   `Get` is linearizable by default: it goes through the leader's read-index
   protocol, confirming with a quorum that the leader is still the leader before
   answering. distrikv reads route to the key's ring-primary and answer from its
   memtable/LSM with no coordination at all. distrikv's read path therefore does
   strictly less work, and buys a strictly weaker guarantee — it is a read of
   the primary's latest state, which is only linearizable to the extent the
   primary has not just crashed with an in-flight write. **This is the largest
   single confound in the read comparison and the reason the read rows should
   not be read as "distrikv's reads are better".**

3. **Durability quorum: majority-of-3 vs all-of-R.** etcd commits on a majority
   — leader plus one follower — so it tolerates one dead or slow member and
   keeps serving writes. distrikv requires *every* replica in `ring.GetN(key,R)`
   to ACK, so with R=2 a single unreachable replica means `503` for every key
   whose replica set contains it. Both designs wait for exactly one remote ACK
   on the happy path, which is why their per-write latency is closer than the
   throughput difference suggests. The divergence is entirely in the unhappy
   path: etcd degrades gracefully, distrikv refuses. The
   [nemesis runs](chaos-harness.md) quantify that refusal (48,283 refused-but-applied writes across four fault
   windows) — and it is a deliberate choice, not an oversight, because refusing
   is what keeps the store CP without a consensus log.

#### What etcd buys that distrikv does not have

Worth stating plainly, because the throughput table flatters distrikv and the
list below is where the decade of engineering actually went:

- **Linearizable reads**, guaranteed by protocol rather than by "reads go to
  the primary and the primary usually has the data".
- **Consensus-backed durability.** A committed etcd write survives any minority
  failure with the "if committed, all future leaders have it" guarantee.
  distrikv's ring replication has no such property — the ring-primary and its
  replicas can diverge under partition. Anti-entropy repairs that divergence
  afterwards, within the limits
  [its own section](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not)
  states, but there is no
  protocol-level guarantee at commit time.
- **Dynamic membership.** etcd adds and removes members through a Raft config
  change, online. distrikv's ring is static and requires a cluster restart.
- **Availability under single-member failure**, per the quorum point above.
- **Watches, leases, MVCC revisions, and mini-transactions** (`Txn`,
  compare-and-swap). distrikv has none of these; `distrikv-cli watch` polls.
- **Multi-key atomicity** via `Txn`. distrikv has no cross-key primitive at all.

#### What distrikv trades for

- **Write throughput that scales with node count** rather than being pinned to
  one leader's disk.
- **Lower read latency**, by charging the caller a weaker guarantee for it.
- **A far smaller operational surface**: no consensus log to compact, no
  snapshot transfer to tune, no revision history to defragment.
- **Legible failure semantics** in exchange for availability: `503` = the
  primary took it but a replica did not ACK; `502` = the primary was
  unreachable, with `forward_outcome` saying whether anything was written.

#### Confounds this comparison does not control for

Disclosed rather than corrected, because pretending they are not there would be
worse than the asymmetry itself:

- **Durability cost (the dominant write confound).** etcd fsyncs its Raft log
  per commit using `F_FULLFSYNC` on macOS — a real barrier to stable storage
  that costs ~10–30 ms per flush and accounts for nearly all of etcd's 37 ms
  write p50. distrikv's WAL fsyncs go through Colima's virtiofs, where flushes
  are likely absorbed by the VM page cache and may never reach stable storage
  on the host. **The write comparison is therefore substantially "real
  durability vs maybe-durability", not architecture.** The architectural claim
  — that Raft serialises all writes through one log while the ring fans out to
  parallel primaries — is supported by structure (three independent write
  streams vs one serialisation point), but these numbers cannot cleanly
  separate it from the durability asymmetry. Write latencies are not directly
  comparable across these two storage stacks.
- **Deployment shape (previously misstated).** An earlier draft of this section
  said the native-vs-VM difference "favours etcd". The opposite is true for
  writes: native APFS with `F_FULLFSYNC` is *expensive* (~10–30 ms), while
  virtiofs through a VM page cache is *cheap* (flushes may be no-ops). The
  controlled variant — running distrikv natively on APFS too — would make its
  write latencies materially *worse*, not better, because its fsyncs would
  then pay the same real cost etcd's do. That variant is in the runbook below.
- **Transport and encoding.** distrikv speaks HTTP/1.1 with JSON bodies; etcd
  speaks gRPC with protobuf over HTTP/2 with request multiplexing. The 256-byte
  value is identical, but distrikv additionally pays JSON framing and one
  connection per in-flight request.
- **Read consistency**, as above. etcd is left at its default linearizable read
  rather than being weakened with `WithSerializable()`, so etcd is measured at
  the guarantee it advertises.
- **Maturity.** etcd's numbers reflect years of tuning against exactly this
  kind of benchmark. distrikv's reflect a few weeks.

#### Running it (Mac validation runbook)

```bash
# 1. Install etcd (the harness launches it; it never needs to be running first)
brew install etcd
etcd --version        # the published table was measured on 3.7.1

# 2. Build the harness. Separate module, so build from its own directory.
cd ~/documents/github/distrikv/bench/etcd
GOPROXY=direct go mod download
CGO_ENABLED=0 go build -o etcd-ceiling .

# 3. Run the three workloads exactly as the published table was measured:
#    offered 2000 / 6000 / 3000 QPS, 60s after a 5s warmup, 256 B values,
#    100k-key uniform distribution and 256 workers (both harness defaults,
#    left explicit here so the run is reproducible from this block alone).
./etcd-ceiling --qps 2000 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform \
  2>&1 | tee etcd_writes.txt

./etcd-ceiling --qps 6000 --duration 60s --warmup 5s \
  --mix 0:100:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform \
  2>&1 | tee etcd_reads.txt

./etcd-ceiling --qps 3000 --duration 60s --warmup 5s \
  --mix 20:80:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform \
  2>&1 | tee etcd_mixed.txt
```

**Matched-parameter variant** — removes the two disclosed asymmetries by driving
etcd at distrikv's distribution, worker count and offered write load. Run it in
addition to the block above, not instead of it, so the published numbers stay
reproducible:

```bash
./etcd-ceiling --qps 1200 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf \
  2>&1 | tee etcd_writes_matched.txt
```

Then re-measure distrikv so both columns come from one sitting on one machine.
Note this reproduces the distrikv column *as published* — Zipf, 128 workers,
1,200 QPS offered on writes — which is why it is not flag-for-flag identical to
the etcd block above; the matched-parameter variant is the one that closes that
gap, from the etcd side:

```bash
cd ~/documents/github/distrikv
GOPROXY=direct docker compose -f docker/docker-compose.yml up -d --build && sleep 20
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench

for spec in "1200 100:0:0 writes" "6000 0:100:0 reads" "3000 20:80:0 mixed"; do
  set -- $spec
  docker run --rm --network docker_default -v "$PWD:/b" alpine \
    /b/bench-linux --target node1:8001 --qps $1 --duration 60s --warmup 5s \
    --mix $2 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf \
    2>&1 | tee distrikv_$3.txt
done
```

Optional, and the thing that removes the biggest confound — the same distrikv
workload against three **native** nodes, so both stores are measured outside a VM:

```bash
cd ~/documents/github/distrikv
go build -o distrikv-node ./cmd/node
# start three nodes on 8001/8002/8003 per the "Running the Cluster" section, then:
go build -o bench ./cmd/bench
./bench --target localhost:8001 --qps 1200 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf
```

**Validity gates, in order of what invalidates a run:**

1. `errors: 0` in every run. A non-zero count means the run measured failure
   handling, not the store.
2. `saturation: false`. A saturated queue means the tail is arrival-queue wait
   and the run describes the harness, not the store.
3. `achieved_qps ≈ target`. If achieved falls materially below target with
   `saturation: false`, the arrival process was starved and the run is suspect.

To drive an etcd cluster that is already running instead of launching one:

```bash
./etcd-ceiling --no-cluster --endpoints 127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383 --qps 2000
```

`bench/etcd/README.md` documents the harness's flags and the exact points where
its methodology matches — and deliberately diverges from — `cmd/bench`.

### Read-path engine metrics (`--prefill`)

Every cluster bench before this one reported `bloom_hits=0`, `bloom_misses=0`,
`block_cache_hits=0`, `block_cache_misses=0` and `compactions=0`. The counters
were never broken — they are wired from `lsm.WithMetrics` through
`store.NewWithMetrics` (`cmd/node/node.go`) and incremented in `sstable.go`
(Bloom, block cache) and `compaction.go`. The workload simply never left memory.

**Mechanism.** `Memtable.Put` subtracts the replaced entry's bytes before adding
the new ones, so the memtable accounts for **live** entries only. A Zipfian
workload rewrites a small hot set, so resident bytes plateau far below the 4 MB
flush threshold no matter how many ops are issued: at 16 B keys and 256 B values
it takes roughly 15,400 *distinct* keys to fill a memtable, and a
Zipfian α = 1.1 draw over 100,000 keys does not reach that many distinct keys in
a 60 s run. No flush means no SSTable, and no SSTable means there is no Bloom
filter, block cache or compaction to measure. The zeros were honest.

**`--prefill`** writes every key in `[0, keyspace)` exactly once, before the
warmup window, so resident bytes grow monotonically: the memtable fills,
flushes, L0 accumulates, compaction runs, and reads must then traverse SSTables.

Why a flag and not just existing flags: `--mix 100:0:0 --keydist sequential`
gets close, but coverage of the keyspace becomes a function of
`--qps × --duration`, arrivals are Poisson-scheduled rather than exhaustive, and
any write that fails leaves a hole. A hit rate measured over a partly-written
keyspace is not a hit rate — reads of never-written keys are Bloom *misses* that
inflate the miss count and deflate the cache hit rate. `--prefill` is therefore
exhaustive, closed-loop (not rate-limited), and retries transient
`503 ErrWriteStalled` backpressure, which a prefill is the workload most likely
to provoke. If any key cannot be written after its retry budget, the run
**aborts** rather than publishing a number that looks measured but is not.

```bash
# Prefill 500k keys, then measure a read-only Zipfian window over them.
# (This is the command that produced the measured table below. The older
#  dev-container table further down used --keyspace 200000.)
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --prefill \
    --qps 4000 --duration 60s --warmup 60s \
    --mix 0:100:0 --keyspace 500000 --keydist zipf \
    --workers 128 --valuesize 256
```

#### Measured (2026-08-17, 3-node cluster, Apple M4 Pro, Colima VM 8 CPU / 8 GB, 500k-key prefill)

Prefill phase — 500,000 keys × 256 B, written once each:

| Prefill | Value |
| --- | ---: |
| Keys written / failed | 500,000 / **0** |
| Throughput | 3,071 keys/s |
| `compactions_total` | **6** |
| Compacted data | 314.5 MB |
| Write amplification (WAF) | **4.28×** |
| `write_stall_count` | 0 |
| Retries / failures | 0 / 0 |

Read phase — 60 s read-only windows at 4,000 QPS target, 0 errors across all
three patterns:

| Read pattern (60s, 4,000 QPS, 0 errors) | `bloom_hits` | `bloom_misses` | `bloom_fp_rate` | **cache hit rate** | p50 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Zipf α=1.1, 500k keys | 125,648 | 0 | — | **97.2%** | 634 µs | 1.60 ms |
| Uniform, 500k keys | 77,948 | 0 | — | **70.9%** | 633 µs | 1.62 ms |
| Uniform, 1M keys (half absent) | 39,078 | 39,755 | **0.01%** | 71.1% | 632 µs | 1.62 ms |

Three things the Mac validation adds over the DevSpaces measurement (below):

1. **The Zipf-vs-uniform cache gap at 500k keys is 26 points** (97.2% vs 70.9%),
   compared to 32 points at 200k on the dev container (99.3% vs 60.8%). A
   larger working set at fixed 64 MB cache naturally compresses the gap — the
   cache covers fewer of the keyspace's distinct blocks — and the effect is in
   the expected direction.
2. **Bloom false-positive rate measured on the negative-lookup run: 0.01%.**
   Of 39,078 bloom hits in the 1M-key run (half absent), 0.01% were false
   positives — well below the 1% design target. This is measured, not estimated:
   `bloom_fp_rate` is `BloomFalsePositives / BloomHits`, and both are hard
   `atomic.Uint64` counters in `internal/metrics`, incremented in
   `SSTableReader.Get` (`internal/store/lsm/sstable.go`) — `BloomHits` on every
   filter positive, `BloomFalsePositives` at both sites where that positive then
   fails to produce the key (no block covers it, and the block scan misses). The
   only imprecision is display: the harness prints the rate to two decimals, so
   0.01% pins the underlying false-positive count to single digits rather than to
   an exact value. The DevSpaces run reported exactly 0.00% at 200k scale; 500k
   keys with more L0 residency between compactions produced the first measurable
   (but negligible) FP rate.
3. **Latency stable across patterns.** p50 ~633 µs and p99 ~1.6 ms regardless
   of access pattern — the SSTable read path is dominated by the HTTP + ring
   forward hop, not by cache hits/misses. Cache misses trade a `ReadAt` syscall
   for a sharded map lookup but both are sub-microsecond relative to the network.

#### Earlier measurement (2026-08-17, 3-node cluster, Linux dev container, 200k-key prefill)

**Read this table for the engine ratios, not for throughput.** All three nodes
ran on one fsync-bound container filesystem, which held the prefill to 406
keys/s and every read pass to ~400 QPS; the cluster throughput and latency
numbers remain the M4 Pro figures in the table above. What transfers across
hardware is which counters move and in what proportion.

Prefill phase — 200,000 keys × 256 B, written once each:

| Prefill | Value |
| --- | ---: |
| Keys written / failed | 200,000 / **0** |
| Wall time (throughput) | 492 s (406 keys/s) |
| `flush_bytes` (node1) | 34.9 MB across 8 flushes |
| `compactions_total` (node1) | **2** |
| `compaction_bytes_written` | 52.3 MB |
| `write_stall_count` | 0 |
| Write amplification (WAF) | **2.50×** |

Read phase — three 60 s read-only windows over that same prefilled data,
changing only the access pattern:

| Read pattern | `bloom_hits` | `bloom_misses` | cache hits | cache misses | **hit rate** | `bloom_fp_rate` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Zipf α=1.1, 200k keys, 20 s warm | 12,624 | 0 | 11,675 | 949 | **92.5%** | 0.00% |
| Zipf α=1.1, 200k keys, 60 s warm (cache already warmed by prior passes) | 12,547 | 0 | 12,465 | 82 | **99.3%** | 0.00% |
| Uniform, 200k keys | 7,136 | 0 | 4,338 | 2,798 | **60.8%** | 0.00% |
| Uniform, 400k keys (half absent) | 3,536 | **3,963** | 2,908 | 628 | 82.2% | 0.00% |

Four things this measures that the unit-level benchmarks could not:

1. **The read path is real end to end.** Client → HTTP → ring → gRPC forward →
   LSM → SSTable → Bloom filter → block cache all execute, and the counters
   reconcile: of 23,918 reads, 11,253 were forwarded to peers, leaving 12,665
   served locally against 12,624 Bloom lookups — essentially every locally
   served read consulted an SSTable.
2. **The cache earns its keep on skew, and only on skew.** Same data, same
   cluster, same cache: 92.5% under Zipfian versus **60.8% under uniform**. The
   32-point gap is the block cache's entire value proposition, isolated by
   changing one flag.
3. **The hit rate is warm-window-dependent, and converges on the unit-test
   figure.** A 20 s warmup still pays first-touch misses on cold blocks (92.5%);
   a longer warm window reaches **99.3%**, which agrees with
   `TestBlockCache_ZipfHitRate`'s 99.2% at the same 64 MB default cache. The
   cluster read path and the synthetic bench measure the same thing.
4. **Bloom filters only pay off on absent keys — and here they pay off
   perfectly.** In these runs `bloom_misses` is 0, and the reason has two parts.
   Structurally, `Compactor.Compact` merges *all* L0 plus *all* L1 into a single
   output SSTable with no size-based split, so **L1 is always exactly one file**
   at any data volume, and neither `LSMTree.Get` nor `SSTableReader.Get` prunes
   by key range before consulting the filter. Contingently, the prefill happened
   to end just after a compaction, leaving `l0_file_count=0` — so a read
   consulted exactly one filter for a key that was present, which a Bloom filter
   can only answer "might contain". Had 1–3 L0 files been resident (the normal
   state between compactions), each read would have paid one Bloom *miss* per L0
   file lacking the key, so this row is a floor on Bloom activity, not a ceiling.
   Point half the reads at keys that were never written and the negative path
   appears unambiguously: **3,963 of 7,499 lookups (52.8%) were answered
   "definitely absent" without touching a block**, at a **0.00% false-positive
   rate** (0 of 3,536 positives).

#### Reproducing on a Docker cluster

```bash
# 0. Fresh cluster — a prefill measurement must not inherit another run's data
docker compose -f docker/docker-compose.yml down -v
GOPROXY=direct docker compose -f docker/docker-compose.yml up -d --build && sleep 20
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench

# 1. Prefill + Zipfian read window (one command: prefill runs before warmup)
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --prefill \
    --qps 4000 --duration 60s --warmup 60s --mix 0:100:0 \
    --keyspace 500000 --keydist zipf --workers 128 --valuesize 256 \
  2>&1 | tee bench_readpath_zipf.txt

# 2. Same data, uniform reads — isolates what the cache is worth on skew
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 \
    --qps 4000 --duration 60s --warmup 60s --mix 0:100:0 \
    --keyspace 500000 --keydist uniform --workers 128 --valuesize 256 \
  2>&1 | tee bench_readpath_uniform.txt

# 3. Same data, double the keyspace — half the reads are absent keys, which is
#    the only condition under which the Bloom filter's negative path is visible
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 \
    --qps 4000 --duration 60s --warmup 60s --mix 0:100:0 \
    --keyspace 1000000 --keydist uniform --workers 128 --valuesize 256 \
  2>&1 | tee bench_readpath_negative.txt
```

Validity gates, in order of what invalidates the run:

- `failed=0` on the prefill line. Any other value aborts the run by design.
- `errors: 0` and `saturation: false` on each read pass.
- `compactions` non-zero on the `during prefill` line — if it is 0, the keyspace
  was too small to fill 4 memtables per node and no SSTable merge ever ran.
- `block_cache_hits` non-zero in steps 1–3 and `bloom_misses` non-zero in step 3.
  A zero there means reads never left the memtable.

Steps 2 and 3 deliberately skip `--prefill`: the cluster already holds the data,
and re-prefilling would only rewrite it.

#### On the 86.3% cache hit rate

The frequently quoted 86.3% is the **8 MB point of a cache-size sweep** in
`TestBlockCache_ZipfHitRate`, not the shipped configuration: `defaultBlockCacheBytes`
is **64 MB**, which that same sweep measures at 99.2%. The cluster-level number
above (99.3% warm) matches the default-configuration figure, not the 8 MB one.
Any summary of this project's cache performance should quote 99.2–99.3% at the
64 MB default, or state the cache size alongside 86.3%.

#### What the scrape can and cannot see

`cmd/bench` scrapes `/metrics` from the **first `--target` only**. Reads for keys
owned by another node are forwarded and served there, so their Bloom and cache
counters are recorded on that peer: roughly half of the ops above never touch
node1's engine. Absolute counter values are therefore one node's share, not the
cluster total — but every **ratio** (hit rate, FP rate, WAF) is computed from
counters taken on the same node over the same window and is unaffected.

Write-path counters (`flush_bytes`, `compactions_total`, `write_stall_count`) are
reported for the **prefill phase** rather than the measurement window, because a
read-only window cannot produce a flush — and neither can a *write* window after
a full prefill: once every key exists, further writes are same-size overwrites,
which the memtable's live-entry accounting does not count as growth. The flushes
and compactions that put the data on disk all belong to the prefill.

## Operator notes

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
- A node started on a volume with accumulated data logs
  `lsm: armed compaction at open` when it inherits an L0 backlog; the recovery
  window is then bounded by one compaction pass rather than being open-ended.
  If writes come back `503` with `write stalled: L0 compaction backlog` in the
  message, the node is alive and merging — check `l0_file_count` and
  `write_stall_count` on `/metrics` and expect them to fall. A node that is
  *unreachable* fails differently (`502`, or a replication deadline), which is
  the distinction `ErrWriteStalled` exists to preserve.

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
