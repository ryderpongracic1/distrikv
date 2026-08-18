# distrikv

[![CI](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml/badge.svg)](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml)

A distributed key-value store written in Go from scratch, hardened through eight
phases of measured bug-hunting (P1–P8 under [Status](docs/architecture.md#status))
on top of the seven developmental phases that built it. The defects those phases
found — including a compaction path that dropped acknowledged writes, and a leader
that had never sent a heartbeat — are documented where they happened rather than
quietly fixed. It is not production-grade; it is measured, and it says what the
measurements found.

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

## Documentation

The deep material lives in [`docs/`](docs/). Start wherever the question is.

| Document | What is in it |
| --- | --- |
| [architecture.md](docs/architecture.md) | How the pieces fit, the consistent hash ring, the CAP position, and the phase-by-phase status |
| [lsm-engine.md](docs/lsm-engine.md) | MemTable → SSTable, the WAL, leveled compaction, write-stall backpressure, the sharded block cache |
| [replication-and-anti-entropy.md](docs/replication-and-anti-entropy.md) | The CP write path, *a refused write is not an undone write*, WAL-cursor catch-up, and the convergence claims this design withholds |
| [raft.md](docs/raft.md) | Raft's honest scope: leader election, heartbeats, a complete §5.3/§5.4.2 log, and the node-health transitions it carries |
| [chaos-harness.md](docs/chaos-harness.md) | The Porcupine model, the nemesis, the convergence gate, the counterexample output, and every measured run |
| [benchmarks.md](docs/benchmarks.md) | Every table, including the etcd ceiling comparison with its durability confound stated rather than corrected |
| [defect-log.md](docs/defect-log.md) | **Thirteen real defects**, numbered, with the evidence that exposed each one |
| [api-and-cli.md](docs/api-and-cli.md) | The HTTP REST surface and `distrikv-cli` |
| [development.md](docs/development.md) | Running the tests, repository layout, code-quality conventions |
| [MIGRATION.md](MIGRATION.md) | v1 → v2 data migration |

---

## Architecture in one screen

**Raft decides who is alive. The hash ring decides where data lives.** That split
is a deliberate design decision, not an omission — the Cassandra-style arrangement
rather than the etcd-style one. Raft settles "who is alive" *through its own log*:
the leader commits node-health transitions, so every node reads the same
cluster-consistent health view rather than only the leader knowing anything.

- **Consistent hash ring** (`internal/cluster`) — 150 virtual positions per node on
  a `uint32` ring. `GetN(key, R)` returns the `R` distinct physical nodes that own
  a key: the ring-primary plus its replicas.
- **Raft** (`internal/raft`) — randomised election timeouts with a pre-vote phase,
  majority-vote elections, heartbeats, and log replication implemented to §5.3 and
  §5.4.2 with apply-on-commit against an opaque `StateMachine`. **Data never
  touches the Raft log**: the log carries cluster control state, and what it
  carries is **node health**. The leader aggregates its own heartbeat outcomes
  with hysteresis and commits `health-down`/`health-up` transitions, so every node
  — not just the leader — reads the same cluster-consistent health view;
  [raft.md](docs/raft.md) states the encoding, the defaults, and the test that
  proves a follower learns health from consensus alone.
- **LSM-Tree engine** (`internal/store/lsm`) — MemTable → immutable buffer →
  L0 SSTables → one merged L1, each SSTable carrying a Bloom filter, fronted by a
  32-shard LRU block cache, with every write WAL-fsynced first.
- **Replication** — the ring-primary applies the mutation locally, then
  synchronously replicates to the other `R−1` replicas over gRPC, returning success
  only if **every** replica ACKs and `503` otherwise. That makes distrikv **CP**.

The consequence of the split is that Raft's "if committed, all future leaders have
it" guarantee does **not** apply to data. Under partition the ring-primary and its
replicas can diverge; anti-entropy repairs that divergence after the fact, within
the limits [its own section](docs/replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not)
states rather than glosses.

---

## Measured results

Every number below was measured on the hardware named next to it, with the load
generator running **inside** the cluster's Docker network. Full tables, methodology
and confounds are in [benchmarks.md](docs/benchmarks.md) and
[chaos-harness.md](docs/chaos-harness.md).

**Throughput** — 3-node docker-compose, Apple M4 Pro, Colima VM 8 CPU / 8 GB,
open-loop Poisson arrivals, 256 B values, 0 errors, not saturated. Every PUT pays
the full replicated write path: WAL fsync on the primary plus a synchronous gRPC
ACK from each replica.

| Workload | Achieved | p50 | p99 |
| --- | ---: | ---: | ---: |
| 100% reads (GET) | **6,017 /s** | 0.54 ms | **1.7 ms** |
| 100% replicated writes (PUT) | **1,199 /s** | 1.7 ms | **4.6 ms** |
| 20% write / 80% read | 3,000 /s | 0.70 ms | 2.5 ms |

Re-measured on 2026-08-17/18 after replicated writes began carrying a per-key
sequence, in both the configuration above and one that forces the comparison onto
the SSTable path: **no evidence of regression from three independent directions**
(unsaturated latencies, saturation behaviour, and engine counters proving the
comparison path live in one build and absent in the other). The flushed-key
configuration turned out to sit at the cluster's capacity knee, so no per-build
p99 ranking is drawn from it — the eight-run table with the saturation column is in
[benchmarks.md → Post-H2 re-measurement](docs/benchmarks.md#post-h2-re-measurement-2026-08-17-same-cluster-and-method).

**LSM read path** — 500k-key prefill so reads must traverse SSTables, then three
60 s read-only windows at 4,000 QPS over the same data, changing only the access
pattern:

| Read pattern | Block-cache hit rate | Bloom FP rate |
| --- | ---: | ---: |
| Zipf α=1.1, 500k keys | **97.2%** | — |
| Uniform, 500k keys | **70.9%** | — |
| Uniform, 1M keys (half absent) | 71.1% | **0.01%** |

The 26-point Zipf-vs-uniform gap is the block cache's entire value proposition,
isolated by changing one flag. The 0.01% Bloom false-positive rate is **measured,
not estimated** — `BloomFalsePositives / BloomHits`, both hard counters.

**Chaos and convergence** — Jepsen-style nemesis with a Porcupine linearizability
check and a convergence gate that reads every measured key from every node the ring
says should hold it:

```
  refused-but-applied:     33,449
  converged:               true (after 562ms, 20 keys × 40 node reads)
  linearizable:            PASS
```

That line is the design's central caveat and its repair in one place: a write
refused with `503` during a fault window was still applied on the primary, and
anti-entropy replays it from the primary's own WAL once the replica is stable
again.

**Capstone (2026-08-17, dirty volumes):** the `stop-restart` nemesis — the run that
used to fail deterministically — now passes **4/4** with `converged: true` on every
run (4.3–6.0 s, 9–12 attempts), `linearizable: PASS`, `indeterminate writes: 0`,
14,295–29,949 refused-but-applied per run, check durations 169–206 ms.
`kill-restart` passed 2/2 the same day as the control. Full table in
[chaos-harness.md](docs/chaos-harness.md).

**Final state (2026-08-17, all thirteen defects fixed):** the same gate on the
finished build converged in **20 ms on the first attempt** — the fastest
convergence any run has recorded, because idempotent replay finishes the repair
during the run and the post-run check finds nothing left to do. 18,008
refused-but-applied writes, `linearizable: PASS`, and both epoch-regression
counters at 0 on every node — a reading the replay-classification fix makes
structurally meaningful, since a catch-up replay can no longer touch them.

**Positioned against etcd**, the reference CP key-value store: at matched
throughput (~6,000 reads/s) etcd's read p99 is **0.79 ms vs distrikv's 1.7 ms** —
etcd is ~2× faster at the tail despite paying a linearizable quorum round-trip.
The write column of that comparison is **not** clean and is not quoted here; the
durability confound is stated in full in
[benchmarks.md](docs/benchmarks.md#confounds-this-comparison-does-not-control-for).

---

## Quickstart

```bash
# Build
go build ./...
make build-cli            # → bin/distrikv-cli

# Run a 3-node cluster
docker compose -f docker/docker-compose.yml up -d

# Smoke it
curl -X PUT localhost:8001/keys/hello -d '{"value":"world"}'
curl        localhost:8002/keys/hello
curl        localhost:8001/status
curl        localhost:8001/metrics

# Or with the CLI
distrikv-cli put hello world
distrikv-cli status --all --peers localhost:8002,localhost:8003
```

**Benchmark it.** Run the load generator from inside the cluster's network — a host
port forward measures the plumbing, not the store:

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --qps 6000 --duration 60s --warmup 5s \
    --mix 0:100:0 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf
```

Add `--prefill` with a keyspace larger than the memtable to exercise the LSM read
path (Bloom filters, block cache, compaction) rather than the memtable — see
[benchmarks.md](docs/benchmarks.md#read-path-engine-metrics---prefill).

**Break it.** The chaos runner kills and restarts cluster members *during* the
measurement phase, then checks the history for linearizability and asserts every
replica converged:

```bash
go run ./cmd/chaos --target localhost:8001 --peers localhost:8002,localhost:8003 \
  --duration 60s --warmup 5s --workers 8 --keyspace 20 --put 50 --delete 5 \
  --nemesis kill-restart --nemesis-services node2,node3 \
  --nemesis-interval 10s --nemesis-downtime 5s \
  --check-convergence --convergence-grace 30s
```

On FAIL it prints the offending key's operation window — how each operation was
modelled, and which fault window it intersected — plus a JSON dump for diffing
runs.

**Test it.**

```bash
go test ./... -race
```

> The HTTP and gRPC surfaces are **unauthenticated** and assume a trusted cluster
> network. There is no authn, authz or TLS; anything that can reach a node's ports
> can read and write every key.

---

## Engineering Deep Dive

An audio overview of distrikv's distributed systems design — generated with [Google NotebookLM](https://notebooklm.google/). Thanks Google!

[![distrikv NotebookLM overview](https://drive.google.com/thumbnail?id=12ibGA01jQrEr-3HEpLdM_G-kAlQxX379&sz=w1280)](https://drive.google.com/file/d/12ibGA01jQrEr-3HEpLdM_G-kAlQxX379/view?usp=sharing)
