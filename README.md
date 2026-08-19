# distrikv

[![CI](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml/badge.svg)](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml)

Built a strictly consistent (CP) distributed key-value store in Go from scratch:
consistent-hash-ring data placement with synchronous replication, an LSM-tree
storage engine (WAL, leveled compaction, Bloom filters, sharded block cache),
and Raft consensus carrying cluster node health — verified linearizable under
fault injection with a Porcupine checker and a Jepsen-style chaos runner.

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
   │ Consistent  │ │ Raft Node   │ │   LSM-Tree +    │
   │ Hash Ring   │ │   Health    │ │   WAL (fsync)   │
   └─────────────┘ └─────────────┘ └─────────────────┘
```

**Stack:** Go 1.25 · gRPC · `net/http` REST · LSM-Tree storage · Raft consensus · Docker Compose

## Overview

distrikv is a 3-node replicated key-value store built around one deliberate
design decision: **the consistent hash ring owns data placement and
replication, and Raft carries only cluster node health.** This is the
Cassandra-style arrangement rather than the etcd-style one — key/value data
never touches the Raft log.

Writes take the full replicated path: the ring-primary appends to its
WAL (fsync), applies locally, then synchronously replicates to the other
`R−1` replicas over gRPC, returning success only if **every** replica ACKs and
`503` otherwise. That makes distrikv CP: under partition it refuses writes
rather than serving stale ones.

Node health travels through consensus. The Raft leader observes heartbeat
outcomes, applies hysteresis, and commits `health-down`/`health-up` transitions
to the log — so every node, not just the leader, reads the same
cluster-consistent health view. There is no dedicated liveness-probing
machinery anywhere in the node: health is the committed consensus view plus the
outcomes of traffic the cluster was already sending (this node's own
replication RPCs, and the leader's heartbeats).

The consequence of the split is that Raft's "if committed, all future leaders
have it" guarantee does **not** apply to data. A write refused with `503`
during a fault window was still durably applied on the ring-primary; WAL-cursor
**anti-entropy** replays exactly those writes to a replica once it is healthy
again, deduplicating per key so ~104,000 refused-but-applied writes converge as
373 catch-up entries.

## Architecture

- **Consistent hash ring** (`internal/cluster`) — 150 virtual positions per
  node on a `uint32` ring. `GetN(key, R)` returns the `R` distinct physical
  nodes that own a key: the ring-primary plus its replicas.
- **Raft** (`internal/raft`) — randomised election timeouts with a pre-vote
  phase, majority elections, heartbeats, and log replication implemented to
  §5.3 and §5.4.2 with a crash-persistent log and apply-on-commit against an
  opaque `StateMachine`. The log carries node-health transitions;
  [raft.md](docs/raft.md) states the encoding, the defaults, and the test that
  proves a follower learns health from consensus alone.
- **LSM-tree engine** (`internal/store/lsm`) — MemTable → immutable buffer →
  L0 SSTables → one merged L1, each SSTable carrying a Bloom filter, fronted by
  a 32-shard LRU block cache, with every write WAL-fsynced first. Write-stall
  backpressure bounds L0 growth. Restart-safe write ordering comes from a
  42-bit incarnation epoch + 22-bit counter packed into the existing `uint64`
  sequence — zero wire-format changes.
- **Replication & anti-entropy** (`cmd/node`) — synchronous CP writes;
  per-replica WAL cursors drive catch-up passes scheduled off committed health
  transitions and the sender's own replication outcomes
  ([replication-and-anti-entropy.md](docs/replication-and-anti-entropy.md)).
- **Chaos harness** (`cmd/chaos`) — Jepsen-style nemesis (stop-restart,
  kill-restart, leader-kill), a Porcupine linearizability check over every
  recorded operation, and a convergence gate that reads every measured key from
  every node the ring says should hold it
  ([chaos-harness.md](docs/chaos-harness.md)).

## Measured results

Every number was measured on the hardware named next to it, with the load
generator running **inside** the cluster's Docker network. Full tables,
methodology and confounds: [benchmarks.md](docs/benchmarks.md).

**Throughput** — 3-node docker-compose, Apple M4 Pro (Colima VM 8 CPU / 8 GB),
open-loop Poisson arrivals, 256 B values, 0 errors, not saturated. Every PUT
pays the full replicated write path: WAL fsync on the primary plus a
synchronous gRPC ACK from each replica.

| Workload | Achieved | p50 | p99 |
| --- | ---: | ---: | ---: |
| 100% reads (GET) | **6,017 /s** | 0.54 ms | **1.7 ms** |
| 100% replicated writes (PUT) | **1,199 /s** | 1.7 ms | **4.6 ms** |
| 20% write / 80% read | 3,000 /s | 0.70 ms | 2.5 ms |

**LSM read path** — 500k-key prefill so reads traverse SSTables, then three
60 s read-only windows at 4,000 QPS over the same data, changing only the
access pattern:

| Read pattern | Block-cache hit rate | Bloom FP rate |
| --- | ---: | ---: |
| Zipf α=1.1, 500k keys | **97.2%** | — |
| Uniform, 500k keys | **70.9%** | — |
| Uniform, 1M keys (half absent) | 71.1% | **0.01%** |

The 26-point Zipf-vs-uniform gap is the block cache's value proposition
isolated by changing one flag. The 0.01% Bloom false-positive rate is measured,
not estimated — `BloomFalsePositives / BloomHits`, both hard counters.

**Fault tolerance** — the chaos suite passes across every nemesis, on dirty
volumes, with the linearizability check and the convergence gate both green:

| Nemesis | Result | Convergence |
| --- | --- | ---: |
| stop-restart (graceful) | 4/4 PASS | 1–5 s |
| kill-restart (SIGKILL) | PASS | 1–5 s |
| leader-kill (forced elections) | 4/4 PASS | 4–6 s |

A representative run: `refused-but-applied: 33,449 · converged: true ·
linearizable: PASS`. Best recorded convergence is 20 ms first-attempt, on a run
where idempotent replay finished the repair before the check started. The
leader-kill runs force a leaderless window under load — one election per kill,
terms otherwise flat. Per-run tables: [chaos-harness.md](docs/chaos-harness.md).

**Positioned against etcd**, the reference CP key-value store: at matched
throughput (~6,000 reads/s) etcd's read p99 is **0.79 ms vs distrikv's
1.7 ms** — etcd is ~2× faster at the tail despite paying a linearizable quorum
round-trip. The write column of that comparison is not clean and is not quoted
here; the durability confound is stated in full in
[benchmarks.md](docs/benchmarks.md#confounds-this-comparison-does-not-control-for).

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

**Benchmark it** — run the load generator from inside the cluster's network; a
host port forward measures the plumbing, not the store:

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --qps 6000 --duration 60s --warmup 5s \
    --mix 0:100:0 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf
```

Add `--prefill` with a keyspace larger than the memtable to exercise the LSM
read path rather than the memtable — see
[benchmarks.md](docs/benchmarks.md#read-path-engine-metrics---prefill).

**Break it** — the chaos runner kills and restarts cluster members *during* the
measurement phase, then checks the history for linearizability and asserts
every replica converged:

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

**Test it:**

```bash
go test ./... -race
```

## Documentation

The deep material lives in [`docs/`](docs/):

| Document | What is in it |
| --- | --- |
| [architecture.md](docs/architecture.md) | How the pieces fit, the consistent hash ring, the CAP position, and the phase-by-phase status |
| [lsm-engine.md](docs/lsm-engine.md) | MemTable → SSTable, the WAL, leveled compaction, write-stall backpressure, the sharded block cache |
| [replication-and-anti-entropy.md](docs/replication-and-anti-entropy.md) | The CP write path, *a refused write is not an undone write*, WAL-cursor catch-up, and the convergence claims this design withholds |
| [raft.md](docs/raft.md) | Raft's scope: leader election, heartbeats, a complete §5.3/§5.4.2 log, and the node-health transitions it carries |
| [chaos-harness.md](docs/chaos-harness.md) | The Porcupine model, the nemesis, the convergence gate, the counterexample output, and every measured run |
| [benchmarks.md](docs/benchmarks.md) | Every table, including the etcd ceiling comparison with its durability confound stated rather than corrected |
| [defect-log.md](docs/defect-log.md) | Fourteen real defects, numbered, with the evidence that exposed each one |
| [api-and-cli.md](docs/api-and-cli.md) | The HTTP REST surface and `distrikv-cli` |
| [development.md](docs/development.md) | Running the tests, repository layout, code-quality conventions |
| [MIGRATION.md](MIGRATION.md) | v1 → v2 data migration |

## Scope and limitations

distrikv is a research/portfolio system, hardened by measurement rather than by
production traffic. The specific boundaries:

- **No authn, authz or TLS.** The HTTP and gRPC surfaces are unauthenticated
  and assume a trusted cluster network; anything that can reach a node's ports
  can read and write every key.
- **Benchmarks are single-machine.** All numbers come from a 3-node
  docker-compose cluster on one host; they demonstrate the design's behaviour,
  not fleet-scale performance.
- **Fault coverage is bounded by what the harness injects.** Node stop, kill
  and forced leader elections are covered; asymmetric network partitions are
  not injectable yet, which is exactly why node health keeps a
  healthy-direction-only veto from local replication outcomes.
- **Anti-entropy guarantees are stated, not assumed** — including what it
  cannot converge from the log alone
  ([the full list](docs/replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not)).

The engineering process behind those boundaries is itself documented: fourteen
defects — including a compaction path that silently dropped acknowledged writes
and a leader that had never sent a heartbeat — were found with the project's own
chaos tooling and external review, and each entry in
[defect-log.md](docs/defect-log.md) records the evidence that exposed it, the
fix, and the test that pins it.

## Engineering Deep Dive

An audio overview of distrikv's distributed systems design — generated with [Google NotebookLM](https://notebooklm.google/). Thanks Google!

[![distrikv NotebookLM overview](https://drive.google.com/thumbnail?id=12ibGA01jQrEr-3HEpLdM_G-kAlQxX379&sz=w1280)](https://drive.google.com/file/d/12ibGA01jQrEr-3HEpLdM_G-kAlQxX379/view?usp=sharing)
