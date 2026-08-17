# Development

What the test suite covers, where everything lives, and the conventions the code
holds itself to.

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
- **Recovery availability** — a store reopened on an L0 backlog accepts writes and drains it; hard-stopped writes honour their caller's context and report `ErrWriteStalled` within the stall budget; snapshot restore bulk-loads with no WAL appends and survives a reopen
- **Replication deadline** — the fan-out deadline is independent of `HeartbeatInterval` (a 120 ms replica succeeds under a 5 ms heartbeat) while a silent replica is still bounded
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
├── bench/
│   └── etcd/                # Separate module: etcd ceiling comparison harness
│       ├── main.go          # Orchestration: cluster launch, warmup, measurement
│       ├── cluster.go       # 3-member etcd launcher (loopback 2379/2381/2383)
│       ├── workload.go      # Port of cmd/bench's workload — identical keys/values/mix
│       ├── openloop.go      # Poisson arrivals, per-op latency samples
│       └── report.go        # Exact percentiles, per-op-type results table
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

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
