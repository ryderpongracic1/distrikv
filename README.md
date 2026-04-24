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

**Stack:** Go 1.25 · gRPC + protobuf · `net/http` REST · LSM-Tree storage · Raft consensus · Docker Compose

---

## Status

| Phase | Description | Status |
|---|---|---|
| 1 | Single-node KV + WAL + HTTP REST | ✅ Done |
| 2 | Consistent hash ring + gRPC request forwarding | ✅ Done |
| 3 | Write replication to R=2 replicas via gRPC | ✅ Done |
| 4 | Raft leader election + heartbeats | ✅ Done |
| 5 | Docker Compose cluster + demo script | ✅ Done |
| 6 | LSM-Tree storage engine + complete Raft (snapshots, pre-vote) | ✅ Done |
| 7 | `distrikv-cli` — first-class CLI tool | ✅ Done |

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
- **LSM-Tree correctness** — MemTable flush, SSTable read, compaction, Bloom filter false-positive rate
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
│   └── cli/
│       └── main.go          # distrikv-cli entrypoint (ldflags version injection)
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
│   │       └── manifest.go  # SSTable metadata and level membership
│   ├── server/
│   │   ├── grpc_server.go   # KVService: ForwardKey, Replicate, RequestVote, AppendEntries, InstallSnapshot
│   │   └── http_server.go   # REST handlers, ring-based routing, gRPC forwarding
│   ├── metrics/
│   │   └── metrics.go       # Atomic counters, Snapshot() for /metrics
│   └── config/
│       └── config.go        # LoadFromEnv(): typed config from env vars
├── proto/
│   └── kv.proto             # KVService protobuf definitions
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
