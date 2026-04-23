# distrikv

A production-quality distributed key-value store written in Go from scratch.

```
                        ┌─────────────────────────────────────────────┐
                        │              Client (curl / SDK)            │
                        └───────┬─────────────────────┬───────────────┘
                                │ HTTP REST            │ HTTP REST
                        ┌───────▼──────┐       ┌──────▼───────┐
                        │    node1     │       │    node2     │  ...
                        │  :8001/:9001 │◄─────►│  :8002/:9002 │
                        └──────────────┘  gRPC └──────────────┘
                               │
               ┌───────────────┼───────────────────┐
               │               │                   │
        ┌──────▼──────┐ ┌──────▼──────┐ ┌──────────▼──────┐
        │ Consistent  │ │ Raft Leader │ │  In-memory KV   │
        │ Hash Ring   │ │  Election   │ │  + WAL (fsync)  │
        └─────────────┘ └─────────────┘ └─────────────────┘
```

**Stack:** Go 1.22+ · gRPC + protobuf · `net/http` REST · `log/slog` · Docker Compose

---

## Status

| Phase | Description | Status |
|---|---|---|
| 1 | Single-node KV + WAL + HTTP REST | ✅ Done |
| 2 | Consistent hash ring + gRPC request forwarding | ✅ Done |
| 3 | Write replication to R=2 replicas via gRPC | ✅ Done |
| 4 | Raft leader election + heartbeats | ✅ Done |
| 5 | Docker Compose cluster + demo script | ✅ Done |

---

## Architecture

### Consistent Hash Ring (`internal/cluster`)

Each physical node is assigned 150 virtual positions on a `uint32` ring. Keys are placed by taking the first 4 bytes of `MD5(key)` as a big-endian uint32 and walking clockwise to the next virtual node.

`GetN(key, R)` returns `R` **distinct physical nodes** starting from the primary — these are the replication targets. The naive approach of returning the next N vnodes can return duplicates for the same physical node; `GetN` skips those.

### Write-Ahead Log (`internal/store`)

Every `Put` and `Delete` is appended to a binary WAL file **before** the in-memory map is updated. Each entry is framed as:

```
[1B op][4B key-len][key][4B val-len][val][4B CRC32]
```

`Append` calls `f.Sync()` (not just `bufio.Flush`) before returning. On restart, `Replay` reads entries sequentially and stops cleanly at a CRC mismatch — the expected signature of a crash-at-tail.

### Simplified Raft (`internal/raft`)

Covers: randomised election timeouts (150–300 ms), `RequestVote` with log-up-to-date check, majority-vote election, `AppendEntries` heartbeats (75 ms), term-based split-brain prevention, and atomic persistence of `currentTerm`/`votedFor` via write-temp-then-rename.

**Deviations from the paper (important for reviewers):**

1. **Data writes bypass Raft consensus.** Writes flow through the consistent-hash ring's `ReplicationManager`, not through the Raft log. Raft here is a leader election and failure-detection mechanism only. This means Raft's "if committed, all future leaders have it" guarantee does **not** apply to data. Under partition the ring-primary and its replicas can diverge.

2. **No log truncation on leader change.** A new leader doesn't truncate uncommitted follower entries. Mitigation: all reads route to the ring-primary, so stale reads are bounded to the in-flight crash window.

3. **No pre-vote phase.** A partitioned rejoining node may trigger a re-election by presenting a higher term. `stepDown` handles it correctly but causes a brief leadership disruption.

4. **Static membership.** No membership-change protocol. Adding/removing a node requires a cluster restart.

5. **No snapshot/compaction.** The WAL and Raft log grow unboundedly. Production fix: snapshots + WAL segment rotation.

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
export WAL_PATH=/tmp/distrikv/wal.log
export DATA_DIR=/tmp/distrikv
export PEERS=""

mkdir -p /tmp/distrikv
go run ./cmd/node

# In another terminal:
curl -X PUT  localhost:8001/keys/hello -d '{"value":"world"}'
curl         localhost:8001/keys/hello
curl -X DELETE localhost:8001/keys/hello
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

---

## Project Structure

```
distrikv/
├── cmd/node/
│   ├── main.go          # Entrypoint: config → Node → Run
│   └── node.go          # Node struct: wires all subsystems, owns shutdown order
├── internal/
│   ├── cluster/
│   │   ├── ring.go      # Consistent hash ring (MD5, 150 vnodes/node)
│   │   └── ring_test.go
│   ├── raft/
│   │   ├── raft.go      # Leader election, heartbeats, stepDown
│   │   ├── log.go       # LogEntry, PersistentState (atomic write-rename)
│   │   └── raft_test.go
│   ├── store/
│   │   ├── store.go     # Thread-safe in-memory KV map
│   │   ├── wal.go       # Binary WAL: CRC32 framing, fsync, replay-on-truncation
│   │   └── store_test.go
│   ├── server/
│   │   ├── grpc_server.go  # KVService impl: ForwardKey, Replicate, RequestVote, AppendEntries
│   │   └── http_server.go  # REST handlers, ring-based routing, gRPC forwarding
│   ├── metrics/
│   │   └── metrics.go   # Atomic counters, Snapshot() for /metrics
│   └── config/
│       └── config.go    # LoadFromEnv(): typed config from env vars
├── proto/
│   └── kv.proto         # KVService protobuf definitions
├── scripts/
│   └── gen.sh           # protoc code generation
└── go.mod
```

---

## Code Quality Notes

- **No global mutable state** — all state lives on a `Node` struct; subsystems receive dependencies through constructors.
- **`context.Context` everywhere** — every I/O function takes a context as its first argument.
- **Goroutine ownership** — every goroutine has a named owner and a clean shutdown path. `runLeader` exits via a per-term `leaderStop` channel; no goroutine leaks.
- **Error wrapping** — `fmt.Errorf("component.Op %q: %w", key, err)` throughout; `errors.Is` contract preserved.
- **Atomic WAL writes** — `PersistentState.Save` uses write-to-temp + `Sync` + `os.Rename`; `WAL.Append` calls `f.Sync()` before returning.
