# HTTP API and `distrikv-cli`

The REST surface every client uses, the two behaviours that surprise people
(`DELETE` is idempotent, `key_count` is approximate), and the CLI that wraps it.

> This API is **unauthenticated** and assumes a trusted cluster network. There is
> no authn, authz or TLS on either the HTTP or the gRPC surface; anything that can
> reach a node's ports can read and write every key. `GET /keys/{key}?local=true`
> exposes no data an ordinary GET could not already reach, but the same caveat
> applies to it.

---

## HTTP REST API

```
PUT    /keys/{key}   body: {"value": "..."}
GET    /keys/{key}
DELETE /keys/{key}
GET    /status       → {node_id, leader, term, role, key_count, key_count_approximate}
GET    /metrics      → atomic counters (put_total, get_miss, wal_writes, raft_terms, …)
GET    /metrics/prometheus → the same values in Prometheus text format
GET    /healthz      → process liveness
GET    /readyz       → 200 only after this node knows the Raft leader
```

All error responses: `{"error": "..."}`.

Every response includes `X-Request-ID`. A caller-supplied ID up to 128 bytes is
preserved; otherwise the node generates one and includes it in structured
request-completion logs.

**`DELETE` is idempotent and never returns 404.** The storage engine writes tombstones blindly, so deleting a key that does not exist returns `200 {"status":"ok"}`. The previous behaviour — a `Get` before the `Delete` to synthesise a 404 — was removed because it was racy (another writer could insert or remove the key between the two calls, so the answer was never authoritative) and cost a full read-path traversal through the memtable, every L0 SSTable and L1 on every delete. This matches RocksDB, Cassandra and DynamoDB `DeleteItem`. `distrikv-cli delete <key>` therefore reports success for keys that were never there; use `get` first if you need to know.

**`key_count` is approximate**, which is why `/status` also returns `key_count_approximate: true`. The LSM engine maintains the count incrementally: it is exact for a workload of distinct keys, drifts up when a key that has already been flushed to an SSTable is overwritten, and drifts down when a key that does not exist is deleted. Classifying either case correctly would require a read on the write hot path. The count is recorded in the manifest at each memtable flush and re-applied from the WAL at startup, so it survives restarts and crash recovery without scanning the SSTables. `wal_writes` is the engine's real count of fsync'd WAL appends — one per successful `PUT`/`DELETE`.

### Quick start (single node)

```bash
export NODE_ID=node1
export HTTP_ADDR=:8001
export GRPC_ADDR=:9001
export DATA_DIR=/tmp/distrikv
export PEERS=""
export REPLICA_COUNT=1

mkdir -p /tmp/distrikv
go run ./cmd/node
```

---

## distrikv-cli

A first-class CLI tool that wraps the HTTP API — no direct gRPC, no internal imports. Feels like `redis-cli` or `psql`.

### Install

```bash
go install github.com/ryderpongracic1/distrikv/cmd/distrikv-cli@latest
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

# Using the locally built CLI:
./bin/distrikv-cli put hello world
./bin/distrikv-cli get hello
./bin/distrikv-cli status --all --peers localhost:8002,localhost:8003
./bin/distrikv-cli metrics --watch

# Or raw curl:
curl -X PUT  localhost:8001/keys/hello -d '{"value":"world"}'
curl         localhost:8001/keys/hello
curl         localhost:8001/status
curl         localhost:8001/metrics
```

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
