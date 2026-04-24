# v1 → v2 Data Migration

## What changed

| Area | v1 | v2 |
|------|----|----|
| Storage engine | `map[string]record` + WAL (`/data/wal.log`) | LSM-Tree (`/data/lsm/`) |
| Raft persistence | Custom binary format | JSON |
| Raft snapshots | Not supported | Supported (§7) |
| Raft pre-vote | Not supported | Supported (§9.6) |
| `WAL_PATH` env var | Required | Optional (unused) |

## Upgrading

The v1 WAL format (`/data/wal.log`) is **not** compatible with the v2 LSM engine and will be silently ignored on startup. All existing data must be considered lost.

Before upgrading, wipe all node data volumes:

```bash
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up --build
```

This is expected for a development cluster. There is no automatic migration path.

## Configuration changes

`WAL_PATH` is no longer required. If it is set it has no effect. You may remove it from your Docker Compose or environment files.

All other environment variables (`NODE_ID`, `HTTP_ADDR`, `GRPC_ADDR`, `DATA_DIR`, `PEERS`, `REPLICA_COUNT`, `ELECTION_TIMEOUT_MIN`, `ELECTION_TIMEOUT_MAX`, `HEARTBEAT_INTERVAL`) are unchanged.

## Smoke test after upgrade

```bash
# 1. Start cluster
docker compose -f docker/docker-compose.yml up --build -d

# 2. Run demo script
./scripts/demo.sh

# 3. Manual snapshot test (optional)
#    a. Write 2000 keys via HTTP
#    b. docker stop <node1>
#    c. Write 200 more keys via remaining nodes
#    d. docker start <node1>
#    e. Wait for log line: "snapshot installed" on node1
#    f. Read all 2200 keys from node1 to confirm they are present

# 4. Pre-vote test (optional)
#    a. docker network disconnect distrikv_net <node3>
#    b. Wait 30 seconds (without pre-vote, node3 would spam term increments)
#    c. docker network connect distrikv_net <node3>
#    d. Verify cluster still has same leader (check /metrics on any node)
```
