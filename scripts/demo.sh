#!/usr/bin/env bash
# Demo: start a 3-node distrikv cluster and exercise the full feature set.
set -euo pipefail

BASE_PORTS=(8001 8002 8003)

wait_healthy() {
    local port=$1
    local tries=0
    until curl -sf "localhost:${port}/status" >/dev/null 2>&1; do
        tries=$((tries+1))
        if [ "$tries" -ge 30 ]; then
            echo "ERROR: node on port ${port} did not become healthy in 15s"
            exit 1
        fi
        sleep 0.5
    done
}

echo "=== Starting cluster ==="
docker compose -f "$(dirname "$0")/../docker/docker-compose.yml" up -d --build

echo "Waiting for all nodes to be ready..."
for port in "${BASE_PORTS[@]}"; do
    wait_healthy "$port"
done
echo "All nodes healthy."
echo

echo "=== Cluster status ==="
for port in "${BASE_PORTS[@]}"; do
    echo -n "node:${port}  "
    curl -sf "localhost:${port}/status" | python3 -m json.tool --no-indent 2>/dev/null \
        || curl -sf "localhost:${port}/status"
done
echo

echo "=== Write keys (all sent to node1, ring routes automatically) ==="
curl -sf -X PUT localhost:8001/keys/foo   -H 'Content-Type: application/json' -d '{"value":"bar"}'
curl -sf -X PUT localhost:8001/keys/hello -H 'Content-Type: application/json' -d '{"value":"world"}'
curl -sf -X PUT localhost:8001/keys/dist  -H 'Content-Type: application/json' -d '{"value":"rikv"}'
echo "(3 keys written)"
echo

echo "=== Read keys from each node (ring-forwarding in action) ==="
for key in foo hello dist; do
    for port in "${BASE_PORTS[@]}"; do
        result=$(curl -sf "localhost:${port}/keys/${key}" 2>/dev/null || echo '{"error":"unreachable"}')
        echo "  GET ${key} via :${port} → ${result}"
    done
done
echo

echo "=== Delete a key ==="
curl -sf -X DELETE localhost:8001/keys/hello
echo "(deleted hello)"
result=$(curl -sf localhost:8001/keys/hello 2>/dev/null || echo '{"error":"not found"}')
echo "  GET hello → ${result}"
echo

echo "=== Metrics snapshot (node1) ==="
curl -sf localhost:8001/metrics | python3 -m json.tool 2>/dev/null \
    || curl -sf localhost:8001/metrics
echo

echo "=== Leader election: kill the current leader ==="
leader_port=""
for port in "${BASE_PORTS[@]}"; do
    role=$(curl -sf "localhost:${port}/status" 2>/dev/null | grep -o '"role":"[^"]*"' | cut -d'"' -f4)
    if [ "$role" = "leader" ]; then
        leader_port=$port
        break
    fi
done

if [ -n "$leader_port" ]; then
    # Find the service name for that port
    svc=""
    case "$leader_port" in
        8001) svc=node1 ;;
        8002) svc=node2 ;;
        8003) svc=node3 ;;
    esac
    echo "Current leader is on :${leader_port} (${svc}). Stopping it..."
    docker compose -f "$(dirname "$0")/../docker/docker-compose.yml" stop "$svc"
    echo "Waiting 2s for re-election..."
    sleep 2

    echo "New cluster status:"
    for port in "${BASE_PORTS[@]}"; do
        if [ "$port" != "$leader_port" ]; then
            echo -n "  node:${port}  "
            curl -sf "localhost:${port}/status" | python3 -m json.tool --no-indent 2>/dev/null \
                || curl -sf "localhost:${port}/status"
        fi
    done
    echo

    echo "Restarting ${svc}..."
    docker compose -f "$(dirname "$0")/../docker/docker-compose.yml" start "$svc"
    wait_healthy "$leader_port"
    echo "${svc} rejoined."
else
    echo "No leader found — skipping leader-kill demo."
fi

echo
echo "=== Demo complete. Cluster is still running. ==="
echo "To stop: docker compose -f docker/docker-compose.yml down -v"
