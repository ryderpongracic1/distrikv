#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/docker/docker-compose.yml"
cli="$repo_root/bin/distrikv-cli"

cd "$repo_root"

echo "==> Building distrikv and starting the three-node cluster"
docker compose -f "$compose_file" up -d --build

echo "==> Waiting for every node to report ready"
deadline=$((SECONDS + 90))
for port in 8001 8002 8003; do
  until curl --fail --silent "http://localhost:${port}/readyz" >/dev/null; do
    if (( SECONDS >= deadline )); then
      docker compose -f "$compose_file" ps
      docker compose -f "$compose_file" logs --tail=100
      echo "node on port ${port} did not become ready within 90s" >&2
      exit 1
    fi
    sleep 1
  done
done

echo "==> Building distrikv-cli"
make build-cli

key="quickstart-$(date +%s)"
value="cluster-ok"

echo "==> Writing through node1 and reading through node2"
"$cli" --host localhost:8001 put "$key" "$value"
read_json=$("$cli" --host localhost:8002 --output json get "$key")
if [[ "$read_json" != *"\"value\":\"$value\""* ]]; then
  echo "unexpected read response: $read_json" >&2
  exit 1
fi
echo "$read_json"

echo "==> Cluster status"
"$cli" --host localhost:8001 status --all --peers localhost:8002,localhost:8003

echo "==> Prometheus endpoint sample"
curl --fail --silent http://localhost:8001/metrics/prometheus | sed -n '1,12p'

echo
echo "distrikv is ready. Stop it with:"
echo "  docker compose -f docker/docker-compose.yml down"
