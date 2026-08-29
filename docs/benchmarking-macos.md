# macOS benchmark runbook

This runbook produces reviewable distrikv benchmark artifacts on Apple Silicon.
It keeps the load generator inside Docker's network so the published numbers
measure the store rather than the macOS port-forwarding proxy.

Use [`benchmarks.md`](benchmarks.md) to interpret the report and compare results.

## 1. Freeze the environment

Use a clean commit, connect the Mac to power, disable Low Power Mode, and close
CPU-, disk-, and network-heavy applications. Do not compare runs made with
different Docker VM allocations.

Recommended Docker Desktop or Colima allocation:

- 8 CPUs
- 8 GiB memory
- Apple Virtualization Framework on Apple Silicon
- No Kubernetes workload running concurrently

Record the environment before every series:

```bash
mkdir -p benchmark-results
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
result_dir="benchmark-results/$run_id"
mkdir -p "$result_dir"

{
  date -u
  git rev-parse HEAD
  git status --short
  go version
  sw_vers
  uname -m
  system_profiler SPHardwareDataType
  docker version
  docker info --format 'CPUs={{.NCPU}} Memory={{.MemTotal}} Driver={{.Driver}}'
} >"$result_dir/environment.txt" 2>&1
```

If `git status --short` is non-empty, explain why in the result notes or stop.

## 2. Build the exact revision

Use a fixed Compose project name so the benchmark network is predictable.

```bash
project=distrikv-bench
compose="docker compose -p $project -f docker/docker-compose.yml"

$compose down -v --remove-orphans
$compose build --pull
$compose up -d

for port in 8001 8002 8003; do
  until curl --fail --silent "http://localhost:$port/readyz" >/dev/null; do
    sleep 1
  done
done

arch="$(go env GOARCH)" # arm64 on Apple Silicon
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
  go build -trimpath -o "bin/distrikv-bench-linux-$arch" ./cmd/bench
```

Do not rebuild between repetitions in one series. Save the image and binary
digests:

```bash
$compose images >"$result_dir/images.txt"
shasum -a 256 "bin/distrikv-bench-linux-$arch" >"$result_dir/bench.sha256"
```

## 3. Run a smoke measurement

Validate the plumbing before spending time on a series:

```bash
docker run --rm \
  --network "${project}_default" \
  -v "$PWD/bin:/bench:ro" \
  alpine:3.20 \
  "/bench/distrikv-bench-linux-$arch" \
    --target node1:8001 \
    --qps 100 \
    --duration 5s \
    --warmup 2s \
    --mix 20:80:0 \
    --workers 32 \
    --valuesize 256 \
    --keyspace 10000 \
    --keydist zipf
```

Require `errors: 0` and `saturation: false` before continuing.

## 4. Canonical throughput series

Run each workload three times. Reset volumes between runs so every repetition
starts from the same storage state. Allow at least 30 seconds between runs for
the Mac and Docker VM to return to idle.

| Workload | `--mix` | Offered QPS |
| --- | --- | ---: |
| Replicated writes | `100:0:0` | 1,200 |
| Reads | `0:100:0` | 6,000 |
| Mixed | `20:80:0` | 3,000 |

Template for one run:

```bash
name=mixed
mix=20:80:0
qps=3000
run=1

$compose down -v
$compose up -d
for port in 8001 8002 8003; do
  until curl --fail --silent "http://localhost:$port/readyz" >/dev/null; do sleep 1; done
done

docker run --rm \
  --network "${project}_default" \
  -v "$PWD/bin:/bench:ro" \
  alpine:3.20 \
  "/bench/distrikv-bench-linux-$arch" \
    --target node1:8001 \
    --qps "$qps" \
    --duration 60s \
    --warmup 10s \
    --mix "$mix" \
    --workers 128 \
    --queue-cap 512 \
    --valuesize 256 \
    --keyspace 100000 \
    --keydist zipf \
    --output json \
  >"$result_dir/${name}-${run}.json" \
  2>"$result_dir/${name}-${run}.log"

curl --fail --silent localhost:8001/metrics \
  >"$result_dir/${name}-${run}-metrics.json"
$compose logs --no-color >"$result_dir/${name}-${run}-cluster.log"
```

Do not publish latency from a run whose report says `saturation: true`, whose
achieved QPS materially trails offered QPS, or whose error count is non-zero.
Keep the artifact and label it as a capacity-bound run.

## 5. LSM read-path series

The 100k Zipf workload can remain in the memtable and therefore says nothing
about Bloom filters or the block cache. Use `--prefill` with 500k keys to force
SSTables:

```bash
docker run --rm \
  --network "${project}_default" \
  -v "$PWD/bin:/bench:ro" \
  alpine:3.20 \
  "/bench/distrikv-bench-linux-$arch" \
    --target node1:8001 \
    --prefill \
    --qps 4000 \
    --duration 60s \
    --warmup 10s \
    --mix 0:100:0 \
    --workers 128 \
    --queue-cap 512 \
    --valuesize 256 \
    --keyspace 500000 \
    --keydist zipf \
    --output json \
  >"$result_dir/read-zipf-500k.json" \
  2>"$result_dir/read-zipf-500k.log"
```

Keep that cluster and dataset for the two comparison windows: rerun without
`--prefill` using a 500k-key uniform space, then rerun using a 1M-key uniform
space. The latter makes half of reads target absent keys and exposes the Bloom
filter's negative path. This three-step sequence is listed verbatim in
[`benchmarks.md`](benchmarks.md#reproducing-on-a-docker-cluster). Do not restart or prefill
again between the three windows; changing only the access distribution is what
isolates the cache and Bloom-filter effects.

## 6. Capacity-knee sweep

When locating saturation, change only offered QPS. Start below the known stable
point and increase by 10–15% until two consecutive runs saturate. Then test the
midpoint between the highest stable and lowest saturated rate. Preserve every
run, including saturated runs.

Example write sweep: 800, 950, 1,100, 1,200, 1,300 QPS.

## 7. Publication checklist

Before updating a README table, confirm:

- same commit, binary digest, Docker images, VM CPU/memory, and workload flags;
- three repetitions per configuration from fresh volumes;
- zero request errors for quoted healthy-cluster results;
- no quoted latency from saturated runs;
- p50, p90, p99, p999, achieved QPS, queue depth, and engine counters retained;
- raw JSON, stderr logs, cluster logs, and environment capture archived;
- arithmetic uses the median of repetitions, with the individual runs linked or
  included in `docs/benchmarks.md`;
- confounds are written beside the result rather than omitted.

## 8. Cleanup

```bash
$compose down -v --remove-orphans
```

Keep `benchmark-results/` out of commits unless a curated result set is being
added intentionally.
