# `bench/etcd` — etcd ceiling harness

Runs distrikv's own benchmark workload against a 3-member [etcd](https://etcd.io)
cluster, so distrikv's throughput and latency numbers have a state-of-the-art
reference point instead of standing alone.

This is a **ceiling measurement, not a competition**. etcd serialises every write
through a single Raft leader and answers reads linearizably; distrikv places keys
on a consistent hash ring and commits a write once its R=2 replicas ACK, refusing
the write when a replica is unreachable. The repository README's
*Ceiling — vs etcd* section carries the architecture decomposition, the
disclosed confounds, and the results table this harness fills in.

## Why a separate module

`go.mod` here is its own module. The etcd client pulls in gRPC, protobuf, zap and
a slice of `google.golang.org/genproto`; none of that belongs in the dependency
tree of the store itself. Nothing in distrikv imports this module, and this
module imports nothing from distrikv — the workload is a deliberate copy rather
than a shared package, for the reason in *Methodology parity* below.

Consequence worth knowing: the repository's CI runs `go build`, `go vet` and
`go test` on the root module, which **excludes** this one. `gofmt -l .` does walk
into it, so formatting is enforced; compilation and tests are not. Run them here
before committing changes:

```bash
cd bench/etcd
CGO_ENABLED=0 go build ./... && go vet ./... && go test ./... -race -count=1
```

## Usage

The harness launches its own cluster by default — etcd does not need to be
running, only installed.

```bash
brew install etcd                      # or apt, or a release tarball on PATH
CGO_ENABLED=0 go build -o etcd-ceiling .

# The write workload as the repository README's ceiling table was measured:
# offered 2000 QPS, 60s after a 5s warmup, harness defaults for distribution
# (uniform) and worker count (256), left explicit so the run is reproducible.
./etcd-ceiling --qps 2000 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform
```

The published reads and mixed rows are the same command with
`--qps 6000 --mix 0:100:0` and `--qps 3000 --mix 20:80:0`. To remove the two
disclosed asymmetries against the distrikv baseline instead — matching its
distribution, worker count and offered write load — use
`--qps 1200 --workers 128 --keydist zipf`; that variant is not what the published
table reports.

Members are launched on loopback with client ports **2379 / 2381 / 2383** and
peer ports **2380 / 2382 / 2384**, into a temporary data directory that is
removed on shutdown. `SIGINT`/`SIGTERM` tears the cluster down cleanly, as does
any startup failure — a caller that gets an error owns no stray processes.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--qps` | *(required)* | Target ops/sec, open-loop. Must be > 0. |
| `--duration` | `60s` | Measurement window after warmup. |
| `--warmup` | `10s` | Warmup window; results discarded. Populates the keyspace so measured GETs hit existing keys. |
| `--workers` | `256` | Concurrent worker pool size. Use `128` to match the published distrikv baseline. |
| `--valuesize` | `256` | Bytes per value. |
| `--keyspace` | `100000` | Number of distinct keys. |
| `--keydist` | `uniform` | `uniform` \| `zipf` \| `sequential`. |
| `--mix` | `20:80:0` | `put:get:delete` ratio. |
| `--endpoints` | *(unset)* | Comma-separated client endpoints. Requires `--no-cluster`. |
| `--no-cluster` | `false` | Drive an already-running cluster instead of launching one. Defaults to the launch topology's three endpoints when `--endpoints` is omitted. |
| `--etcd-bin` | `etcd` | etcd executable to launch. Resolved via `PATH`. Ignored with `--no-cluster`. |

Defaults are copied from `cmd/bench`, so running both harnesses with no flags
compares like with like. `--endpoints` without `--no-cluster` is a hard error
rather than a silent no-op: reading numbers from a freshly launched local cluster
while believing they came from a named remote one is the worst available outcome.

## Methodology parity with `cmd/bench`

`workload.go` is a deliberate port of `cmd/bench/workload.go`, not an
interpretation of it. Identical by construction:

- **Keys** — `fmt.Sprintf("k%015d", n)`, 16 chars, zero-padded so they sort
  lexicographically.
- **Zipf** — `rand.NewZipf(rand.New(rand.NewSource(1)), 1.1, 1.0, keyspace-1)`.
  Same seed, so the key sequence is reproducible across runs and across stores.
- **Values** — `'a' + i%26`, printable ASCII, `valuesize` bytes.
- **Mix** — cumulative thresholds from `put:get:delete`, selected with one
  `Intn` per op.
- **Arrivals** — one arrival goroutine, exponential inter-arrival times
  (`gap = -ln(u)/qps`) for a Poisson process. Open-loop: arrivals do not wait on
  completions.
- **Latency origin** — measured from the *scheduled* arrival time, not from
  dispatch, so queue wait is included and coordinated omission is eliminated.
- **Queue capacity** — `workers × 4`, and the same saturation rule: a run whose
  max queue depth reached `cap-1` is flagged, because its tail is queue wait
  rather than store latency.
- **Per-op timeout** — 10s.

Because parity is the entire value of this harness, **a change to one workload
file must be mirrored in the other**. `workload_test.go` pins the key format,
the value bytes, the Zipf determinism and the mix thresholds so a silent drift
fails a test instead of quietly invalidating a comparison.

### Deliberate divergences

Each of these exists because the two stores are not the same shape, and each is
a superset or a strict improvement rather than a loosening:

| Divergence | Why |
| --- | --- |
| Latencies live in pre-allocated slices; `cmd/bench` uses an HDR histogram. | Percentiles come out exact instead of within ~0.1%, and `max` is the true maximum rather than clamped at the histogram's 60s ceiling. |
| Percentiles are reported **per operation type** as well as overall. | A Raft store's write and read paths cost very different amounts; one merged number hides exactly the thing being measured. |
| Not-found is structural, not an error. | distrikv's HTTP client returns `ErrNotFound`, which `cmd/bench` swallows. etcd reports the same condition without an error — empty `Kvs` on `Get`, `Deleted == 0` on `Delete` — so the tolerance is implicit. The observable outcome matches: reading an unwritten key is a successful op in both. |
| No engine-side counters section. | `cmd/bench` scrapes distrikv's `/metrics` for bloom/cache/compaction deltas. etcd exposes a different metric set entirely; inventing a mapping would be worse than omitting it. |
| No `--output json`, no `--queue-cap`, no `--client-timeout`. | Kept to the flag surface the comparison needs. Queue capacity is derived (`workers × 4`) and the per-op timeout is fixed at 10s, both matching `cmd/bench`'s effective values. |
| Workers are pinned to an endpoint by index (`clients[idx%len(clients)]`). | Mirrors `cmd/bench`. One `clientv3.Client` per endpoint rather than one client holding all three, so load distribution is deterministic instead of depending on etcd's internal balancer. |

## Files

| File | Contents |
| --- | --- |
| `main.go` | Orchestration: cluster launch, client setup, fail-fast probe, warmup and measurement phases. |
| `flags.go` | Flag definitions, defaults and validation. Usage errors exit 2. |
| `cluster.go` | 3-member etcd launcher: argv construction, health/leader wait, teardown, data-dir cleanup. |
| `workload.go` | Key/value generation, op selection, etcd dispatch. Port of `cmd/bench/workload.go`. |
| `openloop.go` | Poisson arrival process, worker pool, per-op latency recording. |
| `report.go` | Exact percentiles and the results table. |

The driver takes a `dispatchFunc` rather than etcd clients directly, which is
what lets `openloop_test.go` verify the arrival rate, the queue-wait accounting
and the per-op bookkeeping without a cluster.
