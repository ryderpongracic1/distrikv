# Chaos Harness and Linearizability Verification

The Porcupine model, the crash-recovery suite, the nemesis, the convergence gate,
and the counterexample output — plus every measured run, including the four
failing-then-passing stages that each exposed a real defect.

The defects those runs found are indexed in [defect-log.md](defect-log.md).

---

## Phase 4 — Deterministic Fault Injection & Jepsen-style Linearizability Verification (2026-05-21)

### What changed

Phase 4 adds a complete correctness verification layer consisting of three components:

**1. Porcupine linearizability harness (`internal/linearizability`)**

Every concurrent put/get/delete operation is bracketed with `rec.Begin(input)` /
`rec.End(id, output)` calls that record a [Porcupine](https://github.com/anishathalye/porcupine)
event timeline. After all goroutines finish, `rec.CheckTimeout(d)` verifies that
the observed history is linearisable — i.e., every read returns a value consistent
with *some* sequential ordering of the concurrent writes.

The KV model treats each key as an independent register
(`State = map[string]string`). The `PartitionEvent` hook splits the history by key
before checking, reducing complexity from O(exp(N)) to O(K × exp(N/K)) where K is
the keyspace size — essential for large histories.

**Failed operations** are classified rather than assumed. A failed read
constrains nothing. A failed *write* is modelled as a no-op only when it provably
never reached the store; a write distrikv refused with 503 after applying it
locally is modelled as **applied** (`Output.Applied`), and a write whose effect is
unknown becomes a **pending operation** (`Recorder.EndUnknown`) the checker may
place anywhere or nowhere. Recording every failure as a no-op — the original
model — is exact for in-process failures but reports a correct store as broken
under replication; see "How failed operations are modelled" below for the
evidence that forced the change.

**2. Crash recovery tests (`internal/store/lsm/crash_test.go`)**

Six deterministic crash scenarios, each pairing a specific failure mode with an
expected recovery guarantee:

| Test | Failure injected | Recovery guarantee |
| --- | --- | --- |
| `TestCrash_WalReplayDurability` | Clean close + reopen | All 200 pre-close keys readable |
| `TestCrash_TornWALEntry` | WAL file truncated mid-entry (simulates power-loss byte tear) | Pre-tear keys present; post-tear key absent |
| `TestCrash_FlushedDataSurvivesTruncation` | Post-flush WAL removed | L0 SSTable survives; keys readable |
| `TestCrash_ConcurrentWriteDurability` | 6-goroutine write storm + clean close | All successfully-acknowledged keys durable |
| `TestCrash_RestoreSentinelRecovery` | Manually planted "restore-in-progress" sentinel | Data dir wiped on reopen; sentinel removed |
| `TestCrash_NoDataLossUnderFlushedAndUnflushed` | Both flushed (L0 SSTable) and unflushed (WAL-only) data present | Both batches readable after close/reopen |

Torn-write simulation stops goroutines directly (`close(tree.stopCh); tree.wg.Wait()`) without calling `tree.Close()`, preserving the WAL on disk for truncation. `os.Truncate` removes trailing bytes before reopening. The test is in `package lsm` (white-box) so it can reach unexported fields.

The sentinel-recovery test exposed a bug: `wipeLSMDir()` removed WAL/SST/manifest files but not the sentinel itself, causing an infinite wipe loop on every subsequent open. Fixed by adding `os.Remove(sentinelPath)` after `wipeLSMDir()` succeeds in `NewLSMTree`.

**3. Distributed chaos runner (`cmd/chaos`)**

A standalone `cmd/chaos` binary runs a Jepsen-style chaos test against a live
cluster over HTTP:

```bash
docker compose -f docker/docker-compose.yml up -d
go run ./cmd/chaos \
  --target    localhost:8001 \
  --duration  30s \
  --workers   8 \
  --keyspace  20 \
  --put       50 \
  --delete    5
```

The binary runs a **warmup phase** (ops issued but not recorded), then a
**measurement phase** (all ops recorded as Porcupine events), and finally checks
the full history for linearizability.

*Fault injection (nemesis).* Load alone does not make a chaos test. With
`--nemesis` the runner kills and restarts cluster members *during the
measurement phase*, so the history is checked against a cluster that is actually
losing and regaining nodes:

```bash
go run ./cmd/chaos \
  --target            localhost:8001 \
  --duration          60s \
  --nemesis           kill-restart \
  --nemesis-services  node2,node3 \
  --nemesis-interval  10s \
  --nemesis-downtime  5s
```

The nemesis loop picks a random victim from `--nemesis-services`, takes it down,
leaves it down for `--nemesis-downtime`, brings it back, waits
`--nemesis-interval`, and repeats. The first strike lands immediately so short
runs still see a fault. It never runs during warmup — warmup exists to fill
connection pools, not to be measured — and it heals its victim on **every path
the runner controls**: a completed cycle, the run deadline, SIGINT/SIGTERM, a
failed disrupt, or a panic in the nemesis loop. It cannot cover what it never
runs through: a `SIGKILL` of the runner itself, or a `Heal` that fails, which is
reported as `heal error` on the window.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--nemesis` | `none` | `none` \| `kill-restart` \| `stop-restart` |
| `--nemesis-services` | *(empty)* | Comma-separated compose service names to draw victims from. Required unless `--nemesis=none` |
| `--nemesis-interval` | `10s` | Delay between the end of one outage and the start of the next |
| `--nemesis-downtime` | `5s` | How long a victim stays down |
| `--nemesis-compose-file` | `docker/docker-compose.yml` | Compose file the nemesis operates on |
| `--peers` | *(empty)* | Every **other** node's client HTTP address, comma-separated. Required by the convergence gate |
| `--check-convergence` | `true` | After the run, assert every replica agrees on every key. Only applies when a nemesis is enabled |
| `--convergence-grace` | `30s` | How long to keep re-checking before declaring the replicas divergent |
| `--replicas` | `2` | Replication factor R, so the check asks the same question of the ring that the nodes do |
| `--counterexample-file` | *(auto)* | Where to write the counterexample on FAIL. Default names it `chaos-counterexample-<timestamp>.json`; `none` disables writing |

*Counterexample on FAIL.* A verdict of FAIL used to be one word. The history
behind it is half a million events long and the anomaly lives on one of 20 keys,
so diagnosing a 1-in-4 race meant re-running until it happened again. The runner
now localises the failure and prints it.

Porcupine reports a verdict over the whole history, and with the verbose check
the *maximal partial linearizations* it found — but not which partition failed,
and `LinearizationInfo`'s internals are unexported, so there is no public way to
ask. The model already partitions by key and KV keys are independent registers,
so the localisation comes from re-checking each key's sub-history on its own: a
whole-history FAIL means at least one of them is illegal, and checking them one at
a time says which. Within that key, the longest partial linearization is the
frontier — the operations the checker could order — and the earliest operation
outside it is where the history stopped being satisfiable.

```
  counterexample — key chaos-m1786979100942982000-00004
    1 of 20 key(s) failed to linearize
    the checker ordered 81 of 83 operation(s) on this key; the earliest one it
    could not place is marked ✗ — that is where the history stopped being
    satisfiable, not a proof that this one operation is the defect
    offsets are from measurement start, so they line up with the fault windows above
────────────────────────────────────────────────────────────
    … 69 earlier operation(s) on this key omitted
        +46.412s→+46.413s w3   refused-applied put "w3-t1786983730621729510"   [fault #3 node3]
        +46.418s→+46.419s w6   ok              get -> "w3-t1786983730621729510"   [fault #3 node3]
        +46.702s→+46.703s w3   ok              put "w3-t1786979165937481000"   [fault #3 node3]
    ✗   +46.884s→+46.885s w1   ok              get -> <absent>   [fault #3 node3]
        +46.901s→+46.902s w5   no-op           delete   [fault #3 node3]
    full window written to chaos-counterexample-20260817-161122.json
```

Four things make the window readable, and each one is there because its absence
cost a debugging session: the **modelling column** (`ok`,
`refused-applied`, `no-op`, `pending`, `failed-read`) says how the operation
entered the history, since a failing read next to a refused-but-applied write
means something different from the same read next to a no-op; **offsets from
measurement start** match the fault-window table above rather than being
wall-clock; the **worker id** identifies which concurrent client issued it; and
the **fault annotation** marks every operation whose call/return interval
intersected an outage, so "which fault exposed this" is answered on the line
rather than by arithmetic.

The window is bounded — 12 operations before the frontier and 6 after on stdout,
120 and 40 in the file — because a 60s run records tens of thousands of
operations per key. Both ends report how many were omitted. The file carries the
same detail as JSON plus the run's context and its fault windows, so two runs can
be diffed.

Two limits are stated rather than papered over. The localisation is a separate
claim from the verdict: if the per-key scan cannot reproduce the illegality inside
its budget, the report says so and leaves the whole-history verdict standing. And
the marked operation is where the search got stuck, not an accusation — an anomaly
is a property of a set of operations, not of one.


*Convergence gate.* A legal history is not the whole story. Linearizability is
judged on what clients observed **through the ring-primary**, so a replica that is
missing everything written during a fault window is invisible to it by
construction — and that divergence is exactly what a refused-but-applied write
leaves behind. After the nemesis heals, the runner reads every measured key from
every node the ring says should hold it (`?local=true`, so each node answers from
its own store) and asserts they all agree:

```bash
go run ./cmd/chaos \
  --target             localhost:8001 \
  --peers              localhost:8002,localhost:8003 \
  --duration           60s --warmup 5s --workers 8 \
  --keyspace           20 --put 50 --delete 5 \
  --nemesis            kill-restart \
  --nemesis-services   node2,node3 \
  --nemesis-interval   10s --nemesis-downtime 5s \
  --check-convergence  --convergence-grace 30s
```

```
  refused-but-applied:     48283
  converged:               true (after 1.4s, 20 keys × 40 node reads)
```

**Live validation (2026-08-17, Apple M4 Pro, Colima VM 8 CPU / 8 GB).** Kill-restart
nemesis against the same 3-node cluster used for the throughput baseline: 4/4 faults
injected, all targeting node3. PASS.

```
  refused-but-applied:     33,449
  converged:               true (after 562ms, 20 keys × 40 node reads, 2 attempts)
  divergent:               0
  indeterminate:           13
  check_duration:          170ms
  anti_entropy_passes:     25 / 22 / 4  (node1 / node2 / node3)
```

562 ms time-to-converge means the WAL catch-up pass shipped every missed write and
confirmed nothing remained — two passes per affected replica (one ships, one
confirms empty) — before the convergence grace window's first re-read poll. The 4
passes on node3 (the victim) are self-directed: it processes no catch-up because it
was the one missing data, not the one holding it.

**Capstone — the convergence/chaos arc, validated 4/4 (2026-08-17, Apple M4 Pro,
Colima VM 8 CPU / 8 GB, accumulated data volumes).** After the WAL segment-number
reuse and LSM sequence-counter defects were closed
([defect-log.md](defect-log.md), defects 10 and 11), the `stop-restart` nemesis was
run four consecutive times with `--check-convergence`. This is the run that used to
fail: it failed *deterministically* — 2/2 divergent replicas — under the graceful
nemesis while `kill-restart` passed, and once the segment numbering was fixed it
exposed the sequence-counter defect as a 1-in-4 `converged: true` +
`linearizable: FAIL`. Both verdicts now hold on every run:

| Run | Victims (4/4 strikes) | Ops | Errors | Refused-but-applied | Converged | Attempts | Indeterminate | Check | Verdict |
| ---: | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |
| 1 | node3 ×4 | 294,163 | 92,649 | 29,949 | **true** (4.355 s) | 9 | 0 | 206 ms | **PASS** |
| 2 | node3, node2, node3, node3 | 273,511 | 51,831 | 16,666 | **true** (5.994 s) | 12 | 0 | 171 ms | **PASS** |
| 3 | node3 ×4 | 263,342 | 58,792 | 14,295 | **true** (4.838 s) | 10 | 0 | 171 ms | **PASS** |
| 4 | node2, node3, node2, node2 | 280,447 | 60,446 | 23,892 | **true** (4.357 s) | 9 | 0 | 169 ms | **PASS** |

`indeterminate writes: 0` on all four is the SIGTERM signature — a graceful stop
drains in flight requests, so no connection dies mid-write and nothing is
genuinely ambiguous. `converged: true` in 4.3–6.0 s over 9–12 poll attempts is
slower than the 562 ms above, and expectedly so: retention now pins the oldest
segment while a replica is known behind and has no cursor, so a pass reads further
back. Check durations of 169–206 ms against a 60 s budget are three orders of
magnitude clear of the timeout the `UNKNOWN` runs used to hit.

**Run 4 is the one that matters most.** Its four strikes fell mostly on node2 and
crossed compaction boundaries on volumes carrying every earlier bench and chaos
run's data — precisely the condition under which the sequence-counter defect used
to resurrect stale values and drop winning tombstones, because a compaction merging
across a restart boundary is what made the inversion visible. It passes.

Earlier the same day, on the same cluster, `kill-restart` passed 2/2 as the control
that the fix did not regress the path which already worked:

| Nemesis | Ops | Errors | Refused-but-applied | Converged | Indeterminate | Check | Verdict |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- |
| `kill-restart` (SIGKILL) | 280,664 | 66,568 | 27,904 | **true** (1.651 s) | 8 | 180 ms | **PASS** |
| `kill-restart` (SIGKILL) | 270,513 | 56,110 | 24,961 | **true** (4.374 s) | 8 | 210 ms | **PASS** |

The 8 indeterminate writes per SIGKILL run are the genuinely ambiguous handful the
`forward_outcome` field cannot resolve — connections that broke mid-flight where
neither side can prove delivery. They are pending operations and cannot reject a
history on their own.

The check re-reads on a 500 ms poll until every replica agrees or the grace window
expires, so the reported elapsed time is the observed time-to-converge. Three
outcomes are deliberately distinct: **converged**, **divergent** (the replicas were
read and they disagree — exit 1, with the first few disagreeing keys printed and a
note pointing at the primaries' catch-up logs), and **skipped/unverified** (no
`--peers`, or a node could not be read at all — never reported as a pass, because
a node that cannot be asked cannot be shown to have converged).

`kill-restart` runs `docker compose kill` (SIGKILL) — a real crash, no graceful
drain, recovery driven entirely by what reached disk. `stop-restart` runs
`docker compose stop` (SIGTERM) and is a strictly weaker fault, useful for
A/B-ing whether an anomaly needs a hard crash to reproduce. Both restart with
`docker compose start`, so the victim keeps its named volume and recovers from
its own WAL rather than starting empty. Compose *service* names are used rather
than container names because container names are project- and
compose-version-dependent (`docker-node2-1`, `docker_node2_1`, …).

Before any load is issued, the nemesis is **preflighted**: the docker daemon must
answer and every named victim must be a service the compose file actually
defines. A misspelled service or a missing compose plugin exits 3 rather than
silently degrading the run into a no-fault run that passes for the wrong reason.

*Fault windows.* Every outage is recorded as a `(victim, down-at, up-at)` window
and printed with offsets relative to the start of measurement, so a failure can
be correlated with the fault that produced it. Verbatim from the `stop-restart`
run tabulated below:

```
────────────────────────────────────────────────────────────
  distrikv chaos  PASS  9s @ 4 workers, 5-key space
────────────────────────────────────────────────────────────
  ops:                     89649
  errors:                  85775
  indeterminate writes:    0
  events:                  179298
  nemesis:                 stop-restart on [node1] interval=2s downtime=1s
  faults injected:         3 of 3 attempted
  check_duration:          238ms
  linearizable:            PASS
────────────────────────────────────────────────────────────
  fault windows (offsets from measurement start):
    #1   node1        down +0s      up +1.1s    (1.1s)
    #2   node1        down +3.1s    up +4.1s    (1.1s)
    #3   node1        down +6.1s    up +7.2s    (1.1s)
────────────────────────────────────────────────────────────
```

A window measures the *observed* outage — down-at is stamped before the disrupt
command and up-at after the heal returns — so its span is `--nemesis-downtime`
plus the two commands, which is why 1s of configured downtime reads as 1.1s here
and why strikes are 3.1s apart rather than 3s. The 86k errors against 90k ops are
the expected shape, not breakage: the runner talks only to `--target`, and this
run's only victim *is* the target, so every request during 3.3s of a 9s run is a
refused connection.

A strike counts as *injected* only if its `Disrupt` returned success, and a
strike is never started once the run has ended — the disrupt and heal commands
each run on a context detached from the run deadline, so a command is never
killed halfway and `injected` never has to mean "unknown". `--output=json` emits
the same windows as a `fault_windows` array with millisecond offsets, RFC3339
timestamps, and `up_at`/`up_at_offset_ms`/`down_ms` all `null` together for a
window whose victim never came back. An interrupted run reports `interrupted` and
the truncated duration its verdict actually covers.

*How failed operations are modelled.* A failed operation is **classified, not
assumed**. A read that fails constrains nothing — the value it returned is not
asserted — but a *write* that fails has three possible effects on the store, and
collapsing them into one is how a correct store gets reported as broken:

| Outcome | When | Encoding | What the checker may conclude |
| --- | --- | --- | --- |
| **Never applied** | The transport delivered nothing: a connection refused before the request could be written, an address that was never dialed. Either observed by the client directly, or reported by the forwarding node as `forward_outcome: never-sent` | `Output{Err: true}` — no-op | The value cannot appear. A read that returns it is an anomaly |
| **Applied anyway** | HTTP 503: the ring-primary wrote to its own store and then failed to replicate. There is no rollback, and reads are served by that primary | `Output{Err: true, Applied: true}` — the write happened | The value is present. A read that *misses* it is an anomaly |
| **Unknown** | The connection died mid-request, a deadline expired, or a forwarding hop failed in a way that may already have been applied — `forward_outcome: unknown` | `Recorder.EndUnknown` — a **pending operation** | Either. It may be linearized anywhere, including after the whole history |

Pending is Porcupine's treatment of an unfinished operation and Jepsen's `:info`.
Porcupine's event API cannot express it by dropping the return — an unmatched call
is a dead end for the checker, pinned by `TestKVModelRequiresAReturnForEveryCall`
— so `EndUnknown` synthesizes a return placed after every other event. Because
that API uses each event's index as its timestamp, that is the equivalent of
`Return = +∞`: the operation's interval extends past everything observed, so
"anywhere or nowhere" is exactly what the checker gets to choose from.

The classification reads the **error chain**, not the message, wherever a chain
exists. `internal/client` returns a typed `*StatusError`, so 503 (applied), 502
(the forward hop failed) and other 5xx (unknown) are separated by `errors.As` on
the status code; transport failures the client observes itself are separated by
`errors.Is` down to the `syscall.Errno`.
`TestClassifyWriteEffectDecidesFromTheChain` proves it by wrapping each error in a
shell whose text names nothing, so a correct answer can only come from the chain.

Within the 502 class there is no chain to read — a gRPC failure keeps neither —
so the *forwarding node* classifies it and sends a typed `forward_outcome` field,
which the runner reads in preference to the prose. That is the subject of *How a
502 is separated, and where* below, including why the decision cannot be made
anywhere else and what makes `"never-sent"` a proof rather than a guess.

*What the first real fault-injection runs found.* Measured 2026-08-16 against a
live 3-node docker-compose cluster on an Apple M4 Pro (Colima VM, 8 CPU / 8 GB),
60s, 8 workers, 20-key space, `--put 50 --delete 5`, victims `node2,node3`,
`--nemesis-interval 10s --nemesis-downtime 5s`, 4 of 4 strikes landing — the first
runs with write replication actually wired:

| Nemesis | Ops | Errors | Indeterminate writes | Verdict |
| --- | ---: | ---: | ---: | --- |
| `kill-restart` (SIGKILL) | 242,476 | 47,266 | 13 | **FAIL** |
| `stop-restart` (SIGTERM) | 288,377 | 65,932 | **0** | **FAIL** |

```
  fault windows (kill-restart)          fault windows (stop-restart)
    #1  node2  down +0s     up +5.4s      #1  node2  down +0s     up +5.4s
    #2  node2  down +15.4s  up +20.8s     #2  node3  down +15.4s  up +20.8s
    #3  node2  down +30.8s  up +36.2s     #3  node3  down +30.8s  up +36.2s
    #4  node3  down +46.2s  up +51.7s     #4  node2  down +46.2s  up +51.6s
```

The graceful nemesis is the ablation. SIGTERM drains in-flight requests, so it
produced **zero** unknown-outcome writes — and the history was still rejected.
That eliminates the torn-connection artefact as the cause and leaves exactly one
mechanism: while a replica was down, the ring-primary answered 503 for writes it
had **already applied to its own store**, the model recorded each of them as a
no-op, and the next read of that key — served by the same primary — returned the
refused value. With a 20-key space and tens of thousands of refused writes per
run, that is not a probabilistic artefact; it happens in every fault window.

The checker was right. It detected precisely the caveat
[CAP Position](architecture.md#cap-position) states in bold — *a refused write is not an undone write* — and the model was the
thing that did not encode it. The fix is the three-outcome table above:
`internal/linearizability` learned `Applied` and `EndUnknown`, and the runner
learned to tell the classes apart.

*What re-running them then found.* The refused-but-applied encoding worked — 18,623
and 20,474 writes classified, and **no FAIL**, so the false anomalies were gone.
But both runs came back `UNKNOWN (timeout)`, and for an instructive reason: the
same release bounded the forward RPC with its own deadline, and its rewritten 502
body no longer quoted the underlying transport failure. The runner's text scan was
the only thing separating never-sent from ambiguous, so *every* forward to a downed
primary became a pending operation — 27,356 and 20,519 of them, each overlapping
every later operation on its key.

| Nemesis | Ops | Errors | Refused-but-applied | Indeterminate writes | Verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| `kill-restart` (SIGKILL) | 282,708 | 68,119 | 18,623 | 27,356 | `UNKNOWN` (60.1s) |
| `stop-restart` (SIGTERM) | 259,793 | 57,641 | 20,474 | 20,519 | `UNKNOWN` (60.1s) |

No `--check-timeout` fixes that shape of failure: the search space is combinatorial
in the number of pending operations, not linear in time. Two fixes composed into a
regression, and each was individually correct — which is what made the response a
typed `forward_outcome` field rather than a bigger budget or a restored substring.
With never-sent forwards classified as no-ops again, the pending count falls to the
genuinely ambiguous handful and the checker reaches a verdict.

*What the final runs found (2026-08-16, commit e59a545).* With the typed
`forward_outcome` field, the election storm fixed (heartbeats now unconditional),
and the recovery deadlock resolved (compaction armed at open), both nemesis modes
pass — including `kill-restart`, the first PASS ever on this project:

| Nemesis | Ops | Errors | Refused-but-applied | Indeterminate writes | Check duration | Verdict |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `kill-restart` (SIGKILL) | 323,477 | 129,081 | 48,283 | 11 | 222 ms | **PASS** |
| `stop-restart` (SIGTERM) | 37,678 | — | 5,159 | 178 | 6.4 s | **PASS** |

The kill-restart run is the capstone — highest throughput of any chaos run on this
project (323k ops / 60 s *with* faults injected, 4/4 strikes landing, fault windows
~5.4 s each on node2/node3). The 11 indeterminate writes (down from 27,356 before
the `forward_outcome` fix) are the genuinely ambiguous handful: connections that
broke mid-flight where neither side can prove delivery. The checker places them in
222 ms — three orders of magnitude under the 60 s timeout that the UNKNOWN runs hit.

The stop-restart run's lower throughput (37,678 ops) reflects the state of the
cluster at the time of that intermediate run: the election storm was still active
(~1.7 terms/s) and the recovery deadlock was still present, depressing throughput
on every restart. Those defects are fixed in the final run above; the intermediate
result is kept for the A/B evidence it provides.

The progression tells the story of the harness working as designed — each stage
exposed a real defect:

| Stage | Defect exposed | Fix |
| --- | --- | --- |
| Pre-model-fix **FAIL** | Refused-but-applied writes modelled as no-ops | Three-outcome model (`Applied`, `EndUnknown`) |
| Post-model-fix **UNKNOWN** | Never-sent forwards lost their classification | Typed `forward_outcome` field on 502 |
| Post-forward-outcome **PASS** (stop-restart) | — (stop-restart passed; kill-restart still depressed) | — |
| Final **PASS** (kill-restart) | Election storm + recovery deadlock suppressed throughput and recovery | Unconditional heartbeats + compaction armed at open |
| Convergence gate **FAIL** (stop-restart, 2/2 divergent) | WAL segment numbers reused across a graceful restart, so every persisted replica cursor addressed a different log and a pass that shipped nothing logged `replica caught up` | Seed segment numbering from every segment on disk, `wal-retained/` included — [defect 10](defect-log.md#defect-10-wal-segment-numbers-reused-across-a-graceful-restart) |
| `converged: true` + linearizable **FAIL** (1 in 4) | The LSM sequence counter reopened at zero, so the first compaction merging across a restart boundary resolved "newer" backwards — resurrecting stale values and dropping winning tombstones | Record each SSTable's max sequence in the manifest and seed the counter above all of them at open — [defect 11](defect-log.md#defect-11-lsm-sequence-counter-reopened-at-zero) |
| Capstone **PASS** 4/4 (stop-restart) + 2/2 (kill-restart) | — | — |

The standing property is unchanged and is the point: **a FAIL is now a real
consistency bug**, and the printed note says so instead of telling the operator to
discount anomalies near a fault window.

For the record, the pre-fix artefact this replaces was also measured, on a single
local node (Linux x86-64, Intel Xeon 6975P-C, 4 cores), 3 runs per nemesis,
`--duration 9s --warmup 1s --workers 4 --keyspace 5`, with a test stub standing in
for `docker compose`:

| Nemesis | Indeterminate writes (3 runs) | Verdicts (pre-fix) |
| --- | --- | --- |
| `kill-restart` (SIGKILL) | 9, 9, 12 | PASS, **FAIL**, **FAIL** |
| `stop-restart` (SIGTERM) | 0, 0, 1 | PASS, PASS, PASS |

~10 durable-but-unacknowledged writes per run were enough to reject an otherwise
correct history, 2 runs in 3. Those are now pending operations and cannot reject
anything on their own.

*How a 502 is separated, and where.* A 502 from `forwardRequest` hides two causes
behind one code: a hop that was never made, and a `ForwardKey` RPC that failed
*after* the primary may have applied the mutation. The status code cannot separate
them, and the second is genuinely ambiguous — so 502 alone would have to be
unknown. That default is expensive: a pending operation overlaps every later
operation on its key, and a fault window produces thousands of forwarded writes to
a node that is down, which pushes the checker into a timeout instead of a verdict.
Both nemesis runs did exactly that, at 20,519 and 27,356 pending operations.

So the forwarding node decides, and says so: every 502 carries a
`forward_outcome` field, `"never-sent"` or `"unknown"`, and the runner reads it
in preference to the prose. The decision belongs there because that is the last
point at which the error still has its gRPC code, and the message has not yet
been flattened into a sentence.

It is also, measurably, the last point at which the error has *any* identity. A
grpc-go RPC failure is a `*status.Error` carrying a code and a string and nothing
else: `errors.Unwrap` returns nil, and `errors.Is` against `syscall.ECONNREFUSED`
and `errors.As` against `*net.OpError` and `*net.DNSError` all fail.
`TestForwardErrorsCarryNoTypedCause` asserts that, so a future grpc-go that starts
preserving the cause will fail the test and invite the stronger implementation.
There is therefore no chain to reach for on either side of the HTTP boundary — the
code plus the message is the whole of the available evidence, and what changed is
not the kind of evidence but where it is read and what crosses the wire.

What the code buys is the proof. A gRPC stream is only created once the HTTP/2
transport is `READY`, so an error raised *inside* transport creation cannot have
carried any part of the request — and grpc-go frames exactly those errors
distinctively:

```
code = Unavailable  desc = connection error: desc = "transport: Error while dialing:
                             dial tcp 127.0.0.1:45983: connect: connection refused"
```

Requiring that framing *and* a delivery-impossible cause is what makes
`"never-sent"` a claim rather than a guess. A connection that broke after the
request went out reads differently — `transport is closing`, `error reading from
server: EOF`, `connection reset by peer` — and stays unknown, correctly, because
those may have been applied. The framing is also what defuses the trap that
`codes.Unavailable` is a legal *application* code: a primary that could not reach
a replica can produce a message quoting `connection refused` from its own fan-out,
and without the framing requirement that would read as never-sent — the same
mistake, one layer up, that made the runner classify refused-but-applied writes as
no-ops.

| gRPC code | message signature | outcome |
| --- | --- | --- |
| `Unavailable` | dial framing **and** `connection refused` / `no such host` / `no route to host` / `network is unreachable` | **never-sent** |
| `Unavailable` | `name resolver error` — no address was ever dialed | **never-sent** |
| `Unavailable` | anything else: broken stream, draining connection, remote-generated status | unknown |
| `DeadlineExceeded` | any — the 2s bound can fire after the primary applied the write | unknown |
| `Canceled` | any — the caller gave up; the server may not have | unknown |
| anything else | any — a code only the remote can produce implies delivery | unknown |

Two cases are left unknown deliberately even though they are *probably*
never-sent. A dial that fails with `i/o timeout` also could not have created a
transport, but "probably" is not the bar for a never-sent claim. And a blackholed
address — a host that completes the TCP handshake but never finishes the HTTP/2
one, which is what a stopped container looks like — surfaces as
`DeadlineExceeded` with a message about waiting for a load-balancer update that
names no transport failure at all. The asymmetry justifies the caution: a wrong
`"never-sent"` tells the model a write did not happen when it may have, which can
invent an anomaly out of correct behaviour, whereas a wrong `"unknown"` costs only
checker time.

The runner's side of the contract is three-valued rather than two, and the
distinction between the last two is the point. A recognised value is trusted. A
value present but *unrecognised* is unknown, and the text is **not** consulted — a
server that speaks this field is authoritative even when its answer is
unintelligible, and substituting a weaker signal that might contradict it would be
worse than declining to answer. Only an *absent* field falls back to scanning the
message, for a server predating the field.

That fallback is retained, and its limits are the epilogue to this argument. Two
of its four markers could never have fired on this path: gRPC reports an
unresolvable target as `name resolver error: produced zero addresses`, not "no
such host", and an unroutable address as a plain `DeadlineExceeded` naming no
route at all. Matching wording chosen by a library two hops away, for an audience
of humans, is what the typed field replaces. It stays bounded in the safe
direction regardless — an unrecognised body is unknown — so a rewording costs
checker time, never a verdict.

Ordering inside the classifier matters for the same reason. A 503's body quotes
the replication failure underneath it, which during an outage reads
`…connect: connection refused` from the fan-out to the dead replica. The status
code therefore has to be consulted *first*: the message-text path used to see that
body and declare a refused-but-applied write "provably never sent", which is why
the `stop-restart` run above reported 0 indeterminate writes while failing.

The report accounts for both classes separately — `refused-but-applied` for writes
the primary kept, `indeterminate writes` for pending ones — so an operator can see
which mechanism a run exercised. (The first pair of runs above predates the row and
reports no `refused-but-applied` count; the re-run pair has it.)

*Why keys carry a per-run nonce.* `KVModel.Init` is an empty map, so a recorded
history has to start against an empty keyspace. Keys are therefore prefixed with
a per-run nonce, and warmup writes to a disjoint set from measurement — otherwise
the first measured read of a key that warmup already wrote (or that an earlier run
left in a persistent volume) is a value the model believes cannot exist, and the
run reports FAIL on a completely healthy cluster. It did: the pre-nemesis runner
failed 4 out of 4 default-flag runs against a healthy single node with zero
errors, and passes 5 out of 5 with the nonce, on the same populated store. Two
costs, both accepted deliberately: each run leaves 2 × `--keyspace` keys behind,
and the measured keys start cold in the block cache and Bloom filters because
warmup no longer touches them.

*What this test can and cannot detect today.* A kill-restart nemesis is the test
that can actually catch replica divergence — two copies of a key disagreeing after
a crash and recovery — and with write replication wired, more than one copy now
exists, so that is live. Killing a node exercises three things at once: crash
recovery under concurrent load, WAL replay correctness (each restart logs a
`wal_replayed` open), and whether a replica that missed writes while it was down
can serve a stale value afterwards.

What it still cannot detect is bounded by where reads go. Reads are served by the
ring-primary only, so a replica that fell behind is invisible to the checker until
it becomes the primary for that key — the divergence
[CAP Position](architecture.md#cap-position) documents (repaired after the fact by
anti-entropy, but with no hinted handoff and no read repair) is real but unobserved
by this history. Killing the *primary* of a key makes that key unavailable rather than
inconsistent, which the model records as a no-op or a pending operation depending
on how the request failed. Closing that gap means reading from replicas, which the
current design deliberately does not do.

Exit codes:

| Code | Meaning |
| --- | --- |
| 0 | PASS — history is linearisable |
| 1 | FAIL — non-linearisable anomaly detected |
| 2 | UNKNOWN — check timed out (`--check-timeout`) |
| 3 | Bad flags / startup error (including nemesis preflight failure) |

The chaos runner uses an explicit `http.Transport` with
`MaxIdleConnsPerHost = workers + 64` — the same TCP-pool fix applied in
`cmd/bench` — so high worker counts don't generate `TIME_WAIT` storms.

### Phase 4 test coverage

In-process linearizability and crash-recovery tests (`internal/store`):

| Test | What it verifies |
| --- | --- |
| `TestLinearizability_ConcurrentOps` | 5 goroutines × 80 ops on a 5-key space; full Porcupine history is linearisable |
| `TestLinearizability_WithLevels` | Same check after forcing L0→L1 compaction; reads exercise the full mem→L0→L1 path |
| `TestCrash_WalReplayDurability` | 200-key WAL round-trip through close/reopen |
| `TestCrash_TornWALEntry` | Byte-level WAL truncation; pre-tear durability, post-tear absence |
| `TestCrash_FlushedDataSurvivesTruncation` | Flushed SSTable data outlives WAL removal |
| `TestCrash_ConcurrentWriteDurability` | 6-writer concurrent storm; all ACK'd keys survive |
| `TestCrash_RestoreSentinelRecovery` | Sentinel wipe + sentinel self-removal |
| `TestCrash_NoDataLossUnderFlushedAndUnflushed` | Mixed flushed + in-flight WAL data both survive |
| `BenchmarkCrash_RecoveryThroughput` | WAL replay throughput for 1000-key history |

Nemesis suite (`cmd/chaos`) — the fault injector and the accounting a verdict
rests on. Docker is never invoked: the scheduler is driven against a fake
nemesis, and the compose driver's command execution is injected, so the
guarantees below are asserted deterministically rather than by observing a live
cluster:

| Test | What it verifies |
| --- | --- |
| `TestSchedulerAlternatesDisruptAndHeal` | Strike loop runs disrupt → outage → heal in order, one recorded window per strike |
| `TestSchedulerHealsWhenCancelledMidOutage` | Cancellation during an outage still heals — a run cannot exit leaving a node down |
| `TestSchedulerHealsAfterPanic` | A panicking disrupt heals on the way out and does not take the run down |
| `TestSchedulerHealsEvenWhenDisruptFails` | A failed disrupt is still healed, since it may have landed partially |
| `TestSchedulerRecordsHealFailure` | A heal that fails is recorded in the window, not swallowed |
| `TestSchedulerDoesNotStrikeAfterCancellation` | No new strike begins once the run context is done |
| `TestSchedulerDisruptIsDetachedFromTheRunContext` | An in-flight disrupt is not interrupted by shutdown, so `disrupt_error` means "failed", never "interrupted" |
| `TestSchedulerVictimsComeOnlyFromTheConfiguredSet` | Victims are drawn only from `--nemesis-services` |
| `TestSchedulerNoopWithoutVictimsOrNemesis` | Absent a nemesis or targets the scheduler is inert; default invocations are unchanged |
| `TestFaultWindowReports` | Fault-window accounting: down/up offsets and duration; an unhealed window reports `up_at`, `up_at_offset_ms` and `down_ms` as null together |
| `TestCountInjectedExcludesFailedStrikes` | `faults_injected` counts landed outages only — a failed disrupt is not an outage |
| `TestFormatFaultWindows` | Human-readable window lines carry index, victim, offsets and duration |
| `TestComposeNemesisPreflight` | Preflight validation: targets must be services the compose file defines, and an unreachable daemon fails the run at startup (exit 3) rather than mid-measurement |
| `TestComposeNemesisBuildsExpectedArgv` | `kill-restart` and `stop-restart` emit the expected `docker compose` argv, healing with `start` |
| `TestComposeNemesisWrapsFailuresWithTheCommand` | A docker failure names the command that produced it |
| `TestParseNemesisFlags` | Flag parsing and rejection of unusable nemesis configurations |
| `TestKVModelTreatsFailedOpsAsNoOps` | Porcupine model contract: a write that never reached the store is a no-op, and one known applied is not |
| `TestKVModelRequiresAReturnForEveryCall` | An unmatched call is a dead end for the checker, which is why an unknown outcome goes through `EndUnknown` rather than dropping the return |
| `TestMakeKeysAreRunScoped` | Warmup and measured keyspaces are separated by a run nonce, so unrecorded warmup writes cannot make a recorded history look illegal |
| `TestProvablyNeverSentClassifiesRealClientErrors` | A refused write from `internal/client` is classified through `errors.Is`/`errors.As` on the preserved chain, not by message text |
| `TestClassifyWriteEffect` | One case per error class: 503 applied, 502 never-sent vs ambiguous, other 5xx unknown, refused/unresolvable never sent, reset/timeout/cancelled unknown, unrecognised shapes unknown |
| `TestClassifyWriteEffectDecidesFromTheChain` | Each error wrapped in a shell whose message names nothing still classifies correctly, so the status code and syscall are read from the chain |
| `TestNeverSentTextIsBoundedToDeliveryFailures` | The one remaining message path admits only failures that delivered nothing; anything ambiguous falls through to unknown |
| `TestClassifyDeleteErr` | A 404 delete is recorded as applied; every other failure survives |
| `TestFinishWriteEncodesEachOutcome` | End-to-end encoding, asserted through the checker: for each failure class, whether a later read may see the value and whether it may miss it |
| `TestFinishWriteToleratesANilRecorder` | Statistics are still counted when history recording is off |
| `TestVerdictNotesMatchTheEncoding` | The printed guidance matches the current encoding — a FAIL is reported as real, and the superseded "failed writes are no-ops" explanation is gone |
| `TestCounterexampleReportsTheOffendingWindow` | A synthetic lost-write history produces a printed window naming the key, marking the operation the checker could not place, and labelling how each one was modelled |
| `TestCounterexampleCorrelatesWithFaultWindows` | Operations inside an outage are annotated with it, and operations outside one are not |
| `TestOverlappingWindowBoundaries` | The overlap relation, case by case: an interval that touches an outage counts, one that finished before it does not, a failed strike is not an outage, and an unhealed one stays open |
| `TestCounterexampleFileIsWrittenAndComparable` | The file is valid JSON carrying the key, the frontier, the run context and the omission accounting, so two runs can be diffed |
| `TestCounterexampleFileCanBeDisabled` | `--counterexample-file none` writes nothing |
| `TestCounterexampleNoteWhenNotLocalisable` | A FAIL the per-key scan cannot localise says so, and leaves the whole-history verdict's authority intact |

Model semantics (`internal/linearizability`) — the three outcomes a failed write
can have, each asserted by running a hand-built history through the checker:

| Test | What it verifies |
| --- | --- |
| `TestRefusedButAppliedWriteIsNotAnAnomaly` | The observed failure shape: a 503-refused put or delete followed by a read of its effect is legal, including a concurrent read of the pre-refusal value |
| `TestNeverSentWriteStaysANoOp` | A write that provably never reached the store did not happen — a read returning its value is still illegal |
| `TestLostAcknowledgedWriteStillFails` | The relaxation does not leak: an acknowledged write that disappears, and a stale read after a known-applied write, both still FAIL |
| `TestPendingWriteIsUnconstrained` | A pending write is legal whether or not any read observes it, may linearize between two reads, and does not license a lost write on another key |
| `TestPendingReturnsComeAfterEveryRecordedEvent` | The mechanism: a pending operation recorded first is still placeable after 20 later reads that never observed it |
| `TestDescribeOperationLabelsEachOutcome` | A failing history distinguishes "the store confirmed this" from "we never found out" |
| `TestCounterexampleLocalizesTheFailingKey` | A history illegal on one key names that key and leaves the legal ones unaccused, and marks the operation the checker could not place |
| `TestCounterexampleIsNilForALegalHistory` | Nothing to localise reads as nothing, rather than as the first key |
| `TestCounterexampleLabelsHowEachOpWasModelled` | Every modelling class — ok, refused-applied, no-op, pending, failed read — is labelled in the report |
| `TestCounterexampleWindowIsBounded` | The printed window is capped and centred on the frontier, and its omission counts add up to the full operation list |

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
