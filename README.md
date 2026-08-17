# distrikv

[![CI](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml/badge.svg)](https://github.com/ryderpongracic1/distrikv/actions/workflows/ci.yml)

A distributed key-value store written in Go from scratch, hardened through seven
phases of measured bug-hunting (P1–P7 under [Status](#status)) on top of the
seven developmental phases that built it. The defects those phases found —
including a compaction path that dropped acknowledged writes, and a leader that
had never sent a heartbeat — are documented where they happened rather than
quietly fixed. It is not production-grade; it is measured, and it says what the
measurements found.

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

**Stack:** Go 1.25 · gRPC · `net/http` REST · LSM-Tree storage · Raft consensus · Docker Compose

---

## Engineering Deep Dive

An audio overview of distrikv's distributed systems design — generated with [Google NotebookLM](https://notebooklm.google/). Thanks Google!

[![distrikv NotebookLM overview](https://drive.google.com/thumbnail?id=12ibGA01jQrEr-3HEpLdM_G-kAlQxX379&sz=w1280)](https://drive.google.com/file/d/12ibGA01jQrEr-3HEpLdM_G-kAlQxX379/view?usp=sharing)

---

## Status

distrikv was built across two tracks. **Phases 1–7** below are the original
developmental phases that took the project from a single-node prototype to a
fully-distributed, CLI-equipped cluster. **Production-grade upgrade phases**
(P1–P8) are the ongoing engineering uplift — benchmarking, storage
optimisation, chaos testing, and operational hardening.

### Original developmental phases

| Phase | Description | Status |
|---|---|---|
| 1 | Single-node KV + WAL + HTTP REST | ✅ Done |
| 2 | Consistent hash ring + gRPC request forwarding | ✅ Done |
| 3 | Write replication to R=2 replicas via gRPC | ✅ Done |
| 4 | Raft leader election + heartbeats | ✅ Done |
| 5 | Docker Compose cluster + demo script | ✅ Done |
| 6 | LSM-Tree storage engine + Raft pre-vote and snapshot codec (the log path is a stub — see [Raft](#raft-internalraft)) | ✅ Done |
| 7 | `distrikv-cli` — first-class CLI tool | ✅ Done |

> **Phase 3 note — honest disclosure.** The replication fan-out
> (`Node.ReplicateWrite`) was written during Phase 3, but nothing ever called
> it: no caller existed in any commit from the initial one onward. Until it was
> wired into the ring-primary's write path, every key lived on exactly one node
> and the receive side (`Replicate` → `ApplyReplica`) was never exercised in
> production. Any benchmark table or `replication_errors=0` reading produced
> before that wiring therefore describes a single-copy sharded store; those are
> labelled where they appear.

### Production-grade upgrade phases

| Phase | Description | Status |
|---|---|---|
| P1 | Quantified benchmarking & metrics harness (`cmd/bench`, HDR histograms, bloom + WAF counters) | ✅ Done |
| P2 | WAL GC optimisation & zero-copy parsing (`sync.Pool` buffers, single-pass CRC, torn-write hardening) | ✅ Done |
| P3 | Leveled compaction strategy + write-stall backpressure | ✅ Done |
| P4 | Deterministic fault injection & Jepsen-style linearizability verification | ✅ Done |
| P5 | In-process sharded LRU block cache (64 MB default, configurable) | ✅ Done |
| P6 | Cluster-level LSM read-path measurement (`cmd/bench --prefill`: Bloom, block cache and compaction counters proven under load) | ✅ Done |
| P7 | WAL-segment anti-entropy — replica catch-up from the primary's log, with a convergence gate in the chaos harness | ✅ Done |
| P8 | TBD | 🔲 Planned |

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

**Raft here is a leader-election and failure-detection mechanism, not a data replication mechanism.** Read that before the feature list below — it is the single most important thing to know about this package, and the feature list is easy to misread without it.

**Live in production traffic:**

- Randomised election timeouts (150–300 ms) with **pre-vote phase** — a candidate first checks it can win a real election before incrementing its term, preventing a rejoining partitioned node from disrupting a stable leader.
- `RequestVote` with log-up-to-date check, majority-vote election, term-based split-brain prevention.
- `AppendEntries` **heartbeats only** (75 ms send period). Heartbeats carry no entries, so they serve purely as a liveness signal and a leader-authority assertion. Each heartbeat RPC carries its own deadline, deliberately decoupled from the send period and bounded by the minimum election timeout: a heartbeat is useful to a follower right up until that follower's election timer expires, and pointless after it.
- Atomic persistence of `currentTerm`/`votedFor` via write-temp-then-`os.Rename`.

**Stubs and dormant paths — exercised by unit tests, never by a running cluster:**

- **Log replication is a stub, not a finished implementation parked outside the data path.** Nothing in the system ever proposes an entry to the Raft log — client writes go to the storage engine directly (see deviation 1) — so `r.log` stays empty for the lifetime of the process, and the entry-handling code has never had to be correct. Read against the paper, it is not. In `HandleAppendEntries` (`internal/raft/raft.go`): `PrevLogIndex`/`PrevLogTerm` are never checked, a conflicting suffix is never truncated, and `Success = true` is set for every request whose term is not stale — so the §5.3 Log Matching Property is neither enforced nor detectable. `LeaderCommit` is ignored, and there is no `commitIndex` field in the struct at all, so nothing tracks what is committed. `applyEntryLocked` writes each entry to the store **on receipt** rather than on commit, which inverts the paper's ordering. On the leader side, `nextIndex` is initialised at election and overwritten after a snapshot install, but never decremented on a rejection — the backtracking loop that repairs a divergent follower does not exist — and `matchIndex` never advances. Before this path could carry data it would need §5.3 log matching (consistency check, conflict truncation, `nextIndex` backoff) and commit-index tracking (majority `matchIndex` → `commitIndex` → apply) built on top of it.
- **Snapshot delivery.** `InstallSnapshot` and the snapshot codec work, and `internal/raft/snapshot_test.go` covers them. But snapshots are triggered by log growth past `snapshotThreshold: 1000`, and since the log never grows, that trigger never fires. No `InstallSnapshot` RPC is ever sent outside tests.

    Traced end to end, because it decides whether the state-machine restore path can ever run in production: `takeSnapshot` is called from exactly one place, `applyEntryLocked`, which only runs when an `AppendEntries` request carries entries. Heartbeats never carry any and nothing proposes, so `takeSnapshot` never runs, `SnapshotStore.Save` is never called, and **`raft_snapshot.bin` never exists**. That makes *both* callers of `store.RestoreFromSnapshot` unreachable in a running cluster: the startup restore in `raft.New` (guarded on a snapshot file being present) and `HandleInstallSnapshot` (a leader must send one first, and `sendInstallSnapshot` returns immediately when there is no snapshot to send). The restore path is still kept correct and fast — see the bulk-load note below — but it is dead code today, so it was **not** the cause of the recovery-availability collapse the write-stall section describes.

    Also found while tracing: `broadcastHeartbeat` chose between `AppendEntries` and `InstallSnapshot` on `peerNext > 0 && peerNext-1 <= snapLastIndex`. With an empty log, leader initialisation sets `nextIndex = lastLogIndex+1 = 1` and `snapLastIndex` is 0, so the condition was **always true** and every heartbeat was routed to `sendInstallSnapshot`, which returned without sending anything because no snapshot exists. This is the root cause of the election storm described in the correction section below — it has since been fixed (the condition is now `nextIndex <= snapLastIndex`, and heartbeats are sent unconditionally to every peer).

- **State-machine restore is a bulk load, not a replay.** `LSMTree.Restore` writes the snapshot payload straight into one L0 SSTable and commits it with a single manifest add. It used to replay the payload through the normal write path, which fsyncs the WAL once per key — at snapshot scale (200k keys) that is 200k fsyncs of total write unavailability, to make durable data that is already durable in the snapshot file it was read from. Crash atomicity is unchanged: the `restore-in-progress` sentinel means an open that finds a half-done restore wipes the directory rather than exposing it, and within the bulk load the manifest add is the commit point, so an SSTable written but not recorded is invisible to every reader.

Both paths are kept rather than deleted because they are the natural landing spot if writes are ever moved onto the Raft log. They are listed here so that "distrikv implements InstallSnapshot" is not mistaken for "distrikv ships snapshots between nodes at runtime" — it doesn't.

**Correction — the leader-election storm (fixed).** Until recently the two claims above were not true of a *running* cluster, and the gap was a chronic election storm: a live 3-node Docker cluster burned roughly 1.7 terms per second indefinitely, passing term 900 within nine minutes of startup, alternating leadership every ~500 ms. Two defects combined:

1. `broadcastHeartbeat` classified a peer as needing a snapshot when `nextIndex-1 <= snapLastIndex` — but a fully caught-up peer sits at exactly `nextIndex == snapLastIndex+1` and satisfies that test. Since the Raft log never grows, *every* peer matched on *every* tick, and the snapshot was sent **instead of** the heartbeat. With no snapshot on disk that send returned early, so no heartbeat ever left the leader at all, and every follower elected the moment its timer expired. A heartbeat now goes to every peer unconditionally; snapshot delivery (still dormant, for the reason above) rides alongside it rather than replacing it.
2. Each heartbeat RPC was given a deadline of exactly one send interval, so any RPC delayed past 150 ms by gRPC connection setup or container scheduling was cancelled by its own deadline and never reached the follower.

Because the data path is ring-based and never consults Raft leadership, the storm cost nothing but log volume and wasted CPU — which is why it went unnoticed until the container logs were read directly. `internal/raft/cluster_test.go` now stands an in-process 3-node cluster up and fails if the term advances more than once over three seconds, both on an idle cluster and with per-RPC latency injected above the send interval.

**Field validation (2026-08-16, commit e59a545).** Measured on a live 3-node
docker-compose cluster (Apple M4 Pro, Colima VM 8 CPU / 8 GB) with accumulated
data volumes from prior bench and chaos runs: one election at startup
(term 5130 → 5131 — the inherited term being a fossil of the storm era), then term
**flat for 2+ minutes**, all three nodes agreeing on the same leader, exactly one
leader at all times. Before the fix: ~1.7 terms/second, indefinitely, since the
project's first boot.

**Intentional deviations from the paper (important for reviewers):**

1. **Data writes bypass Raft consensus.** Writes flow through the consistent-hash ring, not through the Raft log. Raft here is a leader-election and failure-detection mechanism only. This means Raft's "if committed, all future leaders have it" guarantee does **not** apply to data. Under partition the ring-primary and its replicas can diverge.

2. **The Raft log is never written, so log-based guarantees are vacuous.** There is no log truncation on leader change because there are no entries to truncate, and no commit index at all — the field does not exist, because nothing is proposed. Mitigation for data: all reads route to the ring-primary, so stale reads are bounded to the in-flight crash window.

3. **Static membership.** No membership-change protocol. Adding/removing a node requires a cluster restart.

The package doc comment at the top of `internal/raft/raft.go` states the same deviations for anyone reading the code first.

### CAP Position

With R=2 and both-replicas-must-ACK writes, distrikv is **CP**. The ring-primary
applies a mutation to its own store and then synchronously replicates it over
gRPC to the other R−1 replicas that `ring.GetN(key, R)` selects, and it returns
success to the client only if **every** replica acknowledges. If any replica is
unreachable or rejects the write, the client receives `503 Service Unavailable`
and must treat the write as refused. Both client entry points behave identically
— a request that lands on the ring-primary directly and one forwarded to it by a
peer via gRPC `ForwardKey` share a single primary-write path, so they cannot
drift apart on durability or on failure semantics.

If the ring-primary itself is unreachable, the forwarding node answers
`502 Bad Gateway` within ~2s: the forward RPC carries its own deadline, so a
primary whose host has vanished (which never sends a TCP RST, leaving the gRPC
channel stuck in `CONNECTING`) can no longer outlast the HTTP `WriteTimeout` and
leave the client with no response at all. `503` still means "the primary took the
write but a replica did not ACK"; `502` means "the primary could not be reached".

Whether a 502 means anything was written is a separate question, and the response
answers it rather than leaving it to be inferred: every 502 carries a
`forward_outcome` field, `"never-sent"` when the request provably never left this
node and `"unknown"` when the RPC failed in a way that may have been applied
before the response was lost. The distinction is not cosmetic — the
linearizability model needs it, and cannot derive it downstream. Phase 4's
*How failed operations are modelled* below carries the decision table and the
proof behind each row.

**The replication deadline is its own knob.** Each fan-out RPC is bounded by
`defaultReplicateTimeout` = **2 s** (`cmd/node/node.go`). It used to be
2×`HeartbeatInterval`, which tied the write path's failure threshold to a Raft
election-timing knob: the docker-compose `HEARTBEAT_INTERVAL` of 150 ms made it
300 ms, so tuning election sensitivity silently changed when writes start
failing — and 300 ms is below the legitimate worst case, since a replica draining
a compaction backlog holds a write for up to the engine's 1 s stall budget before
answering `ErrWriteStalled`. A replica that was merely busy was therefore
reported as failed and the client got a 503 for a write the primary had already
applied. 2 s is the stall budget plus margin for the RPC itself (a healthy
in-cluster replicate is sub-millisecond), sits well below the HTTP server's 10 s
`WriteTimeout`, and matches the forward deadline so the client's worst case is
the same 2 s whichever hop fails. A stalled replica answering `ErrWriteStalled`
inside that window surfaces as `replica X rejected write` rather than a deadline,
which is what separates "overloaded" from "unreachable" in the primary's logs.

Reads are served from the ring-primary's local store and are never replicated,
so a node on the minority side of a partition still answers reads while
refusing writes. That is the right trade-off for a store where a stale read is
more tolerable than a split-brain write.

**A refused write is not an undone write.** There is no rollback. By the time
replication is attempted the mutation is already durable on the primary
(WAL-fsynced, then applied to the memtable), so a failed fan-out leaves the
primary **ahead of** the replicas that did not ACK: the client sees a 5xx for a
write that is in fact present on the primary and that subsequent reads will
return.

That divergence is now repaired after the fact. **Anti-entropy** replays the
missed writes from the primary's own WAL once the replica is reachable and stable
again — see "Anti-entropy: replica catch-up" below for the design, the
convergence guarantee it does and does not make, and the chaos-harness gate that
measures it. What has *not* changed is the write path: a write during a fault
still returns 503, because making it succeed would need either two-phase commit
across the replica set or routing data writes through the Raft log, both of which
remain out of scope (see "Intentional deviations from the paper" above). Read
repair and hinted handoff are still absent.

This is not a theoretical caveat: it is what the first real fault-injection runs
detected. The linearizability checker rejected both a SIGKILL and a SIGTERM run
purely because refused-but-applied writes were being read back afterwards — see
Phase 4's "How failed operations are modelled" for the A/B evidence and the model
change it forced.

Three smaller guarantees, stated because they are load-bearing:

- **Replicas do not re-replicate.** A replicated mutation is applied straight to
  the receiving node's local store and is never fanned out again. A second
  fan-out from a replica would replicate forever.
- **Replicated deletes are idempotent.** A `delete` for a key a replica never
  received is treated as already applied, so a replica that missed an earlier
  write cannot block the primary's deletes indefinitely.
- **A single-node deployment still works.** With no other node in the replica
  set (one node, or R=1) the fan-out has no targets and the write completes
  local-only.

---

### Anti-entropy: replica catch-up (`internal/store/wal`, `cmd/node/antientropy.go`)

Every fault window leaves the ring-primary ahead of its replicas, because a write
whose replica does not ACK is refused to the client and kept locally anyway. The
chaos harness has always counted those writes — the last kill-restart run measured
**48,283 refused-but-applied** keys — and nothing converged them afterwards. A
replica that had been down stayed wrong for every key it missed until that key
happened to be written again.

Anti-entropy closes that gap after the fact, from the log the primary already
keeps.

**What it does not change.** The CP write path is untouched: a write during a
fault still returns 503, and no code here ever makes one succeed. The hash ring
remains the only authority on key placement — Raft contributes a liveness signal
and nothing else. And there is no second replication log: the primary's WAL *is*
the record of what a replica missed.

#### The cursor

Each replica has a **high-water mark**: the WAL position through which it is known
caught up, addressed as `(segment, byte offset)`. Segment numbers only increase and
each segment is append-only, so that pair orders exactly as the engine wrote the
entries. Cursors are persisted to `replica-cursors.json` (atomic temp-file rename,
directory fsync), so a primary that restarts still knows which replicas it is
ahead of — that seeding is what makes a restart trigger repair even though no
health transition is coming to announce it.

Cursors advance two ways:

- **At the end of a pass**, past each entry the replica ACKs.
- **Over a fault-free window**, on a cluster that is simply working. A tip is
  captured, and adopted once it is older than `CursorHoldback` (2× the replication
  deadline) *provided no replication to that replica failed in the meantime*.
  Because the holdback exceeds the replication deadline, every write appended
  before that tip has necessarily resolved by the time it is adopted, so "no
  failures since" really does mean "everything before this reached the replica".
  Without this, a node that never suffers a fault never advances a cursor, and WAL
  retention would pin every segment since startup — the happy path has to make
  progress too, and it has to do so without the write path paying for per-write
  bookkeeping.

#### The trigger

A catch-up runs when a replica transitions **unreachable → healthy**, gated on
`DefaultStableChecks` = 3 consecutive healthy observations. A node that has just
restarted accepts connections before it is useful (WAL to replay, compaction
backlog to arm), and a flapping peer would otherwise trigger a pass per flap.

Health merges three signals, because no single one is enough:

| Signal | Available on | Detects |
|---|---|---|
| Raft heartbeat outcomes | the leader only | failure and recovery, most directly |
| Replication RPC outcomes | every ring-primary | failure |
| gRPC channel state probe | every node | recovery |

The heartbeat signal is the one the design called for and is wired in
(`raft.SetPeerHealthObserver`), but only the Raft leader sends heartbeats — and in
distrikv ring ownership is deliberately unrelated to Raft leadership, so a
follower that is nonetheless a ring-primary would learn nothing from it. The
probe is what lets that node notice its replica came *back*: during a fault,
writes to the replica are being refused, so the write path produces no successes
to count.

Two things also schedule a pass: a replica still marked behind is retried every
`RetryInterval` (5 s), and a durable cursor behind the tip at startup queues one
immediately.

#### A pass

1. Read the WAL forward from the cursor to the tip observed when the pass started.
   The tip is a pin, so concurrent writes cannot chase a pass forward indefinitely.
2. Keep only entries that are this replica's business: keys where **this node is
   the ring-primary** and the target is in the replica set. The primary check
   matters because a node's WAL also holds writes it accepted *as* a replica for
   keys owned elsewhere; replaying those would have it speak for a range it does
   not own. This is a catch-up of one key range, not a shipment of the log.
3. **Deduplicate by key, newest wins.** A replica is not a client — reads are
   served by the ring-primary — so only the final value per key matters. Pass cost
   becomes proportional to the *distinct keys* written during the fault rather than
   to the write count: in the measured run, 20 entries instead of 48,283.
4. Send each surviving entry with the ordinary `Replicate` RPC, in ascending WAL
   position order, advancing the cursor past each ACK. A replica that dies mid-pass
   is resumed from exactly where it stopped.
5. Repeat after a settle delay until a pass ships nothing, which is what proves
   there is nothing left.

Entries are sent in position order deliberately: the cursor is monotonic, so
sending a lower position after a higher one would silently drop the lower entry's
progress. Skipping a superseded entry is safe for the same reason — its
replacement always lies at a higher position, so it is still ahead of the cursor.

**Why the existing `Replicate` RPC and not a new streaming one.** A per-entry ACK
gives exact resume-on-failure, which a batch ACK cannot, and deduplication already
bounds the entry count by the keyspace rather than the write count — so the
round-trip count a stream would save is small in the case that matters. It also
needs no regeneration of the protobufs, whose current generated files this
workspace cannot reproduce byte-for-byte. A `SyncEntries` stream is the right
optimisation once a pass routinely ships tens of thousands of distinct keys; it is
noted as future work rather than pretended away.

#### WAL retention

A cursor is only useful while the segment it points into still exists, but the
engine deletes a segment as soon as its memtable is flushed. Segments at or above
the retention floor (the oldest position any cursor holds) are therefore **parked**
in `wal-retained/` instead of deleted.

Parking rather than leaving them in place is the load-bearing detail: recovery
replays every `wal-NNNN.log` in the data directory, so a flushed segment left
there would be replayed on the next open — re-applying writes already in an
SSTable and double-counting them in the live-key estimate the manifest carries.
One directory down, the segment stays readable by the catch-up reader and invisible
to recovery, with no new bookkeeping to keep in sync.

Retention is bounded at `maxRetainedWALSegments` = 128. A replica that is gone for
good must not turn into unbounded disk growth, so past the cap the oldest parked
segments are dropped with a warning.

#### What is guaranteed, and what is not

**Guaranteed: convergence once writes quiesce.** A repair cycle keeps passing until
a pass finds nothing to ship, so the final pass in a quiet cluster sees a settled
log and leaves every affected key equal on primary and replica. That is the
property `--check-convergence` measures.

**Not guaranteed: convergence under continuous write load.** Live replication is
deliberately *not* blocked during a pass, so a live RPC for a write inside the
pass's range can land at the replica after the pass has already shipped a newer
value for that key, leaving that key stale. The race needs two client-concurrent
writes to the same key straddling the pass, and it self-corrects the next time the
key is written. Blocking replication to a recovering replica for the duration of a
pass would close it — at the cost of refusing writes to a replica that has just
come back, trading a rare stale key for guaranteed unavailability. That trade was
declined.

**Bounded recovery.** If a cursor points into a segment that has already been
collected, the gap cannot be closed from the log. The pass says so
(`wal.ErrCursorStale`, counted as `anti_entropy_stale`), then catches up from the
oldest surviving segment — which converges every key written since, and leaves any
key whose *only* write fell in the lost range divergent until it is written again.
A full keyspace scan to repair that is deliberately out of scope for v1.

**Not guaranteed after a snapshot restore: convergence of any kind.**
`lsm.Restore` bulk-loads the snapshot payload straight into an L0 SSTable and
performs zero WAL appends (pinned by `TestRestore_BulkLoadPerformsNoWALAppends`),
because at snapshot scale the write path's per-key fsync costs minutes of write
unavailability for data that is already durable in the snapshot file. The
consequence for catch-up is total: the restored keys were never in the log, so
**no WAL pass can ever ship them**. A replica that was down while its primary
restored stays divergent on every snapshot key until each is written again.

The node therefore does two things rather than pretending otherwise:

- **It invalidates every replica cursor** (`store.CursorStore.InvalidateAll`,
  called from `Store.RestoreFromSnapshot` *before* the store is replaced, so a
  crash mid-restore leaves the safe state — losing cursors is recoverable, stale
  cursors are not). A cursor that survived a restore is worse than stale, because
  a restore starts a fresh WAL at segment 1 and **reuses segment numbers the old
  log had already used**: `wal.ErrCursorStale` is keyed on the segment *number*, so
  it never fires. The surviving cursor instead (1) orders *after* the new tip, so
  the "cursor behind tip" check reads the replica as up to date and schedules no
  catch-up; (2) cannot be moved back, since cursors are monotonic, while
  `RetentionFloor` keeps naming a segment of a log that no longer exists — which
  makes the engine delete freshly flushed segments instead of parking them for
  catch-up; and (3) once the new log grows past the old offset, makes a pass read
  from a byte offset that is mid-entry in a different log, where the CRC catches
  it and the reader reports a torn tail — which on the newest segment is a *clean
  stop with no error*. The pass then ships nothing, reports no failure, and the
  engine concludes the replica is caught up. That last one is a silently wrong
  convergence claim, which is the reason this is fixed rather than documented.
- **It refuses to claim convergence it cannot deliver.** The condition is latched
  durably in the cursor file and surfaced as the `anti_entropy_full_sync_required`
  gauge plus a startup warning. While it is set, a pass that finds nothing to ship
  is reported as *"the replica is NOT known to agree on keys restored from the
  snapshot"* rather than `replica caught up`. The gauge never clears — v1 has no
  full-sync mechanism — so it keeps reading 1 even after the affected keys have
  organically been rewritten. It over-reports a problem rather than going quiet
  while divergence remains, which is the only safe direction for a convergence
  claim.

**Reachability, stated plainly: this path is currently unreachable in a running
cluster.** No Raft snapshot file is ever created — `takeSnapshot` fires only from
`applyEntryLocked`, and nothing proposes log entries, because the ring (not the
Raft log) carries the data. So `RestoreFromSnapshot` is reached today only by
tests calling it directly. This is defensive hardening for whenever snapshots
become real, plus correctness for direct callers; it fixes no live incident. It is
recorded here rather than in a commit message because the same reasoning is what
makes the missing full-sync mechanism a *known* gap rather than an oversight.

*Designed but not built (v1):* full sync is a key-range scan shipped to the
replica — walk this node's live keys (the engine already iterates them for
`Snapshot`), filter to the keys this node is ring-primary for with that replica in
the replica set, send them with the ordinary `Replicate` RPC in bounded batches,
and clear the flag per replica once a scan completes without a replication
failure. It is not built here because, unlike a WAL pass, it is unbounded in the
store's size and so needs its own throttling, resumability, and interaction with
the write path.

#### Observability

`/metrics` gains `anti_entropy_passes`, `anti_entropy_entries`,
`anti_entropy_errors`, `anti_entropy_stale` and
`anti_entropy_full_sync_required`. Node logs carry `catch-up scheduled`,
`catch-up pass shipped missed writes`, `replica caught up`,
`replica cursor is older than the retained WAL`, and — after a snapshot restore —
`this node cannot converge its replicas from its WAL`.

`GET /keys/{key}?local=true` answers from the node's own store without
forwarding. It is the only way to ask a *replica* what it holds — a plain GET on a
non-owning node forwards to the ring-primary and would report the primary's value
from every node, making a divergent replica indistinguishable from a converged
one. It exposes no data an ordinary GET could not already reach, but like the rest
of this API it is unauthenticated and assumes a trusted cluster network.

---

## HTTP REST API

```
PUT    /keys/{key}   body: {"value": "..."}
GET    /keys/{key}
DELETE /keys/{key}
GET    /status       → {node_id, leader, term, role, key_count, key_count_approximate}
GET    /metrics      → atomic counters (put_total, get_miss, wal_writes, raft_terms, …)
```

All error responses: `{"error": "..."}`.

**`DELETE` is idempotent and never returns 404.** The storage engine writes tombstones blindly, so deleting a key that does not exist returns `200 {"status":"ok"}`. The previous behaviour — a `Get` before the `Delete` to synthesise a 404 — was removed because it was racy (another writer could insert or remove the key between the two calls, so the answer was never authoritative) and cost a full read-path traversal through the memtable, every L0 SSTable and L1 on every delete. This matches RocksDB, Cassandra and DynamoDB `DeleteItem`. `distrikv-cli delete <key>` therefore reports success for keys that were never there; use `get` first if you need to know.

**`key_count` is approximate**, which is why `/status` also returns `key_count_approximate: true`. The LSM engine maintains the count incrementally: it is exact for a workload of distinct keys, drifts up when a key that has already been flushed to an SSTable is overwritten, and drifts down when a key that does not exist is deleted. Classifying either case correctly would require a read on the write hot path. The count is recorded in the manifest at each memtable flush and re-applied from the WAL at startup, so it survives restarts and crash recovery without scanning the SSTables. `wal_writes` is the engine's real count of fsync'd WAL appends — one per successful `PUT`/`DELETE`.

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
| 1 | Key not found (`get` and `watch` only — `delete` is idempotent, see the HTTP API notes) |
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

## Benchmarking (`cmd/bench`)

`cmd/bench` is a standalone open-loop load generator that drives a running
cluster over HTTP and reports HDR-histogram latency percentiles plus
engine-side counters scraped from `/metrics`.

```bash
# 1. Start the cluster
docker compose -f docker/docker-compose.yml up -d

# 2. Run the bench (60s @ 1000 QPS, 80% PUT / 20% GET)
go run ./cmd/bench \
    --target   localhost:8001 \
    --qps      1000 \
    --duration 60s \
    --warmup   5s \
    --mix      80:20:0 \
    --workers  128 \
    --valuesize 1024
```

That form goes through the host's port forward, which is fine for a smoke run but
**not** for a number worth publishing: on macOS it adds a proxy hop and caps
concurrent connections at the ephemeral port range. Any figure meant for a table
must be measured from inside the cluster's Docker network — see the methodology
note under
[Throughput baseline with replication](#throughput-baseline-with-replication-2026-08-16-3-node-docker-compose-apple-m4-pro-colima-vm-8-cpu--8-gb).

To measure the **LSM read path** (Bloom filters, block cache, compaction) rather
than the memtable, add `--prefill` and a keyspace larger than the memtable can
hold — see [Read-path engine metrics](#read-path-engine-metrics---prefill) for
why this is required and what the numbers look like.

### Why open-loop?

A naive closed-loop bench (N workers each `for { client.Put(...) }`) hides
tail latency under saturation: when the cluster slows down, workers
automatically slow down their request rate, so the recorded p99 reflects
*how fast workers happened to issue requests*, not *how slow the system
actually got*. This is called **coordinated omission**.

`cmd/bench` instead generates Poisson arrivals at the configured QPS and
records latency from each request's *scheduled* time (not its dispatch
time). Queue wait counts as latency. If the cluster falls behind, the
report flags `saturation: TRUE` and `max_queue_depth` climbs to the cap.

### Interpreting the report

```
== distrikv bench: 1m0s @ target 1000 qps, mix=80:20:0, keyspace=100000 uniform ==
ops:           59937      achieved_qps: 998.6    errors: 0
latency (us):  p50=1734    p90=4403    p99=26767   p999=81151   max=108863
engine-side:   bloom_hits=673  bloom_misses=4939  bloom_fp_rate=0.00%
               flush_bytes=16.7MB  compaction_bytes_written=14.0MB  WAF=1.83
               compactions=1  forwarded_requests=40107  replication_errors=0
saturation:    false   max_queue_depth=30
```

| Metric | Meaning |
| --- | --- |
| `bloom_misses` | Bloom said *definitely absent* — saved a block read. Higher is better for negative lookups. |
| `bloom_hits` | Bloom said *might be present* — block was read. Includes true hits and false positives. |
| `bloom_fp_rate` | `false_positives / bloom_hits`. A correctly sized Bloom filter stays well under 1%. |
| `block_cache_hits` / `block_cache_misses` / `hit_rate` | Whether an SSTable block read was served from the in-process LRU cache or cost a `ReadAt`. Zero on a memtable-resident workload — nothing reached an SSTable. |
| `prefill` | Present only with `--prefill`: keys written, achieved keys/s, retries, and failures. A non-zero `failed` aborts the run, because a read-path ratio measured over a partly-written keyspace is not a measurement. |
| `during prefill` | Write-path counters (`flush_bytes`, `compactions`, `write_stalls`) attributed to the prefill phase, which is where they are earned — a read-only measurement window cannot produce a flush. |
| `WAF` (Write Amplification Factor) | `(flush_bytes + compaction_bytes_written) / flush_bytes`. 1.0× means no compaction overhead; 2-3× is normal for size-tiered compaction; Phase 3 (leveled) will trade higher WAF for better read amp. |
| `forwarded_requests` | Ring routed the key to a peer instead of handling it locally. |
| `replication_errors` | Replica fan-out failures — a replica that did not ACK a primary's PUT/DELETE (unreachable, or an explicit rejection). Each one also fails the client's write with `503`, so a non-zero value means writes were **refused**, not merely under-replicated. Reports produced before replication was wired read `0` by construction; see the Phase 3 note under Status. |
| `saturation` | `TRUE` if the arrival queue reached its cap → the cluster couldn't keep up; tail latencies include queue wait. |

### Phase 1 baseline (2026-05-21, 3-node docker-compose on M-series laptop)

| Workload | QPS achieved | p50 | p99 | p999 | WAF | Bloom FP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 80:20 PUT/GET, 1KB values, 60s @ 1000 QPS | 998.6 | 1.7 ms | 26.8 ms | 81.2 ms | 1.83× | 0.00% |

These numbers establish the baseline for Phases 2 (WAL GC optimisation), 3
(leveled compaction), and 4 (chaos). Any regression in p99 or WAF in a
later phase has to be justified.

### Throughput baseline with replication (2026-08-16, 3-node docker-compose, Apple M4 Pro, Colima VM 8 CPU / 8 GB)

Measured with `cmd/bench` open-loop Poisson arrivals against a healthy 3-node
cluster; Zipfian α=1.1, 100k-key space, 256 B values, 128 workers. "Not
saturated" means `max_queue_depth ≪ cap` and achieved QPS ≈ target QPS.
**Every PUT here pays the full replicated write path**: WAL fsync on the
ring-primary plus a synchronous gRPC ACK from each of the R−1 replicas.

**Methodology note:** the bench binary runs *inside* the cluster's Docker
network (`docker run --network docker_default … bench-linux --target
node1:8001`), not through the host's port forward. Measuring through a
localhost forwarder (Docker Desktop or Colima/Lima) adds a proxy hop to every
request and, on macOS, caps concurrent connections at the host's ephemeral
port range — both of which measure the plumbing rather than the store.

| Workload | Target QPS | Achieved | p50 | p90 | p99 | p999 | Errors | Saturated? |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100% writes (PUT) | 1,200 | **1,199 /s** | 1.7 ms | 2.7 ms | 4.6 ms | 13.6 ms | 0 | No |
| 100% reads (GET) | 6,000 | **6,017 /s** | 0.54 ms | 1.3 ms | 1.7 ms | 2.0 ms | 0 | No |
| 20% write / 80% read | 3,000 | **3,000 /s** | 0.70 ms | 1.5 ms | 2.5 ms | 3.7 ms | 0 | No |

Engine-side counters for these runs show `flush_bytes=0`, `bloom_*=0` and
`block_cache_*=0` — that is expected, not broken: a Zipfian workload at 256 B
accumulates *unique*-key bytes slowly (overwrites replace in place in the
memtable), so each node stays under the 4 MB flush threshold and every read is
served from the memtable. The Bloom filter and block cache are only consulted
on the SSTable path. Reaching that path from a cluster bench requires writing
each key once, which `--prefill` does — see
[Read-path engine metrics](#read-path-engine-metrics---prefill) for the measured
counters and the mechanism behind the zeros.

#### Why these numbers replaced the earlier table

The previous baseline (2026-05-22, Apple M1 Max) reported 1,191 /s writes at
p99 = 70 ms and 5,977 /s reads at p99 = 11 ms, measured **without replication**
(the fan-out was never invoked — see the Phase 3 note under Status) and through
a client bug that has since been fixed: `internal/client` never drained HTTP
response bodies, so Go's transport discarded every connection instead of
returning it to the idle pool, and **every request paid a fresh TCP dial**.
Under load this exhausted ephemeral ports (`cannot assign requested address`)
and inflated tail latencies with handshake and TIME_WAIT stalls — it is also
the real cause behind the TIME_WAIT warning that used to live in the operator
notes. The fix is a drain-before-close helper pinned by a test asserting 60
sequential calls arrive over one connection.

The two tables are therefore not comparable (different hardware, fixed client,
replication on), but the direction is telling: **writes now do strictly more
work per request — a synchronous replica ACK — and still post a 15× better
p99**, because the connection churn cost far more than replication does.

Reads are never replicated; read throughput is bounded by ring lookup,
memtable/LSM reads, and HTTP round-trip overhead. A request that lands on a node
which is not the key's ring-primary is forwarded there over gRPC, and that
forwarding overhead is already inside every percentile above. The measured
fraction is **distribution-dependent, not the 2/3 a uniformly-owned 3-node ring
would predict**: the read run above forwarded 167,334 of 361,026 ops (**46%**),
while the uniform read-path runs in
[Read-path engine metrics](#read-path-engine-metrics---prefill) forwarded 160,486
of 239,923 (**67%**, i.e. 2/3 as predicted). The Zipfian shortfall is consistent
with node1 owning more than a third of the *hot* keys — under skew the forward
fraction tracks ownership of the small hot set, not of the whole key space.

### Ceiling — vs etcd (SOTA CP key-value store)

The tables above say what distrikv does. They do not say whether that is fast,
because they have nothing to sit next to. [etcd](https://etcd.io) is the
reference implementation of a consistent distributed key-value store — the
thing Kubernetes stores its entire cluster state in, a decade of production
hardening, Raft consensus done properly — so it is the honest ceiling to
measure against.

**These are different classes of system, and the comparison is a reference
point rather than a competition.** Both are CP, but they buy consistency in
structurally different ways:

- **etcd** commits every write through a single Raft leader. The leader appends
  to its log, replicates to followers, and applies once a majority has
  persisted the entry. Every key in the cluster is serialised through that one
  log. Reads are linearizable by default, which also costs a leader round-trip.
- **distrikv** never puts data on a Raft log (see *Intentional deviations*
  above). A key's ring-primary applies the write locally and synchronously
  replicates to its R−1 replicas, returning success only if **every** replica
  ACKs and `503` otherwise. Different keys have different primaries, so writes
  to different parts of the key space proceed in parallel. Reads go to the
  ring-primary directly, with no quorum step.

So the interesting output is not a winner. It is the shape of the gap, and what
the shape says about the trade each design made.

`bench/etcd/` is a separate Go module containing a harness able to run **the same
workload** against etcd: the same key format, the same value bytes, the same
open-loop Poisson arrivals, and the same Zipfian generator (α=1.1, seeded
identically) when asked for it. It launches its own 3-member etcd cluster on
loopback and reports exact latency percentiles per operation type. Keeping it in
its own module keeps etcd's dependency tree out of distrikv's `go.mod`.

The published run below did **not** use every one of those knobs: it ran the
harness at its defaults for key distribution (uniform) and worker count (256),
against a distrikv baseline that used Zipf α=1.1 and 128 workers. Both
asymmetries are disclosed under the table; the runbook reproduces the run as it
actually happened, and notes the matched-parameter variant alongside it.

#### Results (2026-08-17, Apple M4 Pro, etcd 3.7.1 native vs distrikv 3-node Colima)

etcd: 3-member cluster on loopback, native APFS, uniform 100k-key distribution,
256 B values, 60 s runs after a 5 s warmup, **256 workers** (harness default),
0 errors across all three workloads. Offered load: 2,000 / 6,000 / 3,000 QPS.
distrikv: 3-node docker-compose inside Colima VM (8 CPU / 8 GB), Zipfian α=1.1,
**128 workers**, same value size and duration, offered 1,200 / 6,000 / 3,000 QPS
— reproduced from the validated baseline above.

| Workload (60s, 256 B values) | distrikv offered | distrikv achieved | etcd offered | etcd achieved | distrikv p99 | etcd p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100% writes (PUT) | 1,200 /s | **1,199 /s** | 2,000 /s | **1,995 /s** | 4.6 ms | 73.7 ms |
| 100% reads (GET) | 6,000 /s | **6,017 /s** | 6,000 /s | **6,009 /s** | 1.7 ms | 0.79 ms |
| 20% write / 80% read | 3,000 /s | **3,000 /s** | 3,000 /s | **2,993 /s** | 2.5 ms | 80.9 ms† |

† Mixed p99 is the worse of the two op types; etcd's gets queue behind puts.

**Both write rows are achieved-equals-offered, so neither is a throughput
ceiling** — and the two rows were not offered the same load. See mechanism 1
below before reading anything about write throughput out of this table.

| Per-op latency, mixed workload (20:80) | distrikv p50‡ | etcd p50 | distrikv p99‡ | etcd p99 |
| --- | ---: | ---: | ---: | ---: |
| PUT | 0.70 ms | 34.1 ms | 2.5 ms | 70.3 ms |
| GET | 0.70 ms | 36.2 ms | 2.5 ms | 80.9 ms |

‡ `cmd/bench` reports one merged latency distribution, not a per-op split, so
both distrikv rows repeat the overall figure — its PUT and GET tails are not
measured separately. Only the etcd columns are genuinely per-op (that split is
one of the harness's deliberate divergences; see `bench/etcd/README.md`).

**The honest headline is the read comparison, not the write comparison.** At
matched throughput (~6,000 reads/s), etcd's p99 is **0.79 ms vs distrikv's
1.7 ms** — etcd reads are ~2× faster at the tail despite paying a linearizable
quorum round-trip. distrikv's extra latency is consistent with the HTTP hop plus
the internal gRPC forward that a non-primary node pays — measured at 46% of
requests on this Zipfian baseline (167,334 forwards over 361,026 reads). The
read path has no fsync on either side, so this comparison is clean.

The write comparison is **not clean** — see the durability confound below.

> **Parameter mismatches (disclosed).** Two knobs differed between the columns,
> both because the etcd run took the value both harnesses default to while
> distrikv's baseline had overridden it on the command line:
>
> - **Key distribution.** The etcd harness used uniform random over 100k keys
>   (its default generator); distrikv's baseline used Zipfian α=1.1 over 100k
>   keys. For the read and mixed rows this means etcd paid uniform cache
>   pressure while distrikv benefited from hot-key locality. For writes the
>   difference is smaller (both distributions produce distinct keys at similar
>   rates over 60 s at these QPS levels), but it is not zero.
> - **Worker count.** etcd ran with 256 concurrent workers, distrikv with 128.
>   Neither run saturated (`saturation: false`, `max_queue_depth ≪ cap` on both
>   sides), and latency is measured from the *scheduled* arrival time on both
>   harnesses, so the concurrency difference does not inflate either tail
>   through coordinated omission — but it does mean the two stores were held at
>   different in-flight depths, and etcd's deeper pool is a mild handicap on its
>   fsync-bound write path.
>
> The runbook below reproduces the run as it happened; matching both knobs is
> listed there as the variant that removes these two asymmetries.

#### Why the gap has the shape it does

Three mechanisms account for it, and they push in different directions — which
is why a single "distrikv is N× faster/slower" number would be meaningless.

1. **Write throughput: Raft log serialisation vs ring fan-out.** etcd's leader
   is a single serialisation point for the whole key space: every PUT is an
   append to one log, in one order, on one node, whose disk and CPU are shared
   by all keys. distrikv has three concurrent write paths, one per ring
   primary, each with its own WAL and its own fsync stream. A 3-node distrikv
   ring can therefore absorb roughly 3× the independent write streams before
   any single node's disk becomes the bottleneck, and that advantage grows with
   node count, where etcd's write throughput is flat in node count (and mildly
   *decreasing*, since a larger majority means more replication work per
   commit).

   **This claim is supported by structure only — nothing on this page measures
   it.** distrikv's write row was offered 1,200 QPS and achieved 1,199; etcd's
   was offered ~2,000 and achieved 1,995. Both systems delivered the load they
   were asked for, unsaturated (`saturation: false` on both), so **neither
   number is a throughput ceiling** — they are two different offered rates, and
   the 1,199-vs-1,995 gap in the table is a gap in what was *requested*, not in
   what either store could do. Nothing here establishes that ring fan-out has a
   higher write ceiling than a Raft log; it establishes only that both stores
   were comfortable below theirs.

   The measurement that would settle it is a **saturation sweep**: raise offered
   write QPS on each store in steps until achieved QPS stops tracking target or
   `saturation: true` trips, and report the knee. Run against distrikv at 1, 2
   and 3 nodes, that also tests the scaling half of the claim — the ceiling
   should move with node count for the ring and stay flat for etcd. Until that
   exists, treat this mechanism as a design argument, not a result. (The
   durability confound below would have to be neutralised first — a ceiling
   measured against fsyncs that may be absorbed by a VM page cache is not a
   ceiling.)

2. **Read latency: linearizable quorum read vs primary-local read.** An etcd
   `Get` is linearizable by default: it goes through the leader's read-index
   protocol, confirming with a quorum that the leader is still the leader before
   answering. distrikv reads route to the key's ring-primary and answer from its
   memtable/LSM with no coordination at all. distrikv's read path therefore does
   strictly less work, and buys a strictly weaker guarantee — it is a read of
   the primary's latest state, which is only linearizable to the extent the
   primary has not just crashed with an in-flight write. **This is the largest
   single confound in the read comparison and the reason the read rows should
   not be read as "distrikv's reads are better".**

3. **Durability quorum: majority-of-3 vs all-of-R.** etcd commits on a majority
   — leader plus one follower — so it tolerates one dead or slow member and
   keeps serving writes. distrikv requires *every* replica in `ring.GetN(key,R)`
   to ACK, so with R=2 a single unreachable replica means `503` for every key
   whose replica set contains it. Both designs wait for exactly one remote ACK
   on the happy path, which is why their per-write latency is closer than the
   throughput difference suggests. The divergence is entirely in the unhappy
   path: etcd degrades gracefully, distrikv refuses. Phase 4's nemesis runs
   quantify that refusal (48,283 refused-but-applied writes across four fault
   windows) — and it is a deliberate choice, not an oversight, because refusing
   is what keeps the store CP without a consensus log.

#### What etcd buys that distrikv does not have

Worth stating plainly, because the throughput table flatters distrikv and the
list below is where the decade of engineering actually went:

- **Linearizable reads**, guaranteed by protocol rather than by "reads go to
  the primary and the primary usually has the data".
- **Consensus-backed durability.** A committed etcd write survives any minority
  failure with the "if committed, all future leaders have it" guarantee.
  distrikv's ring replication has no such property — the ring-primary and its
  replicas can diverge under partition. Anti-entropy repairs that divergence
  afterwards, within the limits its own section states, but there is no
  protocol-level guarantee at commit time.
- **Dynamic membership.** etcd adds and removes members through a Raft config
  change, online. distrikv's ring is static and requires a cluster restart.
- **Availability under single-member failure**, per the quorum point above.
- **Watches, leases, MVCC revisions, and mini-transactions** (`Txn`,
  compare-and-swap). distrikv has none of these; `distrikv-cli watch` polls.
- **Multi-key atomicity** via `Txn`. distrikv has no cross-key primitive at all.

#### What distrikv trades for

- **Write throughput that scales with node count** rather than being pinned to
  one leader's disk.
- **Lower read latency**, by charging the caller a weaker guarantee for it.
- **A far smaller operational surface**: no consensus log to compact, no
  snapshot transfer to tune, no revision history to defragment.
- **Legible failure semantics** in exchange for availability: `503` = the
  primary took it but a replica did not ACK; `502` = the primary was
  unreachable, with `forward_outcome` saying whether anything was written.

#### Confounds this comparison does not control for

Disclosed rather than corrected, because pretending they are not there would be
worse than the asymmetry itself:

- **Durability cost (the dominant write confound).** etcd fsyncs its Raft log
  per commit using `F_FULLFSYNC` on macOS — a real barrier to stable storage
  that costs ~10–30 ms per flush and accounts for nearly all of etcd's 37 ms
  write p50. distrikv's WAL fsyncs go through Colima's virtiofs, where flushes
  are likely absorbed by the VM page cache and may never reach stable storage
  on the host. **The write comparison is therefore substantially "real
  durability vs maybe-durability", not architecture.** The architectural claim
  — that Raft serialises all writes through one log while the ring fans out to
  parallel primaries — is supported by structure (three independent write
  streams vs one serialisation point), but these numbers cannot cleanly
  separate it from the durability asymmetry. Write latencies are not directly
  comparable across these two storage stacks.
- **Deployment shape (previously misstated).** An earlier draft of this section
  said the native-vs-VM difference "favours etcd". The opposite is true for
  writes: native APFS with `F_FULLFSYNC` is *expensive* (~10–30 ms), while
  virtiofs through a VM page cache is *cheap* (flushes may be no-ops). The
  controlled variant — running distrikv natively on APFS too — would make its
  write latencies materially *worse*, not better, because its fsyncs would
  then pay the same real cost etcd's do. That variant is in the runbook below.
- **Transport and encoding.** distrikv speaks HTTP/1.1 with JSON bodies; etcd
  speaks gRPC with protobuf over HTTP/2 with request multiplexing. The 256-byte
  value is identical, but distrikv additionally pays JSON framing and one
  connection per in-flight request.
- **Read consistency**, as above. etcd is left at its default linearizable read
  rather than being weakened with `WithSerializable()`, so etcd is measured at
  the guarantee it advertises.
- **Maturity.** etcd's numbers reflect years of tuning against exactly this
  kind of benchmark. distrikv's reflect a few weeks.

#### Running it (Mac validation runbook)

```bash
# 1. Install etcd (the harness launches it; it never needs to be running first)
brew install etcd
etcd --version        # the published table was measured on 3.7.1

# 2. Build the harness. Separate module, so build from its own directory.
cd ~/documents/github/distrikv/bench/etcd
GOPROXY=direct go mod download
CGO_ENABLED=0 go build -o etcd-ceiling .

# 3. Run the three workloads exactly as the published table was measured:
#    offered 2000 / 6000 / 3000 QPS, 60s after a 5s warmup, 256 B values,
#    100k-key uniform distribution and 256 workers (both harness defaults,
#    left explicit here so the run is reproducible from this block alone).
./etcd-ceiling --qps 2000 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform \
  2>&1 | tee etcd_writes.txt

./etcd-ceiling --qps 6000 --duration 60s --warmup 5s \
  --mix 0:100:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform \
  2>&1 | tee etcd_reads.txt

./etcd-ceiling --qps 3000 --duration 60s --warmup 5s \
  --mix 20:80:0 --workers 256 --valuesize 256 --keyspace 100000 --keydist uniform \
  2>&1 | tee etcd_mixed.txt
```

**Matched-parameter variant** — removes the two disclosed asymmetries by driving
etcd at distrikv's distribution, worker count and offered write load. Run it in
addition to the block above, not instead of it, so the published numbers stay
reproducible:

```bash
./etcd-ceiling --qps 1200 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf \
  2>&1 | tee etcd_writes_matched.txt
```

Then re-measure distrikv so both columns come from one sitting on one machine.
Note this reproduces the distrikv column *as published* — Zipf, 128 workers,
1,200 QPS offered on writes — which is why it is not flag-for-flag identical to
the etcd block above; the matched-parameter variant is the one that closes that
gap, from the etcd side:

```bash
cd ~/documents/github/distrikv
GOPROXY=direct docker compose -f docker/docker-compose.yml up -d --build && sleep 20
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench

for spec in "1200 100:0:0 writes" "6000 0:100:0 reads" "3000 20:80:0 mixed"; do
  set -- $spec
  docker run --rm --network docker_default -v "$PWD:/b" alpine \
    /b/bench-linux --target node1:8001 --qps $1 --duration 60s --warmup 5s \
    --mix $2 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf \
    2>&1 | tee distrikv_$3.txt
done
```

Optional, and the thing that removes the biggest confound — the same distrikv
workload against three **native** nodes, so both stores are measured outside a VM:

```bash
cd ~/documents/github/distrikv
go build -o distrikv-node ./cmd/node
# start three nodes on 8001/8002/8003 per the "Running the Cluster" section, then:
go build -o bench ./cmd/bench
./bench --target localhost:8001 --qps 1200 --duration 60s --warmup 5s \
  --mix 100:0:0 --workers 128 --valuesize 256 --keyspace 100000 --keydist zipf
```

**Validity gates, in order of what invalidates a run:**

1. `errors: 0` in every run. A non-zero count means the run measured failure
   handling, not the store.
2. `saturation: false`. A saturated queue means the tail is arrival-queue wait
   and the run describes the harness, not the store.
3. `achieved_qps ≈ target`. If achieved falls materially below target with
   `saturation: false`, the arrival process was starved and the run is suspect.

To drive an etcd cluster that is already running instead of launching one:

```bash
./etcd-ceiling --no-cluster --endpoints 127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383 --qps 2000
```

`bench/etcd/README.md` documents the harness's flags and the exact points where
its methodology matches — and deliberately diverges from — `cmd/bench`.

### Read-path engine metrics (`--prefill`)

Every cluster bench before this one reported `bloom_hits=0`, `bloom_misses=0`,
`block_cache_hits=0`, `block_cache_misses=0` and `compactions=0`. The counters
were never broken — they are wired from `lsm.WithMetrics` through
`store.NewWithMetrics` (`cmd/node/node.go`) and incremented in `sstable.go`
(Bloom, block cache) and `compaction.go`. The workload simply never left memory.

**Mechanism.** `Memtable.Put` subtracts the replaced entry's bytes before adding
the new ones, so the memtable accounts for **live** entries only. A Zipfian
workload rewrites a small hot set, so resident bytes plateau far below the 4 MB
flush threshold no matter how many ops are issued: at 16 B keys and 256 B values
it takes roughly 15,400 *distinct* keys to fill a memtable, and a
Zipfian α = 1.1 draw over 100,000 keys does not reach that many distinct keys in
a 60 s run. No flush means no SSTable, and no SSTable means there is no Bloom
filter, block cache or compaction to measure. The zeros were honest.

**`--prefill`** writes every key in `[0, keyspace)` exactly once, before the
warmup window, so resident bytes grow monotonically: the memtable fills,
flushes, L0 accumulates, compaction runs, and reads must then traverse SSTables.

Why a flag and not just existing flags: `--mix 100:0:0 --keydist sequential`
gets close, but coverage of the keyspace becomes a function of
`--qps × --duration`, arrivals are Poisson-scheduled rather than exhaustive, and
any write that fails leaves a hole. A hit rate measured over a partly-written
keyspace is not a hit rate — reads of never-written keys are Bloom *misses* that
inflate the miss count and deflate the cache hit rate. `--prefill` is therefore
exhaustive, closed-loop (not rate-limited), and retries transient
`503 ErrWriteStalled` backpressure, which a prefill is the workload most likely
to provoke. If any key cannot be written after its retry budget, the run
**aborts** rather than publishing a number that looks measured but is not.

```bash
# Prefill 500k keys, then measure a read-only Zipfian window over them.
# (This is the command that produced the measured table below. The older
#  dev-container table further down used --keyspace 200000.)
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --prefill \
    --qps 4000 --duration 60s --warmup 60s \
    --mix 0:100:0 --keyspace 500000 --keydist zipf \
    --workers 128 --valuesize 256
```

#### Measured (2026-08-17, 3-node cluster, Apple M4 Pro, Colima VM 8 CPU / 8 GB, 500k-key prefill)

Prefill phase — 500,000 keys × 256 B, written once each:

| Prefill | Value |
| --- | ---: |
| Keys written / failed | 500,000 / **0** |
| Throughput | 3,071 keys/s |
| `compactions_total` | **6** |
| Compacted data | 314.5 MB |
| Write amplification (WAF) | **4.28×** |
| `write_stall_count` | 0 |
| Retries / failures | 0 / 0 |

Read phase — 60 s read-only windows at 4,000 QPS target, 0 errors across all
three patterns:

| Read pattern (60s, 4,000 QPS, 0 errors) | `bloom_hits` | `bloom_misses` | `bloom_fp_rate` | **cache hit rate** | p50 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Zipf α=1.1, 500k keys | 125,648 | 0 | — | **97.2%** | 634 µs | 1.60 ms |
| Uniform, 500k keys | 77,948 | 0 | — | **70.9%** | 633 µs | 1.62 ms |
| Uniform, 1M keys (half absent) | 39,078 | 39,755 | **0.01%** | 71.1% | 632 µs | 1.62 ms |

Three things the Mac validation adds over the DevSpaces measurement (below):

1. **The Zipf-vs-uniform cache gap at 500k keys is 26 points** (97.2% vs 70.9%),
   compared to 32 points at 200k on the dev container (99.3% vs 60.8%). A
   larger working set at fixed 64 MB cache naturally compresses the gap — the
   cache covers fewer of the keyspace's distinct blocks — and the effect is in
   the expected direction.
2. **Bloom false-positive rate measured on the negative-lookup run: 0.01%.**
   Of 39,078 bloom hits in the 1M-key run (half absent), 0.01% were false
   positives — well below the 1% design target. This is measured, not estimated:
   `bloom_fp_rate` is `BloomFalsePositives / BloomHits`, and both are hard
   `atomic.Uint64` counters in `internal/metrics`, incremented in
   `SSTableReader.Get` (`internal/store/lsm/sstable.go`) — `BloomHits` on every
   filter positive, `BloomFalsePositives` at both sites where that positive then
   fails to produce the key (no block covers it, and the block scan misses). The
   only imprecision is display: the harness prints the rate to two decimals, so
   0.01% pins the underlying false-positive count to single digits rather than to
   an exact value. The DevSpaces run reported exactly 0.00% at 200k scale; 500k
   keys with more L0 residency between compactions produced the first measurable
   (but negligible) FP rate.
3. **Latency stable across patterns.** p50 ~633 µs and p99 ~1.6 ms regardless
   of access pattern — the SSTable read path is dominated by the HTTP + ring
   forward hop, not by cache hits/misses. Cache misses trade a `ReadAt` syscall
   for a sharded map lookup but both are sub-microsecond relative to the network.

#### Earlier measurement (2026-08-17, 3-node cluster, Linux dev container, 200k-key prefill)

**Read this table for the engine ratios, not for throughput.** All three nodes
ran on one fsync-bound container filesystem, which held the prefill to 406
keys/s and every read pass to ~400 QPS; the cluster throughput and latency
numbers remain the M4 Pro figures in the table above. What transfers across
hardware is which counters move and in what proportion.

Prefill phase — 200,000 keys × 256 B, written once each:

| Prefill | Value |
| --- | ---: |
| Keys written / failed | 200,000 / **0** |
| Wall time (throughput) | 492 s (406 keys/s) |
| `flush_bytes` (node1) | 34.9 MB across 8 flushes |
| `compactions_total` (node1) | **2** |
| `compaction_bytes_written` | 52.3 MB |
| `write_stall_count` | 0 |
| Write amplification (WAF) | **2.50×** |

Read phase — three 60 s read-only windows over that same prefilled data,
changing only the access pattern:

| Read pattern | `bloom_hits` | `bloom_misses` | cache hits | cache misses | **hit rate** | `bloom_fp_rate` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Zipf α=1.1, 200k keys, 20 s warm | 12,624 | 0 | 11,675 | 949 | **92.5%** | 0.00% |
| Zipf α=1.1, 200k keys, 60 s warm (cache already warmed by prior passes) | 12,547 | 0 | 12,465 | 82 | **99.3%** | 0.00% |
| Uniform, 200k keys | 7,136 | 0 | 4,338 | 2,798 | **60.8%** | 0.00% |
| Uniform, 400k keys (half absent) | 3,536 | **3,963** | 2,908 | 628 | 82.2% | 0.00% |

Four things this measures that the unit-level benchmarks could not:

1. **The read path is real end to end.** Client → HTTP → ring → gRPC forward →
   LSM → SSTable → Bloom filter → block cache all execute, and the counters
   reconcile: of 23,918 reads, 11,253 were forwarded to peers, leaving 12,665
   served locally against 12,624 Bloom lookups — essentially every locally
   served read consulted an SSTable.
2. **The cache earns its keep on skew, and only on skew.** Same data, same
   cluster, same cache: 92.5% under Zipfian versus **60.8% under uniform**. The
   32-point gap is the block cache's entire value proposition, isolated by
   changing one flag.
3. **The hit rate is warm-window-dependent, and converges on the unit-test
   figure.** A 20 s warmup still pays first-touch misses on cold blocks (92.5%);
   a longer warm window reaches **99.3%**, which agrees with
   `TestBlockCache_ZipfHitRate`'s 99.2% at the same 64 MB default cache. The
   cluster read path and the synthetic bench measure the same thing.
4. **Bloom filters only pay off on absent keys — and here they pay off
   perfectly.** In these runs `bloom_misses` is 0, and the reason has two parts.
   Structurally, `Compactor.Compact` merges *all* L0 plus *all* L1 into a single
   output SSTable with no size-based split, so **L1 is always exactly one file**
   at any data volume, and neither `LSMTree.Get` nor `SSTableReader.Get` prunes
   by key range before consulting the filter. Contingently, the prefill happened
   to end just after a compaction, leaving `l0_file_count=0` — so a read
   consulted exactly one filter for a key that was present, which a Bloom filter
   can only answer "might contain". Had 1–3 L0 files been resident (the normal
   state between compactions), each read would have paid one Bloom *miss* per L0
   file lacking the key, so this row is a floor on Bloom activity, not a ceiling.
   Point half the reads at keys that were never written and the negative path
   appears unambiguously: **3,963 of 7,499 lookups (52.8%) were answered
   "definitely absent" without touching a block**, at a **0.00% false-positive
   rate** (0 of 3,536 positives).

#### Reproducing on a Docker cluster

```bash
# 0. Fresh cluster — a prefill measurement must not inherit another run's data
docker compose -f docker/docker-compose.yml down -v
GOPROXY=direct docker compose -f docker/docker-compose.yml up -d --build && sleep 20
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bench-linux ./cmd/bench

# 1. Prefill + Zipfian read window (one command: prefill runs before warmup)
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 --prefill \
    --qps 4000 --duration 60s --warmup 60s --mix 0:100:0 \
    --keyspace 500000 --keydist zipf --workers 128 --valuesize 256 \
  2>&1 | tee bench_readpath_zipf.txt

# 2. Same data, uniform reads — isolates what the cache is worth on skew
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 \
    --qps 4000 --duration 60s --warmup 60s --mix 0:100:0 \
    --keyspace 500000 --keydist uniform --workers 128 --valuesize 256 \
  2>&1 | tee bench_readpath_uniform.txt

# 3. Same data, double the keyspace — half the reads are absent keys, which is
#    the only condition under which the Bloom filter's negative path is visible
docker run --rm --network docker_default -v "$PWD:/b" alpine \
  /b/bench-linux --target node1:8001 \
    --qps 4000 --duration 60s --warmup 60s --mix 0:100:0 \
    --keyspace 1000000 --keydist uniform --workers 128 --valuesize 256 \
  2>&1 | tee bench_readpath_negative.txt
```

Validity gates, in order of what invalidates the run:

- `failed=0` on the prefill line. Any other value aborts the run by design.
- `errors: 0` and `saturation: false` on each read pass.
- `compactions` non-zero on the `during prefill` line — if it is 0, the keyspace
  was too small to fill 4 memtables per node and no SSTable merge ever ran.
- `block_cache_hits` non-zero in steps 1–3 and `bloom_misses` non-zero in step 3.
  A zero there means reads never left the memtable.

Steps 2 and 3 deliberately skip `--prefill`: the cluster already holds the data,
and re-prefilling would only rewrite it.



#### On the 86.3% cache hit rate

The frequently quoted 86.3% is the **8 MB point of a cache-size sweep** in
`TestBlockCache_ZipfHitRate`, not the shipped configuration: `defaultBlockCacheBytes`
is **64 MB**, which that same sweep measures at 99.2%. The cluster-level number
above (99.3% warm) matches the default-configuration figure, not the 8 MB one.
Any summary of this project's cache performance should quote 99.2–99.3% at the
64 MB default, or state the cache size alongside 86.3%.

#### What the scrape can and cannot see

`cmd/bench` scrapes `/metrics` from the **first `--target` only**. Reads for keys
owned by another node are forwarded and served there, so their Bloom and cache
counters are recorded on that peer: roughly half of the ops above never touch
node1's engine. Absolute counter values are therefore one node's share, not the
cluster total — but every **ratio** (hit rate, FP rate, WAF) is computed from
counters taken on the same node over the same window and is unaffected.

Write-path counters (`flush_bytes`, `compactions_total`, `write_stall_count`) are
reported for the **prefill phase** rather than the measurement window, because a
read-only window cannot produce a flush — and neither can a *write* window after
a full prefill: once every key exists, further writes are same-size overwrites,
which the memtable's live-entry accounting does not count as growth. The flushes
and compactions that put the data on disk all belong to the prefill.

### Phase 2 WAL allocation profile (2026-05-21, Apple M1 Max)

`go test -bench=BenchmarkWAL_Append -benchmem` after the Phase 2 refactor:

| Benchmark | ns/op | B/op | allocs/op | vs. original |
| --- | ---: | ---: | ---: | --- |
| `Append` (256B value) | 4,643–4,687 | 8 | 2 | −3 allocs (bufio.Writer, CRC hasher, []byte(key) eliminated) |
| `Append` (64KB value) | 4,844–4,960 | 8 | 2 | same — value is passed by pointer, not copied |

The 2 remaining allocs/op are baseline OS-call overhead (error interface values
from `f.Sync`), not WAL logic. The dominant hot-path allocations from the
original implementation are gone:

| Source | Original | Phase 2 |
| --- | --- | --- |
| `bufio.NewWriter(f)` per Append | 1 alloc (~4 KB) | 0 — persistent `bw` on struct |
| `crc32.NewIEEE()` per Append | 1 alloc (hash object) | 0 — `crc32.Update` rolling accumulator |
| `[]byte(key)` for CRC pass | 1 alloc (key size) | 0 — `unsafe.Slice` zero-copy view |
| `bufio.NewReader(f)` per Replay | 1 alloc (read buffer) | 0 — pooled `*[]byte` from `sync.Pool` |
| `make([]byte, keyLen)` per entry | 1 alloc per WAL entry | 0 — pool buffer reused across entries |

Replay delivers owned copies of values (1 alloc per entry, unavoidable since
the Memtable tree retains value references); keys produce 1 `string(buf)` copy
per entry (unavoidable for string immutability). All other Replay allocations
are eliminated on the warm path.

**Torn-write contract (hardened in Phase 2):**
- `Replay` stops cleanly (`nil` return) for `io.EOF`, `io.ErrUnexpectedEOF`,
  or CRC mismatch at any byte offset within an entry — the expected signature
  of a sudden power loss or `kill -9`.
- `Replay` returns a **non-nil error** only for genuine I/O failures (e.g.
  disk read error mid-entry). Previously, all errors were silently swallowed
  as clean truncations — a disk failure would have been misread as a torn
  write, masking data loss from the operator.

### Phase 3 — Leveled Compaction Strategy (LCS) + Write-Stall Backpressure

#### What changed

The LSM storage engine was restructured from a flat `readers []*SSTableReader`
slice to a two-level model:

| Level | Produced by | Key-range overlap | Read order |
| --- | --- | --- | --- |
| **L0** | Memtable flushes | May overlap (newest-first wins) | All L0 files checked, newest first |
| **L1** | L0→L1 compaction | Non-overlapping | Single merged output, newest first |

A compaction triggers when `len(l0) ≥ 4` (configurable). The compaction job
merges all L0 files **plus** the existing L1 file into a single new L1 file
using the existing k-way `MergeIterator`, which deduplicates by sequence number
and drops tombstones (safe because L1 is always the bottom level — no L2+
exists yet).

The Manifest tracks each SSTable's level so the engine reconstructs the correct
L0/L1 split on restart. Pre-Phase-3 manifest events default to `level=0`.

#### Write-stall backpressure

Unthrottled writes can produce L0 files faster than the background compaction
goroutine can drain them. Left unchecked, this degrades read performance (more
L0 files to scan) and eventually exhausts disk space. The new
`maybeStallWrite` gate applies two tiers of backpressure **before** the write
lock is taken, avoiding any lock-ordering issue:

| L0 file count | Behaviour | Default threshold |
| --- | --- | --- |
| < `l0SlowThreshold` | No stall | — |
| `l0SlowThreshold` ≤ n < `l0StopThreshold` | **Soft stall** — proportional sleep 5–50 ms, repeated until L0 drains or the caller's context is done | 8 files (2× compaction trigger) |
| ≥ `l0StopThreshold` | **Hard stop** — waits for compaction to signal that L0 drained, giving up with `ErrWriteStalled` after `maxStallWait` | 12 files (3× compaction trigger), 1 s budget |

The two tiers bound differently on purpose. Soft stall is a throttle — the write
is going to be accepted, just later — so it waits exactly as long as its caller
is willing to. Hard stop means the engine is *refusing* writes, so it says so:
after `maxStallWait` (1 s) the write returns `lsm.ErrWriteStalled`, re-exported as
`store.ErrWriteStalled`. An unbounded wait there is worse than an error, because
it makes an overloaded node indistinguishable from a dead one — which is exactly
how a replica ends up consuming its primary's entire replication deadline and
being reported as unreachable. `ErrWriteStalled` says "alive, overloaded, retry".

Every stall event increments `metrics.WriteStallCount` and adds elapsed
microseconds to `metrics.WriteStallMicros` — both visible via `/metrics`.
The `l0_file_count` gauge tracks the current L0 depth in real time.

Both thresholds are tunable via `WithL0StallConfig(slowThreshold, stopThreshold)`
for tests or custom deployments; `WithCompactThreshold(n)` moves the compaction
trigger itself.

#### Reopening on an accumulated store (write-availability)

Backpressure keys off the **live L0 file count**, which is restored from the
manifest at open — so a store that was closed with a compaction backlog reopens
already stalled. Compaction is otherwise only ever armed by a memtable flush, or
by a compaction that left work behind. Neither happens at open, which produced a
deadlock rather than a delay:

```
writes stall (L0 ≥ 12 from the manifest)
   → no write reaches the memtable
   → no memtable fills
   → nothing flushes
   → nothing signals compaction
   → L0 never drains → writes stall …
```

`NewLSMTree` now arms compaction at open whenever the restored L0 set is already
over the compaction threshold, and logs `lsm: armed compaction at open`.

This is what made a three-node cluster inherited from a busy data volume serve KV
traffic terribly while every container reported healthy: a chaos baseline with no
faults injected managed **287 ops / 30 s with 164 errors**, and stayed that way
for 80+ minutes, where a clean-slate cluster on the identical binary served
**115,170 ops / 30 s with 0 errors**. The failure surfaced as replication and
forward errors (503s and 502s) because the writes never returned — the stalled
node was a black hole, not a slow node.

**Validated on dirty volumes (2026-08-16, commit e59a545).** The same accumulated
data volumes (bench + chaos residue) that produced 287 ops / 164 errors before the
fix now produce **122,551 ops / 30 s with 0 errors**, PASS in 58 ms. The
compaction-arm-at-open fix converts a permanent write outage into a sub-100 ms
recovery window.

Measured on Linux with `internal/store/lsm/recovery_availability_test.go`, a store
closed with 13 live L0 SSTables and reopened with production defaults:

| | Before | After |
| --- | --- | --- |
| Time until the first write is accepted | never (no write accepted in 30 s, `compactions=0`) | **63 ms** |
| L0 depth after that first write | 13 (unchanged) | 0 (backlog merged) |
| A hard-stopped write whose context expires | never returns — the wait ignored `ctx` entirely | returns `context.DeadlineExceeded` at the deadline |
| A hard-stopped write with no deadline of its own | never returns | `ErrWriteStalled` after the stall budget |

The bench-scale end of the same file (`TestRecovery_BenchScaleReopen`, gated on
`DISTRIKV_RECOVERY_REPRO=1`) builds the ~200k × 256 B state the field cluster
held and reports time-to-first-accepted-write plus write p50/p99 through the
recovery window. At **production settings on a local ext4 disk it does not
reproduce**, and that is the informative part: 200k keys took 9 m 20 s to write
(357 writes/s, WAL-fsynced), compaction kept up (3 compactions, **1 live L0 file**
at close), and the reopen was healthy — `NewLSMTree` returned in **1.7 ms**, the
first write was accepted in **2.1 ms**, and the following 2000 writes ran
p50 = 2.7 ms / p99 = 3.2 ms with zero stalls. Key *count* is not what breaks
recovery; the L0 *backlog* is, and whether one accumulates depends on how
expensive compaction is on the volume — on Colima/virtiofs each compaction pays
six manifest fsyncs and falls behind the flush rate, where ext4 keeps up.
`DISTRIKV_RECOVERY_COMPACT_THRESHOLD` raises the compaction trigger to emulate
that, and the harness always prints the L0 depth it actually achieved so a run
is never read as more than it is.

With the trigger raised (`DISTRIKV_RECOVERY_COMPACT_THRESHOLD=1000`), the same
200k × 256 B build closes with **13 live L0 SSTables** — the field condition at
field scale — and the reopen behaves as the deterministic test predicts: open in
1.7 ms, one stall of 268 ms while a single compaction merges the whole ~50 MB
backlog, **first write accepted at 270 ms**, L0 drained to 0, and the following
2000 writes at p50 = 2.7 ms / p99 = 3.1 ms — indistinguishable from a store that
never had a backlog. The recovery window is therefore bounded by one compaction
pass over the accumulated data, not open-ended. On the unfixed engine the same
directory accepts no write at all.

That 1.7 ms reopen also settles startup ordering as a factor: a node opens its
store in `NewNode` and only starts its gRPC/HTTP listeners in `Run`, so a slow
recovery would be indistinguishable from a down node — but recovery itself is
cheap (WAL replay loads a single memtable with no per-entry fsync), so the
black-hole window came from the stall above, not from recovery time, and the
ordering was left alone.

#### Read path after Phase 3

```
Get(key)
 └─ mem (active Memtable)
 └─ imm (immutable Memtable, non-nil only during a flush)
 └─ l0[0], l0[1], … l0[n-1]   ← newest-first, ALL files checked (keys may overlap)
 └─ l1[0]                      ← single merged output, non-overlapping
```

The overhead of scanning all L0 files is bounded by the compaction trigger
threshold (≤ 4 files in steady state), so read amplification stays predictable.

#### Phase 3 test coverage

| Test | What it verifies |
| --- | --- |
| `TestLCS_ReadCorrectness` | All keys readable after L0→L1 compaction; last writer wins across rounds |
| `TestLCS_TombstoneDroppedAfterCompaction` | Deleted keys return `ErrNotFound`; no tombstone entries in L1 SSTables |
| `TestRecovery_ReopenWithL0Backlog_AcceptsWrites` | A store closed with L0 at the hard-stop threshold accepts a write after reopening, and the backlog drains (fails by hanging on the unfixed engine) |
| `TestRecovery_HardStopRespectsContext` | A hard-stopped write returns when its caller's context expires |
| `TestRecovery_HardStopReturnsStallError` | A hard-stopped write with no deadline of its own returns `ErrWriteStalled` within the stall budget |
| `TestRecovery_BenchScaleReopen` | Env-gated harness: ~200k × 256 B state, reopen, time-to-first-accepted-write and write p50/p99 through recovery |
| `TestRestore_BulkLoadPerformsNoWALAppends` | Snapshot restore performs zero WAL appends and lands as one L0 SSTable |
| `TestRestore_SurvivesReopen` | Restored state, live-key count included, survives close/open; sentinel removed |
| `TestRestore_EmptySnapshotEmptiesStore` | An empty payload empties the store and leaves it writable |
| `TestLCS_WriteStallMetrics` | `WriteStallCount` and `WriteStallMicros` increment correctly during soft-stall loops |
| `TestLCS_L0CountTracking` | `l0Count` atomic, `L0FileCount` metric, and `len(l.l0)` stay consistent before and after compaction |

### Phase 4 — Deterministic Fault Injection & Jepsen-style Linearizability Verification (2026-05-21)

#### What changed

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

The checker was right. It detected precisely the caveat the CAP section above
states in bold — *a refused write is not an undone write* — and the model was the
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
it becomes the primary for that key — the divergence the CAP section documents
(no anti-entropy, no hinted handoff, no read repair) is real but unobserved by this
history. Killing the *primary* of a key makes that key unavailable rather than
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

#### Phase 4 test coverage

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

### Phase 5 — In-process Sharded LRU Block Cache

#### What changed

The read path previously issued a `f.ReadAt` syscall for every SSTable data block
that passed the Bloom filter.  Under a hot-key workload this means the same
blocks are fetched from the OS page cache on every request — wasting a syscall
and a kernel context switch per block even when the data is already in RAM.

Phase 5 introduces an **in-process sharded LRU block cache** that intercepts
`readBlock` before it touches the file:

```
Get(key)
 └─ Bloom filter: MightContain?  no → return (no disk I/O, no cache)
 └─ blockIndexFor(key)
 └─ readBlock(idx):
     ├─ cache.Get(path, offset)?  yes → BlockCacheHits++, decode, return
     └─ no → f.ReadAt + cache.Put(path, offset, raw) + BlockCacheMisses++
```

The Bloom filter still runs first — the cache only sees keys that pass the
filter.  On a cache miss the block is read from disk and inserted; on subsequent
reads (same key, same block, or adjacent keys sharing a block) the `ReadAt` is
skipped entirely.

#### Design

| Property | Value |
| --- | --- |
| Shards | 32 (`numCacheShards`) — one `sync.Mutex` per shard, zero cross-shard contention |
| Default capacity | 64 MB (`defaultBlockCacheBytes`) — configurable via `WithBlockCacheBytes(n)` |
| Eviction policy | LRU per shard — `container/list`, O(1) promote / evict |
| Cache key | `(sstable-path, block-offset)` — unique across all live SSTables |
| Stored value | Raw block bytes (pre-decode) — avoids the `ReadAt` syscall; decoding is CPU-cheap |
| Shard routing | `FNV-1a(path + offset) % 32` — balances entries from the same SSTable across shards |
| Disabled | `WithBlockCacheBytes(0)` — returns `nil *BlockCache`; all methods are no-ops |

Entries for compacted-away SSTables are proactively evicted via `cache.Evict(path)`
right after the compaction swap, so stale memory is freed without waiting for LRU pressure.

#### Metrics

Two new counters are visible via `/metrics`:

| Counter | Meaning |
| --- | --- |
| `block_cache_hits` | `readBlock` calls served from the in-memory cache (syscall skipped) |
| `block_cache_misses` | `readBlock` calls that required a disk read and subsequently populated the cache |

`cmd/bench` now reports a `hit_rate` line in the engine-side section:

```
engine-side:   bloom_hits=240118  bloom_misses=119  bloom_fp_rate=0.04%
               flush_bytes=42MB  compaction_bytes_written=64MB  WAF=2.52
               compactions=6  forwarded_requests=99882  replication_errors=0
               block_cache_hits=198432  block_cache_misses=1680  hit_rate=99.2%
```

A hit rate above 95% on a hot-key workload (`--keyspace 20`) is expected; a
workload larger than the cache (`--keyspace 100000 --valuesize 4096`) will produce
a lower hit rate due to natural working-set pressure.

#### Phase 5 test coverage

| Test | What it verifies |
| --- | --- |
| `TestBlockCache_HitRateAfterWarmup` | Cold pass: `BlockCacheMisses > 0`; warm pass: `BlockCacheHits ≥ cold misses`, zero misses |
| `TestBlockCache_NilSafe` | `WithBlockCacheBytes(0)` produces no panics and all keys are still readable |
| `TestBlockCache_EvictOnCompaction` | Post-compaction keys remain readable; cache eviction does not corrupt data |
| `BenchmarkBlockCache_ColdVsWarm` | Microbenchmark isolating cold `f.ReadAt` vs warm in-process cache path |
| `TestBlockCache_ZipfHitRate` | Zipfian α=1.1 hit-rate sweep across 4 cache sizes (0, 8, 32, 64 MB); 100k-key dataset, 768 B values, reverse-scan warm + 10k measurement pass |

#### Phase 5 latency profile (2026-05-21, Apple M1 Max)

`go test -bench=BenchmarkBlockCache_ColdVsWarm -benchtime=3s -count=3 -benchmem`

| Path | ns/op | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| **Cold** (no cache, `ReadAt`) | 8,013 | 24,576 | 227 | OS page cache warm; SSD not involved |
| **Warm** (in-process LRU cache) | 7,480 | 24,576 | 227 | ~7% faster; identical allocation profile |

**Why the gap is small here:** the test SSTable is tiny (~6 KB) and was just written, so it is fully resident in the OS page cache. A page-cached `ReadAt` on M1 costs ~500 ns — competitive with a sharded map lookup. The 227 allocs/op are dominated by `string(...)` copies during block decoding, which happen equally on both paths. Both paths also allocate a fresh `[]byte` for the block data (cold: `make([]byte, blockLen)` inside `ReadAt`; warm: the copy returned from `cache.Get`).

**When the cache earns its keep:**

| Scenario | Benefit |
| --- | --- |
| Working set > OS page cache | Blocks are in Go heap instead of requiring kernel I/O + context switch; warm reads stay in user space |
| High concurrency (many goroutines, same hot blocks) | Eliminates serialization on the kernel page-cache lock; sharded in-process cache has finer-grained locking |
| Large SSTables with cold blocks (post-restart) | First access warms the in-process cache; subsequent accesses skip the syscall and kernel→user copy |
| Mixed read/write (compaction pressure) | Compaction evicts old SSTables from both the in-process cache and triggers OS cache pressure; in-process cache survives the swap since it evicts by path, not by fd |

The Phase 1 benchmark baseline used a 100k-key uniform workload where the full dataset is many times larger than any reasonable in-process cache — that workload will show minimal cache benefit. The cache targets hot-key / Zipfian workloads (`--keydist zipf`) where a small fraction of keys accounts for most reads. The `block_cache_hits` / `hit_rate` fields in `cmd/bench` output make this visible without guesswork.

#### Phase 5 Zipfian hit-rate baseline (2026-05-22, Apple M1 Max)

`go test ./internal/store/lsm/ -run TestBlockCache_ZipfHitRate -v -timeout 120s`

**Setup:** 100,000 keys × 768-byte values (≈ 76 MB) across 4 L1 SSTable files (~19 MB each, simulating 4 compaction rounds at the default 4 MB memtable threshold). **Distribution:** Zipfian α = 1.1 — the YCSB/RocksDB canonical exponent, where the top 1% of keys account for ~54% of reads and the top 10% account for ~88%. **Warm pass:** reverse-rank sequential scan (rank 99,999 → 0) so the LRU eviction leaves the hottest blocks resident. **Measurement:** 10,000 seeded-random (seed=42) Zipfian reads.

| Cache size | Hits | Misses | Hit rate |
| ---: | ---: | ---: | ---: |
| 0 MB (disabled) | 0 | 10,000 | 0.0% |
| 8 MB | 8,633 | 1,367 | **86.3%** |
| 32 MB | 9,579 | 421 | **95.8%** |
| 64 MB (default) | 9,920 | 80 | **99.2%** |

The 86.3% figure for 8 MB is analytically consistent: 8 MB holds ~2,048 blocks, covering the top ~10,240 keys, which at α = 1.1 capture ≈ 88% of reads (harmonic sum H ≈ 6.84). The cache earns disproportionate returns on the first few megabytes — 8 MB captures 86% of the benefit that 64 MB achieves.

### Operator notes

- `--workers` sizes the worker pool *and* the HTTP connection pool
  (`MaxIdleConnsPerHost = workers + 64`). Go's defaults cap idle conns/host
  at 2; the bench overrides this so the pool can actually hold a connection
  per worker. Connection reuse also depends on the client draining response
  bodies — see the throughput baseline notes for the bug where it didn't.
- Prefer running the bench inside the cluster's Docker network
  (`docker run --network docker_default …`) rather than through a localhost
  port forward: forwarders (Docker Desktop, Colima/Lima) add a proxy hop to
  every request, and macOS additionally caps concurrency at its ephemeral
  port range. If you must bench through the host, raise
  `kern.ipc.somaxconn` and widen `net.inet.ip.portrange.first`.
- Pass `--output json` for a machine-readable report (suitable for
  piping into diff tools across runs).
- A node started on a volume with accumulated data logs
  `lsm: armed compaction at open` when it inherits an L0 backlog; the recovery
  window is then bounded by one compaction pass rather than being open-ended.
  If writes come back `503` with `write stalled: L0 compaction backlog` in the
  message, the node is alive and merging — check `l0_file_count` and
  `write_stall_count` on `/metrics` and expect them to fall. A node that is
  *unreachable* fails differently (`502`, or a replication deadline), which is
  the distinction `ErrWriteStalled` exists to preserve.

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
