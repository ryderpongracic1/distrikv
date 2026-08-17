# Defect Log

Eleven real defects, found and closed. Each one is recorded with the evidence that
exposed it rather than quietly fixed, because in most cases the *way* it surfaced
is the more interesting half: the measurement infrastructure kept paying compound
interest, and every fix made the next layer visible.

The pattern, stated once so the individual entries can be read quickly:

- the **benchmark harness** caught a client bug ([2](#defect-2-the-http-client-never-drained-response-bodies))
- the **linearizability checker** caught a model bug ([3](#defect-3-the-linearizability-model-recorded-refused-but-applied-writes-as-no-ops))
- the **model fix** caught a classifier bug ([4](#defect-4-the-502-rewrite-destroyed-the-never-sent-classification))
- a **deterministic test failure on the author's Mac** caught data loss Linux never showed ([5](#defect-5-compaction-dropped-acknowledged-writes))
- the **container logs**, read while investigating something else, caught a Raft layer that had never worked ([6](#defect-6-no-leader-had-ever-sent-a-heartbeat))
- the **convergence gate** caught WAL segment reuse ([10](#defect-10-wal-segment-numbers-reused-across-a-graceful-restart))
- and the **Porcupine checker**, within hours of that convergence noise being cleared out of its way, caught the sequence counter ([11](#defect-11-lsm-sequence-counter-reopened-at-zero))

> **On the numbering.** These numbers are assigned here, in this document, for
> reference. They are not a pre-existing scheme carried over from anywhere: before
> this document existed the defects were narrated in place, in the subsystem
> sections they belonged to, with no index. The ordering is roughly chronological
> by discovery. Each entry links to the full narrative, which remains where the
> defect happened — this is an index, not a replacement.
>
> Eleven is the count of defects serious enough to be narrated. Smaller findings
> that were fixed alongside them are listed under
> [Smaller findings](#smaller-findings) rather than folded into the count, so the
> headline number is not inflated by them.

---

## Defect 1: The replication fan-out had no callers

**Class:** the feature did not exist · **Found by:** code review · **Narrated in:** [architecture.md → Status, Phase 3 note](architecture.md#status)

`Node.ReplicateWrite` was written during Phase 3, and nothing ever called it — no
caller existed in any commit from the initial one onward. Until it was wired into
the ring-primary's write path, **every key lived on exactly one node** and the
receive side (`Replicate` → `ApplyReplica`) was never exercised outside tests. The
README's replication, its CAP claims and its throughput model all described code
that never ran.

**Fix.** Both client entry points — a request that lands on the ring-primary
directly, and one forwarded to it by a peer over gRPC `ForwardKey` — were routed
through a single `primaryWriter`, so they cannot drift apart on durability or on
failure semantics.

**Consequence that outlived the fix.** Any benchmark table or
`replication_errors=0` reading produced before that wiring describes a single-copy
sharded store, not a replicated one. Those tables are labelled where they appear
rather than deleted.

---

## Defect 2: The HTTP client never drained response bodies

**Class:** performance / resource exhaustion · **Found by:** the benchmark harness · **Narrated in:** [benchmarks.md → Why these numbers replaced the earlier table](benchmarks.md#why-these-numbers-replaced-the-earlier-table)

`internal/client` never drained HTTP response bodies, so Go's transport discarded
every connection instead of returning it to the idle pool and **every request paid
a fresh TCP dial**. Under load this exhausted ephemeral ports
(`cannot assign requested address`) and inflated tail latencies with handshake and
`TIME_WAIT` stalls. It is also the real cause behind the `TIME_WAIT` warning that
used to live in the operator notes.

**Signature.** Benchmark runs reporting tens of thousands of errors
(`errors: 11,913`–`35,327`) with p99s in the *seconds*, and `saturation: TRUE` on
a cluster that was not actually saturated. Running the bench from inside the Linux
network namespace reproduced the port exhaustion, which acquitted macOS and Colima
and convicted the code.

**Fix.** A drain-before-close helper, pinned by a test asserting that 60
sequential calls arrive over one connection.

**What it changed about the numbers.** Writes now do strictly more work per
request — a synchronous replica ACK — and still post a **15× better p99**, because
the connection churn cost far more than replication does.

---

## Defect 3: The linearizability model recorded refused-but-applied writes as no-ops

**Class:** verification tooling reporting a correct store as broken · **Found by:** the first real fault-injection runs · **Narrated in:** [chaos-harness.md → How failed operations are modelled](chaos-harness.md#phase-4--deterministic-fault-injection--jepsen-style-linearizability-verification-2026-05-21)

The first nemesis runs with replication actually wired rejected **both** a SIGKILL
and a SIGTERM history. The graceful nemesis is what settled it: SIGTERM drains
in-flight requests, so it produced **zero** unknown-outcome writes — and the
history was still rejected. That eliminated the torn-connection artefact and left
exactly one mechanism. While a replica was down, the ring-primary answered `503`
for writes it had **already applied to its own store**, the model recorded each of
them as a no-op, and the next read of that key — served by the same primary —
returned the refused value.

The checker was right. It detected precisely the caveat the CAP section states in
bold — *a refused write is not an undone write* — and the model was the thing that
did not encode it.

**Fix.** A three-outcome classification: never-applied (no-op), applied-anyway
(`Output.Applied`), and unknown (`Recorder.EndUnknown`, a pending operation the
checker may place anywhere or nowhere).

---

## Defect 4: The 502 rewrite destroyed the never-sent classification

**Class:** two individually-correct fixes composing into a regression · **Found by:** re-running the nemesis after defect 3 was fixed · **Narrated in:** [chaos-harness.md → How a 502 is separated, and where](chaos-harness.md#phase-4--deterministic-fault-injection--jepsen-style-linearizability-verification-2026-05-21)

The refused-but-applied encoding worked — the false anomalies were gone — but both
runs came back `UNKNOWN (timeout)`. The same release had bounded the forward RPC
with its own deadline and rewritten the 502 body so that it no longer quoted the
underlying transport failure. The runner's *text scan* was the only thing
separating never-sent from ambiguous, so every forward to a downed primary became
a pending operation: **27,356 and 20,519** of them, each overlapping every later
operation on its key.

No larger `--check-timeout` fixes that shape of failure — the search space is
combinatorial in the number of pending operations, not linear in time.

**Fix.** A typed `forward_outcome` field on every 502, `"never-sent"` or
`"unknown"`, decided by the forwarding node — the last point at which the error
still has its gRPC code and has not been flattened into a sentence. The runner
reads the field in preference to the prose.

---

## Defect 5: Compaction dropped acknowledged writes

**Class:** data loss · **Found by:** a deterministic unit-test failure on the author's Mac · **Narrated in:** [lsm-engine.md](lsm-engine.md) and the package doc comment in `internal/store/lsm/lsm.go`

L0→L1 compaction replaced the engine's **whole** live file set with the merge
output. A memtable flush that landed *during* a merge produces an L0 file that is
newer than every input and is therefore not among them — and the swap discarded it.
Acknowledged, durable, WAL-fsynced writes vanished at the moment an unrelated
background merge completed.

**Signature.** `TestBlockCache_HitRateAfterWarmup` failed **5/5** with
`cold Get 108: key not found` — the same key, the same ~0.8 s runtime, no flake.
On the unfixed engine keys 108–134 were absent and 135 onward present: one whole
dropped table, a contiguous run rather than scattered losses. The failure was
deterministic on macOS and had been latent since May.

**Blast radius.** Because the same swap set `l0Count` to 0 unconditionally,
write-stall backpressure could never fire — part of the engine's apparent speed
was the bug skipping compaction work and losing tables. The Raft snapshot path
consumed the same live set, so it was in scope too.

**Fix.** `installCompactionResult` removes only the files it actually merged, by
identity, and retains the rest at L0 *ahead of* the new L1 file in the read path,
which is their correct precedence. The manifest removal in `Compact` touches only
the input set, so a retained table survives a restart as well. Pinned by
`TestCompaction_RetainsL0FlushedDuringMerge` and
`TestInstallCompactionResult_KeepsUnmergedL0`.

---

## Defect 6: No leader had ever sent a heartbeat

**Class:** a subsystem that had never worked in production · **Found by:** reading container logs during an unrelated investigation · **Narrated in:** [raft.md → Correction — the leader-election storm](raft.md#raft-internalraft)

`broadcastHeartbeat` chose between `AppendEntries` and `InstallSnapshot` on
`peerNext > 0 && peerNext-1 <= snapLastIndex`. With an empty log, leader
initialisation sets `nextIndex = lastLogIndex+1 = 1` and `snapLastIndex` is 0, so
the condition was **always true**: every heartbeat tick routed to
`sendInstallSnapshot`, which returned without sending anything because no snapshot
file has ever existed. No heartbeat ever left the leader, and every follower
elected the moment its timer expired.

A second, independently real defect combined with it: each heartbeat RPC was given
a deadline of exactly one send interval, so any RPC delayed past 150 ms by gRPC
connection setup or container scheduling was cancelled by its own deadline. A
genuine `nextIndex` data race was found along the way.

**Signature.** A live 3-node cluster burning roughly **1.7 terms per second
indefinitely**, passing term 900 within nine minutes of startup, alternating
leadership every ~500 ms — since the project's first boot. It went unnoticed
because the data path is ring-based and never consults Raft leadership, so the
storm cost nothing but log volume and CPU.

**Fix.** Heartbeats go to every peer unconditionally; the snapshot condition is
now `nextIndex <= snapLastIndex` and rides alongside the heartbeat rather than
replacing it; the RPC deadline is decoupled from the send period and bounded by
the minimum election timeout.

**Field validation (2026-08-16, commit e59a545).** One election at startup
(term 5130 → 5131, the inherited number a fossil of the storm era), then term
**flat for 2+ minutes**, all three nodes agreeing, exactly one leader at all
times. `internal/raft/cluster_test.go` now fails if the term advances more than
once over three seconds, both idle and with per-RPC latency injected above the
send interval.

**Corroboration worth recording.** Two agents tracing independently from opposite
ends — one in `internal/raft`, one in the storage layer — reached the same
conclusion about this heartbeat condition, and the storage-side one reported it
across an ownership boundary rather than fixing it.

---

## Defect 7: Recovery deadlock — compaction was never armed at open

**Class:** permanent write outage · **Found by:** a chaos baseline on volumes carrying accumulated data · **Narrated in:** [lsm-engine.md → Reopening on an accumulated store](lsm-engine.md#reopening-on-an-accumulated-store-write-availability)

Write-stall backpressure keys off the **live L0 file count**, which is restored
from the manifest at open — so a store closed with a compaction backlog reopens
already stalled. Compaction was otherwise only ever armed by a memtable flush.
Neither happens at open, which is a deadlock rather than a delay: writes stall, so
no memtable fills, so nothing flushes, so nothing signals compaction, so L0 never
drains.

**Signature.** A three-node cluster inherited from a busy data volume served KV
traffic terribly while every container reported healthy: a chaos baseline with
**no faults injected** managed **287 ops / 30 s with 164 errors**, and stayed that
way for 80+ minutes, where a clean-slate cluster on the identical binary served
**115,170 ops / 30 s with 0 errors**. The stalled node was a black hole, not a
slow node, so it surfaced as replication and forward errors (503s and 502s).

Two hypotheses were refuted along the way, and both refutations are kept because
they were cheap and each would otherwise still be believed: stale gRPC channels to
restarted peers (refuted — the container kept its IP and recovered instantly, all
200s), and listeners opening only after recovery completed (refuted — a reopen
takes 1.7 ms, so recovery time was never the black-hole window).

**Fix.** `NewLSMTree` arms compaction at open whenever the restored L0 set is
already over the compaction threshold, logging `lsm: armed compaction at open`.
Alongside it: the hard stop is bounded and returns a distinguishable
`ErrWriteStalled` → `503`, so a stalling node reads as *alive and converging*
rather than dead; `Restore` bulk-loads instead of fsyncing per key; and the
replication deadline became an independent 2 s instead of 2× a Raft tuning knob.

**Validated on the same dirty volumes (2026-08-16, commit e59a545).** The volumes
that produced 287 ops / 164 errors now produce **122,551 ops / 30 s with 0
errors**, PASS in 58 ms. Deterministically: time-to-first-accepted-write goes from
*never* (no write accepted in 30 s) to **63 ms**, and at bench scale to 270 ms.

**Honest negative result.** 200k keys on a local ext4 disk **does not reproduce**
this. The trigger is the L0 *backlog*, not the key count, and whether one
accumulates depends on how expensive fsync is on the volume — on Colima/virtiofs
each compaction pays six manifest fsyncs and falls behind the flush rate, where
ext4 keeps up. The repro harness always prints the L0 depth it actually achieved,
so a run is never read as more than it is.

---

## Defect 8: A replica cursor that survived a snapshot restore was worse than stale

**Class:** silently wrong convergence claim (latent) · **Found by:** a cross-check during merge, then a probe that found the hazard worse than briefed · **Narrated in:** [replication-and-anti-entropy.md → Not guaranteed after a snapshot restore](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not)

`lsm.Restore` starts a fresh WAL at segment 1 and therefore **reuses segment
numbers the old log had already used**. `wal.ErrCursorStale` is keyed on the
segment *number*, so it never fires. The surviving cursor instead (1) orders
*after* the new tip, so the "cursor behind tip" check reads the replica as up to
date and schedules no catch-up; (2) cannot be moved back, since cursors are
monotonic, while `RetentionFloor` keeps naming a segment of a log that no longer
exists — which makes the engine delete freshly flushed segments instead of parking
them; and (3) once the new log grows past the old offset, makes a pass read from a
byte offset that is mid-entry in a different log, where a torn tail on the newest
segment is a **clean stop with no error**. The pass then ships nothing, reports no
failure, and the engine concludes the replica is caught up.

The brief for this fix expected graceful degradation. The probe found the frozen
cursor was additionally corrupting WAL retention and logging false "caught up"
claims — a silently wrong convergence claim, which is why it is fixed rather than
documented.

**Fix.** `store.CursorStore.InvalidateAll`, called from
`Store.RestoreFromSnapshot` *before* the store is replaced, so a crash mid-restore
leaves the safe state — losing cursors is recoverable, stale cursors are not. The
condition is latched durably in the cursor file and surfaced as
`anti_entropy_full_sync_required`, and while it is set a pass that finds nothing
to ship is reported as *"this node cannot converge this replica from its WAL"*
rather than `replica caught up`.

**Reachability, stated plainly.** This path is currently unreachable in a running
cluster: no Raft snapshot file is ever created, because nothing proposes log
entries. It is defensive hardening for whenever snapshots become real, plus
correctness for direct callers. It fixes no live incident, and it is recorded that
way rather than as a save.

---

## Defect 9: Anti-entropy claimed "replica caught up" on evidence it did not have

**Class:** silently wrong convergence claim (four paths, one theme) · **Found by:** external code review, then a probe that found one premise understated · **Narrated in:** [replication-and-anti-entropy.md → WAL retention](replication-and-anti-entropy.md#wal-retention)

Four defects, unified by the rule that no path may log `replica caught up` when
the log cannot prove it:

1. **The retention floor was the minimum over *recorded* cursors.** The replica
   that needs retention most is the one that has been behind since *before* its
   first cursor was persisted: it has no recorded cursor, contributes nothing to
   that minimum, and `advanceQuietCursors` will not give it one. Healthy replicas
   keep adopting tips, so the floor marches forward and the engine deletes
   precisely the segments the down replica is owed. The premise turned out
   **understated**: with *no* cursors recorded at all, `RetentionFloor` returned 0,
   which `releaseWALSegment` reads as *retention off* — so every flushed segment
   was deleted outright. That is the state a freshly started node is in for its
   first few seconds, which is exactly where a chaos run against a fresh cluster
   begins.
2. **A stale-cursor pass returned `(0, nil)`** into `markCaughtUp`. `runPass`
   handled the stale cursor correctly — warned, counted, resumed from the oldest
   survivor — and then reported convergence ten lines above the code that does the
   right thing for the identical condition.
3. **`checkConvergence` never reset `res.Unreachable` between attempts**, so a
   transient read error on attempt 1 left entries printing under a
   `converged: true` summary.
4. **`ReplicateWrite`'s "no client for replica" branch skipped
   `noteReplicationFailure`** — the one hole in "any replication failure marks the
   replica behind".

**Fix.** `publishRetentionFloor` pins the floor at the oldest segment whenever a
replica is known to be behind and has no cursor, armed on the
not-behind → behind transition so the window cannot open mid-fault, and still
bounded by the 128-segment cap. The stale-cursor path latches
`anti_entropy_full_sync_required` instead of claiming convergence.
`res.Unreachable` is cleared per attempt. `RetentionFloor` returns `(uint64, bool)`
so segment 0 stops doubling as "no cursors". All four revert-checks fail on
pre-fix code.

**A fix deliberately rejected.** Seeding each tracked replica's cursor at the tip
was the obvious repair and is wrong: a primary that replayed its WAL after a
restart holds writes *below* the tip that the replica never saw, so seeding at the
tip converts an over-shipping bug into **permanent silent divergence**.

---

## Defect 10: WAL segment numbers reused across a graceful restart

**Class:** silently wrong convergence claim, live · **Fixed in:** `868bad3` · **Found by:** the convergence gate · **Narrated in:** [replication-and-anti-entropy.md → Guaranteed across a primary restart](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not)

A replica cursor is a `(segment, byte offset)` pair, so it only means anything
while segment numbers are never reused. They were. A graceful `Close` flushes the
active memtable into an SSTable and **parks its WAL segment under
`wal-retained/`** — but `NewLSMTree` seeded segment numbering from
`findWALFiles`, which excludes the retained directory. A gracefully stopped
primary could therefore be left with no *live* segment at all, and the next open
reopened numbering at 1 while a parked segment 1 already existed.

Every cursor the primary had persisted then addressed a *different* log, with the
three consequences defect 8 spells out for a snapshot restore: the cursor orders
after the new tip so the replica reads as up to date, `wal.ErrCursorStale` is keyed
on the segment *number* and cannot fire, and a pass reads from a byte offset past
the end of a shorter log — a clean stop with no error, so it shipped nothing and
the engine reported `replica caught up`.

**Signature, and it is the whole diagnosis.** The convergence gate failed
**deterministically under the graceful `stop-restart` nemesis, 2/2, with divergent
replicas, while `kill-restart` passed**. A SIGKILL leaves the segment on disk, so
numbering continued past it and the cursors stayed valid. That asymmetry was the
bug: nothing else in the system distinguishes the two nemeses that way.

Three hypotheses in the dispatch brief were refuted first, and are kept because a
refutation is evidence: all three tombstone-mishandling candidates (the reader
surfaces deletes, dedup keeps the newest, the RPC expresses deletes fine), and the
assumption that node1 was the divergent key's ring primary — it does not even own
the key, so its `not found` was meaningless. The convergence report lists owners in
ring order; the first node listed is the primary.

**Fix.** Segment numbers are seeded from every segment on disk, parked ones
included, so they are monotonic across a restart. A cursor into a segment that was
genuinely released now names a number that no longer exists, which is exactly the
condition `ErrCursorStale` reports and the engine already handles by withholding
the claim. As a backstop, a cursor that orders *after* the tip — impossible in a
log this node kept appending to, so evidence the log was replaced — is dropped at
open, the replica is marked behind rather than assumed fine, and
`anti_entropy_full_sync_required` is latched. Pinned by
`internal/store/lsm/wal_segment_reuse_test.go` and
`cmd/node/antientropy_restart_test.go`, including an in-process repro of the exact
field symptom: a cursor at `1:62` against a tip of `1:0`, and a pass that ships
nothing.

---

## Defect 11: LSM sequence counter reopened at zero

**Class:** data loss / consistency · **Fixed in:** `0ca6c3b` · **Found by:** the Porcupine checker, within hours of defect 10 clearing the convergence noise out of its way · **Narrated in:** [lsm-engine.md → Write sequence numbers](lsm-engine.md#lsm-tree-storage-engine-internalstorelsm)

Every entry carries a sequence number, and that number is how compaction decides
between two versions of a key: the higher one wins and the loser is **dropped**,
not shadowed. `LSMTree.seqNum` lived only in memory, so it restarted at zero on
every open while the SSTables on disk still carried the previous process's numbers.
"Newer" therefore came to mean **"written earlier, in a longer-lived process"**: a
value written before a restart outranked the value that replaced it after, and the
first compaction to merge the two files kept the stale one. The same inversion
resurrects a deleted key, because a tombstone that loses is discarded outright at
the bottom level.

Nothing is visible until that compaction runs, which is what makes it a
*consistency* bug rather than a crash: reads are served newest-file-first, so the
correct value is returned right up until the merge silently replaces it on disk.

**Signature.** `converged: true` with `linearizable: FAIL` — data consistent at
rest, history illegal — flaky at roughly **1 in 4** runs, with one bad read
mid-run and re-convergence by quiescence. That pair is not something the model can
invent: indeterminate writes were 0, and the classifier was re-verified in code.
It is exactly what a resurrected value produces.

Three restart-window hypotheses were refuted with file:line evidence before this
was found — shutdown ordering, startup ordering, and whether a refused-but-applied
write is durable in the WAL before the 503 goes out — as were ring-ownership
migration during a fault (the ring has no health awareness, so a fault never moves
ownership) and replica-side staleness (reads always route to the ring-primary, so
it is invisible to the checker).

**Fix.** Each SSTable's highest sequence number is recorded in the manifest
(`ManifestEvent.MaxSeqNum`), and `seedSeqNum` puts the counter above all of them at
open, **before** WAL replay. A data directory written before that field existed
cannot answer, and the numbers live in the entries themselves, so those files are
scanned once — logged as
`lsm: manifest predates per-SSTable sequence numbers`, and self-limiting, since the
first compaction replaces the whole live set with a file that records it. Pinned by
`TestCompactionKeepsTheNewerWriteAcrossARestart`,
`TestCompactionKeepsATombstoneAcrossARestart`,
`TestLegacyManifestRecoversTheWriteOrderByScanning` and
`TestSequenceNumbersAreMonotonicAcrossARestart` — the last one asserting the
invariant on its own, so a future change that zeroes the counter fails with a
readable reason.

**Tooling shipped in the same commit.** A FAIL used to be one word. The runner now
localises the anomaly and prints the offending key's operation window — op types,
values, timestamps as offsets aligned with the fault-window table, how each
operation was modelled, and which fault window each intersected — plus a JSON dump
for diffing runs. See
[chaos-harness.md → Counterexample on FAIL](chaos-harness.md#phase-4--deterministic-fault-injection--jepsen-style-linearizability-verification-2026-05-21).

**Validation.** [Four consecutive `stop-restart` runs with `--check-convergence`
pass](chaos-harness.md#phase-4--deterministic-fault-injection--jepsen-style-linearizability-verification-2026-05-21),
`converged: true` and `linearizable: PASS` on every one, with run 4 repeatedly
restarting nodes across compaction boundaries on dirty volumes — the exact
condition that used to resurrect stale values.

---

## Smaller findings

Fixed alongside the above, and not counted in the eleven — recorded because
leaving them out would make the list look tidier than the work was:

| Finding | Where |
| --- | --- |
| `wipeLSMDir()` removed WAL/SST/manifest files but not the restore sentinel, so every subsequent open wiped again — an infinite wipe loop. Exposed by the sentinel-recovery test it was written to satisfy | [chaos-harness.md](chaos-harness.md) |
| Three uncoordinated catch-up schedulers queued duplicate cycles for the same replica, so a converged cluster logged six `replica caught up, entries_sent=0, took=0` lines inside one millisecond. Now coalesced per replica | [replication-and-anti-entropy.md → The trigger](replication-and-anti-entropy.md#the-trigger) |
| `LSMTree.Close` was not idempotent — a second call panicked | `internal/store/lsm/lsm.go` |
| The chaos runner failed **4 out of 4** default-flag runs against a healthy single node with zero errors, because `KVModel.Init` is an empty map and warmup writes were being read back as values the model believed could not exist. Fixed with a per-run key nonce; 5/5 pass on the same populated store | [chaos-harness.md → Why keys carry a per-run nonce](chaos-harness.md#phase-4--deterministic-fault-injection--jepsen-style-linearizability-verification-2026-05-21) |
| `Replay` silently swallowed genuine I/O errors as clean truncations, so a disk failure would have been misread as a torn write | [lsm-engine.md → Torn-write contract](lsm-engine.md#phase-2-wal-allocation-profile-2026-05-21-apple-m1-max) |

---

## Recently closed

- **Per-key write ordering** was listed here as a known limit rather than a defect:
  a mutation carried no version, so two client-concurrent writes to one key could
  be applied in one order on the primary and the opposite order at a replica, and
  nothing marked that replica behind — it had acknowledged both writes. Closed by
  P8: every write now carries the sequence its ring-primary assigned, replicas
  apply-if-newer against their full read path, and the WAL persists the sequence so
  the comparison survives a restart. The narrower limit that replaces it — the
  sequence is per-primary, so a ring rebalance would need an epoch or node-id
  tiebreak — is unreachable today and is written up in
  [replication-and-anti-entropy.md → What is guaranteed, and what is not](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not).

---

## Still open

Recorded here so that a known gap is not mistaken for an oversight. Its full
reasoning is in
[replication-and-anti-entropy.md](replication-and-anti-entropy.md#what-is-guaranteed-and-what-is-not).

- **`anti_entropy_full_sync_required` has no remedy in v1.** The gauge's documented
  meaning is that this node's WAL is not a complete record of the data it holds.
  Full sync — a key-range scan shipped to the replica — is designed and not built:
  unlike a WAL pass it is unbounded in the store's size, so it needs its own
  throttling, resumability, and interaction with the write path. The gauge never
  clears, which over-reports rather than going quiet while divergence remains.

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
