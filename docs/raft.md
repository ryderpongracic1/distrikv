# Raft

**Raft here is a leader-election and failure-detection mechanism, not a data
replication mechanism.** The section below states that first, then separates what
runs in production traffic from what is a stub, and closes with the deviations
from the paper that a reviewer needs in order to read the package correctly.

---

## Raft (`internal/raft`)

**Raft here is a leader-election and failure-detection mechanism, not a data replication mechanism.** Read that before the feature list below — it is the single most important thing to know about this package, and the feature list is easy to misread without it.

**Live in production traffic:**

- Randomised election timeouts (150–300 ms) with **pre-vote phase** — a candidate first checks it can win a real election before incrementing its term, preventing a rejoining partitioned node from disrupting a stable leader.
- `RequestVote` with log-up-to-date check, majority-vote election, term-based split-brain prevention.
- `AppendEntries` **heartbeats only** (75 ms send period). Heartbeats carry no entries, so they serve purely as a liveness signal and a leader-authority assertion. Each heartbeat RPC carries its own deadline, deliberately decoupled from the send period and bounded by the minimum election timeout: a heartbeat is useful to a follower right up until that follower's election timer expires, and pointless after it.
- Atomic persistence of `currentTerm`/`votedFor` via write-temp-then-`os.Rename`.

**Stubs and dormant paths — exercised by unit tests, never by a running cluster:**

- **Log replication is a stub, not a finished implementation parked outside the data path.** Nothing in the system ever proposes an entry to the Raft log — client writes go to the storage engine directly (see deviation 1) — so `r.log` stays empty for the lifetime of the process, and the entry-handling code has never had to be correct. Read against the paper, it is not. In `HandleAppendEntries` (`internal/raft/raft.go`): `PrevLogIndex`/`PrevLogTerm` are never checked, a conflicting suffix is never truncated, and `Success = true` is set for every request whose term is not stale — so the §5.3 Log Matching Property is neither enforced nor detectable. `LeaderCommit` is ignored, and there is no `commitIndex` field in the struct at all, so nothing tracks what is committed. `applyEntryLocked` writes each entry to the store **on receipt** rather than on commit, which inverts the paper's ordering. On the leader side, `nextIndex` is initialised at election and overwritten after a snapshot install, but never decremented on a rejection — the backtracking loop that repairs a divergent follower does not exist — and `matchIndex` never advances. Before this path could carry data it would need §5.3 log matching (consistency check, conflict truncation, `nextIndex` backoff) and commit-index tracking (majority `matchIndex` → `commitIndex` → apply) built on top of it.
- **Snapshot delivery.** `InstallSnapshot` and the snapshot codec work, and `internal/raft/snapshot_test.go` covers them. But snapshots are triggered by log growth past `snapshotThreshold: 1000`, and since the log never grows, that trigger never fires. No `InstallSnapshot` RPC is ever sent outside tests.

    Traced end to end, because it decides whether the state-machine restore path can ever run in production: `takeSnapshot` is called from exactly one place, `applyEntryLocked`, which only runs when an `AppendEntries` request carries entries. Heartbeats never carry any and nothing proposes, so `takeSnapshot` never runs, `SnapshotStore.Save` is never called, and **`raft_snapshot.bin` never exists**. That makes *both* callers of `store.RestoreFromSnapshot` unreachable in a running cluster: the startup restore in `raft.New` (guarded on a snapshot file being present) and `HandleInstallSnapshot` (a leader must send one first, and `sendInstallSnapshot` returns immediately when there is no snapshot to send). The restore path is still kept correct and fast — see the bulk-load note below — but it is dead code today, so it was **not** the cause of the recovery-availability collapse described under [Reopening on an accumulated store](lsm-engine.md#reopening-on-an-accumulated-store-write-availability).

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

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
