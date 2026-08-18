# Raft

**Raft here replicates cluster control state, not data.** It is the single most
important thing to know about this package, and everything below is easy to
misread without it: leader election and failure detection run in production
traffic, the log machinery is complete and will carry node-health transitions,
and key/value data never touches it — that is the hash ring's job. The section
states that first, then separates what runs in production traffic from what is
finished but not yet driven by anything, and closes with the deviations from the
paper that a reviewer needs in order to read the package correctly.

---

## Raft (`internal/raft`)

**Live in production traffic:**

- Randomised election timeouts (150–300 ms) with **pre-vote phase** — a candidate first checks it can win a real election before incrementing its term, preventing a rejoining partitioned node from disrupting a stable leader.
- `RequestVote` with log-up-to-date check, majority-vote election, term-based split-brain prevention.
- `AppendEntries` (75 ms send period), which is both the heartbeat and the log-replication RPC. In a running cluster today it carries no entries, because nothing proposes — so in practice it serves as a liveness signal and a leader-authority assertion, and the replication machinery behind it is complete but idle (see the next section). Each heartbeat RPC carries its own deadline, deliberately decoupled from the send period and bounded by the minimum election timeout: a heartbeat is useful to a follower right up until that follower's election timer expires, and pointless after it.
- Atomic persistence of `currentTerm`/`votedFor` — and of the log, now that the log is persistent state — via write-temp-then-`os.Rename`.

**Finished, tested, and not yet driven by anything — exercised by unit tests, never by a running cluster:**

- **Log replication is complete, and nothing proposes to it yet.** The two halves of that sentence are both load-bearing. The machinery is finished and tested against the paper: `HandleAppendEntries` (`internal/raft/raft.go`) rejects a stale term, runs the §5.3 consistency check against `PrevLogIndex`/`PrevLogTerm` — including the snapshot boundary, whose term survives compaction — truncates a conflicting suffix, appends idempotently so a retransmission is free, **persists before it acknowledges**, and advances `commitIndex` to `min(leaderCommit, PrevLogIndex + len(entries))`. Entries reach the state machine on commit only, in index order, through `applyCommitted` (`internal/raft/replication.go`); the old apply-on-receipt path is gone. On the leader side, `Propose` appends and persists before returning, replication rides the heartbeat ticker with per-peer `nextIndex`/`matchIndex`, a rejection walks `nextIndex` back one index per interval until the logs agree, and `commitIndex` advances only to an index that a majority stores **and** that was created in the leader's own term (§5.4.2 — the Figure-8 rule).

    What is missing is a producer. Client writes go to the storage engine through the hash ring, never through Raft (see deviation 1), so in a running cluster `r.log` stays empty and `Propose` is never called. The intended producer is **Phase B**: a leader-side aggregator that commits node-health transitions — "node3 is down", "node3 is up" — through this log, so that every ring-primary reads the same health view instead of only the Raft leader learning anything. Until that lands, `cmd/node` hands Raft a deliberately inert placeholder state machine.

    Tested with, among others, the paper's Figure 7: all six follower shapes are reproduced and driven to convergence against the figure's leader log. Two of them — the followers holding *extra* entries past the leader's last index — do not converge on backoff alone, and the test says so rather than working around it: §5.3 truncates on a conflicting entry *received*, so an entry past the end of the leader's log is removed only once the leader has something to put at that index. In the interval it is provably harmless, because a follower's `commitIndex` is bounded by what the request in hand vouched for, so an entry the leader has never mentioned cannot be committed.

- **Snapshot delivery.** `InstallSnapshot` and the snapshot codec work, and are now correct about their own contents in two ways they previously had no occasion to be. The payload is **opaque bytes** produced by `StateMachine.SnapshotState` and handed back verbatim to `RestoreFromSnapshot`, so Raft no longer knows or cares that a key/value store exists. And a snapshot is cut at **`lastApplied`**, not at the last appended index: labelling a payload with an index the state machine had not yet consumed would promise a follower entries the bytes do not contain, and compact those entries away locally so nothing could supply them again. Installing one now also follows §7's retention rule — a follower holding the snapshot's last included entry at the same term keeps the entries after it — and sets `commitIndex = lastApplied = lastIncludedIndex`, since a snapshot's contents are applied and committed by definition.

    Still dormant for the same reason as above: snapshots are triggered by the log growing past `snapshotThreshold` (1000), and with no producer the log does not grow, so no `InstallSnapshot` RPC is sent outside tests. The trigger now hangs off the apply path rather than off entry receipt, which is where it belongs.

- **`StateMachine`, and the KV coupling that is gone.** `internal/raft` previously took a `StoreInterface` and applied `put`/`delete` entries straight into the storage engine. That code was unreachable — nothing had ever proposed an entry, verified by grep across `cmd/`, `internal/server` and `internal/cluster`: nothing outside the raft package constructs a `kvpb.LogEntry` or sets `Entries` — and it described an architecture this system deliberately does not have. It has been replaced by a three-method `StateMachine` (`Apply`, `SnapshotState`, `RestoreFromSnapshot`) and Raft no longer imports the store at all.

    One consequence worth stating plainly, because it is a requirement on Phase B rather than a detail: **`Apply` must be idempotent.** `commitIndex` and `lastApplied` are volatile, as in the paper, so after a restart entries between the snapshot boundary and the pre-crash frontier are applied again once the leader re-announces its commit index. `internal/raft/persistence_test.go` pins that replay so the requirement is observable rather than merely documented.

- **The log is persistent state now.** `persistedState` carries the entries alongside `currentTerm`, `votedFor` and the snapshot bounds, written atomically with the same temp-then-`os.Rename` pattern. Each save is a full rewrite rather than an append, which is a deliberate trade for a log that carries a handful of tiny control-plane entries: crash atomicity stays a one-sentence argument (the rename is the commit point) at a cost of O(entries) per persisted append. The honest ceiling is stated in the code — a workload proposing thousands of entries a second would need a segmented append-only log with checkpoints, and that is the first thing to change if the Raft log is ever asked to carry data.

    A pure heartbeat and a duplicate `AppendEntries` both write **nothing**: persistence on the interval every follower's election timer depends on is the shape of the defect described below, and a test asserts the disk stays untouched rather than trusting the code to be read correctly.

- **The transport probe stays.** `cluster.PeerHealth` still merges three signals, and the ticker probe is deliberately retained rather than replaced. A consensus-backed health view freezes during a leaderless window — no leader, no new committed entries — and the probe is what covers that window. Removing it in favour of a pure-Raft signal is a documented future goal, not this change.

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

2. **The Raft log carries control-plane entries, never data.** `commitIndex`, log truncation on leader change, and apply-on-commit all exist and are tested — but nothing proposes yet, so in a running cluster the log is empty and those guarantees currently apply to nothing. When Phase B starts proposing health transitions they will apply to health, and still not to data. Mitigation for data remains what it was: all reads route to the ring-primary, so stale reads are bounded to the in-flight crash window.

3. **Applying is at-least-once, not exactly-once, across a restart.** `commitIndex` and `lastApplied` are volatile, as in the paper, so a restarted node re-applies entries between the snapshot boundary and its pre-crash frontier. `StateMachine.Apply` must therefore be idempotent. Persisting the two indices would trade that requirement for an fsync on every commit advance, which is the wrong trade for a state machine whose entries are idempotent by nature ("node3 is down").

4. **A new leader appends no no-op entry.** Entries inherited from a previous term stay uncommitted until the leader commits one of its own (§5.4.2), which for a control-plane log costs a bounded delay and saves the state machine from having to tolerate a meaningless entry.

5. **Static membership.** No membership-change protocol. Adding/removing a node requires a cluster restart.

6. **`store.RestoreFromSnapshot` now has no caller.** Raft used to be the only one, and it takes a `StateMachine` instead. The LSM bulk-load path it fronts (documented in [replication-and-anti-entropy.md](replication-and-anti-entropy.md) and [lsm-engine.md](lsm-engine.md)) was already unreachable in a running cluster before this change — the snapshot that would trigger it never existed — so nothing that used to happen has stopped. It is now dead by structure rather than dead by circumstance, which is worth saying out loud rather than leaving for a reader to discover.

The package doc comment at the top of `internal/raft/raft.go` states the same deviations for anyone reading the code first.

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
