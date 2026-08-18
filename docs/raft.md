# Raft

**Raft here replicates cluster control state, not data.** It is the single most
important thing to know about this package, and everything below is easy to
misread without it: leader election, failure detection and node-health
replication run in production traffic, and key/value data never touches the log —
that is the hash ring's job. The section states that first, then separates what
runs in production traffic from what is finished but not yet driven by anything,
and closes with the deviations from the paper that a reviewer needs in order to
read the package correctly.

---

## Raft (`internal/raft`)

**Live in production traffic:**

- Randomised election timeouts (150–300 ms) with **pre-vote phase** — a candidate first checks it can win a real election before incrementing its term, preventing a rejoining partitioned node from disrupting a stable leader.
- `RequestVote` with log-up-to-date check, majority-vote election, term-based split-brain prevention.
- `AppendEntries` (75 ms send period), which is both the heartbeat and the log-replication RPC. It carries node-health entries when the leader has any to send, and serves as a liveness signal and a leader-authority assertion the rest of the time. Each heartbeat RPC carries its own deadline, deliberately decoupled from the send period and bounded by the minimum election timeout: a heartbeat is useful to a follower right up until that follower's election timer expires, and pointless after it.
- Atomic persistence of `currentTerm`/`votedFor` — and of the log, now that the log is persistent state — via write-temp-then-`os.Rename`.
- **Log replication carrying node health.** The leader-side aggregator observes its own heartbeat outcomes, proposes `health-down`/`health-up` transitions, and every node applies the committed sequence into the same state machine. This is what makes the design's premise — Raft owns node health — true of every node rather than only of the leader. See "Consensus node health" below.

**Consensus node health (`cmd/node/health_statemachine.go`, `cmd/node/health_aggregator.go`):**

This is the log's producer, and the reason the log exists.

- **The problem it closes.** Only the leader sends heartbeats, so only the leader ever observed a peer's reachability directly. Ring ownership and Raft leadership are deliberately unrelated here, so on a 3-node cluster two of the three ring-primaries learned nothing from Raft at all — which is why `cluster.PeerHealth` had to merge two further local signals to cover them, and why the transport probe ended up load-bearing for recovery detection on two nodes out of three. Committing the transition turns the leader's monopoly on heartbeats from a limitation into the design: one node observes, every node reads the same committed view.

- **The transition encoding.** `Op` is `health-down` or `health-up`, `Key` is the node ID, `Value` is the observation timestamp (diagnostic only — the state machine reads the op and the key). No proto change was needed: `LogEntry` already had the shape.

- **Hysteresis, and its defaults.** `health-down` after **3** consecutive failed heartbeats to a peer, `health-up` after **2** consecutive successes; a success resets a failure run and vice versa, so a peer that flaps produces no entry. Down is the slower of the two because the costs are asymmetric: a premature "down" gates every ring-primary's retry loop against a peer that was merely slow for one tick, while a premature "up" only schedules a catch-up pass that finds nothing to ship. The down threshold matches `cluster.DefaultStableChecks` so the consensus signal and the local one do not disagree about how much evidence a transition needs. At the compose heartbeat interval of 150 ms that is ~450 ms to declare a peer down and ~300 ms to declare it back.

- **One entry per change, not one per tick.** The aggregator remembers what it last proposed for each peer and proposes only on a change. That is not tidiness: `persistedState` rewrites the whole log on every persisted append, and the trade is only defensible for a log whose write rate is one entry per genuine change in a peer's reachability.

- **Proposing does not happen on the heartbeat path.** `ObserveHeartbeat` is called on the per-peer heartbeat goroutine, and `Propose` fsyncs the log before returning. Putting a disk write on the one path every follower's election timer depends on is precisely the shape of the defect described below, so the observation path only updates in-memory counters and hands the transition to the aggregator's own goroutine through a buffered channel. A test asserts `Propose` is never reached from `ObserveHeartbeat`.

- **A transition that fails to reach the log is re-proposed.** A step-down race (`ErrNotLeader`), a persist error, or a full queue reverts the aggregator's record of what it proposed, so the hysteresis fires again while the condition holds. Recording a transition that never reached disk would lose it permanently, since the counters have already passed the threshold.

- **A new leader corrects its predecessor's view.** The aggregator seeds its per-peer "last proposed" state from the *committed* view rather than assuming healthy. A leader that assumed healthy would, for a peer its predecessor marked down that is now back, observe nothing but successes, conclude there was no change to propose, and leave that stale `down` entry standing with nobody left to correct it — while every ring-primary's retry loop stayed gated against a peer that was fine.

- **A node ignores committed entries about itself.** A leader can legitimately commit "node2 is down" while node2 is alive and applying that very entry — it was unreachable *from the leader*. A node can neither act on nor usefully believe a claim about its own reachability, and recording it would put its own ID on the recovery channel, where anti-entropy would try to schedule a catch-up pass to a replica that is itself.

- **`Apply` is idempotent, and the recovery notification is inside the transition check.** The state change is a map assignment, so a replay after a restart lands on the same state; the one non-idempotent side effect — announcing a recovery — only fires when the entry actually changes the view. Without that, every restart would replay the log and fire a burst of spurious catch-up passes on every node.

- **An unrecognised op succeeds.** Raft retries a failed `Apply` rather than skipping it, so returning an error for an entry from a future version would wedge the apply loop forever and freeze the health view of every node that received it.

- **Observables, and how to read them together.** The three counters are scoped differently on purpose, and comparing two of them on one node is not a reconciliation:

  | Metric | Counts |
  | --- | --- |
  | `health_transitions_proposed` | Entries **this node appended** while it was leader. Non-zero on exactly one node at a time, so it identifies whose observations the current view came from. It counts local appends, so it also counts an entry an isolated leader wrote that a later leader truncated |
  | `health_transitions_committed` | Health entries **this node applied** from the committed log, whoever proposed them — excluding entries about itself, and including entries that restate the view |
  | `raft_last_applied_index` | The highest log index this node has applied. Because the log carries health transitions and nothing else, this *is* the number of entries the cluster has ever committed |

  So the number to check `committed` against is `raft_last_applied_index`, never `proposed`. On a healthy 3-node cluster with one leader for the whole session, `proposed` on the leader equals the index, and `committed` equals the index minus the entries about the reading node — which is zero for a node no leader ever marked down.

  Four things make `committed` and `proposed` diverge on one node, none of them a fault: leadership moving (entries this node applied but did not author), an isolated leader's truncated appends (`proposed` counted, never committed by anyone), entries about the reading node (applied by others, skipped here), and a **restart** — `commitIndex` and `lastApplied` are volatile, so a node that comes up to an existing log re-applies it and counts the whole thing, while its in-memory `proposed` starts again at zero. That last one alone can make `committed` an exact multiple of `proposed`.

  The counters that do *not* settle the question: `raft_terms` counts the elections this node **started** (`internal/raft/raft.go:377`) and `leader_elections` the ones it **won** (`raft.go:509`). Neither moves when a node steps down on a peer's higher term, so a flat `raft_terms` is not evidence that leadership never moved. All of this is pinned in `cmd/node/health_counter_reconciliation_test.go`.

- **The transport probe stays.** `cluster.PeerHealth`'s local signals are kept and the consensus view is added as a fourth signal, not a replacement. A consensus-backed view freezes during a leaderless window — no leader, no new committed entries — and the local signals are what cover it. Making the twist pure, with Raft alone carrying node health, is a documented future goal rather than this change; the discriminating test below is the evidence that removal would be safe, not the removal itself.

**Finished and tested — the log-replication half now runs in production traffic; the snapshot half is still exercised only by unit tests:**

- **Log replication is complete, and it has a producer now.** The machinery is finished and tested against the paper: `HandleAppendEntries` (`internal/raft/raft.go`) rejects a stale term, runs the §5.3 consistency check against `PrevLogIndex`/`PrevLogTerm` — including the snapshot boundary, whose term survives compaction — truncates a conflicting suffix, appends idempotently so a retransmission is free, **persists before it acknowledges**, and advances `commitIndex` to `min(leaderCommit, PrevLogIndex + len(entries))`. Entries reach the state machine on commit only, in index order, through `applyCommitted` (`internal/raft/replication.go`); the old apply-on-receipt path is gone. On the leader side, `Propose` appends and persists before returning, replication rides the heartbeat ticker with per-peer `nextIndex`/`matchIndex`, a rejection walks `nextIndex` back one index per interval until the logs agree, and `commitIndex` advances only to an index that a majority stores **and** that was created in the leader's own term (§5.4.2 — the Figure-8 rule).

    The producer is the health aggregator described above. Client writes still go to the storage engine through the hash ring and never through Raft (see deviation 1), so the log carries health transitions and nothing else — a handful of tiny entries per genuine change in a peer's reachability, which is the write rate the full-rewrite persistence format was designed around.

    Tested with, among others, the paper's Figure 7: all six follower shapes are reproduced and driven to convergence against the figure's leader log. Two of them — the followers holding *extra* entries past the leader's last index — do not converge on backoff alone, and the test says so rather than working around it: §5.3 truncates on a conflicting entry *received*, so an entry past the end of the leader's log is removed only once the leader has something to put at that index. In the interval it is provably harmless, because a follower's `commitIndex` is bounded by what the request in hand vouched for, so an entry the leader has never mentioned cannot be committed.

- **Snapshot delivery.** `InstallSnapshot` and the snapshot codec work, and are now correct about their own contents in two ways they previously had no occasion to be. The payload is **opaque bytes** produced by `StateMachine.SnapshotState` and handed back verbatim to `RestoreFromSnapshot`, so Raft no longer knows or cares that a key/value store exists. And a snapshot is cut at **`lastApplied`**, not at the last appended index: labelling a payload with an index the state machine had not yet consumed would promise a follower entries the bytes do not contain, and compact those entries away locally so nothing could supply them again. Installing one now also follows §7's retention rule — a follower holding the snapshot's last included entry at the same term keeps the entries after it — and sets `commitIndex = lastApplied = lastIncludedIndex`, since a snapshot's contents are applied and committed by definition.

    Rare rather than dormant now, and the distinction is worth keeping straight: snapshots are triggered by the log growing past `snapshotThreshold` (1000), and the log grows by one entry per genuine change in a peer's reachability — so a cluster needs a thousand health transitions before it compacts, which no test cluster has yet reached. The path is reachable in production for the first time; it is simply not reached often. The trigger now hangs off the apply path rather than off entry receipt, which is where it belongs.

- **`StateMachine`, and the KV coupling that is gone.** `internal/raft` previously took a `StoreInterface` and applied `put`/`delete` entries straight into the storage engine. That code was unreachable — nothing had ever proposed an entry, verified by grep across `cmd/`, `internal/server` and `internal/cluster`: nothing outside the raft package constructs a `kvpb.LogEntry` or sets `Entries` — and it described an architecture this system deliberately does not have. It has been replaced by a three-method `StateMachine` (`Apply`, `SnapshotState`, `RestoreFromSnapshot`) and Raft no longer imports the store at all.

    One consequence worth stating plainly, because it is a requirement on the health state machine rather than a detail: **`Apply` must be idempotent.** `commitIndex` and `lastApplied` are volatile, as in the paper, so after a restart entries between the snapshot boundary and the pre-crash frontier are applied again once the leader re-announces its commit index. `internal/raft/persistence_test.go` pins that replay so the requirement is observable rather than merely documented, and `cmd/node/health_statemachine_test.go` pins the state machine's side of it — including that a replayed entry announces no recovery.

- **The log is persistent state now.** `persistedState` carries the entries alongside `currentTerm`, `votedFor` and the snapshot bounds, written atomically with the same temp-then-`os.Rename` pattern. Each save is a full rewrite rather than an append, which is a deliberate trade for a log that carries a handful of tiny control-plane entries: crash atomicity stays a one-sentence argument (the rename is the commit point) at a cost of O(entries) per persisted append. The honest ceiling is stated in the code — a workload proposing thousands of entries a second would need a segmented append-only log with checkpoints, and that is the first thing to change if the Raft log is ever asked to carry data.

    A pure heartbeat and a duplicate `AppendEntries` both write **nothing**: persistence on the interval every follower's election timer depends on is the shape of the defect described below, and a test asserts the disk stays untouched rather than trusting the code to be read correctly.

- **The transport probe stays.** Covered above under "Consensus node health": the local signals are retained and the committed view is added as a fourth signal, because a consensus-backed view freezes during a leaderless window and the probe is what covers it.

**The discriminating test (`cmd/node/consensus_health_test.go`).** Everything above can be true while the design premise stays false, so one test is built to fail if it is. It stands up a 3-node in-process cluster, **disables the transport probe** (its ticker is never started and its interval is set beyond the life of the test), issues no replication RPCs, and then observes a **follower** — a node that sends no heartbeats and therefore has no direct observation of anything. It isolates one of the other followers bidirectionally, and asserts the observing follower learns the peer is unhealthy, learns it is healthy again after the link is restored, and **schedules a replica catch-up pass** for it. With every local signal removed, the only path that information can have taken is the committed log. The same test asserts the leader's term does not move while the transitions flow, so the aggregator cannot be trading health for elections.

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

1. **Data writes bypass Raft consensus.** Writes flow through the consistent-hash ring, not through the Raft log. Raft here is a leader-election, failure-detection and control-plane replication mechanism only. This means Raft's "if committed, all future leaders have it" guarantee does **not** apply to data — it applies to node health, which is what the log carries. Under partition the ring-primary and its replicas can diverge.

2. **The Raft log carries control-plane entries, never data.** `commitIndex`, log truncation on leader change, and apply-on-commit all exist, are tested, and now apply to something: the node-health transitions the leader proposes. They still do not apply to data. Mitigation for data remains what it was: all reads route to the ring-primary, so stale reads are bounded to the in-flight crash window.

3. **Applying is at-least-once, not exactly-once, across a restart.** `commitIndex` and `lastApplied` are volatile, as in the paper, so a restarted node re-applies entries between the snapshot boundary and its pre-crash frontier. `StateMachine.Apply` must therefore be idempotent. Persisting the two indices would trade that requirement for an fsync on every commit advance, which is the wrong trade for a state machine whose entries are idempotent by nature ("node3 is down").

4. **A new leader appends no no-op entry.** Entries inherited from a previous term stay uncommitted until the leader commits one of its own (§5.4.2), which for a control-plane log costs a bounded delay and saves the state machine from having to tolerate a meaningless entry.

5. **Static membership.** No membership-change protocol. Adding/removing a node requires a cluster restart.

6. **`store.RestoreFromSnapshot` now has no caller.** Raft used to be the only one, and it takes a `StateMachine` instead. The LSM bulk-load path it fronts (documented in [replication-and-anti-entropy.md](replication-and-anti-entropy.md) and [lsm-engine.md](lsm-engine.md)) was already unreachable in a running cluster before this change — the snapshot that would trigger it never existed — so nothing that used to happen has stopped. It is now dead by structure rather than dead by circumstance, which is worth saying out loud rather than leaving for a reader to discover.

The package doc comment at the top of `internal/raft/raft.go` states the same deviations for anyone reading the code first.

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
