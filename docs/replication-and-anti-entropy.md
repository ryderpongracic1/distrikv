# Replication and Anti-Entropy

distrikv's consistency story in one place: what the CP write path guarantees, why
a refused write is still a durable write, how the primary's own WAL is replayed
to a replica that missed writes, and — at length, because they are the load-
bearing part — the convergence claims this design deliberately withholds.

The CP write path itself — the 503/502 contract, the `forward_outcome` field, and
why reads are never replicated — is stated in
[architecture.md → CAP Position](architecture.md#cap-position).

The known limits are not an appendix. *What is guaranteed, and what is not*
carries the per-key ordering limit (H2), the retention gap, the post-snapshot-
restore case, and the `anti_entropy_full_sync_required` gauge whose remedy is a
mechanism v1 does not ship.

---

## Anti-entropy: replica catch-up (`internal/store/wal`, `cmd/node/antientropy.go`)

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

### The cursor

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

### The trigger

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

Those three schedulers do not know about each other, so they are **coalesced per
replica**: while a cycle for a replica is queued or running, another request for the
same replica is dropped rather than queued behind it. Without that, a replica could
have several identical cycles pending at once; every one after the first finds
nothing left to ship and completes instantly, which is how a converged cluster
produced six `replica caught up, entries_sent=0, took=0` lines inside one
millisecond — wasted passes, and the same convergence claim logged over and over.
The slot is released when the cycle *ends*, so anything a scheduler wanted while it
ran is already covered by it, and a replica still behind afterwards is picked up by
the next retry tick.

### A pass

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

### WAL retention

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

**The floor cannot be the minimum over recorded cursors alone.** The replica that
needs retention most is the one that has been behind since *before* its first
cursor was ever persisted: it has no recorded cursor, so it contributes nothing to
that minimum, and `advanceQuietCursors` will not give it one — it deliberately only
adopts a tip for a replica that is **not** behind, since adopting one for a replica
you are ahead of would assert exactly what you know to be false. The healthy
replicas keep adopting tips, so a floor derived from cursors alone marches forward
and the engine deletes precisely the segments the down replica is owed. With *no*
cursors recorded at all it is worse: the floor is "none", which the engine reads as
retention being switched off, so every flushed segment is deleted — and that is the
state a freshly started node is in for its first few seconds, which is where a
chaos run against a fresh cluster begins.

So `publishRetentionFloor` pins the floor at the oldest segment whenever a replica
is known to be behind and has no cursor. A zero cursor means *no evidence about
what this replica has*, and the retention that matches that meaning is to keep
everything still on disk. The pin is armed on the not-behind → behind transition
rather than on the next flush tick, because the window would otherwise open at
exactly the moment a fault starts; it costs one floor recomputation per fault
window, not one per refused write. It lifts on its own, as soon as a completed pass
records a real cursor. And it is still bounded by the 128-segment cap — when the cap
bites, the pass detects the resulting gap and **withholds the convergence claim**
rather than letting the cap become a silent false "caught up" (below).

### What is guaranteed, and what is not

**Guaranteed: convergence once writes quiesce.** A repair cycle keeps passing until
a pass finds nothing to ship, so the final pass in a quiet cluster sees a settled
log and leaves every affected key equal on primary and replica. That is the
property `--check-convergence` measures.

**Guaranteed across a primary restart — and this is what a live run caught.** A
cursor is a `(segment, offset)` pair, so it only means anything while segment
numbers are never reused. They were: `Close` flushes the active memtable into an
SSTable and releases its segment, so a gracefully stopped primary could be left with
no live segment at all, and the next open seeded its numbering from the live segments
only — restarting at 1 while `wal-retained/` already held a segment 1. Every cursor
the primary had persisted then addressed a *different* log, with the same three
consequences this document already spells out for a snapshot restore: the cursor
orders after the new tip so the replica reads as up to date, `wal.ErrCursorStale` is
keyed on the segment *number* and cannot fire, and a pass reads from a byte offset
past the end of a shorter log — a clean stop with no error, so it ships nothing and
the engine reports `replica caught up`. That is a silently wrong convergence claim,
and it is why the chaos gate failed deterministically under the **graceful**
`stop-restart` nemesis while passing under `kill-restart`: a SIGKILL leaves the
segment on disk, so numbering continued past it and the cursors stayed valid.

Segment numbers are now seeded from every segment on disk, parked ones included, so
they are monotonic across a restart. A cursor into a segment that was genuinely
released now names a number that no longer exists, which is exactly the condition
`ErrCursorStale` reports and the engine already handles by withholding the claim.
And as a backstop, a cursor that orders *after* the tip — impossible in a log this
node kept appending to, so evidence that the log was replaced — is dropped at open,
the replica is marked behind rather than assumed fine, and
`anti_entropy_full_sync_required` is latched.

**Not guaranteed: convergence under continuous write load.** Live replication is
deliberately *not* blocked during a pass, so a live RPC for a write inside the
pass's range can land at the replica after the pass has already shipped a newer
value for that key, leaving that key stale. The race needs two client-concurrent
writes to the same key straddling the pass, and it self-corrects the next time the
key is written. Blocking replication to a recovering replica for the duration of a
pass would close it — at the cost of refusing writes to a replica that has just
come back, trading a rare stale key for guaranteed unavailability. That trade was
declined.

**Not guaranteed: per-key write ordering, and anti-entropy cannot see when it goes
wrong.** A mutation carries no version or sequence number, so a replica applies
whatever arrives in the order it arrives. The primary applies locally and *then* fans
out, and the two steps are not atomic across concurrent requests: two client-
concurrent writes to one key can be applied in one order on the primary and the
opposite order at a replica, leaving the replica holding a value the primary does not
have. Nothing marks that replica behind — it acknowledged both writes — so no
health signal and no cursor comparison will ever schedule a pass on account of it.
What does repair it is a pass triggered for any *other* reason: a pass ships the
primary's newest value for every key in its range, so an inverted key inside that
range is corrected as a side effect. A key that is never rewritten and never falls
inside a pass's range stays inverted.

The fix would be a primary-assigned monotonic per-key sequence carried in the
`Replicate` RPC and in replay entries, with the replica applying only when the
sequence is at least the one it has stored — which would also make replay idempotent
and close the live-versus-replay race above. It is **not built here**: it changes the
wire format, needs the sequence persisted alongside every value in the storage
engine, and has to be read back on the replica's write path, which is a storage-
format change rather than a repair-path change. Half-building it — a sequence that
some paths set and others do not — would be worse than not having one, because it
would license exactly the convergence claims this section exists to withhold. It is
recorded here as the known limit, not as future-work decoration: the divergence it
describes is real, reachable with no fault at all, and not detected by anything the
repo currently ships.

**Bounded recovery — and the claim is withheld, not just annotated.** If the log no
longer reaches back far enough to cover what a replica missed, the gap cannot be
closed from the log. The pass says so (counted as `anti_entropy_stale`) and then
catches up from the oldest surviving segment — which converges every key written
since, and leaves any key whose *only* write fell in the lost range divergent until
it is written again. A full keyspace scan to repair that is deliberately out of
scope for v1.

What matters is what the engine is then allowed to *say*. A pass that ships nothing
is ordinarily convergence, but over a log with a hole in it it means only "the log
had nothing left" — so the gap latches per replica and the pass reports *"the
replica is NOT known to agree on the keys the log cannot account for"* instead of
`replica caught up`, exactly as the post-restore case does. There are two ways in:

- **A recorded cursor pointing into a released segment** raises
  `wal.ErrCursorStale`, keyed on the segment number.
- **No recorded cursor at all**, where the pass starts at the oldest surviving
  segment. This one raises nothing: `wal.NewReader` returns on `from.IsZero()`
  *before* it looks for the cursor's segment, so `ErrCursorStale` is structurally
  unreachable on this path. Coverage is therefore checked directly — a zero-cursor
  pass covers the replica's whole gap only if segment 1 is still on disk, i.e.
  nothing has ever been released. The check is gated on the replica being known
  behind, which is the only evidence the primary has that it missed anything at all;
  a replica that merely failed a health probe and recovered has a zero cursor too,
  and treating that as a gap would fire the gauge on a health flap.

The condition is surfaced on both signals because they answer different questions.
`anti_entropy_stale` is a counter of events — a pass could not cover a replica's gap
— and it distinguishes this cause from a snapshot restore, but it cannot say whether
the divergence still stands. `anti_entropy_full_sync_required` is the latched gauge
whose documented meaning is precisely the standing condition here (this node's WAL
is not a complete record of the data it holds) and whose remedy is precisely the
same missing mechanism, the key-range scan below; it is persisted in the cursor
file, because dropped segments do not come back and a restart must not forget the
hole. The gauge is node-wide while the gap is per-replica, so latching it
over-reports — a claim about a different replica that the log *can* still prove is
suppressed too. That is the same direction the gauge already errs in by never
clearing, and it is why the precise per-replica reason travels in the log line.
Unlike a restore, this does **not** discard the cursors: they are the only record of
how far each replica did get.

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
  is reported as *"this node cannot converge this replica from its WAL; the replica
  is NOT known to agree on the keys the log cannot account for"* rather than
  `replica caught up` — the same wording the retention gap above uses, with the
  specific cause carried in the `reason` field. The gauge never clears — v1 has no
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

### Observability

`/metrics` gains `anti_entropy_passes`, `anti_entropy_entries`,
`anti_entropy_errors`, `anti_entropy_stale` and
`anti_entropy_full_sync_required`. Node logs carry `catch-up scheduled`,
`catch-up pass shipped missed writes`, `replica caught up`,
`replica cursor is older than the retained WAL`, `replica has no recorded cursor
and the retained WAL no longer starts at its first segment`, `replica cursor
ordered after this node's WAL tip, so it cannot describe this log`, and — whenever
the log cannot account for the data this node holds, after a snapshot restore or a
retention gap — `this node cannot converge its replicas from its WAL`.

The last three are the ones to grep for when a run reports `converged: false`:
together with the gauge they say whether the cluster failed to converge or the
primary was never able to.

`GET /keys/{key}?local=true` answers from the node's own store without
forwarding. It is the only way to ask a *replica* what it holds — a plain GET on a
non-owning node forwards to the ring-primary and would report the primary's value
from every node, making a divergent replica indistinguishable from a converged
one. It exposes no data an ordinary GET could not already reach, but like the rest
of this API it is unauthenticated and assumes a trusted cluster network.

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
