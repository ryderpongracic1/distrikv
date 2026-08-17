# Architecture

distrikv is a sharded, replicated key-value store. A consistent hash ring decides
where each key lives; the ring-primary owns writes for its keys and replicates
them synchronously to the rest of the replica set; and Raft runs alongside as a
leader-election and failure-detection mechanism that never carries data.

**That split is a deliberate design decision, not an omission.** Raft answers
"which nodes are alive, and who is the leader". The hash ring answers "where does
this key live, and who replicates it". It is the Cassandra-style arrangement
rather than the etcd-style one, and the guarantees distrikv therefore does *not*
have are stated in [CAP Position](#cap-position) and in
[raft.md](raft.md) under *Intentional deviations from the paper* — not left to be
inferred.

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

Each subsystem has its own document:

| Subsystem | Document |
| --- | --- |
| LSM-Tree engine, WAL, compaction, write stalls, block cache | [lsm-engine.md](lsm-engine.md) |
| CP write path, refused-but-applied, anti-entropy and its known limits | [replication-and-anti-entropy.md](replication-and-anti-entropy.md) |
| Raft's honest scope, the stub log path, deviations from the paper | [raft.md](raft.md) |
| Porcupine model, nemesis, convergence gate, counterexample output | [chaos-harness.md](chaos-harness.md) |
| Every measured table, including the etcd ceiling comparison | [benchmarks.md](benchmarks.md) |
| The numbered log of defects the harness found | [defect-log.md](defect-log.md) |
| HTTP API and `distrikv-cli` | [api-and-cli.md](api-and-cli.md) |
| Running the tests, repository layout, code-quality conventions | [development.md](development.md) |

---

## Consistent Hash Ring (`internal/cluster`)

Each physical node is assigned 150 virtual positions on a `uint32` ring. Keys are placed by taking the first 4 bytes of `MD5(key)` as a big-endian uint32 and walking clockwise to the next virtual node.

`GetN(key, R)` returns `R` **distinct physical nodes** starting from the primary — these are the replication targets. The naive approach of returning the next N vnodes can return duplicates for the same physical node; `GetN` skips those.

## CAP Position

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
linearizability model needs it, and cannot derive it downstream.
[*How failed operations are modelled*](chaos-harness.md) carries the decision table
and the proof behind each row.

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
again — see [replication-and-anti-entropy.md](replication-and-anti-entropy.md) for
the design, the convergence guarantee it does and does not make, and the
chaos-harness gate that measures it. What has *not* changed is the write path: a
write during a fault
still returns 503, because making it succeed would need either two-phase commit
across the replica set or routing data writes through the Raft log, both of which
remain out of scope (see *Intentional deviations from the paper* in
[raft.md](raft.md)). Read
repair and hinted handoff are still absent.

This is not a theoretical caveat: it is what the first real fault-injection runs
detected. The linearizability checker rejected both a SIGKILL and a SIGTERM run
purely because refused-but-applied writes were being read back afterwards — see
[*How failed operations are modelled*](chaos-harness.md) for the A/B evidence and
the model change it forced.

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
| 6 | LSM-Tree storage engine + Raft pre-vote and snapshot codec (the log path is a stub — see [Raft](raft.md)) | ✅ Done |
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
| P8 | Per-key write ordering — primary-assigned sequence on the replication wire and in the log, replicas apply-if-newer | ✅ Done |
| P9 | TBD | 🔲 Planned |

---

[← Back to the README](../README.md) · [All documents](../README.md#documentation)
