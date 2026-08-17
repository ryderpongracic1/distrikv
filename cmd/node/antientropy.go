package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/server"
	"github.com/ryderpongracic1/distrikv/internal/store"
	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// Anti-entropy: converging replicas after a fault, from the primary's WAL.
//
// # What this fixes
//
// distrikv refuses a write whose replica does not ACK (HTTP 503) but keeps the
// write locally, because there is no rollback: the primary is durable, the
// replica is behind. A 60-second chaos run with a kill-restart nemesis measured
// 48,283 such refused-but-applied writes. Nothing converged them afterwards, so
// a replica that had been down stayed wrong for every key it missed until that
// key happened to be written again.
//
// # What it does not change
//
// The CP write path is untouched. A write during a fault still returns 503, and
// this code never makes one succeed: it runs strictly after the fact. The hash
// ring remains the only authority on placement — Raft contributes a liveness
// signal and nothing else — and there is no second replication log: the primary's
// own WAL is the record of what a replica missed.
//
// # How a pass works
//
//  1. A cursor (high-water mark) per replica records the WAL position through
//     which that replica is known caught up. It is persisted, so a primary that
//     restarts does not lose track of what its replicas are missing.
//  2. On a replica's unreachable → healthy transition (or on a retry, for a
//     replica known to be behind), the primary reads its WAL forward from the
//     cursor to the tip observed when the pass started.
//  3. Entries are filtered to the ones that are this replica's business: keys for
//     which this node is the ring-primary and the target is in the replica set.
//     This is a catch-up of one replica's key range, not a shipment of the whole
//     log.
//  4. Surviving entries are deduplicated by key, keeping each key's newest entry.
//     A replica is not a client — reads route to the primary — so only the final
//     value per key matters, and the pass cost becomes proportional to the
//     distinct keys written during the fault rather than to the write count. In
//     the measured chaos run that is the difference between 20 entries and 48,283.
//  5. Each surviving entry is replicated with the ordinary Replicate RPC, in WAL
//     order, and the cursor advances past each entry the replica ACKs — so a
//     replica that dies mid-pass is resumed from exactly where it stopped rather
//     than from the beginning.
//
// # Convergence guarantee, stated honestly
//
// A pass delivers the primary's latest value for every affected key as of the
// tip it pinned. Live replication continues concurrently and is not blocked, so a
// live RPC for a write that is *inside* the pass's range can in principle land at
// the replica after the pass has already shipped a newer value for that key,
// leaving that key stale. That race needs two client-concurrent writes to the
// same key straddling the pass, and it self-corrects the next time the key is
// written.
//
// What is guaranteed is convergence once writes quiesce: a repair cycle keeps
// passing until a pass finds nothing left to ship, so the last pass in a quiet
// cluster sees a stable log and leaves every affected key equal on primary and
// replica. That is the property the chaos harness's --check-convergence gate
// measures, and it is deliberately the property claimed.
//
// The alternative — holding replication to a replica still while a pass ran —
// would close the race at the cost of refusing writes to a replica that has just
// come back, which trades a rare stale key for guaranteed unavailability. See
// docs/replication-and-anti-entropy.md for the full reasoning.

// antiEntropyConfig tunes the engine. Zero values take the defaults below.
type antiEntropyConfig struct {
	// RetryInterval is how often a replica known to be behind is retried when no
	// health transition has fired.
	RetryInterval time.Duration

	// SettleDelay is the pause between passes within one repair cycle. It must
	// exceed the replication deadline so that every live RPC issued before the
	// previous pass has resolved before the next pass reads the tip.
	SettleDelay time.Duration

	// MaxPasses bounds one repair cycle. A cluster under continuous write load
	// never reaches a quiet pass, so the cycle must give up and let the retry
	// loop pick it up again rather than spinning.
	MaxPasses int

	// CursorHoldback is how old a tip must be before a fault-free window lets the
	// cursor adopt it. It must exceed the replication deadline: only then is
	// every write appended before that tip known to have resolved, so "no
	// failures since" really means "everything before this tip reached the
	// replica".
	CursorHoldback time.Duration

	// FlushInterval is how often changed cursors are persisted and the WAL
	// retention floor is republished.
	FlushInterval time.Duration

	// SendTimeout bounds one catch-up Replicate RPC.
	SendTimeout time.Duration

	// MaxEntriesPerPass bounds the deduplicated working set of a single pass, so
	// a cursor that has fallen a long way behind cannot make the primary
	// materialise an unbounded amount of the log at once. A pass that hits the
	// bound stops early and the next pass continues from where it stopped.
	MaxEntriesPerPass int
}

func (c antiEntropyConfig) withDefaults() antiEntropyConfig {
	if c.RetryInterval <= 0 {
		c.RetryInterval = 5 * time.Second
	}
	if c.SettleDelay <= 0 {
		c.SettleDelay = defaultReplicateTimeout + time.Second
	}
	if c.MaxPasses <= 0 {
		c.MaxPasses = 4
	}
	if c.CursorHoldback <= 0 {
		c.CursorHoldback = 2 * defaultReplicateTimeout
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 2 * time.Second
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = defaultReplicateTimeout
	}
	if c.MaxEntriesPerPass <= 0 {
		c.MaxEntriesPerPass = 200_000
	}
	return c
}

// replicaState is the engine's per-replica bookkeeping.
type replicaState struct {
	// behind marks a replica this node believes it is ahead of. Set by any
	// replication failure, cleared only by a repair cycle that ends with a pass
	// finding nothing left to ship.
	behind bool

	// failures counts replication failures to this replica. It is a monotonic
	// stamp, not a gauge: comparing it across a window is how the engine decides
	// whether that window was fault-free.
	failures uint64

	lastAttempt time.Time

	// queued marks that a repair cycle for this replica is already waiting to run
	// or running. Three independent sources schedule a cycle — a health-transition
	// recovery, the retry ticker for a replica still behind, and the startup seed —
	// and none of them knew about the others, so a replica could have several
	// identical cycles queued at once. Every one after the first finds nothing left
	// to ship and completes instantly, which is how a converged cluster produced
	// six "replica caught up, entries_sent=0, took=0" lines inside one millisecond:
	// wasted passes, and a convergence claim logged repeatedly for the same fact.
	//
	// Cleared when the cycle finishes rather than when it starts, so the burst
	// collapses to exactly one cycle: anything that would have been scheduled while
	// the cycle was running is already covered by it, and if the replica is still
	// behind afterwards the retry ticker picks it up again.
	queued bool

	// candidate is a tip observed at candidateAt with failures == candidateFail.
	// If the failure count is still unchanged once the tip is CursorHoldback old,
	// every write before it resolved without a single failure, so the cursor can
	// safely adopt it. This is what keeps the cursor (and therefore WAL
	// retention) moving on a healthy cluster, where no pass ever runs.
	candidate     storewal.Position
	candidateFail uint64
	candidateAt   time.Time
	hasCandidate  bool

	// logGap latches that this node's WAL can no longer account for everything
	// this replica is owed, so no pass over it may be reported as the replica
	// being caught up. Set by noteLogGap when a pass discovers the gap; never
	// cleared, because dropped segments do not come back and the keys they held
	// stay divergent until they are written again. Like the
	// anti_entropy_full_sync_required gauge it therefore over-reports — it keeps
	// suppressing the claim after the affected keys have organically been
	// rewritten — which is the only safe direction for a convergence claim.
	logGap       bool
	logGapReason string
}

// antiEntropy converges this node's replicas with its own WAL. One instance per
// node; safe for concurrent use.
type antiEntropy struct {
	nodeID       string
	replicaCount int
	cfg          antiEntropyConfig

	store   *store.Store
	cursors *store.CursorStore
	ring    *cluster.Ring
	peers   map[string]kvpb.KVServiceClient
	health  *cluster.PeerHealth
	term    func() uint64
	metrics *metrics.Metrics
	logger  *slog.Logger

	mu      sync.Mutex
	replica map[string]*replicaState

	// fullSync records that this node's WAL cannot converge its replicas, because
	// it is not a complete record of the data this node holds: the store was
	// replaced from a snapshot whose payload was never appended to the log, or a
	// pass found retention had dropped segments a replica was owed. Read at
	// construction from the durable cursor state and latched by noteLogGap
	// thereafter; guarded by mu, and read through convergenceClaimBlocked. It
	// gates the convergence claims this engine is allowed to make.
	fullSync       bool
	fullSyncReason string
}

// newAntiEntropy builds the engine for the given peer node IDs.
func newAntiEntropy(
	nodeID string,
	replicaCount int,
	peerIDs []string,
	s *store.Store,
	cursors *store.CursorStore,
	ring *cluster.Ring,
	peers map[string]kvpb.KVServiceClient,
	health *cluster.PeerHealth,
	term func() uint64,
	m *metrics.Metrics,
	logger *slog.Logger,
	cfg antiEntropyConfig,
) *antiEntropy {
	ae := &antiEntropy{
		nodeID:       nodeID,
		replicaCount: replicaCount,
		cfg:          cfg.withDefaults(),
		store:        s,
		cursors:      cursors,
		ring:         ring,
		peers:        peers,
		health:       health,
		term:         term,
		metrics:      m,
		logger:       logger.With("component", "anti-entropy"),
		replica:      make(map[string]*replicaState, len(peerIDs)),
	}
	tip := s.WALTip()
	fullSync, fullSyncReason := cursors.FullSyncRequired()
	ae.fullSync, ae.fullSyncReason = fullSync, fullSyncReason
	if fullSync {
		// The durable latch says this node's WAL is not a complete record of the
		// data it holds — a restore replaced the store from a snapshot that never
		// passed through the log, or a previous process found retention had dropped
		// segments a replica was owed. Say so once, loudly, and hold the gauge up
		// for as long as the condition stands — this engine must not let a pass over
		// an incomplete log be read as evidence that the replicas agree.
		if m != nil {
			m.AntiEntropyFullSyncRequired.Store(1)
		}
		ae.logger.Warn("this node cannot converge its replicas from its WAL: "+
			"the log is not a complete record of the data this node holds. "+
			"Catch-up passes cover only the writes still in the log; keys outside it stay "+
			"divergent on any replica that missed them until they are rewritten. "+
			"A full key-scan sync would close this and does not exist in v1",
			"reason", fullSyncReason, "wal_tip", tip)
	}
	// Cursors that cannot possibly describe this node's log are collected here and
	// dealt with after the replica map is populated, because noteLogGap records the
	// reason on the replica's own state.
	var orphaned []string

	for _, id := range peerIDs {
		st := &replicaState{}
		// A persisted cursor behind the tip means this node stopped — crashed, or
		// was restarted by an operator — while it was ahead of that replica. The
		// replica is owed those writes, and no health transition is coming to say
		// so, because from this process's point of view the peer was never seen to
		// fail. Seeding `behind` from the durable cursor is what makes the cursor
		// worth persisting at all.
		//
		// A *zero* cursor is not evidence of anything: it means no cursor has ever
		// been recorded, which is also the state of a node that has just been
		// created. Treating it as "behind" would make every fresh node open by
		// re-sending its whole retained log to every replica.
		switch cur := cursors.Get(id); {
		case cur.IsZero():
			// No evidence either way.
		case cur.Before(tip):
			st.behind = true
		case tip.Before(cur):
			// A cursor ahead of the tip cannot arise in a log this node simply kept
			// appending to: offsets only grow and segment numbers only increase. It
			// means the log the cursor described is gone and a different one stands in
			// its place — a snapshot restore that failed to invalidate cursors, a
			// rebuilt data directory, or (before segment numbers were made monotonic
			// across a restart) a clean shutdown that released every live segment and
			// restarted numbering at 1.
			//
			// Left in place it is worse than stale. It cannot be moved back, because
			// cursors are monotonic; the retention floor derived from it names a
			// segment of a log that no longer exists, so freshly flushed segments are
			// deleted instead of parked for catch-up; and the first pass reads from a
			// byte offset past the end of a shorter log, which is a clean stop with no
			// error — so the pass ships nothing and the engine reports the replica as
			// caught up. That last one is a silently wrong convergence claim, which is
			// the specific failure this whole file is built to refuse.
			//
			// So drop it (a zero cursor honestly says "no evidence"), mark the replica
			// behind so it is examined rather than assumed fine, and latch the gap so
			// no pass over this log may be read as the replica agreeing.
			st.behind = true
			orphaned = append(orphaned, id)
		}
		ae.replica[id] = st
	}

	for _, id := range orphaned {
		cursors.Forget(id)
		ae.noteLogGap(id, fmt.Sprintf(
			"replica %s had a cursor ordered after this node's WAL tip %s, so it "+
				"described a different log; the cursor has been dropped", id, tip))
		ae.logger.Warn("replica cursor ordered after this node's WAL tip, so it cannot "+
			"describe this log; dropping it and catching up from the oldest surviving "+
			"segment. Keys this node holds only in an SSTable are not in the log and "+
			"stay divergent on that replica until they are rewritten (v1 limitation)",
			"replica", id, "wal_tip", tip)
	}
	return ae
}

// NoteReplicationFailure records that replicating to nodeID failed. Called from
// the write path, so it must stay cheap and non-blocking.
func (ae *antiEntropy) NoteReplicationFailure(nodeID string) {
	ae.mu.Lock()
	newlyBehind := false
	if st := ae.replica[nodeID]; st != nil {
		st.failures++
		newlyBehind = !st.behind
		st.behind = true
	}
	ae.mu.Unlock()

	// A replica that has just gone behind may be one with no cursor, in which case
	// the WAL must be pinned before the next memtable flush releases segments it is
	// owed. Waiting for the flush ticker would leave a window of up to
	// FlushInterval in which that happens — and the window opens at exactly the
	// moment a fault starts, which is when flushes are most likely. This runs only
	// on the not-behind → behind transition, so a fault window costs one floor
	// recomputation and not one per refused write.
	if newlyBehind {
		ae.publishRetentionFloor()
	}

	if ae.health != nil {
		ae.health.ObserveReplication(nodeID, false)
	}
}

// NoteReplicationSuccess records a successful replication to nodeID.
func (ae *antiEntropy) NoteReplicationSuccess(nodeID string) {
	if ae.health != nil {
		ae.health.ObserveReplication(nodeID, true)
	}
}

// Run drives the engine until ctx is cancelled: it reacts to peer recoveries,
// retries replicas still known to be behind, keeps cursors moving on a healthy
// cluster, and persists them.
func (ae *antiEntropy) Run(ctx context.Context) {
	ae.logger.Info("anti-entropy started",
		"replicas", len(ae.replica),
		"retry_interval", ae.cfg.RetryInterval,
		"settle_delay", ae.cfg.SettleDelay)

	retry := time.NewTicker(ae.cfg.RetryInterval)
	defer retry.Stop()
	flush := time.NewTicker(ae.cfg.FlushInterval)
	defer flush.Stop()

	var recovered <-chan string
	if ae.health != nil {
		recovered = ae.health.Recovered()
	}

	// One repair cycle at a time, cluster-wide for this node: passes read the
	// same WAL and the point of a pass is to catch up, not to race.
	var wg sync.WaitGroup
	defer wg.Wait()
	cycles := make(chan string, len(ae.replica)+4)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case nodeID := <-cycles:
				ae.repair(ctx, nodeID)
			}
		}
	}()

	enqueue := func(nodeID, reason string) {
		if !ae.tryEnqueue(nodeID) {
			// A cycle for this replica is already pending or running, and it will
			// cover everything this one would have. Queueing it anyway is what
			// produced bursts of instantly-completing passes after convergence.
			ae.logger.Debug("catch-up already pending for this replica; not queueing again",
				"replica", nodeID, "reason", reason)
			return
		}
		select {
		case cycles <- nodeID:
			ae.logger.Info("catch-up scheduled", "replica", nodeID, "reason", reason)
		default:
			ae.clearQueued(nodeID)
			ae.logger.Warn("catch-up queue full; will retry", "replica", nodeID)
		}
	}

	// A replica already known to be behind at startup is repaired without
	// waiting for the first retry tick.
	for _, id := range ae.behindReplicas() {
		enqueue(id, "cursor behind WAL tip at startup")
	}

	for {
		select {
		case <-ctx.Done():
			ae.finalFlush()
			return

		case nodeID := <-recovered:
			enqueue(nodeID, "peer recovered")

		case <-retry.C:
			for _, id := range ae.behindReplicas() {
				if ae.health != nil && !ae.health.Healthy(id) {
					continue // still down; catching it up would just fail
				}
				enqueue(id, "replica still behind")
			}

		case <-flush.C:
			ae.advanceQuietCursors()
			ae.publishRetentionFloor()
			if err := ae.cursors.Flush(); err != nil {
				ae.logger.Warn("persist replica cursors", "err", err)
			}
		}
	}
}

// finalFlush persists cursors on shutdown so a clean restart resumes where this
// process left off instead of re-sending the whole retained log.
func (ae *antiEntropy) finalFlush() {
	ae.publishRetentionFloor()
	if err := ae.cursors.Flush(); err != nil {
		ae.logger.Warn("persist replica cursors on shutdown", "err", err)
	}
}

// behindReplicas returns the replicas currently believed to be behind.
func (ae *antiEntropy) behindReplicas() []string {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	var out []string
	for id, st := range ae.replica {
		if st.behind {
			out = append(out, id)
		}
	}
	return out
}

// retainAllWALSegments is the retention floor that keeps every segment still on
// disk. Segment numbers start at 1, so a floor of 1 excludes nothing.
const retainAllWALSegments uint64 = 1

// publishRetentionFloor tells the engine which WAL segments it must keep.
//
// The obvious answer — the oldest segment any *recorded* cursor points into — is
// wrong for the replica that needs retention most. A replica that has been behind
// since before its first cursor was ever persisted has no recorded cursor at all,
// so it contributes nothing to that minimum; and advanceQuietCursors will not give
// it one, because it deliberately only adopts a tip for a replica that is *not*
// behind. Meanwhile the healthy replicas keep adopting tips, so the floor derived
// from cursors alone marches forward and the engine deletes exactly the segments
// the down replica is owed. Worse, with no cursors recorded at all the floor is
// "none", which the engine reads as retention being switched off — so every
// flushed segment is deleted outright, which is the state a freshly started node
// is in for its first few seconds.
//
// A zero cursor means "no evidence about what this replica has" (see
// newAntiEntropy), and the retention that matches that meaning is to keep
// everything still on disk. So: if any replica is known to be behind and has no
// cursor, pin the floor at the oldest segment. That is bounded already —
// lsm.maxRetainedWALSegments caps the parked segments, and when the cap bites,
// runPass detects the resulting gap and withholds the convergence claim rather
// than letting the cap turn into a silent false "caught up".
//
// The pin lifts on its own: the first pass that completes records a real cursor
// for that replica, and the floor goes back to being derived from cursors.
func (ae *antiEntropy) publishRetentionFloor() {
	floor, ok := ae.cursors.RetentionFloor()

	if ae.anyBehindWithoutCursor() {
		if !ok || floor > retainAllWALSegments {
			floor, ok = retainAllWALSegments, true
		}
	}
	if !ok {
		floor = 0 // no replica is owed anything: retention off
	}
	if floor != ae.store.WALRetentionFloor() {
		ae.store.RetainWALFrom(floor)
	}
}

// anyBehindWithoutCursor reports whether some replica this node believes it is
// ahead of has no recorded cursor — the state in which the log is the only thing
// that can converge it and nothing says how far back it needs.
func (ae *antiEntropy) anyBehindWithoutCursor() bool {
	ae.mu.Lock()
	behind := make([]string, 0, len(ae.replica))
	for id, st := range ae.replica {
		if st.behind {
			behind = append(behind, id)
		}
	}
	ae.mu.Unlock()

	for _, id := range behind {
		if ae.cursors.Get(id).IsZero() {
			return true
		}
	}
	return false
}

// advanceQuietCursors moves cursors forward on a cluster that is simply working.
//
// Without this, a node that never suffers a fault never advances a cursor, so
// WAL retention would pin every segment since startup: the cursor has to make
// progress on the happy path too, and it has to do so without the write path
// paying for per-write bookkeeping. The rule is a fault-free window: adopt a tip
// only once it is old enough that every write before it must have resolved
// (CursorHoldback > the replication deadline) and no replication to that replica
// failed in the meantime.
func (ae *antiEntropy) advanceQuietCursors() {
	tip := ae.store.WALTip()
	now := time.Now()

	type adoption struct {
		nodeID string
		pos    storewal.Position
	}
	var adopt []adoption

	ae.mu.Lock()
	for id, st := range ae.replica {
		if st.hasCandidate && now.Sub(st.candidateAt) >= ae.cfg.CursorHoldback {
			if st.failures == st.candidateFail && !st.behind {
				adopt = append(adopt, adoption{id, st.candidate})
			}
			st.hasCandidate = false
		}
		if !st.hasCandidate {
			st.candidate, st.candidateFail, st.candidateAt, st.hasCandidate = tip, st.failures, now, true
		}
	}
	ae.mu.Unlock()

	for _, a := range adopt {
		if ae.cursors.Advance(a.nodeID, a.pos) {
			ae.logger.Debug("cursor advanced over a fault-free window",
				"replica", a.nodeID, "cursor", a.pos)
		}
	}
}

// tryEnqueue claims the pending slot for nodeID and reports whether the caller
// is the one that should queue a cycle. It returns false when a cycle for that
// replica is already pending or running, which is what keeps three independent
// schedulers from queueing the same work several times over.
func (ae *antiEntropy) tryEnqueue(nodeID string) bool {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	st := ae.replica[nodeID]
	if st == nil || st.queued {
		return false
	}
	st.queued = true
	return true
}

// clearQueued releases the pending slot claimed by tryEnqueue.
func (ae *antiEntropy) clearQueued(nodeID string) {
	ae.mu.Lock()
	if st := ae.replica[nodeID]; st != nil {
		st.queued = false
	}
	ae.mu.Unlock()
}

// repair runs one repair cycle for a replica: passes until a pass ships nothing,
// which is what proves there is nothing left to converge.
func (ae *antiEntropy) repair(ctx context.Context, nodeID string) {
	// Release the pending slot however this cycle ends, so the next scheduler tick
	// can queue a fresh one. Deferred rather than cleared up front on purpose: a
	// cycle already in flight covers anything that would have been scheduled while
	// it ran.
	defer ae.clearQueued(nodeID)

	ae.mu.Lock()
	st := ae.replica[nodeID]
	if st == nil {
		ae.mu.Unlock()
		return
	}
	st.lastAttempt = time.Now()
	failuresAtStart := st.failures
	ae.mu.Unlock()

	start := time.Now()
	var totalSent int

	for pass := 1; pass <= ae.cfg.MaxPasses; pass++ {
		from := ae.cursors.Get(nodeID)
		tip := ae.store.WALTip()

		sent, err := ae.runPass(ctx, nodeID, from, tip)
		totalSent += sent
		if ae.metrics != nil {
			ae.metrics.AntiEntropyPasses.Add(1)
		}
		if err != nil {
			if ae.metrics != nil {
				ae.metrics.AntiEntropyPassErrors.Add(1)
			}
			ae.logger.Warn("catch-up pass ended early",
				"replica", nodeID, "pass", pass, "entries_sent", sent, "err", err)
			return // stays behind; the retry loop will come back to it
		}

		if sent == 0 {
			// Nothing was left to ship over a range that ends at a tip read after
			// the previous pass finished. For a quiet cluster that is the
			// definition of converged for every key this replica owns with us —
			// but only when the log is actually a complete record of this node's
			// data.
			ae.markCaughtUp(nodeID, failuresAtStart)
			if blocked, reason := ae.convergenceClaimBlocked(nodeID); blocked {
				// It is not. Either a snapshot restore replaced the store with a
				// payload that never passed through the log, or retention dropped
				// segments this replica was owed. Either way an empty pass means
				// "the log had nothing left", not "the replica agrees", and the
				// keys the log cannot account for were never shipped. Reporting
				// this as caught up is precisely the convergence claim this node
				// cannot deliver.
				ae.logger.Warn("catch-up pass found nothing to ship, but this node "+
					"cannot converge this replica from its WAL; the replica is "+
					"NOT known to agree on the keys the log cannot account for",
					"replica", nodeID, "passes", pass, "entries_sent", totalSent,
					"reason", reason)
			} else {
				ae.logger.Info("replica caught up",
					"replica", nodeID, "passes", pass, "entries_sent", totalSent,
					"took", time.Since(start).Round(time.Millisecond),
					"cursor", ae.cursors.Get(nodeID))
			}
			if err := ae.cursors.Flush(); err != nil {
				ae.logger.Warn("persist replica cursors after catch-up", "err", err)
			}
			return
		}

		ae.logger.Info("catch-up pass shipped missed writes",
			"replica", nodeID, "pass", pass, "entries", sent,
			"cursor", ae.cursors.Get(nodeID))

		// Let every replication RPC issued before this pass resolve before
		// reading the tip again, so the next pass sees a settled log.
		select {
		case <-ctx.Done():
			return
		case <-time.After(ae.cfg.SettleDelay):
		}
	}

	ae.logger.Info("catch-up cycle hit its pass limit; the log is still moving",
		"replica", nodeID, "passes", ae.cfg.MaxPasses, "entries_sent", totalSent)
}

// noteLogGap records that this node's WAL cannot account for everything nodeID is
// owed, so nothing this engine observes about nodeID may be reported as
// convergence.
//
// It surfaces the condition on two signals, because they answer different
// questions and only one of them is already wired up:
//
//   - anti_entropy_stale is a counter of events: "a pass could not cover a
//     replica's gap from the log", bumped every time a pass rediscovers it. It
//     says something happened, and it distinguishes this cause from a snapshot
//     restore. It cannot say whether the divergence still stands.
//
//   - anti_entropy_full_sync_required is the latched gauge whose documented
//     meaning is exactly the standing condition here — this node's WAL is not a
//     complete record of the data it holds, so it cannot converge its replicas
//     from the log — and whose remedy is exactly the same missing mechanism, the
//     key-range scan in CursorStore.FullSyncRequired's TODO. Latching it is
//     therefore the honest reading rather than a reuse of a nearby flag.
//
// The gauge is node-wide while the gap is per-replica, so latching it
// over-reports: a claim about a different replica that the log *can* still prove
// is suppressed too. That is the same direction the gauge already errs in by never
// clearing, and it is why the precise per-replica reason is carried in the log
// line and in replicaState.logGapReason.
//
// It is persisted for the same reason the restore case is: dropped segments are
// gone permanently, so a restart must not forget that the log has a hole in it.
func (ae *antiEntropy) noteLogGap(nodeID, reason string) {
	ae.mu.Lock()
	if st := ae.replica[nodeID]; st != nil && !st.logGap {
		st.logGap, st.logGapReason = true, reason
	}
	alreadyLatched := ae.fullSync
	if !alreadyLatched {
		ae.fullSync, ae.fullSyncReason = true, reason
	}
	ae.mu.Unlock()

	if ae.metrics != nil {
		ae.metrics.AntiEntropyStaleCursors.Add(1)
		ae.metrics.AntiEntropyFullSyncRequired.Store(1)
	}
	if alreadyLatched {
		return // MarkFullSyncRequired would be a no-op; skip the flush entirely
	}
	if err := ae.cursors.MarkFullSyncRequired(reason); err != nil {
		ae.logger.Warn("persist full-sync-required latch", "err", err)
	}
}

// convergenceClaimBlocked reports whether this node is allowed to say that
// nodeID is caught up, and if not, why. The per-replica reason wins over the
// node-wide one because it is the more specific account of the same fact.
func (ae *antiEntropy) convergenceClaimBlocked(nodeID string) (bool, string) {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	if st := ae.replica[nodeID]; st != nil && st.logGap {
		return true, st.logGapReason
	}
	if ae.fullSync {
		return true, ae.fullSyncReason
	}
	return false, ""
}

// isBehind reports whether this node believes it is ahead of nodeID.
func (ae *antiEntropy) isBehind(nodeID string) bool {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	st := ae.replica[nodeID]
	return st != nil && st.behind
}

// markCaughtUp clears the behind flag unless a replication failed during the
// cycle, in which case there is fresh divergence the cycle did not cover.
func (ae *antiEntropy) markCaughtUp(nodeID string, failuresAtStart uint64) {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	st := ae.replica[nodeID]
	if st == nil {
		return
	}
	if st.failures != failuresAtStart {
		return
	}
	st.behind = false
	// The cycle just established the cursor; a candidate captured before it is
	// stale bookkeeping.
	st.hasCandidate = false
}

// runPass ships one batch of missed writes to nodeID, covering [from, limit).
// It returns the number of entries the replica ACKed.
func (ae *antiEntropy) runPass(ctx context.Context, nodeID string, from, limit storewal.Position) (int, error) {
	client, ok := ae.peers[nodeID]
	if !ok {
		return 0, fmt.Errorf("no gRPC client for replica %s", nodeID)
	}

	segments, err := ae.store.WALSegments()
	if err != nil {
		return 0, fmt.Errorf("list WAL segments: %w", err)
	}

	// A zero cursor means "no evidence about what this replica has", so the pass
	// starts at the oldest segment on disk. That covers the replica's whole gap
	// only if nothing has ever been released — if segment 1 is still here, the
	// pass reads this node's entire history. Once an earlier segment is gone there
	// is a gap of unknown size, and nothing else will report it: NewReader returns
	// on from.IsZero() *before* it looks for the cursor's segment, so ErrCursorStale
	// is structurally unreachable on this path. Without this check the pass ships
	// whatever survives, reports no error, and the next pass's empty result is read
	// as convergence.
	//
	// Gated on the replica being known behind, which is the only evidence this node
	// has that the replica missed anything at all. A replica that merely failed a
	// health probe and recovered has a zero cursor too, and treating that as a gap
	// would latch the full-sync gauge on a health flap.
	if from.IsZero() && ae.isBehind(nodeID) &&
		len(segments) > 0 && segments[0].Seq > retainAllWALSegments {
		ae.noteLogGap(nodeID, fmt.Sprintf(
			"replica %s has no recorded cursor and WAL segments below %d have been released",
			nodeID, segments[0].Seq))
		ae.logger.Warn("replica has no recorded cursor and the retained WAL no longer "+
			"starts at its first segment; catching up from the oldest surviving segment. "+
			"Keys whose only write fell in the released range stay divergent until "+
			"rewritten (v1 limitation)",
			"replica", nodeID, "oldest_segment", segments[0].Seq)
	}

	reader, err := storewal.NewReader(segments, from)
	if err != nil {
		if !errors.Is(err, storewal.ErrCursorStale) {
			return 0, err
		}
		// The gap cannot be closed from the log. Continue from the oldest
		// surviving segment: that converges every key written since, which is
		// strictly better than converging nothing, and record it loudly because
		// the keys in the lost range stay divergent until they are written again.
		ae.noteLogGap(nodeID, fmt.Sprintf(
			"replica %s cursor %s points into a released WAL segment", nodeID, from))
		ae.logger.Warn("replica cursor is older than the retained WAL; "+
			"catching up from the oldest surviving segment. Keys whose only write "+
			"fell in the lost range stay divergent until rewritten (v1 limitation)",
			"replica", nodeID, "cursor", from, "err", err)
	}
	defer reader.Close()
	reader.LimitTo(limit)

	batch, scanned, truncated, err := ae.collect(reader, nodeID)
	if err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		// Nothing for this replica in the range, but the range itself is now
		// accounted for: without this the cursor would never move past a stretch
		// of the log that happens to hold no keys this replica owns.
		ae.cursors.Advance(nodeID, limit)
		return 0, nil
	}
	ae.logger.Debug("catch-up range prepared", "replica", nodeID,
		"from", from, "to", limit, "scanned", scanned, "to_send", len(batch))

	term := ae.term()
	sent := 0
	for _, e := range batch {
		op := server.OpPut
		if e.Op == storewal.OpDelete {
			op = server.OpDelete
		}

		sendCtx, cancel := context.WithTimeout(ctx, ae.cfg.SendTimeout)
		resp, err := client.Replicate(sendCtx, &kvpb.ReplicateRequest{
			Op:    op,
			Key:   e.Key,
			Value: e.Value,
			Term:  term,
			// The sequence the entry was written with, read back out of the log
			// rather than assigned here. That is what makes a pass idempotent and
			// safe to race against live replication: an entry the replica already
			// has arrives with a sequence it already holds and is discarded, and
			// an entry superseded by a write that landed after this range was
			// pinned loses to that write's higher sequence instead of reverting
			// it. Assigning a fresh sequence at replay time would invert exactly
			// that case — the replay would look newer than the newer write.
			//
			// Entries from a WAL record written before the log carried sequences
			// report 0, which the replica applies unconditionally; that is the
			// arrival-order behaviour this replaces, and it only affects segments
			// already on disk at upgrade time.
			Seq: e.Seq,
			// This is a replay, and saying so is what keeps the receiver's
			// epoch-regression alarm meaningful. The range above was pinned when
			// this pass started, so an entry in it can predate a restart of this
			// node while the replica already holds a post-restart version of the
			// same key — delivered live, past the pinned range. The replica is
			// right to refuse it, and refusing it is not evidence that any
			// incarnation went backwards, which is what the receiver would
			// otherwise record.
			Replay: true,
		})
		cancel()
		if err != nil {
			return sent, fmt.Errorf("replicate %s to %s: %w", e.Key, nodeID, err)
		}
		if !resp.Success {
			return sent, fmt.Errorf("replica %s rejected catch-up write for %s", nodeID, e.Key)
		}

		sent++
		if ae.metrics != nil {
			ae.metrics.AntiEntropyEntriesSent.Add(1)
		}
		// Advance past the entry the replica just ACKed. Entries are sent in WAL
		// order and an entry is only skipped when a newer entry for the same key
		// follows it in this range, so nothing the cursor steps over is left
		// unshipped: the skipped version's replacement is still ahead of the
		// cursor. This is what lets a replica that dies mid-pass resume exactly
		// where it stopped.
		ae.cursors.Advance(nodeID, e.End)
	}

	// The tail of the range after the last sent entry holds only entries that are
	// not this replica's business, so the whole range is covered — unless the pass
	// stopped at its entry bound, in which case the cursor stays at the last entry
	// the replica ACKed and the next pass resumes from there.
	if !truncated {
		ae.cursors.Advance(nodeID, limit)
	}
	return sent, nil
}

// collect reads the range and returns the entries to ship: those this node is
// ring-primary for with nodeID in the replica set, deduplicated to the newest
// entry per key, in ascending WAL position order.
//
// The position order is load-bearing, not cosmetic. runPass advances the cursor
// past each entry the replica ACKs, and the cursor is monotonic — so if a lower
// position were sent after a higher one, the lower entry's progress would be
// silently dropped and a crash in between would leave the cursor claiming an
// entry that was never shipped. Deduplication rewrites entries in place and
// therefore does not preserve order on its own, so the batch is sorted before it
// is returned.
//
// truncated reports that the entry bound was reached and the range is only
// partially covered.
func (ae *antiEntropy) collect(reader *storewal.Reader, nodeID string) (
	batch []storewal.Entry, scanned int, truncated bool, err error,
) {
	latest := make(map[string]int) // key → index into batch

	for {
		entry, ok, err := reader.Next()
		if err != nil {
			return nil, scanned, false, fmt.Errorf("read WAL: %w", err)
		}
		if !ok {
			break
		}
		scanned++

		if !ae.owedTo(entry.Key, nodeID) {
			continue
		}
		if idx, seen := latest[entry.Key]; seen {
			batch[idx] = entry // newer version of a key already in the batch
			continue
		}
		if len(batch) >= ae.cfg.MaxEntriesPerPass {
			// Stop here rather than materialising an unbounded slice. The cursor
			// advances over what is sent and the next pass continues from there.
			ae.logger.Warn("catch-up pass truncated at its entry bound; "+
				"the next pass will continue from where this one stops",
				"replica", nodeID, "bound", ae.cfg.MaxEntriesPerPass)
			truncated = true
			break
		}
		latest[entry.Key] = len(batch)
		batch = append(batch, entry)
	}

	sort.Slice(batch, func(i, j int) bool { return batch[i].Pos.Before(batch[j].Pos) })
	return batch, scanned, truncated, nil
}

// owedTo reports whether an entry for key is this node's to send to nodeID: this
// node must be the ring-primary, and nodeID must be one of the replicas.
//
// The primary check matters because a node's WAL also contains the writes it
// accepted *as* a replica for keys owned elsewhere. Replaying those would have
// this node speak for a range it does not own.
func (ae *antiEntropy) owedTo(key, nodeID string) bool {
	replicas, err := ae.ring.GetN(key, ae.replicaCount)
	if err != nil || len(replicas) == 0 {
		return false
	}
	if replicas[0].NodeID != ae.nodeID {
		return false
	}
	for _, vn := range replicas[1:] {
		if vn.NodeID == nodeID {
			return true
		}
	}
	return false
}
