package main

// Fail-fast on an unreachable target.
//
// # Why the workload needs this
//
// When the nemesis takes down the node the runner is driving — which only
// leader-kill does, because only leader-kill picks its victim from cluster state
// — every worker keeps issuing requests at the full offered rate into an endpoint
// that is dying, gone, or coming back. Most of those failures are refused dials,
// which are provably harmless and cost the checker nothing. A minority are not:
// anything that reaches an accepting socket with no server behind it returns EOF,
// and an EOF cannot prove the request was unread, so the write becomes a pending
// operation that overlaps every later operation on its key.
//
// A container that is restarting accepts connections before it serves them. At
// ~5,000 ops/s that window does not have to be long to produce hundreds of
// pending operations, which is what made the leader-kill gate's linearizability
// verdict permanently UNKNOWN — the Porcupine search could not finish inside 60s
// or 300s.
//
// So the fix cannot be classification alone: the EOF genuinely is ambiguous. The
// fix is to stop generating it. After a run of consecutive transport failures the
// workload pauses, and a single cheap probe decides when it resumes. Ambiguous
// requests are then confined to the two instants of transition rather than
// spread across the whole outage.
//
// Jepsen does the same thing for the same reason, and so does every production
// client library. It is worth being explicit that this changes the offered-load
// profile during an outage: ops-attempted drops while a target is unreachable.
// That is reported — a log line when it engages and a summary row with the
// episode count and total paused time — because a harness that silently offers
// less load than it claims is a harness whose numbers cannot be compared between
// runs.
//
// # Why it cannot affect the verdict
//
// The breaker decides *whether to start* an operation. It has no reference to the
// recorder, no way to reach an operation already in flight, and no input to
// classifyFailure. An operation it skips is never begun, so it contributes no
// events to the history at all — it is not a failed write, it is a write that
// never happened. Every operation that *is* started is classified exactly as it
// would have been with the breaker disabled.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// breakerPollInterval is how long a worker sleeps before re-checking a breaker
// that is open. It only bounds how quickly the workload resumes after a
// successful probe, so it is short; the probe cadence is set by the backoff.
const breakerPollInterval = 5 * time.Millisecond

// breakerProbeTimeout bounds one probe. A probe that hangs is a target that is
// not serving, which is the same answer as a probe that fails.
const breakerProbeTimeout = time.Second

// targetBreaker pauses the workload while the target is failing at the transport
// level.
//
// It is deliberately not a general-purpose circuit breaker: there is no
// half-open state and no failure-rate window. The only question it answers is
// "is anything answering HTTP at --target right now", because that is the only
// question whose answer changes how many ambiguous writes a run produces.
//
// A nil *targetBreaker is a disabled breaker: every method is a no-op and Allow
// always returns true. That is what --fail-fast-after=0 installs, and it is what
// makes the revert-check a flag change rather than a code change.
type targetBreaker struct {
	// threshold is the number of consecutive transport failures that opens it.
	threshold int
	// backoff is how long the prober waits before each probe.
	backoff time.Duration
	// probe reports whether the target is answering. It must not mutate the
	// store: see newTargetBreaker.
	probe func(context.Context) error
	logf  func(format string, args ...any)

	// skipped counts operations never attempted because the breaker was open.
	// Atomic because every worker touches it on the hot path.
	skipped atomic.Int64

	mu          sync.Mutex
	consecutive int
	open        bool
	openedAt    time.Time
	probing     bool
	episodes    int
	pausedTotal time.Duration
}

// newTargetBreaker builds a breaker, or returns nil when threshold < 1.
//
// probe must be read-only. It is called while the workload is paused and its
// result is not recorded in the history, so a probe that mutated the store would
// be a write the checker never saw — the one way this mechanism could corrupt a
// verdict. The caller in main() passes Status, which reads no keys at all.
func newTargetBreaker(
	threshold int,
	backoff time.Duration,
	probe func(context.Context) error,
	logf func(format string, args ...any),
) *targetBreaker {
	if threshold < 1 {
		return nil
	}
	if backoff <= 0 {
		backoff = 250 * time.Millisecond
	}
	return &targetBreaker{threshold: threshold, backoff: backoff, probe: probe, logf: logf}
}

// Allow reports whether a worker may start an operation. A false answer means
// the operation is not attempted at all — no history event, no counted op.
func (b *targetBreaker) Allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	open := b.open
	b.mu.Unlock()
	if open {
		b.skipped.Add(1)
		return false
	}
	return true
}

// Record feeds one completed operation's outcome back in.
//
//   - A kind that arrived as an HTTP response proves the target is serving, and
//     resets the run. This is what keeps the breaker out of the way during a
//     follower outage: the target answers 503 for every refused write, thousands
//     of times, and never trips it.
//   - A transport failure extends the run, and opens the breaker at the threshold.
//   - kindCanceled does neither. The runner's own context ending says nothing
//     about the target, so it must not be counted as evidence against it, and it
//     must not clear evidence already gathered.
func (b *targetBreaker) Record(kind failureKind) {
	if b == nil {
		return
	}
	switch {
	case kind.answeredByTarget():
		b.mu.Lock()
		b.consecutive = 0
		b.mu.Unlock()
	case kind.countsAgainstTarget():
		b.mu.Lock()
		defer b.mu.Unlock()
		b.consecutive++
		if !b.open && b.consecutive >= b.threshold {
			b.openLocked()
		}
	}
}

// Wait is what a worker calls when Allow said no. Exactly one worker at a time
// becomes the prober; the rest sleep briefly and re-check. Returning without the
// breaker closed is normal — the caller loops.
func (b *targetBreaker) Wait(ctx context.Context) {
	if b == nil {
		return
	}
	if !b.claimProbe() {
		sleep(ctx, breakerPollInterval)
		return
	}
	defer b.releaseProbe()

	if !sleep(ctx, b.backoff) {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, breakerProbeTimeout)
	err := b.probe(pctx)
	cancel()
	if err == nil {
		b.closeBreaker()
	}
}

// Stats is what the report prints.
type breakerStats struct {
	Episodes int           `json:"episodes"`
	Paused   time.Duration `json:"-"`
	PausedMs int64         `json:"paused_ms"`
	Skipped  int64         `json:"ops_skipped"`
}

// Stats snapshots the breaker for the report. An episode still open at the end
// of the run has its elapsed time counted, so the total covers the whole
// measurement rather than only the episodes that closed.
func (b *targetBreaker) Stats() breakerStats {
	if b == nil {
		return breakerStats{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	paused := b.pausedTotal
	if b.open {
		paused += time.Since(b.openedAt)
	}
	return breakerStats{
		Episodes: b.episodes,
		Paused:   paused,
		PausedMs: paused.Milliseconds(),
		Skipped:  b.skipped.Load(),
	}
}

// IsOpen reports the current state. For tests and for nothing else: a worker
// must ask Allow, which also accounts for the skipped operation.
func (b *targetBreaker) IsOpen() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

func (b *targetBreaker) openLocked() {
	b.open = true
	b.openedAt = time.Now()
	b.episodes++
	b.logf("target unreachable: pausing the workload after %d consecutive transport failures "+
		"(episode %d) — probing every %s until it answers",
		b.consecutive, b.episodes, b.backoff)
}

func (b *targetBreaker) closeBreaker() {
	b.mu.Lock()
	if !b.open {
		b.mu.Unlock()
		return
	}
	paused := time.Since(b.openedAt)
	b.open = false
	b.pausedTotal += paused
	b.consecutive = 0
	episode := b.episodes
	b.mu.Unlock()

	b.logf("target reachable again: paused %s (episode %d) — resuming the workload",
		paused.Round(time.Millisecond), episode)
}

// claimProbe elects the calling worker as prober, if the breaker is open and no
// one else holds the claim.
func (b *targetBreaker) claimProbe() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open || b.probing {
		return false
	}
	b.probing = true
	return true
}

func (b *targetBreaker) releaseProbe() {
	b.mu.Lock()
	b.probing = false
	b.mu.Unlock()
}

// answeredByTarget reports whether this kind means a server answered. Every 5xx
// does; a request that never got a response does not.
func (k failureKind) answeredByTarget() bool {
	switch k {
	case kindNone, kindRefusedApplied, kindForwardNeverSent, kindForwardUnknown, kindStatusOther:
		return true
	}
	return false
}

// countsAgainstTarget reports whether this kind is evidence the target itself is
// not serving. Both transport kinds are: a refused dial says so directly, and an
// EOF from an accepting socket with nothing behind it says so too — that second
// one is the shape a restarting container produces, and the reason the breaker
// counts more than just refused dials.
func (k failureKind) countsAgainstTarget() bool {
	return k == kindDial || k == kindSent
}
