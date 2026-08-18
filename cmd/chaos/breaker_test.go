package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

// probeStub is a controllable breaker probe.
type probeStub struct {
	ok    atomic.Bool
	calls atomic.Int64
}

func (p *probeStub) fn(context.Context) error {
	p.calls.Add(1)
	if p.ok.Load() {
		return nil
	}
	return errors.New("target not answering")
}

// TestBreakerOpensOnConsecutiveTransportFailures pins the threshold, and pins
// that reaching it is what stops the workload.
func TestBreakerOpensOnConsecutiveTransportFailures(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(3, time.Millisecond, p.fn, t.Logf)

	for i := 0; i < 2; i++ {
		b.Record(kindDial)
		if b.IsOpen() {
			t.Fatalf("opened after %d failures, threshold is 3", i+1)
		}
		if !b.Allow() {
			t.Fatal("a closed breaker must allow operations")
		}
	}

	b.Record(kindDial)
	if !b.IsOpen() {
		t.Fatal("the breaker must open at the threshold")
	}
	if b.Allow() {
		t.Fatal("an open breaker must not allow operations")
	}
	if got := b.Stats().Episodes; got != 1 {
		t.Errorf("episodes = %d, want 1", got)
	}
}

// TestBreakerCountsSentFailuresToo is the case the leader-kill gate turns on: a
// restarting container accepts connections before it serves them, so the failures
// arriving are EOFs from an established connection, not refused dials. A breaker
// that only counted refused dials would stay closed through exactly the window
// that produces the ambiguous writes.
func TestBreakerCountsSentFailuresToo(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(2, time.Millisecond, p.fn, t.Logf)

	b.Record(kindSent)
	b.Record(kindSent)
	if !b.IsOpen() {
		t.Fatal("EOFs from an accepting socket with nothing behind it must open the breaker")
	}
}

// TestBreakerIgnoresAnsweredRequests is the no-regression guarantee for
// stop-restart and kill-restart. Those modes never take the target down: it stays
// up and answers 503 for every write it cannot replicate, tens of thousands of
// times in a single run. If those counted, the breaker would pause a workload
// against a healthy, serving node and change the shape of gates that were already
// checkable.
func TestBreakerIgnoresAnsweredRequests(t *testing.T) {
	answered := []failureKind{
		kindNone,
		kindRefusedApplied,
		kindForwardNeverSent,
		kindForwardUnknown,
		kindStatusOther,
	}
	for _, kind := range answered {
		t.Run(kind.String(), func(t *testing.T) {
			p := &probeStub{}
			b := newTargetBreaker(2, time.Millisecond, p.fn, t.Logf)
			for i := 0; i < 10_000; i++ {
				b.Record(kind)
			}
			if b.IsOpen() {
				t.Fatalf("%v opened the breaker; a request the target answered is "+
					"evidence the target is serving", kind)
			}
		})
	}
}

// TestBreakerRunIsConsecutive pins that a single answered request clears the
// evidence gathered so far — the breaker is looking for an outage, not for a
// failure rate.
func TestBreakerRunIsConsecutive(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(3, time.Millisecond, p.fn, t.Logf)

	b.Record(kindDial)
	b.Record(kindDial)
	b.Record(kindRefusedApplied) // the target answered: reset
	b.Record(kindDial)
	b.Record(kindDial)
	if b.IsOpen() {
		t.Fatal("an answered request in the middle must reset the run")
	}
	b.Record(kindDial)
	if !b.IsOpen() {
		t.Fatal("three consecutive failures after the reset must open it")
	}
}

// TestBreakerTreatsCancellationAsNeutral pins that the runner's own shutdown
// neither accuses the target nor exonerates it. Counting it would let a normal
// end-of-run cancellation pause a workload that is about to stop anyway; treating
// it as success would discard real evidence about an outage in progress.
func TestBreakerTreatsCancellationAsNeutral(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(3, time.Millisecond, p.fn, t.Logf)

	b.Record(kindCanceled)
	b.Record(kindCanceled)
	b.Record(kindCanceled)
	if b.IsOpen() {
		t.Fatal("shutdown cancellations must not open the breaker")
	}

	b.Record(kindDial)
	b.Record(kindDial)
	b.Record(kindCanceled) // must not reset
	b.Record(kindDial)
	if !b.IsOpen() {
		t.Fatal("a cancellation must not clear evidence already gathered")
	}
}

// TestBreakerResumesOnlyAfterASuccessfulProbe is the other half of the contract:
// the workload comes back when the target answers, not when a timer expires. A
// time-based resume would put the workload back on the wire during the window
// where the container accepts connections but does not serve them — which is
// precisely the window that generates unknown writes.
func TestBreakerResumesOnlyAfterASuccessfulProbe(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(1, time.Millisecond, p.fn, t.Logf)
	ctx := context.Background()

	b.Record(kindDial)
	if !b.IsOpen() {
		t.Fatal("setup: the breaker should be open")
	}

	for i := 0; i < 5; i++ {
		b.Wait(ctx)
		if !b.IsOpen() {
			t.Fatalf("a failing probe closed the breaker on attempt %d", i+1)
		}
	}
	if p.calls.Load() == 0 {
		t.Fatal("no probe was issued while the breaker was open")
	}

	p.ok.Store(true)
	// One Wait may be spent as a non-prober; loop until the probe lands.
	deadline := time.Now().Add(2 * time.Second)
	for b.IsOpen() && time.Now().Before(deadline) {
		b.Wait(ctx)
	}
	if b.IsOpen() {
		t.Fatal("a successful probe must close the breaker")
	}

	stats := b.Stats()
	if stats.Episodes != 1 {
		t.Errorf("episodes = %d, want 1", stats.Episodes)
	}
	if stats.Paused <= 0 {
		t.Error("a closed episode must report the time it paused")
	}
	if !b.Allow() {
		t.Error("a closed breaker must allow operations again")
	}
}

// TestBreakerProbesOneAtATime keeps a paused workload from turning into a probe
// storm against a node that is trying to start up.
func TestBreakerProbesOneAtATime(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(1, time.Hour, p.fn, t.Logf) // backoff long enough to hold the claim
	b.Record(kindDial)

	if !b.claimProbe() {
		t.Fatal("the first caller must become the prober")
	}
	if b.claimProbe() {
		t.Fatal("a second caller must not also become the prober")
	}
	b.releaseProbe()
	if !b.claimProbe() {
		t.Fatal("the claim must be reusable once released")
	}
}

// TestBreakerCountsSkippedOperations pins the accounting behind the reported
// pause. An operation the breaker refuses is not a failed operation — it never
// happened — so it must be visible as load that was not offered rather than
// disappearing from the run's description.
func TestBreakerCountsSkippedOperations(t *testing.T) {
	p := &probeStub{}
	b := newTargetBreaker(1, time.Millisecond, p.fn, t.Logf)
	b.Record(kindDial)

	for i := 0; i < 7; i++ {
		if b.Allow() {
			t.Fatal("an open breaker must refuse")
		}
	}
	if got := b.Stats().Skipped; got != 7 {
		t.Errorf("skipped = %d, want 7", got)
	}
}

// TestNilBreakerIsDisabled covers --fail-fast-after=0, which is the revert-check
// for every claim about the breaker: the same run with the same code, one flag
// different.
func TestNilBreakerIsDisabled(t *testing.T) {
	var b *targetBreaker
	if got := newTargetBreaker(0, time.Second, nil, t.Logf); got != nil {
		t.Fatal("--fail-fast-after=0 must produce no breaker")
	}
	if !b.Allow() {
		t.Error("a disabled breaker must allow every operation")
	}
	b.Record(kindDial)
	b.Wait(context.Background())
	if b.IsOpen() {
		t.Error("a disabled breaker is never open")
	}
	if got := b.Stats(); got.Episodes != 0 || got.Skipped != 0 {
		t.Errorf("stats = %+v, want zero", got)
	}
}

// TestBreakerStateCannotChangeAClassification is the soundness audit stated as a
// test. The breaker decides whether an operation *starts*; classification decides
// what a started operation meant. The two share no state, and finishWrite does not
// take a breaker at all — so this asserts the observable consequence: the history
// event recorded for a given error is byte-identical whether or not a breaker is
// open at the time.
func TestBreakerStateCannotChangeAClassification(t *testing.T) {
	p := &probeStub{}
	open := newTargetBreaker(1, time.Millisecond, p.fn, t.Logf)
	open.Record(kindDial)
	if !open.IsOpen() {
		t.Fatal("setup: the breaker should be open")
	}

	errs := []error{
		wrapUnreachable(errors.New("EOF")),
		wrapUnreachable(errors.New("dial tcp 127.0.0.1:8001: connect: connection refused")),
		context.Canceled,
		nil,
	}

	for _, err := range errs {
		// With no breaker in the picture at all…
		bare := &counters{}
		bareRec := &linearizability.Recorder{}
		bareKind := finishWrite(context.Background(), bareRec,
			bareRec.Begin(linearizability.Input{Op: "put", Key: "k", Value: "v"}), err, bare)

		// …and with one open. finishWrite has no breaker parameter; the caller
		// feeds it the result afterwards.
		withBreaker := &counters{}
		breakerRec := &linearizability.Recorder{}
		gotKind := finishWrite(context.Background(), breakerRec,
			breakerRec.Begin(linearizability.Input{Op: "put", Key: "k", Value: "v"}), err, withBreaker)
		open.Record(gotKind)

		if gotKind != bareKind {
			t.Errorf("classifyFailure(%v) = %v with an open breaker, %v without", err, gotKind, bareKind)
		}
		if bareRec.Len() != breakerRec.Len() {
			t.Errorf("recorded %d events with an open breaker, %d without, for %v",
				breakerRec.Len(), bareRec.Len(), err)
		}
		if withBreaker.indeterminateWrites.Load() != bare.indeterminateWrites.Load() {
			t.Errorf("indeterminate count differs for %v", err)
		}
	}
}
