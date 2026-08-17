package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newDriverConfig wires the driver to an injected dispatcher so the arrival
// process, latency accounting, and per-op bookkeeping can be tested without an
// etcd cluster.
func newDriverConfig(t *testing.T, mix string, qps float64, d time.Duration, workers int, fn dispatchFunc) runConfig {
	t.Helper()
	wl, err := newWorkload(1000, "zipf", mix, 64)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	return runConfig{
		qps:      qps,
		duration: d,
		workers:  workers,
		queueCap: workers * 4,
		wl:       wl,
		dispatch: fn,
	}
}

func TestRunRecordsPerOpSamplesAndErrors(t *testing.T) {
	var puts, gets atomic.Int64
	cfg := newDriverConfig(t, "50:50:0", 2000, 300*time.Millisecond, 8,
		func(_ context.Context, _ int, op opKind, _ string) error {
			switch op {
			case opPut:
				puts.Add(1)
				return errors.New("injected put failure")
			case opGet:
				gets.Add(1)
			}
			return nil
		})

	res := run(context.Background(), cfg)

	if res.ops == 0 {
		t.Fatal("no ops recorded")
	}
	// Every dispatched op contributes exactly one latency sample to its kind.
	for k := opKind(0); k < numOpKinds; k++ {
		s := res.perOp[k]
		if uint64(len(s.latenciesUS)) != s.count {
			t.Errorf("%s: %d samples for %d ops", k, len(s.latenciesUS), s.count)
		}
	}
	total := res.perOp[opPut].count + res.perOp[opGet].count + res.perOp[opDelete].count
	if total != res.ops {
		t.Errorf("per-op counts sum to %d, res.ops = %d", total, res.ops)
	}
	// All errors were injected on puts, so they must be attributed there.
	if res.perOp[opPut].errors != res.perOp[opPut].count {
		t.Errorf("put errors = %d, want all %d puts", res.perOp[opPut].errors, res.perOp[opPut].count)
	}
	if res.perOp[opGet].errors != 0 {
		t.Errorf("get errors = %d, want 0", res.perOp[opGet].errors)
	}
	if res.errors != res.perOp[opPut].errors {
		t.Errorf("aggregate errors = %d, put errors = %d", res.errors, res.perOp[opPut].errors)
	}
	if res.perOp[opDelete].count != 0 {
		t.Errorf("delete ops = %d, want 0 for a 50:50:0 mix", res.perOp[opDelete].count)
	}
}

func TestRunApproximatesTargetRate(t *testing.T) {
	cfg := newDriverConfig(t, "0:100:0", 1000, 500*time.Millisecond, 16,
		func(_ context.Context, _ int, _ opKind, _ string) error { return nil })

	res := run(context.Background(), cfg)

	wall := res.endedAt.Sub(res.startedAt).Seconds()
	achieved := float64(res.ops) / wall
	// Poisson arrivals against a no-op dispatcher: wide band, but an order of
	// magnitude off would mean the inter-arrival maths is wrong.
	if achieved < 500 || achieved > 1600 {
		t.Errorf("achieved %.0f qps against a 1000 qps target (%d ops in %.2fs)", achieved, res.ops, wall)
	}
}

// Latency is measured from the scheduled arrival time, not from dispatch, so
// queue wait is included and coordinated omission is eliminated. A slow
// dispatcher with a backed-up queue must therefore show latencies well above
// the per-op service time.
func TestRunMeasuresFromScheduledTimeIncludingQueueWait(t *testing.T) {
	const serviceTime = 20 * time.Millisecond
	// 2 workers at 20ms service time sustain ~100 ops/s; ask for 500.
	cfg := newDriverConfig(t, "0:100:0", 500, 400*time.Millisecond, 2,
		func(_ context.Context, _ int, _ opKind, _ string) error {
			time.Sleep(serviceTime)
			return nil
		})

	res := run(context.Background(), cfg)
	if res.ops == 0 {
		t.Fatal("no ops recorded")
	}

	p := summarize(res.perOp[opGet].latenciesUS)
	serviceUS := serviceTime.Microseconds()
	if p.Max <= serviceUS {
		t.Errorf("max latency %dµs did not exceed service time %dµs — queue wait was not counted",
			p.Max, serviceUS)
	}
	if res.maxQueueDepth == 0 {
		t.Error("maxQueueDepth = 0 despite an intentionally overloaded worker pool")
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	cfg := newDriverConfig(t, "0:100:0", 1000, 30*time.Second, 4,
		func(_ context.Context, _ int, _ opKind, _ string) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	res := run(ctx, cfg)
	elapsed := time.Since(start)

	// The phase deadline is 30s; cancellation must win, and run must still
	// return a usable result rather than hanging on the arrival goroutine.
	if elapsed > 5*time.Second {
		t.Errorf("run took %s after a 150ms cancellation", elapsed)
	}
	if res.endedAt.IsZero() {
		t.Error("result not finalized on cancellation")
	}
}

func TestRunHandlesZeroDuration(t *testing.T) {
	// A zero-length phase is what a --warmup 0 run reduces to; it must not
	// deadlock or divide by zero downstream.
	cfg := newDriverConfig(t, "0:100:0", 1000, time.Nanosecond, 4,
		func(_ context.Context, _ int, _ opKind, _ string) error { return nil })

	res := run(context.Background(), cfg)
	rep := buildReport(cfg, res, []string{"e"}, 64, "0:100:0")
	if rep.QPSAchieved < 0 {
		t.Errorf("negative QPS %.2f", rep.QPSAchieved)
	}
}
