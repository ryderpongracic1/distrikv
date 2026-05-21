package main

import (
	"context"
	"math"
	mathrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

// arrival is one scheduled op carrying the time it was supposed to start.
// Workers measure latency from this scheduled time (not dispatch time) so the
// recorded number includes any queue wait, eliminating coordinated omission.
type arrival struct {
	scheduledAt time.Time
	op          opKind
	key         string
}

// runResult holds aggregate counters and a merged histogram for one phase
// (warmup or measurement).
type runResult struct {
	ops          uint64
	errors       uint64
	maxQueueDepth uint64
	startedAt    time.Time
	endedAt      time.Time
	hist         *hdrhistogram.Histogram
}

// runConfig drives a single phase of an open-loop bench run.
type runConfig struct {
	qps      float64
	duration time.Duration
	workers  int
	queueCap int
	wl       *workload
	clients  []*client.Client // one per target endpoint; round-robined per op
}

// run executes one phase. It returns when ctx is done OR the configured
// duration has elapsed and all in-flight ops have settled.
func run(ctx context.Context, cfg runConfig) *runResult {
	// HDR histogram: 1µs lower bound, 60s upper bound, 3 sig figs (~0.1% error).
	mergedHist := hdrhistogram.New(1, 60*int64(time.Second/time.Microsecond), 3)
	res := &runResult{
		startedAt: time.Now(),
		hist:      mergedHist,
	}

	// Per-worker histograms; merged at the end. Avoids contention on a single
	// histogram under high QPS.
	workerHists := make([]*hdrhistogram.Histogram, cfg.workers)
	for i := range workerHists {
		workerHists[i] = hdrhistogram.New(1, 60*int64(time.Second/time.Microsecond), 3)
	}

	queue := make(chan arrival, cfg.queueCap)
	var queueDepth atomic.Int64
	var maxDepth atomic.Int64

	// Phase deadline.
	phaseCtx, cancelPhase := context.WithTimeout(ctx, cfg.duration)
	defer cancelPhase()

	var workersWG sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		workersWG.Add(1)
		go func(idx int) {
			defer workersWG.Done()
			hist := workerHists[idx]
			for a := range queue {
				queueDepth.Add(-1)
				cli := cfg.clients[idx%len(cfg.clients)]

				// Per-op timeout — keep total bench responsive even when the
				// cluster is sick.
				opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := cfg.wl.dispatch(opCtx, cli, a.op, a.key)
				cancel()

				elapsedUs := time.Since(a.scheduledAt).Microseconds()
				if elapsedUs < 1 {
					elapsedUs = 1
				}
				if elapsedUs > hist.HighestTrackableValue() {
					elapsedUs = hist.HighestTrackableValue()
				}
				_ = hist.RecordValue(elapsedUs)

				atomic.AddUint64(&res.ops, 1)
				if err != nil {
					atomic.AddUint64(&res.errors, 1)
				}
			}
		}(i)
	}

	// Single arrival goroutine — Poisson process, ~ qps/sec.
	arrivalDone := make(chan struct{})
	go func() {
		defer close(arrivalDone)
		rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
		next := time.Now()
		var seq uint64
		for {
			// Sleep until the next scheduled arrival.
			if d := time.Until(next); d > 0 {
				select {
				case <-phaseCtx.Done():
					return
				case <-time.After(d):
				}
			}
			if phaseCtx.Err() != nil {
				return
			}
			seq++
			a := arrival{
				scheduledAt: next,
				op:          cfg.wl.nextOp(),
				key:         cfg.wl.nextKey(seq),
			}
			select {
			case queue <- a:
				depth := queueDepth.Add(1)
				// Lock-free running max — only swap if our depth is higher.
				for {
					prev := maxDepth.Load()
					if depth <= prev || maxDepth.CompareAndSwap(prev, depth) {
						break
					}
				}
			case <-phaseCtx.Done():
				return
			}

			// Exponential inter-arrival → Poisson process.
			u := rng.Float64()
			if u <= 0 {
				u = 1e-12
			}
			gap := time.Duration(-1e9 * (1.0 / cfg.qps) * math.Log(u))
			next = next.Add(gap)
		}
	}()

	<-arrivalDone
	close(queue)
	workersWG.Wait()

	for _, h := range workerHists {
		mergedHist.Merge(h)
	}
	res.maxQueueDepth = uint64(maxDepth.Load())
	res.endedAt = time.Now()
	return res
}
