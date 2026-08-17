package main

import (
	"context"
	"math"
	mathrand "math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// perOpTimeout bounds a single etcd RPC. Matches cmd/bench's per-op deadline so
// a sick cluster degrades identically in both harnesses instead of one of them
// hanging while the other gives up.
const perOpTimeout = 10 * time.Second

// arrival is one scheduled op carrying the time it was supposed to start.
// Workers measure latency from this scheduled time (not dispatch time) so the
// recorded number includes any queue wait, eliminating coordinated omission.
type arrival struct {
	scheduledAt time.Time
	op          opKind
	key         string
}

// opSamples holds every recorded latency for one op kind, in microseconds.
//
// cmd/bench merges all ops into a single HDR histogram (3 significant figures,
// ~0.1% quantile error, max pinned at its 60s ceiling). Here the samples are
// kept in a pre-allocated slice instead: percentiles come out exact, `max` is
// the true maximum rather than a clamped one, and the split by op kind is what
// makes the etcd/distrikv comparison legible — a Raft store's write path and
// read path have very different costs, and one merged number hides that.
type opSamples struct {
	count       uint64
	errors      uint64
	latenciesUS []int64
}

// runResult holds aggregate counters and per-op latency samples for one phase
// (warmup or measurement).
type runResult struct {
	ops           uint64
	errors        uint64
	maxQueueDepth uint64
	startedAt     time.Time
	endedAt       time.Time
	perOp         [numOpKinds]*opSamples
}

// dispatchFunc issues one op on behalf of worker workerIdx. The worker index is
// passed through so the caller owns endpoint selection, which keeps this file
// free of any etcd dependency and lets the driver be tested without a cluster.
type dispatchFunc func(ctx context.Context, workerIdx int, op opKind, key string) error

// runConfig drives a single phase of an open-loop bench run.
type runConfig struct {
	qps      float64
	duration time.Duration
	workers  int
	queueCap int
	wl       *workload
	dispatch dispatchFunc
}

// shares returns the expected fraction of ops per kind, from the parsed mix.
func (w *workload) shares() [numOpKinds]float64 {
	total := float64(w.deleteThresh)
	return [numOpKinds]float64{
		opPut:    float64(w.putThresh) / total,
		opGet:    float64(w.getThresh-w.putThresh) / total,
		opDelete: float64(w.deleteThresh-w.getThresh) / total,
	}
}

// estimateSamples sizes one worker's latency slice for one op kind.
//
// Sized from the offered load rather than the achieved load, with 25% headroom
// for Poisson burstiness and uneven worker draw. Appends past this still work —
// the estimate only exists to keep growth reallocations out of the hot path, so
// a slow cluster (fewer samples) or a bursty one (a late grow) both stay
// correct.
func estimateSamples(qps float64, duration time.Duration, workers int, share float64) int {
	if workers <= 0 || share <= 0 || qps <= 0 || duration <= 0 {
		return 0
	}
	expected := qps * duration.Seconds() * share / float64(workers)
	return int(expected*1.25) + 64
}

// run executes one phase. It returns when ctx is done OR the configured
// duration has elapsed and all in-flight ops have settled.
func run(ctx context.Context, cfg runConfig) *runResult {
	res := &runResult{startedAt: time.Now()}
	for k := range res.perOp {
		res.perOp[k] = &opSamples{}
	}

	shares := cfg.wl.shares()

	// Per-worker sample buffers, merged at the end. Keeping them worker-local
	// means the recording path needs no lock at all.
	workerSamples := make([][numOpKinds][]int64, cfg.workers)
	workerCounts := make([][numOpKinds]uint64, cfg.workers)
	workerErrors := make([][numOpKinds]uint64, cfg.workers)
	for i := range workerSamples {
		for k := range workerSamples[i] {
			n := estimateSamples(cfg.qps, cfg.duration, cfg.workers, shares[k])
			workerSamples[i][k] = make([]int64, 0, n)
		}
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
			for a := range queue {
				queueDepth.Add(-1)

				// Deliberately not derived from phaseCtx: an op that started
				// just before the deadline must be allowed to finish, otherwise
				// the phase boundary would manufacture errors and truncate the
				// tail we are trying to measure.
				opCtx, cancel := context.WithTimeout(context.Background(), perOpTimeout)
				err := cfg.dispatch(opCtx, idx, a.op, a.key)
				cancel()

				elapsedUs := time.Since(a.scheduledAt).Microseconds()
				if elapsedUs < 1 {
					elapsedUs = 1
				}
				workerSamples[idx][a.op] = append(workerSamples[idx][a.op], elapsedUs)
				workerCounts[idx][a.op]++

				atomic.AddUint64(&res.ops, 1)
				if err != nil {
					workerErrors[idx][a.op]++
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

	for i := range workerSamples {
		for k := range workerSamples[i] {
			res.perOp[k].latenciesUS = append(res.perOp[k].latenciesUS, workerSamples[i][k]...)
			res.perOp[k].count += workerCounts[i][k]
			res.perOp[k].errors += workerErrors[i][k]
		}
	}
	res.maxQueueDepth = uint64(maxDepth.Load())
	res.endedAt = time.Now()
	return res
}
