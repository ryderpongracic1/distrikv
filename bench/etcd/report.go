package main

import (
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

// percentiles is one op kind's latency distribution, in microseconds.
//
// p999 is carried alongside the p50/p90/p99/max set because distrikv's own
// bench prints p999 — dropping it here would leave a hole in the side-by-side
// table for no saving.
type percentiles struct {
	P50  int64
	P90  int64
	P99  int64
	P999 int64
	Max  int64
}

// percentileUS returns the q-th percentile of an ascending-sorted sample slice
// using the nearest-rank method (the smallest sample at or above rank q).
//
// Exact by construction: no interpolation, no histogram bucketing, so a
// reported p99 is a latency some request actually observed.
func percentileUS(sortedUS []int64, q float64) int64 {
	if len(sortedUS) == 0 {
		return 0
	}
	if q <= 0 {
		return sortedUS[0]
	}
	if q >= 100 {
		return sortedUS[len(sortedUS)-1]
	}
	// Nearest-rank: rank = ceil(q/100 * N), 1-based.
	rank := int(math.Ceil(float64(len(sortedUS)) * q / 100))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sortedUS) {
		rank = len(sortedUS)
	}
	return sortedUS[rank-1]
}

// summarize sorts the samples in place and extracts the percentile set.
func summarize(samplesUS []int64) percentiles {
	sort.Slice(samplesUS, func(i, j int) bool { return samplesUS[i] < samplesUS[j] })
	return percentiles{
		P50:  percentileUS(samplesUS, 50),
		P90:  percentileUS(samplesUS, 90),
		P99:  percentileUS(samplesUS, 99),
		P999: percentileUS(samplesUS, 99.9),
		Max:  percentileUS(samplesUS, 100),
	}
}

// opReport is the per-op-kind row of the results table.
type opReport struct {
	Kind        opKind
	Ops         uint64
	Errors      uint64
	QPSAchieved float64
	Latency     percentiles
}

// report is the structured summary emitted at the end of a bench run.
type report struct {
	Endpoints    []string
	QPSRequested float64
	QPSAchieved  float64
	Duration     time.Duration
	Mix          string
	Keyspace     int
	KeyDist      string
	ValueSize    int
	Workers      int

	Ops    uint64
	Errors uint64

	Overall percentiles
	PerOp   []opReport

	MaxQueueDepth uint64
	Saturated     bool
}

// buildReport assembles a report from one phase's runResult.
//
// Sorting happens here rather than in the measurement path so no comparison
// work lands inside the timed window.
func buildReport(cfg runConfig, phase *runResult, endpoints []string, valueSize int, mix string) *report {
	wall := phase.endedAt.Sub(phase.startedAt).Seconds()
	if wall <= 0 {
		wall = 1
	}

	all := make([]int64, 0, phase.ops)
	perOp := make([]opReport, 0, numOpKinds)
	for k := opKind(0); k < numOpKinds; k++ {
		s := phase.perOp[k]
		all = append(all, s.latenciesUS...)
		perOp = append(perOp, opReport{
			Kind:        k,
			Ops:         s.count,
			Errors:      s.errors,
			QPSAchieved: float64(s.count) / wall,
			Latency:     summarize(s.latenciesUS),
		})
	}

	return &report{
		Endpoints:    endpoints,
		QPSRequested: cfg.qps,
		QPSAchieved:  float64(phase.ops) / wall,
		Duration:     cfg.duration,
		Mix:          mix,
		Keyspace:     cfg.wl.keyspace,
		KeyDist:      cfg.wl.keyDist,
		ValueSize:    valueSize,
		Workers:      cfg.workers,

		Ops:    phase.ops,
		Errors: phase.errors,

		Overall: summarize(all),
		PerOp:   perOp,

		MaxQueueDepth: phase.maxQueueDepth,
		// Same rule as cmd/bench: a queue that reached its cap means arrivals
		// outran completions, so the tail is queue wait rather than store
		// latency and the run does not describe the store's capability.
		Saturated: int(phase.maxQueueDepth) >= cfg.queueCap-1,
	}
}

// writeTable renders the results in a layout that lines up with cmd/bench's
// output, so the two can be read side by side without transcription.
func (r *report) writeTable(w io.Writer) {
	fmt.Fprintf(w, "== etcd ceiling bench: %s @ target %.0f qps, mix=%s, keyspace=%d %s ==\n",
		r.Duration, r.QPSRequested, r.Mix, r.Keyspace, r.KeyDist)
	fmt.Fprintf(w, "endpoints:     %v\n", r.Endpoints)
	fmt.Fprintf(w, "workers:       %-10d valuesize: %dB\n", r.Workers, r.ValueSize)
	fmt.Fprintf(w, "ops:           %-10d achieved_qps: %.1f    errors: %d\n",
		r.Ops, r.QPSAchieved, r.Errors)
	fmt.Fprintf(w, "latency (us):  overall  n=%-8d p50=%-7d p90=%-7d p99=%-7d p999=%-7d max=%d\n",
		r.Ops, r.Overall.P50, r.Overall.P90, r.Overall.P99, r.Overall.P999, r.Overall.Max)

	for _, op := range r.PerOp {
		if op.Ops == 0 {
			continue
		}
		fmt.Fprintf(w, "               %-8s n=%-8d p50=%-7d p90=%-7d p99=%-7d p999=%-7d max=%d\n",
			op.Kind, op.Ops, op.Latency.P50, op.Latency.P90, op.Latency.P99, op.Latency.P999, op.Latency.Max)
		fmt.Fprintf(w, "                        qps=%-9.1f errors=%d\n", op.QPSAchieved, op.Errors)
	}

	satStr := "false"
	if r.Saturated {
		satStr = "TRUE (cluster could not keep up — tail latency reflects queue wait)"
	}
	fmt.Fprintf(w, "saturation:    %s   max_queue_depth=%d\n", satStr, r.MaxQueueDepth)
	fmt.Fprintf(w, "wallclock:     %s\n", time.Now().Format(time.RFC3339))
}
