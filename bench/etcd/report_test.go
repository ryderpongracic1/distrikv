package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestPercentileUSExact(t *testing.T) {
	// 100 samples, 1..100 µs. Nearest-rank percentiles land on real samples.
	samples := make([]int64, 100)
	for i := range samples {
		samples[i] = int64(i + 1)
	}
	tests := []struct {
		q    float64
		want int64
	}{
		{q: 50, want: 50},
		{q: 90, want: 90},
		{q: 99, want: 99},
		{q: 99.9, want: 100},
		{q: 100, want: 100},
		{q: 0, want: 1},
	}
	for _, tc := range tests {
		if got := percentileUS(samples, tc.q); got != tc.want {
			t.Errorf("percentileUS(1..100, %v) = %d, want %d", tc.q, got, tc.want)
		}
	}
}

func TestPercentileUSEdgeCases(t *testing.T) {
	if got := percentileUS(nil, 99); got != 0 {
		t.Errorf("percentileUS(nil) = %d, want 0", got)
	}
	if got := percentileUS([]int64{7}, 99); got != 7 {
		t.Errorf("percentileUS(single) = %d, want 7", got)
	}
	// Percentiles never exceed the observed maximum — the failure mode of an
	// off-by-one rank calculation.
	samples := []int64{1, 2, 3}
	for _, q := range []float64{50, 90, 99, 99.9, 100} {
		if got := percentileUS(samples, q); got > 3 {
			t.Errorf("percentileUS(%v) = %d, above max 3", q, got)
		}
	}
}

func TestSummarizeSortsAndReportsTrueMax(t *testing.T) {
	// Unsorted input, including a value far above cmd/bench's 60s histogram
	// ceiling: the slice-based summary reports it verbatim rather than clamping.
	const beyondHDRCeiling = int64(90 * time.Second / time.Microsecond)
	got := summarize([]int64{5, 1, 3, beyondHDRCeiling, 2})
	if got.Max != beyondHDRCeiling {
		t.Errorf("Max = %d, want %d (unclamped)", got.Max, beyondHDRCeiling)
	}
	if got.P50 != 3 {
		t.Errorf("P50 = %d, want 3", got.P50)
	}
}

func newTestReport(t *testing.T) *report {
	t.Helper()
	wl, err := newWorkload(100, "zipf", "20:80:0", 256)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	cfg := runConfig{qps: 1000, duration: 10 * time.Second, workers: 8, queueCap: 32, wl: wl}

	phase := &runResult{startedAt: time.Now().Add(-10 * time.Second), endedAt: time.Now()}
	for k := range phase.perOp {
		phase.perOp[k] = &opSamples{}
	}
	phase.perOp[opPut] = &opSamples{count: 2000, errors: 1, latenciesUS: []int64{100, 200, 300, 400}}
	phase.perOp[opGet] = &opSamples{count: 8000, errors: 0, latenciesUS: []int64{10, 20, 30, 40}}
	phase.ops = 10000
	phase.errors = 1
	phase.maxQueueDepth = 5

	return buildReport(cfg, phase, []string{"127.0.0.1:2379"}, 256, "20:80:0")
}

func TestBuildReportAggregates(t *testing.T) {
	rep := newTestReport(t)

	if rep.QPSAchieved < 990 || rep.QPSAchieved > 1010 {
		t.Errorf("QPSAchieved = %.1f, want ~1000", rep.QPSAchieved)
	}
	if rep.Ops != 10000 || rep.Errors != 1 {
		t.Errorf("ops/errors = %d/%d, want 10000/1", rep.Ops, rep.Errors)
	}
	// Overall percentiles come from the union of every op's samples.
	if rep.Overall.Max != 400 {
		t.Errorf("overall max = %d, want 400", rep.Overall.Max)
	}
	if rep.Overall.P50 != 40 {
		t.Errorf("overall p50 = %d, want 40 (median of 10,20,30,40,100,200,300,400)", rep.Overall.P50)
	}

	byKind := map[opKind]opReport{}
	for _, op := range rep.PerOp {
		byKind[op.Kind] = op
	}
	if len(rep.PerOp) != numOpKinds {
		t.Fatalf("PerOp has %d rows, want %d (one per op kind)", len(rep.PerOp), numOpKinds)
	}
	if got := byKind[opPut]; got.Ops != 2000 || got.Errors != 1 || got.Latency.P50 != 200 {
		t.Errorf("put row = %+v, want ops=2000 errors=1 p50=200", got)
	}
	if got := byKind[opGet]; got.Ops != 8000 || got.Latency.Max != 40 {
		t.Errorf("get row = %+v, want ops=8000 max=40", got)
	}
	// Per-op QPS must sum to the overall achieved QPS.
	var sum float64
	for _, op := range rep.PerOp {
		sum += op.QPSAchieved
	}
	if diff := sum - rep.QPSAchieved; diff > 0.01 || diff < -0.01 {
		t.Errorf("per-op QPS sums to %.2f, overall is %.2f", sum, rep.QPSAchieved)
	}
	if byKind[opDelete].Ops != 0 {
		t.Errorf("delete row should be empty for a 20:80:0 mix, got %d ops", byKind[opDelete].Ops)
	}
}

func TestBuildReportSaturationFlag(t *testing.T) {
	wl, err := newWorkload(100, "uniform", "100:0:0", 8)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	cfg := runConfig{qps: 10, duration: time.Second, workers: 8, queueCap: 32, wl: wl}

	mk := func(depth uint64) *report {
		phase := &runResult{startedAt: time.Now().Add(-time.Second), endedAt: time.Now(), maxQueueDepth: depth}
		for k := range phase.perOp {
			phase.perOp[k] = &opSamples{}
		}
		return buildReport(cfg, phase, []string{"e"}, 8, "100:0:0")
	}
	if mk(30).Saturated {
		t.Error("depth 30 of cap 32 should not be saturated")
	}
	// Same rule as cmd/bench: cap-1 counts as saturated.
	if !mk(31).Saturated {
		t.Error("depth 31 of cap 32 should be saturated")
	}
	if !mk(32).Saturated {
		t.Error("depth 32 of cap 32 should be saturated")
	}
}

func TestWriteTableContainsKeyFields(t *testing.T) {
	rep := newTestReport(t)
	var buf bytes.Buffer
	rep.writeTable(&buf)
	out := buf.String()

	for _, want := range []string{
		"etcd ceiling bench",
		"127.0.0.1:2379",
		"achieved_qps",
		"p50=",
		"p99=",
		"max=",
		"put",
		"get",
		"saturation",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
	// Empty op kinds are omitted rather than printed as rows of zeros.
	if strings.Contains(out, "delete   n=") {
		t.Errorf("delete row printed despite zero ops:\n%s", out)
	}
}

func TestEstimateSamples(t *testing.T) {
	// 1200 qps × 60s × 100% share ÷ 256 workers ≈ 281, +25% headroom +64.
	got := estimateSamples(1200, 60*time.Second, 256, 1.0)
	if got < 281 || got > 500 {
		t.Errorf("estimateSamples = %d, want roughly 350 (281 expected + headroom)", got)
	}
	// A zero-share op kind must not preallocate.
	if got := estimateSamples(1200, 60*time.Second, 256, 0); got != 0 {
		t.Errorf("estimateSamples(share=0) = %d, want 0", got)
	}
	// Degenerate inputs return 0 rather than panicking on a negative make cap.
	for _, tc := range []struct {
		name     string
		qps      float64
		duration time.Duration
		workers  int
	}{
		{name: "zero workers", qps: 100, duration: time.Second, workers: 0},
		{name: "negative workers", qps: 100, duration: time.Second, workers: -1},
		{name: "zero qps", qps: 0, duration: time.Second, workers: 8},
		{name: "zero duration", qps: 100, duration: 0, workers: 8},
	} {
		if got := estimateSamples(tc.qps, tc.duration, tc.workers, 1.0); got != 0 {
			t.Errorf("%s: estimateSamples = %d, want 0", tc.name, got)
		}
	}
}
