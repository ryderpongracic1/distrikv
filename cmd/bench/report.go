package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// report is the structured summary emitted at the end of a bench run.
type report struct {
	Target       string             `json:"target"`
	QPSRequested float64            `json:"qps_requested"`
	QPSAchieved  float64            `json:"qps_achieved"`
	Duration     string             `json:"duration"`
	Mix          string             `json:"mix"`
	Keyspace     int                `json:"keyspace"`
	KeyDist      string             `json:"keydist"`
	ValueSize    int                `json:"value_size"`
	Workers      int                `json:"workers"`

	Ops    uint64 `json:"ops"`
	Errors uint64 `json:"errors"`

	LatencyUS percentiles `json:"latency_us"`

	MaxQueueDepth uint64 `json:"max_queue_depth"`
	Saturated     bool   `json:"saturated"`

	// Engine-side, populated by /metrics polls (delta = end - start).
	EngineMetricsDelta map[string]int64 `json:"engine_metrics_delta"`
	WriteAmpFactor     float64          `json:"write_amp_factor"`
	BloomFPRate        float64          `json:"bloom_fp_rate"`
}

type percentiles struct {
	P50  int64 `json:"p50"`
	P90  int64 `json:"p90"`
	P99  int64 `json:"p99"`
	P999 int64 `json:"p999"`
	Max  int64 `json:"max"`
}

// buildReport assembles a report from one phase's runResult plus the engine
// metrics deltas captured by main.
func buildReport(cfg runConfig, phase *runResult, startMetrics, endMetrics map[string]uint64, target string, valueSize int, mix string) *report {
	wall := phase.endedAt.Sub(phase.startedAt).Seconds()
	if wall <= 0 {
		wall = 1
	}

	delta := make(map[string]int64, len(endMetrics))
	for k, v := range endMetrics {
		delta[k] = int64(v) - int64(startMetrics[k])
	}

	bloomHits := delta["bloom_hits"]
	bloomFP := delta["bloom_false_positives"]
	var fpRate float64
	if bloomHits > 0 {
		fpRate = float64(bloomFP) / float64(bloomHits)
	}

	// WAF from end-state (cumulative is the right number; it already factors in
	// the warmup, but the warmup share is small relative to a steady-state run).
	waf := float64(endMetrics["write_amp_factor_milli"]) / 1000.0

	return &report{
		Target:       target,
		QPSRequested: cfg.qps,
		QPSAchieved:  float64(phase.ops) / wall,
		Duration:     cfg.duration.String(),
		Mix:          mix,
		Keyspace:     cfg.wl.keyspace,
		KeyDist:      cfg.wl.keyDist,
		ValueSize:    valueSize,
		Workers:      cfg.workers,

		Ops:    phase.ops,
		Errors: phase.errors,

		LatencyUS: percentiles{
			P50:  phase.hist.ValueAtQuantile(50),
			P90:  phase.hist.ValueAtQuantile(90),
			P99:  phase.hist.ValueAtQuantile(99),
			P999: phase.hist.ValueAtQuantile(99.9),
			Max:  phase.hist.Max(),
		},

		MaxQueueDepth: phase.maxQueueDepth,
		Saturated:     int(phase.maxQueueDepth) >= cfg.queueCap-1,

		EngineMetricsDelta: delta,
		WriteAmpFactor:     waf,
		BloomFPRate:        fpRate,
	}
}

func (r *report) writeJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func (r *report) writeTable(w io.Writer) {
	fmt.Fprintf(w, "== distrikv bench: %s @ target %.0f qps, mix=%s, keyspace=%d %s ==\n",
		r.Duration, r.QPSRequested, r.Mix, r.Keyspace, r.KeyDist)
	fmt.Fprintf(w, "target:        %s\n", r.Target)
	fmt.Fprintf(w, "ops:           %-10d achieved_qps: %.1f    errors: %d\n",
		r.Ops, r.QPSAchieved, r.Errors)
	fmt.Fprintf(w, "latency (us):  p50=%-7d p90=%-7d p99=%-7d p999=%-7d max=%d\n",
		r.LatencyUS.P50, r.LatencyUS.P90, r.LatencyUS.P99, r.LatencyUS.P999, r.LatencyUS.Max)

	d := r.EngineMetricsDelta
	fmt.Fprintf(w, "engine-side:   bloom_hits=%d  bloom_misses=%d  bloom_fp_rate=%.2f%%\n",
		d["bloom_hits"], d["bloom_misses"], r.BloomFPRate*100)
	fmt.Fprintf(w, "               flush_bytes=%s  compaction_bytes_written=%s  WAF=%.2f\n",
		humanBytes(d["flush_bytes"]), humanBytes(d["compaction_bytes_written"]), r.WriteAmpFactor)
	fmt.Fprintf(w, "               compactions=%d  forwarded_requests=%d  replication_errors=%d\n",
		d["compactions_total"], d["forwarded_requests"], d["replication_errors"])

	satStr := "false"
	if r.Saturated {
		satStr = "TRUE (cluster could not keep up — tail latency reflects queue wait)"
	}
	fmt.Fprintf(w, "saturation:    %s   max_queue_depth=%d\n", satStr, r.MaxQueueDepth)
	fmt.Fprintf(w, "wallclock:     %s\n", time.Now().Format(time.RFC3339))
}

func humanBytes(n int64) string {
	switch {
	case n < 0:
		return fmt.Sprintf("%dB", n)
	case n < 1<<10:
		return fmt.Sprintf("%dB", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.2fGB", float64(n)/(1<<30))
	}
}
