package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// report is the structured summary emitted at the end of a bench run.
type report struct {
	Target       string  `json:"target"`
	QPSRequested float64 `json:"qps_requested"`
	QPSAchieved  float64 `json:"qps_achieved"`
	Duration     string  `json:"duration"`
	Mix          string  `json:"mix"`
	Keyspace     int     `json:"keyspace"`
	KeyDist      string  `json:"keydist"`
	ValueSize    int     `json:"value_size"`
	Workers      int     `json:"workers"`

	Ops    uint64 `json:"ops"`
	Errors uint64 `json:"errors"`

	LatencyUS percentiles `json:"latency_us"`

	MaxQueueDepth uint64 `json:"max_queue_depth"`
	Saturated     bool   `json:"saturated"`

	// Engine-side, populated by /metrics polls (delta = end - start).
	EngineMetricsDelta map[string]int64 `json:"engine_metrics_delta"`
	WriteAmpFactor     float64          `json:"write_amp_factor"`
	BloomFPRate        float64          `json:"bloom_fp_rate"`

	// Prefill is nil unless --prefill ran. Recording it in the report is what
	// makes a read-path measurement auditable: the engine counters below only
	// mean something if the keyspace they were measured over was fully written.
	Prefill *prefillSummary `json:"prefill,omitempty"`
}

// prefillSummary is the report-facing view of a prefill phase.
type prefillSummary struct {
	KeysWritten uint64  `json:"keys_written"`
	Retries     uint64  `json:"retries"`
	Failed      uint64  `json:"failed"`
	Seconds     float64 `json:"seconds"`
	KeysPerSec  float64 `json:"keys_per_sec"`

	// EngineDelta is the target node's /metrics movement across the prefill
	// phase — where flush_bytes and compactions_total are earned. See
	// prefillResult.engineDelta.
	EngineDelta map[string]int64 `json:"engine_delta,omitempty"`
}

// metricsDelta subtracts two /metrics snapshots. Counters are monotonic within a
// process lifetime, but a node restarted mid-run resets them, so the result is
// signed rather than clamped: a negative value is evidence of a restart and
// should be visible rather than silently floored at zero.
func metricsDelta(start, end map[string]uint64) map[string]int64 {
	delta := make(map[string]int64, len(end))
	for k, v := range end {
		delta[k] = int64(v) - int64(start[k])
	}
	return delta
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
func buildReport(cfg runConfig, phase *runResult, startMetrics, endMetrics map[string]uint64, target string, valueSize int, mix string, pf *prefillResult) *report {
	wall := phase.endedAt.Sub(phase.startedAt).Seconds()
	if wall <= 0 {
		wall = 1
	}

	var pfSummary *prefillSummary
	if pf != nil {
		pfSummary = &prefillSummary{
			KeysWritten: pf.keysWritten,
			Retries:     pf.retries,
			Failed:      pf.failed,
			Seconds:     pf.elapsed.Seconds(),
			KeysPerSec:  pf.keysPerSec(),
			EngineDelta: pf.engineDelta,
		}
	}

	delta := metricsDelta(startMetrics, endMetrics)

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
		Prefill:            pfSummary,
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
	if r.Prefill != nil {
		fmt.Fprintf(w, "prefill:       %d keys in %.1fs (%.0f keys/s)  retries=%d  failed=%d\n",
			r.Prefill.KeysWritten, r.Prefill.Seconds, r.Prefill.KeysPerSec,
			r.Prefill.Retries, r.Prefill.Failed)
		if pd := r.Prefill.EngineDelta; pd != nil {
			// Write-path work belongs to the prefill, not the read window.
			fmt.Fprintf(w, "  during prefill:  flush_bytes=%s  compactions=%d  compaction_bytes_written=%s  write_stalls=%d\n",
				humanBytes(pd["flush_bytes"]), pd["compactions_total"],
				humanBytes(pd["compaction_bytes_written"]), pd["write_stall_count"])
		}
	}
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

	cacheHits := d["block_cache_hits"]
	cacheMisses := d["block_cache_misses"]
	cacheTotal := cacheHits + cacheMisses
	var cacheHitPct float64
	if cacheTotal > 0 {
		cacheHitPct = float64(cacheHits) / float64(cacheTotal) * 100
	}
	fmt.Fprintf(w, "               block_cache_hits=%d  block_cache_misses=%d  hit_rate=%.1f%%\n",
		cacheHits, cacheMisses, cacheHitPct)

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
