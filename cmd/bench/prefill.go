package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

// Prefill exists because the LSM read path is unreachable without it.
//
// A node's memtable accounts for *live* entries only — Memtable.Put subtracts
// the replaced entry's bytes before adding the new ones — so a workload that
// rewrites a small hot set never grows the memtable past its flush threshold no
// matter how many ops it issues. That is why every cluster bench run before this
// one reported bloom_hits=0, block_cache_hits=0 and compactions=0: with no
// flush there is no SSTable, and with no SSTable there is no bloom filter,
// block cache or compaction to measure. The counters were correct; the workload
// simply never left memory.
//
// Writing each distinct key once forces resident bytes to grow monotonically,
// which crosses the flush threshold, accumulates L0 files, and triggers
// compaction — after which reads must traverse SSTables and the read-path
// counters become meaningful.
//
// Existing flags can approximate this (--mix 100:0:0 --keydist sequential) but
// cannot guarantee it: coverage of the keyspace becomes a function of
// --qps × --duration, arrivals are Poisson-scheduled rather than exhaustive,
// and a write that fails leaves a hole. A hit rate measured over a keyspace
// that was only partly written is not a hit rate, so prefill is exhaustive,
// closed-loop and retried rather than rate-controlled.
const (
	// prefillMaxAttempts bounds retries for a single key. A prefill deliberately
	// pushes the engine into write-stall backpressure (503, "alive, overloaded,
	// retry"), so a first-attempt failure is expected rather than exceptional.
	prefillMaxAttempts = 10

	// prefillBaseBackoff is the first retry delay; it doubles per attempt up to
	// prefillMaxBackoff. Ten attempts therefore span ~5s, long enough for one
	// compaction pass to drain an L0 backlog.
	prefillBaseBackoff = 50 * time.Millisecond
	prefillMaxBackoff  = 1 * time.Second

	// prefillOpTimeout bounds one Put attempt.
	prefillOpTimeout = 10 * time.Second
)

// prefillConfig describes an exhaustive write of the whole keyspace.
type prefillConfig struct {
	keyspace int
	value    []byte
	workers  int
	clients  []*client.Client
	// progress receives human-readable progress lines. May be nil.
	progress io.Writer
}

// prefillResult reports what the prefill actually accomplished. failed > 0
// means the keyspace has holes and any hit rate measured over it is invalid.
type prefillResult struct {
	keysWritten uint64
	retries     uint64
	failed      uint64
	elapsed     time.Duration
	// firstErr is the first unrecoverable error observed, for diagnostics.
	firstErr error

	// engineDelta holds the target node's /metrics movement across the prefill
	// phase, set by the caller. It carries the write-path evidence — flush_bytes
	// and compactions_total — which a read-only measurement window cannot show:
	// once every key exists, further writes are overwrites, and the memtable's
	// live-entry accounting means an overwrite of equal size does not grow it,
	// so no further flush is due. The flushes and compactions that put the data
	// on disk therefore all belong to this phase.
	engineDelta map[string]int64
}

// keysPerSec reports achieved prefill throughput.
func (r *prefillResult) keysPerSec() float64 {
	if r.elapsed <= 0 {
		return 0
	}
	return float64(r.keysWritten) / r.elapsed.Seconds()
}

// runPrefill writes every key in [0, keyspace) exactly once and returns when
// all of them have been written or ctx is cancelled.
//
// It is closed-loop on purpose: the goal is coverage, not a controlled arrival
// rate, so workers claim the next index as soon as they finish the previous one
// and the phase runs at whatever rate the cluster sustains. No latency is
// recorded — prefill is setup, not measurement.
func runPrefill(ctx context.Context, cfg prefillConfig) *prefillResult {
	res := &prefillResult{}
	started := time.Now()

	var (
		nextIdx  atomic.Uint64 // next key index to claim
		written  atomic.Uint64
		retries  atomic.Uint64
		failed   atomic.Uint64
		errOnce  sync.Once
		firstErr error
	)

	// Report progress on decile boundaries so a multi-minute prefill is not
	// silent, without emitting a line per key.
	progressStep := uint64(cfg.keyspace) / 10
	if progressStep == 0 {
		progressStep = 1
	}
	var nextReport atomic.Uint64
	nextReport.Store(progressStep)

	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := cfg.clients[idx%len(cfg.clients)]
			for {
				n := nextIdx.Add(1) - 1
				if n >= uint64(cfg.keyspace) {
					return
				}
				if ctx.Err() != nil {
					return
				}

				attemptRetries, err := prefillOne(ctx, c, keyForIndex(n), cfg.value)
				retries.Add(attemptRetries)
				if err != nil {
					failed.Add(1)
					errOnce.Do(func() { firstErr = err })
					continue
				}

				done := written.Add(1)
				if cfg.progress != nil && done >= nextReport.Load() {
					// Advance the threshold first so concurrent workers crossing
					// the same boundary do not each print a line.
					mark := nextReport.Load()
					if nextReport.CompareAndSwap(mark, mark+progressStep) {
						fmt.Fprintf(cfg.progress, "bench: prefill %d/%d keys (%.0f%%) ...\n",
							done, cfg.keyspace, float64(done)/float64(cfg.keyspace)*100)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	res.keysWritten = written.Load()
	res.retries = retries.Load()
	res.failed = failed.Load()
	res.firstErr = firstErr
	res.elapsed = time.Since(started)
	return res
}

// prefillOne writes a single key, retrying transient failures with exponential
// backoff. It returns the number of retries consumed and the final error.
func prefillOne(ctx context.Context, c *client.Client, key string, value []byte) (uint64, error) {
	var retried uint64
	backoff := prefillBaseBackoff

	for attempt := 1; attempt <= prefillMaxAttempts; attempt++ {
		opCtx, cancel := context.WithTimeout(ctx, prefillOpTimeout)
		err := c.Put(opCtx, key, string(value))
		cancel()
		if err == nil {
			return retried, nil
		}
		// The caller's context going away is not the key's fault; stop retrying.
		if ctx.Err() != nil {
			return retried, fmt.Errorf("prefill %q: %w", key, errors.Join(err, ctx.Err()))
		}
		if attempt == prefillMaxAttempts {
			return retried, fmt.Errorf("prefill %q: %d attempts exhausted: %w", key, prefillMaxAttempts, err)
		}

		retried++
		select {
		case <-ctx.Done():
			return retried, fmt.Errorf("prefill %q: %w", key, ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < prefillMaxBackoff {
			backoff *= 2
			if backoff > prefillMaxBackoff {
				backoff = prefillMaxBackoff
			}
		}
	}
	// Unreachable: the loop returns on the final attempt.
	return retried, fmt.Errorf("prefill %q: no attempts made", key)
}
