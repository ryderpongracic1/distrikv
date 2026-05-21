// cmd/chaos is a Jepsen-style distributed chaos tester for distrikv.
//
// It drives a running cluster with concurrent put/get/delete operations,
// records every call and return as a Porcupine event, and at the end of the
// run verifies that the observed history is linearizable.
//
// A non-linearizable result means the cluster exposed a consistency
// anomaly — e.g. a stale read, a lost write, or a reordered update — that
// would violate the CP guarantees distrikv is designed to provide.
//
// # Usage
//
//	docker compose -f docker/docker-compose.yml up -d
//	go run ./cmd/chaos \
//	  --target    localhost:8001 \
//	  --peers     localhost:8002,localhost:8003 \
//	  --duration  30s \
//	  --workers   8 \
//	  --keyspace  20 \
//	  --mix       60:40:0
//
// # Linearizability check
//
// The per-key PartitionEvent optimisation in the KVModel means Porcupine
// checks each key's sub-history independently.  For a 20-key, 30s,
// 8-worker run this typically completes in under 5 seconds.
//
// # Exit codes
//
//	0  linearizable (PASS)
//	1  NOT linearizable (FAIL)
//	2  check timed out (UNKNOWN — increase --check-timeout)
//	3  bad flags / startup error
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientpkg "github.com/ryderpongracic1/distrikv/internal/client"
	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

func main() {
	var (
		target       = flag.String("target", "localhost:8001", "primary cluster node host:port")
		peers        = flag.String("peers", "", "additional nodes, comma-separated (for future multi-target support)")
		duration     = flag.Duration("duration", 30*time.Second, "how long to run operations")
		warmup       = flag.Duration("warmup", 2*time.Second, "warmup window — ops issued but not recorded")
		workers      = flag.Int("workers", 8, "number of concurrent client goroutines")
		keyspace     = flag.Int("keyspace", 20, "number of distinct keys (small → richer contention per key)")
		putPct       = flag.Int("put", 50, "percentage of operations that are puts (0-100)")
		delPct       = flag.Int("delete", 5, "percentage of operations that are deletes (0-100)")
		checkTimeout = flag.Duration("check-timeout", 60*time.Second, "time limit for linearizability check")
		output       = flag.String("output", "table", "output format: table|json")
	)
	flag.Parse()

	if *putPct+*delPct > 100 {
		fmt.Fprintln(os.Stderr, "error: --put + --delete must be ≤ 100")
		os.Exit(3)
	}
	_ = *peers // reserved for future multi-target partition tests

	// Build an HTTP client with a connection pool sized to the worker count.
	transport := &http.Transport{
		MaxIdleConns:        *workers + 64,
		MaxIdleConnsPerHost: *workers + 64,
		MaxConnsPerHost:     *workers + 64,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: 5 * time.Second,
	}
	client := clientpkg.NewWithTransport(clientpkg.Config{Host: *target}, transport)

	// Pre-generate the key space.
	keys := make([]string, *keyspace)
	for i := range keys {
		keys[i] = fmt.Sprintf("chaos-%05d", i)
	}

	rec := &linearizability.Recorder{}
	var totalOps, totalErrors atomic.Int64

	// Warmup phase: issue ops but don't record them.
	log.Printf("warmup for %s …", *warmup)
	wctx, wcancel := context.WithTimeout(context.Background(), *warmup)
	runWorkers(wctx, *workers, keys, *putPct, *delPct, client, nil, &totalOps, &totalErrors)
	wcancel()
	totalOps.Store(0)
	totalErrors.Store(0)

	// Measurement phase: record all ops.
	log.Printf("running for %s with %d workers on %d-key space …", *duration, *workers, *keyspace)
	mctx, mcancel := context.WithTimeout(context.Background(), *duration)
	runWorkers(mctx, *workers, keys, *putPct, *delPct, client, rec, &totalOps, &totalErrors)
	mcancel()

	ops := totalOps.Load()
	errs := totalErrors.Load()
	log.Printf("ops=%d errors=%d events=%d; running linearizability check …", ops, errs, rec.Len())

	start := time.Now()
	ok, timedOut := rec.CheckTimeout(*checkTimeout)
	elapsed := time.Since(start)

	result := "PASS"
	exitCode := 0
	if timedOut {
		result = "UNKNOWN (timeout)"
		exitCode = 2
	} else if !ok {
		result = "FAIL"
		exitCode = 1
	}

	if *output == "json" {
		type report struct {
			Target        string        `json:"target"`
			Duration      string        `json:"duration"`
			Workers       int           `json:"workers"`
			Keyspace      int           `json:"keyspace"`
			TotalOps      int64         `json:"total_ops"`
			Errors        int64         `json:"errors"`
			Events        int           `json:"events"`
			Linearizable  bool          `json:"linearizable"`
			CheckTimedOut bool          `json:"check_timed_out"`
			CheckDuration string        `json:"check_duration"`
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report{
			Target:        *target,
			Duration:      duration.String(),
			Workers:       *workers,
			Keyspace:      *keyspace,
			TotalOps:      ops,
			Errors:        errs,
			Events:        rec.Len(),
			Linearizable:  ok && !timedOut,
			CheckTimedOut: timedOut,
			CheckDuration: elapsed.Round(time.Millisecond).String(),
		})
	} else {
		sep := strings.Repeat("─", 60)
		fmt.Printf("\n%s\n", sep)
		fmt.Printf("  distrikv chaos  %s  %s @ %d workers, %d-key space\n",
			result, *duration, *workers, *keyspace)
		fmt.Printf("%s\n", sep)
		fmt.Printf("  %-24s %d\n", "ops:", ops)
		fmt.Printf("  %-24s %d\n", "errors:", errs)
		fmt.Printf("  %-24s %d\n", "events:", rec.Len())
		fmt.Printf("  %-24s %s\n", "check_duration:", elapsed.Round(time.Millisecond))
		fmt.Printf("  %-24s %s\n", "linearizable:", result)
		fmt.Printf("%s\n\n", sep)
	}

	os.Exit(exitCode)
}

// runWorkers starts `n` goroutines that each issue operations until ctx is
// cancelled.  If rec is nil, operations are executed but not recorded
// (warmup mode).
func runWorkers(
	ctx context.Context,
	n int,
	keys []string,
	putPct, delPct int,
	client *clientpkg.Client,
	rec *linearizability.Recorder,
	totalOps, totalErrors *atomic.Int64,
) {
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1e9))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				key := keys[rng.Intn(len(keys))]
				roll := rng.Intn(100)

				totalOps.Add(1)

				switch {
				case roll < putPct:
					val := fmt.Sprintf("w%d-t%d", id, time.Now().UnixNano())
					var cid int
					if rec != nil {
						cid = rec.Begin(linearizability.Input{Op: "put", Key: key, Value: val})
					}
					err := client.Put(ctx, key, val)
					if rec != nil {
						out := linearizability.Output{}
						if err != nil && ctx.Err() == nil {
							out.Err = true
							totalErrors.Add(1)
						}
						rec.End(cid, out)
					} else if err != nil && ctx.Err() == nil {
						totalErrors.Add(1)
					}

				case roll < putPct+delPct:
					var cid int
					if rec != nil {
						cid = rec.Begin(linearizability.Input{Op: "delete", Key: key})
					}
					err := client.Delete(ctx, key)
					if rec != nil {
						out := linearizability.Output{}
						if err != nil && ctx.Err() == nil {
							out.Err = true
							totalErrors.Add(1)
						}
						rec.End(cid, out)
					} else if err != nil && ctx.Err() == nil {
						totalErrors.Add(1)
					}

				default: // get
					var cid int
					if rec != nil {
						cid = rec.Begin(linearizability.Input{Op: "get", Key: key})
					}
					val, err := client.Get(ctx, key)
					if rec != nil {
						out := linearizability.Output{}
						if isNotFound(err) {
							// absent key — Value stays ""
						} else if err != nil && ctx.Err() == nil {
							out.Err = true
							totalErrors.Add(1)
						} else if err == nil {
							out.Value = val
						}
						rec.End(cid, out)
					} else if err != nil && !isNotFound(err) && ctx.Err() == nil {
						totalErrors.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// isNotFound reports whether err represents a 404 / key-not-found response.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), "404")
}
