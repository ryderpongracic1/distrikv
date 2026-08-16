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
// # Fault injection (nemesis)
//
// With --nemesis the runner kills and restarts cluster nodes during the
// measurement phase, so the history is checked against a cluster that is
// actually losing and regaining members:
//
//	go run ./cmd/chaos \
//	  --target            localhost:8001 \
//	  --duration          60s \
//	  --nemesis           kill-restart \
//	  --nemesis-services  node2,node3 \
//	  --nemesis-interval  10s \
//	  --nemesis-downtime  5s
//
// The default is --nemesis=none, which injects nothing — existing invocations
// behave exactly as before. Every outage is recorded as a fault window and
// printed in the final report so a failure can be correlated with the fault
// that produced it. The nemesis never runs during warmup, and any victim it
// takes down is healed before the process exits.
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
//	3  bad flags / startup error (including nemesis preflight failure)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

		nemesisMode     = flag.String("nemesis", nemesisNone, "fault injection during measurement: none|kill-restart|stop-restart")
		nemesisServices = flag.String("nemesis-services", "", "comma-separated compose service names to draw victims from (required unless --nemesis=none)")
		nemesisInterval = flag.Duration("nemesis-interval", 10*time.Second, "delay between the end of one outage and the start of the next")
		nemesisDowntime = flag.Duration("nemesis-downtime", 5*time.Second, "how long a victim stays down")
		nemesisCompose  = flag.String("nemesis-compose-file", "docker/docker-compose.yml", "compose file the nemesis operates on")
	)
	flag.Parse()

	if *putPct+*delPct > 100 {
		fmt.Fprintln(os.Stderr, "error: --put + --delete must be ≤ 100")
		os.Exit(3)
	}
	_ = *peers // reserved for future multi-target partition tests

	ncfg, err := parseNemesisFlags(*nemesisMode, *nemesisServices, *nemesisCompose, *nemesisInterval, *nemesisDowntime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(3)
	}

	// Build the nemesis and prove it works before any load is issued: a nemesis
	// that cannot reach docker would otherwise turn the run into a no-fault run
	// that passes for the wrong reason.
	var scheduler *Scheduler
	if ncfg.Enabled() {
		nem, err := newComposeNemesis(ncfg.Mode, ncfg.ComposeFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(3)
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 2*nemesisCommandTimeout)
		preflightErr := nem.Preflight(pctx, ncfg.Services)
		pcancel()
		if preflightErr != nil {
			fmt.Fprintf(os.Stderr, "error: nemesis preflight failed: %v\n", preflightErr)
			os.Exit(3)
		}
		scheduler = &Scheduler{
			Nemesis:  nem,
			Victims:  ncfg.Services,
			Interval: ncfg.Interval,
			Downtime: ncfg.Downtime,
			Logf:     log.Printf,
		}
	}

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
	//
	// Keys carry a per-run nonce, and warmup uses a disjoint set from
	// measurement. KVModel.Init is an empty map, so the recorded history must
	// genuinely start against an empty keyspace: a key that already holds a
	// value — written by the unrecorded warmup phase, or by an earlier run
	// against the same persistent volume — makes the first measured read of it
	// unexplainable and the history spuriously illegal. The cost is that each
	// run leaves its keys behind in the store.
	runID := time.Now().UnixNano()
	warmKeys := makeKeys(fmt.Sprintf("chaos-w%d", runID), *keyspace)
	keys := makeKeys(fmt.Sprintf("chaos-m%d", runID), *keyspace)

	rec := &linearizability.Recorder{}

	// Warmup phase: issue ops but don't record them, and never inject faults.
	// Warmup fills connection pools; because its keyspace is disjoint from the
	// measured one, it deliberately does *not* warm per-key block-cache or
	// bloom-filter state — a trustworthy verdict is worth a cold first read.
	log.Printf("warmup for %s …", *warmup)
	warm := &counters{}
	wctx, wcancel := context.WithTimeout(context.Background(), *warmup)
	runWorkers(wctx, *workers, warmKeys, *putPct, *delPct, client, nil, warm)
	wcancel()
	log.Printf("warmup done: %d ops, %d errors (not recorded)", warm.ops.Load(), warm.errors.Load())

	// Measurement phase: record all ops, and let the nemesis strike.
	//
	// The phase context also cancels on SIGINT/SIGTERM. Now that the runner can
	// take nodes down, an interrupted run must still unwind: cancelling here
	// stops the workers, runs the scheduler's heal, and prints the report,
	// rather than leaving a container dead on the operator's machine.
	log.Printf("running for %s with %d workers on %d-key space (nemesis: %s) …",
		*duration, *workers, *keyspace, ncfg.Describe())
	measured := &counters{}
	measureStart := time.Now()
	sigctx, sigstop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer sigstop()
	mctx, mcancel := context.WithTimeout(sigctx, *duration)

	var nemesisDone chan struct{}
	if scheduler != nil {
		nemesisDone = make(chan struct{})
		go func() {
			defer close(nemesisDone)
			scheduler.Run(mctx)
		}()
	}

	runWorkers(mctx, *workers, keys, *putPct, *delPct, client, rec, measured)
	interrupted := sigctx.Err() != nil
	measuredFor := time.Since(measureStart)
	mcancel()
	if interrupted && scheduler != nil {
		log.Printf("interrupt received — healing victims before exit …")
	}
	if nemesisDone != nil {
		// Blocks until the scheduler's deferred heal has completed, so the
		// process never exits with a node still down.
		<-nemesisDone
	}
	// Release the signal handler before the linearizability check so a second
	// Ctrl-C during a long check terminates the process normally.
	sigstop()
	if interrupted {
		log.Printf("interrupted after %s — checking the truncated history",
			measuredFor.Round(time.Millisecond))
	}

	var windows []FaultWindow
	if scheduler != nil {
		windows = scheduler.Windows()
	}

	ops := measured.ops.Load()
	errs := measured.errors.Load()
	indeterminate := measured.indeterminateWrites.Load()
	injected := countInjected(windows)
	log.Printf("ops=%d errors=%d events=%d faults=%d/%d; running linearizability check …",
		ops, errs, rec.Len(), injected, len(windows))

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
			Target              string              `json:"target"`
			Duration            string              `json:"duration"`
			MeasuredDuration    string              `json:"measured_duration"`
			Interrupted         bool                `json:"interrupted"`
			Workers             int                 `json:"workers"`
			Keyspace            int                 `json:"keyspace"`
			TotalOps            int64               `json:"total_ops"`
			Errors              int64               `json:"errors"`
			IndeterminateWrites int64               `json:"indeterminate_writes"`
			Events              int                 `json:"events"`
			Linearizable        bool                `json:"linearizable"`
			CheckTimedOut       bool                `json:"check_timed_out"`
			CheckDuration       string              `json:"check_duration"`
			Nemesis             string              `json:"nemesis"`
			NemesisServices     []string            `json:"nemesis_services,omitempty"`
			FaultsInjected      int                 `json:"faults_injected"`
			FaultsAttempted     int                 `json:"faults_attempted"`
			FaultWindows        []faultWindowReport `json:"fault_windows,omitempty"`
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report{
			Target:              *target,
			Duration:            duration.String(),
			MeasuredDuration:    measuredFor.Round(time.Millisecond).String(),
			Interrupted:         interrupted,
			Workers:             *workers,
			Keyspace:            *keyspace,
			TotalOps:            ops,
			Errors:              errs,
			IndeterminateWrites: indeterminate,
			Events:              rec.Len(),
			Linearizable:        ok && !timedOut,
			CheckTimedOut:       timedOut,
			CheckDuration:       elapsed.Round(time.Millisecond).String(),
			Nemesis:             ncfg.Describe(),
			NemesisServices:     ncfg.Services,
			FaultsInjected:      injected,
			FaultsAttempted:     len(windows),
			FaultWindows:        faultWindowReports(windows, measureStart),
		})
	} else {
		sep := strings.Repeat("─", 60)
		fmt.Printf("\n%s\n", sep)
		fmt.Printf("  distrikv chaos  %s  %s @ %d workers, %d-key space\n",
			result, measuredFor.Round(time.Millisecond), *workers, *keyspace)
		fmt.Printf("%s\n", sep)
		if interrupted {
			fmt.Printf("  %-24s yes — verdict covers a truncated %s of the requested %s\n",
				"interrupted:", measuredFor.Round(time.Millisecond), *duration)
		}
		fmt.Printf("  %-24s %d\n", "ops:", ops)
		fmt.Printf("  %-24s %d\n", "errors:", errs)
		fmt.Printf("  %-24s %d\n", "indeterminate writes:", indeterminate)
		fmt.Printf("  %-24s %d\n", "events:", rec.Len())
		fmt.Printf("  %-24s %s\n", "nemesis:", ncfg.Describe())
		fmt.Printf("  %-24s %d of %d attempted\n", "faults injected:", injected, len(windows))
		fmt.Printf("  %-24s %s\n", "check_duration:", elapsed.Round(time.Millisecond))
		fmt.Printf("  %-24s %s\n", "linearizable:", result)
		if len(windows) > 0 {
			fmt.Printf("%s\n", sep)
			fmt.Printf("  fault windows (offsets from measurement start):\n")
			for _, line := range formatFaultWindows(windows, measureStart) {
				fmt.Printf("    %s\n", line)
			}
		}
		if exitCode == 1 && indeterminate > 0 {
			fmt.Printf("%s\n", sep)
			fmt.Printf("  NOTE: %d write(s) failed with an unknown outcome. The KV model records a\n", indeterminate)
			fmt.Printf("        failed write as a no-op, so a write that reached disk before its\n")
			fmt.Printf("        connection died can make a later correct read look non-linearizable.\n")
			fmt.Printf("        Check whether the reported anomaly falls inside a fault window above\n")
			fmt.Printf("        before treating it as a real consistency bug.\n")
		}
		fmt.Printf("%s\n\n", sep)
	}

	os.Exit(exitCode)
}

// countInjected returns the number of windows whose Disrupt actually landed.
// A failed strike still opens a window — the failure is worth reporting — but it
// is not an outage, and counting it would misattribute anomalies to a phantom
// fault.
func countInjected(windows []FaultWindow) int {
	n := 0
	for _, w := range windows {
		if w.Injected() {
			n++
		}
	}
	return n
}

// faultWindowReport is the JSON shape of one recorded outage.
//
// A window that never healed reports null for up_at, up_at_offset_ms and
// down_ms together — one consistent encoding of "the victim did not come back",
// rather than a 0 that would read as an instantaneous outage.
type faultWindowReport struct {
	Victim         string  `json:"victim"`
	Injected       bool    `json:"injected"`
	DownAt         string  `json:"down_at"`
	DownAtOffsetMs int64   `json:"down_at_offset_ms"`
	Healed         bool    `json:"healed"`
	UpAt           *string `json:"up_at"`
	UpAtOffsetMs   *int64  `json:"up_at_offset_ms"`
	DownMs         *int64  `json:"down_ms"`
	DisruptError   string  `json:"disrupt_error,omitempty"`
	HealError      string  `json:"heal_error,omitempty"`
}

// faultWindowReports converts recorded windows to their JSON shape, with
// offsets relative to the start of the measurement phase.
func faultWindowReports(windows []FaultWindow, start time.Time) []faultWindowReport {
	if len(windows) == 0 {
		return nil
	}
	out := make([]faultWindowReport, 0, len(windows))
	for _, w := range windows {
		r := faultWindowReport{
			Victim:         w.Victim,
			Injected:       w.Injected(),
			DownAt:         w.DownAt.Format(time.RFC3339Nano),
			DownAtOffsetMs: w.DownAt.Sub(start).Milliseconds(),
			Healed:         w.Healed(),
			DisruptError:   w.DisruptErr,
			HealError:      w.HealErr,
		}
		if !w.UpAt.IsZero() {
			upAt := w.UpAt.Format(time.RFC3339Nano)
			upOffset := w.UpAt.Sub(start).Milliseconds()
			downMs := w.Down().Milliseconds()
			r.UpAt = &upAt
			r.UpAtOffsetMs = &upOffset
			r.DownMs = &downMs
		}
		out = append(out, r)
	}
	return out
}

// formatFaultWindows renders one line per outage for the table report.
func formatFaultWindows(windows []FaultWindow, start time.Time) []string {
	lines := make([]string, 0, len(windows))
	for i, w := range windows {
		offset := func(t time.Time) string {
			return fmt.Sprintf("+%s", t.Sub(start).Round(100*time.Millisecond))
		}
		up := "NEVER HEALED"
		if !w.UpAt.IsZero() {
			up = offset(w.UpAt)
		}
		line := fmt.Sprintf("#%-3d %-12s down %-8s up %-8s (%s)",
			i+1, w.Victim, offset(w.DownAt), up, w.Down().Round(100*time.Millisecond))
		if w.DisruptErr != "" {
			line += fmt.Sprintf("  disrupt error: %s", w.DisruptErr)
		}
		if w.HealErr != "" {
			line += fmt.Sprintf("  heal error: %s", w.HealErr)
		}
		lines = append(lines, line)
	}
	return lines
}

// makeKeys generates a keyspace of n keys under the given prefix.
func makeKeys(prefix string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s-%05d", prefix, i)
	}
	return keys
}

// counters accumulate per-phase operation statistics across all workers.
type counters struct {
	ops    atomic.Int64
	errors atomic.Int64
	// indeterminateWrites counts failed writes whose outcome is unknown — the
	// request may have been applied before the connection died. See
	// writeIsIndeterminate.
	indeterminateWrites atomic.Int64
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
	c *counters,
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

				c.ops.Add(1)

				switch {
				case roll < putPct:
					val := fmt.Sprintf("w%d-t%d", id, time.Now().UnixNano())
					var cid int
					if rec != nil {
						cid = rec.Begin(linearizability.Input{Op: "put", Key: key, Value: val})
					}
					err := client.Put(ctx, key, val)
					finishWrite(ctx, rec, cid, err, c)

				case roll < putPct+delPct:
					var cid int
					if rec != nil {
						cid = rec.Begin(linearizability.Input{Op: "delete", Key: key})
					}
					err := classifyDeleteErr(client.Delete(ctx, key))
					finishWrite(ctx, rec, cid, err, c)

				default: // get
					var cid int
					if rec != nil {
						cid = rec.Begin(linearizability.Input{Op: "get", Key: key})
					}
					val, err := client.Get(ctx, key)
					if rec != nil {
						out := linearizability.Output{}
						switch {
						case isNotFound(err):
							// absent key — Value stays ""
						case err != nil:
							// Includes reads cut short by the run's own
							// shutdown. Recording Err is what makes them
							// unconstrained; recording a successful read of an
							// absent key would assert something false.
							out.Err = true
						default:
							out.Value = val
						}
						rec.End(cid, out)
					}
					if err != nil && !isNotFound(err) && ctx.Err() == nil {
						c.errors.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// finishWrite records the outcome of a put or delete.
//
// Any error is recorded as Output.Err, which the KV model treats as a no-op —
// including an error caused by the run's own shutdown, because recording a
// failed write as a success would assert it was applied. Only failures that are
// not shutdown artefacts are counted in the reported statistics.
func finishWrite(
	ctx context.Context,
	rec *linearizability.Recorder,
	cid int,
	err error,
	c *counters,
) {
	if err != nil && ctx.Err() == nil {
		c.errors.Add(1)
		if writeIsIndeterminate(err) {
			c.indeterminateWrites.Add(1)
		}
	}
	if rec != nil {
		rec.End(cid, linearizability.Output{Err: err != nil})
	}
}

// classifyDeleteErr maps a delete outcome to what should be recorded.
//
// The server answers 404 when the key was already absent. That is not a failure
// and not an unknown outcome: it leaves the store in exactly the state a
// successful delete would, and the 404 is positive evidence the key was absent
// at some instant inside the call, so recording the delete as applied is sound
// and uses more of the available evidence than recording it as an error.
//
// This is sound only because the server emits 404 on DELETE exclusively for an
// absent key (see handleDelete in internal/server). If a 404 ever comes to mean
// something else — a route miss, "not the owner of this key" — this assertion
// stops being valid and must change with it.
func classifyDeleteErr(err error) error {
	if isNotFound(err) {
		return nil
	}
	return err
}

// writeIsIndeterminate reports whether a failed write may still have taken
// effect on the server.
//
// The KV model records a failed operation as a no-op (state unchanged). That is
// sound when the request provably never reached a server — a refused
// connection, an unresolvable host — which is what a client sees while a node
// is down. It is *not* sound when the outcome is unknown: a write that was
// appended and fsynced just before its node was SIGKILLed really did take
// effect, and after WAL recovery a later read will see it. Modelling that write
// as a no-op can make a correct read look like a linearizability violation.
//
// These writes are therefore counted separately, so a FAIL that coincides with
// a fault window can be judged rather than trusted blindly.
func writeIsIndeterminate(err error) bool {
	if err == nil || isNotFound(err) {
		return false
	}
	return !provablyNeverSent(err)
}

// provablyNeverSent reports whether err means the request never reached a
// server, so treating it as a no-op is exact rather than merely conservative.
//
// The typed checks are the real contract. The substring checks behind them are a
// necessary fallback, not belt-and-braces: internal/client formats a dial
// failure as fmt.Errorf("%w: %v", ErrUnreachable, urlErr.Err), which stringifies
// the cause and breaks the error chain, so errors.Is cannot see the underlying
// syscall error through it today. Repairing that wrapping lives in
// internal/client; until it happens the substrings carry these cases, and they
// are what the tests exercise for client-produced errors.
func provablyNeverSent(err error) bool {
	// Typed: exact, and works for any error that preserves its chain.
	for _, errno := range []syscall.Errno{
		syscall.ECONNREFUSED,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}

	// Fallback: the message text, for errors whose chain was flattened.
	msg := err.Error()
	for _, s := range []string{
		"connection refused",
		"no such host",
		"no route to host",
		"network is unreachable",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// isNotFound reports whether err represents a 404 / key-not-found response.
func isNotFound(err error) bool {
	return errors.Is(err, clientpkg.ErrNotFound)
}
