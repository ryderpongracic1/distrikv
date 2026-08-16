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
// # How a failed write enters the history
//
// A write that returns an error is classified, not assumed — see
// classifyWriteEffect. distrikv answers 503 for a mutation the ring-primary
// applied to its own store and then failed to replicate, and it does not roll
// that back, so such a write is recorded as *applied*; a write the transport
// provably never delivered is recorded as a no-op; anything ambiguous is
// recorded as a pending operation the checker may place anywhere or nowhere.
// Recording every failure as a no-op — the runner's original behaviour — made
// every correct read of a refused value look like an anomaly, which is what the
// first real fault-injection runs reported.
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
	refused := measured.refusedWrites.Load()
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
			RefusedWrites       int64               `json:"refused_writes"`
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
			RefusedWrites:       refused,
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
		fmt.Printf("  %-24s %d\n", "refused-but-applied:", refused)
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
		for _, line := range verdictNotes(exitCode, refused, indeterminate) {
			if line == "" {
				fmt.Printf("%s\n", sep)
				continue
			}
			fmt.Printf("  %s\n", line)
		}
		fmt.Printf("%s\n\n", sep)
	}

	os.Exit(exitCode)
}

// verdictNotes returns the operator guidance printed under a non-PASS verdict.
// An empty string is a separator line.
//
// The old note told the reader to distrust a FAIL that coincided with a fault
// window, because every failed write was modelled as a no-op and a
// durable-but-unacknowledged one could therefore invent an anomaly. Failed
// writes are now classified instead of assumed — a refused write is modelled as
// applied, an unknown one as a pending operation the checker may place anywhere
// — so a FAIL no longer has that escape hatch, and the note says so.
func verdictNotes(exitCode int, refused, indeterminate int64) []string {
	switch exitCode {
	case 1:
		notes := []string{
			"",
			"NOTE: this FAIL is not explained by failed operations. Writes refused with",
			"      503 are modelled as applied (the primary keeps them; see \"CAP Position\"),",
			"      and writes with an unknown outcome are modelled as pending operations the",
			"      checker may linearize anywhere or not at all. Neither can invent an",
			"      anomaly, so treat this as a real consistency anomaly and correlate it",
			"      with the fault windows above to see which fault exposed it.",
		}
		if refused > 0 {
			notes = append(notes,
				fmt.Sprintf("      %d refused write(s) were modelled as applied: that encoding is exact only", refused),
				"      while reads are served by the ring-primary that kept them.")
		}
		return notes
	case 2:
		notes := []string{
			"",
			"NOTE: the check timed out, which is neither a PASS nor a FAIL. Raise",
			"      --check-timeout, or shorten the run.",
		}
		if indeterminate > 0 {
			notes = append(notes,
				fmt.Sprintf("      %d write(s) had an unknown outcome and are pending operations, each of", indeterminate),
				"      which overlaps every later operation on its key — the most likely reason",
				"      the search did not finish. A shorter run, or fewer ambiguous failures,",
				"      brings it back inside the budget.")
		}
		return notes
	default:
		return nil
	}
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
	// indeterminateWrites counts failed writes whose effect on the store is
	// unknown. They are recorded as pending operations, which the checker may
	// place anywhere, so they cannot cause a false anomaly — but they widen the
	// search space, so the count is worth reporting.
	indeterminateWrites atomic.Int64
	// refusedWrites counts failed writes that are known to have taken effect
	// anyway: distrikv answers 503 for a mutation the ring-primary applied and
	// could not replicate, and does not roll it back. See classifyWriteEffect.
	refusedWrites atomic.Int64
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
// How the operation enters the history is decided by classifyWriteEffect: a
// write known to have landed is recorded as applied, one that provably never
// left as a no-op, and one whose effect is unknown as a pending operation. Only
// failures that are not shutdown artefacts are counted in the reported
// statistics — but a shutdown-cancelled write is still an unknown outcome and is
// recorded as one, because the runner cancelling its own context says nothing
// about what the server did with the request.
func finishWrite(
	ctx context.Context,
	rec *linearizability.Recorder,
	cid int,
	err error,
	c *counters,
) {
	effect := classifyWriteEffect(err)

	if err != nil && ctx.Err() == nil {
		c.errors.Add(1)
		switch effect {
		case effectApplied:
			c.refusedWrites.Add(1)
		case effectUnknown:
			c.indeterminateWrites.Add(1)
		}
	}

	if rec == nil {
		return
	}
	switch {
	case err == nil:
		rec.End(cid, linearizability.Output{})
	case effect == effectApplied:
		// The error is positive evidence the mutation landed; recording it as a
		// no-op would make a later correct read of it an anomaly.
		rec.End(cid, linearizability.Output{Err: true, Applied: true})
	case effect == effectNotApplied:
		rec.End(cid, linearizability.Output{Err: true})
	default:
		rec.EndUnknown(cid)
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

// writeEffect is what the runner concluded a failed write did to the store. It
// decides how the operation enters the recorded history — see finishWrite.
type writeEffect int

const (
	// effectApplied: the mutation took effect. Either the call succeeded, or it
	// failed with positive evidence that the write landed anyway.
	effectApplied writeEffect = iota
	// effectNotApplied: the mutation provably did not take effect, so modelling
	// it as a no-op is exact rather than merely convenient.
	effectNotApplied
	// effectUnknown: the mutation may or may not have taken effect. Neither
	// no-op nor applied can be asserted; the operation is left pending.
	effectUnknown
)

func (e writeEffect) String() string {
	switch e {
	case effectApplied:
		return "applied"
	case effectNotApplied:
		return "not applied"
	case effectUnknown:
		return "unknown"
	}
	return fmt.Sprintf("writeEffect(%d)", int(e))
}

// classifyWriteEffect decides which of the three outcomes a write error carries.
//
// The status code decides first, and it decides from the chain: internal/client
// returns *StatusError for a 5xx, so the code is matched with errors.As rather
// than read out of the message.
//
//	503  the ring-primary applied the mutation to its own store and then failed
//	     to replicate it. distrikv does not roll that back (see ErrReplication
//	     in internal/server and the README's "CAP Position"), and reads are
//	     served by the primary, so the write is present and readable. Applied.
//	     Both client entry points report this identically: a forwarded write
//	     returns the primary's status verbatim through ForwardKey.
//	502  the write failed on its way to the primary, in forwardRequest. Two
//	     causes hide behind one code: a request that provably never reached the
//	     primary (never applied), and a ForwardKey RPC that failed in a way that
//	     may have been applied before the response was lost. The server separates
//	     them and says which in the body's forward_outcome field — see
//	     classifyForwardOutcome.
//	5xx  anything else: 500 covers both a ring-lookup failure (never applied)
//	     and a local store error (possibly durable already). Unknown.
//
// Only when there is no status code at all — the request never got a response —
// does the transport classification run.
//
// Ordering matters, and not only for tidiness. A 503's body carries the
// replication error underneath it, which during an outage reads "…connect:
// connection refused" from the fan-out to the dead replica. Classifying on the
// message text first therefore declared refused-but-applied writes "provably
// never sent" — the reason a stop-restart run reported 0 indeterminate writes
// while failing.
func classifyWriteEffect(err error) writeEffect {
	if err == nil || isNotFound(err) {
		return effectApplied
	}

	var se *clientpkg.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusServiceUnavailable:
			return effectApplied
		case http.StatusBadGateway:
			return classifyForwardOutcome(se.Body)
		default:
			return effectUnknown
		}
	}

	if provablyNeverSent(err) {
		return effectNotApplied
	}
	// Everything left is unknown, deliberately: a timeout, a reset mid-request,
	// a cancelled context, an error shape this runner has never seen. Guessing
	// "no-op" here is what produced false anomalies; leaving the operation
	// pending asserts nothing.
	return effectUnknown
}

// forwardOutcome values the server emits in a 502 body. They mirror the
// constants in internal/server; this is the wire contract between the two.
const (
	forwardOutcomeNeverSent = "never-sent"
	forwardOutcomeUnknown   = "unknown"
)

// forwardErrorBody is the 502 body shape written by internal/server's
// writeForwardError. ForwardOutcome is a pointer so an *absent* field — an older
// server that does not emit it — is distinguishable from one present with a value
// this runner does not recognise. The two are treated differently: see
// classifyForwardOutcome.
type forwardErrorBody struct {
	Error          string  `json:"error"`
	ForwardOutcome *string `json:"forward_outcome"`
}

// classifyForwardOutcome decides what a 502 says about the store, reading the
// server's typed verdict in preference to its prose.
//
// The server is the only party that can answer this. A gRPC RPC error is a
// *status.Error carrying a code and a string and nothing else — it does not wrap
// the transport failure underneath, so there is no chain to inspect even on the
// server's side, let alone after the message has crossed two process boundaries
// as text. What the server does have is the code that framed the message, which
// is what separates a connection that was never established from a stream that
// broke after the request went out. It makes that call and sends the answer;
// this function just reads it. See classifyForwardError in internal/server.
//
// Three inputs, three answers, and the difference between the last two matters:
//
//   - field present and recognised → its verdict, trusted. The server made it
//     with strictly more evidence than exists here.
//   - field present but unrecognised → unknown, and the text is *not* consulted.
//     A server that speaks this field is authoritative; falling back to a weaker
//     signal that might contradict it would be worse than declining to answer.
//   - field absent → the text scan, for a server predating the field. See
//     neverSentText.
//
// Every path that is not a recognised never-sent ends in effectUnknown, so the
// bounded-safe property holds: an unparseable, truncated, or unexpected body
// leaves the operation pending rather than asserting a no-op.
func classifyForwardOutcome(body string) writeEffect {
	var parsed forwardErrorBody
	if err := json.Unmarshal([]byte(body), &parsed); err == nil && parsed.ForwardOutcome != nil {
		switch *parsed.ForwardOutcome {
		case forwardOutcomeNeverSent:
			return effectNotApplied
		default:
			// Includes forwardOutcomeUnknown and anything unrecognised.
			return effectUnknown
		}
	}

	if neverSentText(body) {
		return effectNotApplied
	}
	return effectUnknown
}

// neverSentMarkers are transport failures that mean nothing was delivered: a
// refused connection got no SYN-ACK, an unresolvable or unroutable host got no
// packets at all. A connection that died *after* the request went out reads
// differently ("EOF", "connection reset by peer", "transport is closing") and is
// deliberately absent from this list.
var neverSentMarkers = []string{
	"connection refused",
	"no such host",
	"no route to host",
	"network is unreachable",
}

// neverSentText reports whether an error message describes a transport failure
// that delivered nothing.
//
// This is the text path. It is now a **compatibility fallback**, reached only for
// a 502 body with no forward_outcome field — a server older than that field.
// Against a current server the typed field always decides.
//
// It was load-bearing until the server learned to classify, and its limits are
// worth recording, because they are what motivated moving the decision upstream.
// Two of the four markers above could never fire on this path: gRPC reports an
// unresolvable target as `name resolver error: produced zero addresses`, not "no
// such host", and an unroutable address as a plain DeadlineExceeded rather than
// anything naming a route. Reading a failure this far downstream means matching
// wording chosen by a library two hops away for an audience of humans.
//
// It stays bounded in the safe direction either way: an unrecognised body is
// unknown, so a rewording costs checker time, never a wrong verdict.
func neverSentText(msg string) bool {
	for _, s := range neverSentMarkers {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// provablyNeverSent reports whether err means the request never reached a
// server, so treating it as a no-op is exact rather than merely conservative.
//
// It is only consulted for an error with no HTTP status in it — a request that
// never got a response at all. classifyWriteEffect handles status-carrying
// errors before this runs, which is what keeps a 503 (applied) from being read
// as "never sent" because its body quotes a refused replica connection.
//
// The typed checks are the contract, and they cover the errors this runner
// actually produces: internal/client wraps a dial failure as
// fmt.Errorf("%w: %w", ErrUnreachable, urlErr.Err), so the *net.OpError and the
// refusing syscall.Errno underneath stay reachable through errors.Is and
// errors.As. That is asserted end to end against a real refused dial in
// TestProvablyNeverSentClassifiesRealClientErrors.
//
// The substring checks behind them are a last resort for an error that arrives
// as text with no identity left to match — one that crossed a process boundary,
// or came from a source outside this repo. They are deliberately kept because
// misclassifying a never-sent write as unknown only costs checker time, whereas
// the reverse would model a genuinely unknown outcome as a no-op. They are no
// longer load-bearing for client errors.
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

	// Fallback: the message text, for errors that reach us with no chain.
	return neverSentText(err.Error())
}

// isNotFound reports whether err represents a 404 / key-not-found response.
func isNotFound(err error) bool {
	return errors.Is(err, clientpkg.ErrNotFound)
}
