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
// # leader-kill
//
// --nemesis=leader-kill resolves the current Raft leader over /status before
// each fault window and stops that node, forcing a re-election while writes are
// still flowing. It is the only mode that produces a leaderless window under
// load — every other gate takes down a follower — which makes it the
// discriminating case for the planned removal of the transport probe:
//
//	go run ./cmd/chaos \
//	  --target            localhost:8001 \
//	  --peers             localhost:8002,localhost:8003 \
//	  --duration          60s \
//	  --nemesis           leader-kill \
//	  --nemesis-services  node1,node2,node3 \
//	  --nemesis-interval  10s \
//	  --nemesis-downtime  5s \
//	  --check-convergence
//
// Three things to know before reading its output, all argued in leaderkill.go
// and docs/chaos-harness.md:
//
//   - --peers is required, because the leader has to be identified from the
//     cluster rather than assumed to be --target.
//   - Pass every member to --nemesis-services. That list is a fence, not a
//     rotation: the victim is always the resolved leader, and a leader outside
//     the list makes the window skip rather than redirecting the kill.
//   - The runner's own --target may be the node that dies. Operations fail for
//     the duration of that outage. Most of those failures provably never left the
//     client and cost the checker nothing; the ambiguous minority is confined to
//     the transitions by --fail-fast-after, without which a leader-kill run's
//     verdict is unreachable at any --check-timeout. See errclass.go, breaker.go
//     and docs/chaos-harness.md.
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
		peers        = flag.String("peers", "", "other nodes' client HTTP addresses, comma-separated — required by --check-convergence")
		duration     = flag.Duration("duration", 30*time.Second, "how long to run operations")
		warmup       = flag.Duration("warmup", 2*time.Second, "warmup window — ops issued but not recorded")
		workers      = flag.Int("workers", 8, "number of concurrent client goroutines")
		keyspace     = flag.Int("keyspace", 20, "number of distinct keys (small → richer contention per key)")
		putPct       = flag.Int("put", 50, "percentage of operations that are puts (0-100)")
		delPct       = flag.Int("delete", 5, "percentage of operations that are deletes (0-100)")
		checkTimeout = flag.Duration("check-timeout", 60*time.Second, "time limit for linearizability check")
		output       = flag.String("output", "table", "output format: table|json")
		cxFile       = flag.String("counterexample-file", "", "where to write the counterexample on FAIL (default: chaos-counterexample-<timestamp>.json; \"none\" disables)")

		nemesisMode     = flag.String("nemesis", nemesisNone, "fault injection during measurement: none|kill-restart|stop-restart|leader-kill")
		nemesisServices = flag.String("nemesis-services", "", "comma-separated compose service names to draw victims from (required unless --nemesis=none; for leader-kill it is the set the kill must stay inside, so pass every member)")
		nemesisInterval = flag.Duration("nemesis-interval", 10*time.Second, "delay between the end of one outage and the start of the next")
		nemesisDowntime = flag.Duration("nemesis-downtime", 5*time.Second, "how long a victim stays down")
		nemesisCompose  = flag.String("nemesis-compose-file", "docker/docker-compose.yml", "compose file the nemesis operates on")

		// Fail-fast on an unreachable --target. Only matters when the nemesis can
		// take the target down, which in practice means leader-kill. See
		// breaker.go for why offered load is traded for a checkable history, and
		// docs/chaos-harness.md for how to read the pause line it prints.
		failFastAfter   = flag.Int("fail-fast-after", 5, "consecutive transport failures against --target before the workload pauses; 0 disables (restores the old offered-load profile)")
		failFastBackoff = flag.Duration("fail-fast-backoff", 250*time.Millisecond, "how long the workload stays paused between probes of an unreachable --target")

		// Convergence verification: did the cluster ever repair the writes it
		// refused but kept? Default on for the fault-injecting nemeses, where
		// divergence is expected to happen and therefore expected to be fixed.
		checkConverge     = flag.Bool("check-convergence", true, "after the run, assert every replica agrees on every key (requires --peers; only applies when a nemesis is enabled)")
		convergenceGrace  = flag.Duration("convergence-grace", 30*time.Second, "how long to keep re-checking convergence before declaring the replicas divergent")
		convergenceReplic = flag.Int("replicas", 2, "cluster replication factor R, used to decide which nodes must agree on a key")
	)
	flag.Parse()

	if *putPct+*delPct > 100 {
		fmt.Fprintln(os.Stderr, "error: --put + --delete must be ≤ 100")
		os.Exit(3)
	}
	if *convergenceReplic < 1 {
		fmt.Fprintln(os.Stderr, "error: --replicas must be ≥ 1")
		os.Exit(3)
	}

	ncfg, err := parseNemesisFlags(*nemesisMode, *nemesisServices, *nemesisCompose, *nemesisInterval, *nemesisDowntime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(3)
	}

	// leader-kill has to ask the cluster who leads, so it needs every node's
	// client address. Checked here, with the other flag validation, so a bad
	// invocation fails on its own terms rather than on whatever docker says
	// first.
	nodeAddrs := parseNodeAddrs(*target, *peers)
	if ncfg.Mode == nemesisLeaderKill && len(nodeAddrs) < 2 {
		fmt.Fprintln(os.Stderr, "error: --nemesis=leader-kill requires --peers "+
			"(every other node's client HTTP address) so the current leader can be resolved")
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

		// leader-kill needs to ask the cluster who leads, per fault window. It
		// resolves over the same node addresses the convergence check uses, so
		// --peers is required: with only the target's address, a leader that is
		// not the target could only be identified from the target's own view of
		// it, which lags its actual loss.
		if ncfg.Mode == nemesisLeaderKill {
			resolver := newLeaderResolver(nodeAddrs, &http.Client{Timeout: 5 * time.Second})

			// Prove the mode can work before any load is issued, for the same
			// reason the nemesis has a Preflight at all: a run whose every window
			// skips is a run that tested nothing while reporting PASS.
			lctx, lcancel := context.WithTimeout(context.Background(), 2*nemesisCommandTimeout)
			services, servicesErr := nem.definedServices(lctx)
			if servicesErr != nil {
				lcancel()
				fmt.Fprintf(os.Stderr, "error: nemesis preflight failed: %v\n", servicesErr)
				os.Exit(3)
			}
			leaderErr := preflightLeaderKill(lctx, resolver, services)
			lcancel()
			if leaderErr != nil {
				fmt.Fprintf(os.Stderr, "error: nemesis preflight failed: %v\n", leaderErr)
				os.Exit(3)
			}
			scheduler.SelectVictim = leaderVictimSelector(resolver, ncfg.Services, log.Printf)
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

	// The breaker's probe is /status: it reads no keys, so it cannot put a
	// mutation into the store that the recorded history does not contain. See
	// newTargetBreaker.
	breaker := newTargetBreaker(*failFastAfter, *failFastBackoff, func(ctx context.Context) error {
		_, err := client.Status(ctx)
		return err
	}, log.Printf)

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
	// The breaker is deliberately not installed for warmup: no nemesis runs
	// during warmup, so there is nothing for it to react to, and leaving it out
	// keeps warmup's offered-load profile identical to every previous run's.
	runWorkers(wctx, *workers, warmKeys, *putPct, *delPct, client, nil, warm, nil)
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

	runWorkers(mctx, *workers, keys, *putPct, *delPct, client, rec, measured, breaker)
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

	// Convergence gate. It runs before the linearizability check so the grace
	// window is spent waiting for the cluster to repair itself, not waiting behind
	// a check that can take a minute — and because a divergent cluster is worth
	// knowing about even if the history turns out to be unverifiable.
	var converge convergenceResult
	switch {
	case !*checkConverge:
		converge = convergenceResult{}
	case !ncfg.Enabled():
		converge = convergenceResult{Skipped: "no nemesis — nothing was refused, so nothing needed repairing"}
	default:
		log.Printf("waiting up to %s for replicas to converge …", *convergenceGrace)
		converge = checkConvergence(context.Background(), convergenceConfig{
			Enabled:  true,
			Grace:    *convergenceGrace,
			Nodes:    parseNodeAddrs(*target, *peers),
			Replicas: *convergenceReplic,
		}, keys, &http.Client{Transport: transport})
		if converge.Checked {
			log.Printf("convergence: converged=%t divergent=%d attempts=%d in %s",
				converge.Converged, converge.Divergent, converge.Attempts, converge.Elapsed)
		} else {
			log.Printf("convergence check skipped: %s", converge.Skipped)
		}
	}

	ops := measured.ops.Load()
	errs := measured.errors.Load()
	indeterminate := measured.indeterminateWrites.Load()
	refused := measured.refusedWrites.Load()
	pauses := breaker.Stats()
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

	// Localise a FAIL before printing it. The whole-history verdict says only
	// that some key's sub-history is illegal; the counterexample says which key,
	// and which operations the checker could not order. Only a genuine
	// non-linearizable verdict is localised: a timeout has no frontier to find,
	// and a legal history that failed the convergence gate is a different
	// finding with its own note.
	var (
		cx     *linearizability.Counterexample
		cxPath string
	)
	if !ok && !timedOut {
		cx = rec.Counterexample(*checkTimeout)
		var cxErr error
		cxPath, cxErr = writeCounterexampleFile(*cxFile, cx, counterexampleMeta{
			Verdict:          result,
			Target:           *target,
			Nemesis:          ncfg.Describe(),
			MeasurementStart: measureStart,
			MeasuredFor:      measuredFor,
			Windows:          windows,
		})
		if cxErr != nil {
			log.Printf("counterexample: %v", cxErr)
		}
	}

	// A cluster that never repaired its refused-but-applied writes has failed,
	// even with a legal history: linearizability is about what clients observed
	// through the primary, and divergence between a primary and its replicas is
	// invisible to it by construction.
	if converge.Checked && !converge.Converged {
		if exitCode == 0 {
			result = "FAIL (replicas divergent)"
		}
		if exitCode != 2 {
			exitCode = 1
		}
	}

	if *output == "json" {
		type report struct {
			Target              string `json:"target"`
			Duration            string `json:"duration"`
			MeasuredDuration    string `json:"measured_duration"`
			Interrupted         bool   `json:"interrupted"`
			Workers             int    `json:"workers"`
			Keyspace            int    `json:"keyspace"`
			TotalOps            int64  `json:"total_ops"`
			Errors              int64  `json:"errors"`
			IndeterminateWrites int64  `json:"indeterminate_writes"`
			RefusedWrites       int64  `json:"refused_writes"`
			// WriteFailuresByPhase is the taxonomy behind IndeterminateWrites:
			// which phase each failed write reached, and therefore what the store
			// could have seen. Absent when no write failed.
			WriteFailuresByPhase map[string]int64 `json:"write_failures_by_phase,omitempty"`
			// WorkloadPauses reports the fail-fast episodes. Load offered during
			// an outage is lower than the flags request while a pause is open, so
			// this is part of the run's description, not a footnote.
			WorkloadPauses  breakerStats        `json:"workload_pauses"`
			Events          int                 `json:"events"`
			Linearizable    bool                `json:"linearizable"`
			CheckTimedOut   bool                `json:"check_timed_out"`
			CheckDuration   string              `json:"check_duration"`
			Nemesis         string              `json:"nemesis"`
			NemesisServices []string            `json:"nemesis_services,omitempty"`
			FaultsInjected  int                 `json:"faults_injected"`
			FaultsAttempted int                 `json:"faults_attempted"`
			FaultWindows    []faultWindowReport `json:"fault_windows,omitempty"`
			Convergence     convergenceResult   `json:"convergence"`
			// Counterexample is present only on a non-linearizable verdict that
			// the per-key scan could localise.
			Counterexample     *counterexampleReport `json:"counterexample,omitempty"`
			CounterexampleFile string                `json:"counterexample_file,omitempty"`
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report{
			Target:               *target,
			Duration:             duration.String(),
			MeasuredDuration:     measuredFor.Round(time.Millisecond).String(),
			Interrupted:          interrupted,
			Workers:              *workers,
			Keyspace:             *keyspace,
			TotalOps:             ops,
			Errors:               errs,
			IndeterminateWrites:  indeterminate,
			RefusedWrites:        refused,
			WriteFailuresByPhase: measured.failureBreakdownMap(),
			WorkloadPauses:       pauses,
			Events:               rec.Len(),
			Linearizable:         ok && !timedOut,
			CheckTimedOut:        timedOut,
			CheckDuration:        elapsed.Round(time.Millisecond).String(),
			Nemesis:              ncfg.Describe(),
			NemesisServices:      ncfg.Services,
			FaultsInjected:       injected,
			FaultsAttempted:      len(windows),
			FaultWindows:         faultWindowReports(windows, measureStart),
			Convergence:          converge,
			Counterexample: buildCounterexampleReport(cx, counterexampleMeta{
				Verdict:          result,
				Target:           *target,
				Nemesis:          ncfg.Describe(),
				MeasurementStart: measureStart,
				MeasuredFor:      measuredFor,
				Windows:          windows,
			}),
			CounterexampleFile: cxPath,
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
		for _, line := range converge.convergenceLines() {
			fmt.Printf("%s\n", line)
		}
		fmt.Printf("  %-24s %d\n", "indeterminate writes:", indeterminate)
		if pauses.Episodes > 0 {
			// Named plainly, because it changes what "ops" means: the workload
			// offered less load than the flags asked for, on purpose, and a
			// reader comparing two runs has to know that.
			fmt.Printf("  %-24s %d episode(s), %s paused, %d op(s) not attempted\n",
				"workload paused:", pauses.Episodes,
				pauses.Paused.Round(time.Millisecond), pauses.Skipped)
		}
		fmt.Printf("  %-24s %d\n", "events:", rec.Len())
		fmt.Printf("  %-24s %s\n", "nemesis:", ncfg.Describe())
		fmt.Printf("  %-24s %d of %d attempted\n", "faults injected:", injected, len(windows))
		fmt.Printf("  %-24s %s\n", "check_duration:", elapsed.Round(time.Millisecond))
		fmt.Printf("  %-24s %s\n", "linearizable:", result)
		if breakdown := measured.failureBreakdown(); len(breakdown) > 0 {
			fmt.Printf("%s\n", sep)
			fmt.Printf("  write failures by phase (what the store could have seen):\n")
			for _, line := range breakdown {
				fmt.Printf("    %s\n", line)
			}
		}
		if len(windows) > 0 {
			fmt.Printf("%s\n", sep)
			fmt.Printf("  fault windows (offsets from measurement start):\n")
			for _, line := range formatFaultWindows(windows, measureStart) {
				fmt.Printf("    %s\n", line)
			}
		}
		for _, line := range verdictNotes(exitCode, refused, indeterminate, converge) {
			if line == "" {
				fmt.Printf("%s\n", sep)
				continue
			}
			fmt.Printf("  %s\n", line)
		}
		if !ok && !timedOut {
			for _, line := range formatCounterexample(cx, measureStart, windows, cxPath) {
				if line == "" {
					fmt.Printf("%s\n", sep)
					continue
				}
				fmt.Printf("  %s\n", line)
			}
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
func verdictNotes(exitCode int, refused, indeterminate int64, converge convergenceResult) []string {
	// A FAIL caused by divergent replicas is a different finding from an illegal
	// history, and the guidance for it is different: the history may be perfectly
	// legal, because linearizability is judged on what clients saw through the
	// ring-primary and cannot see a replica that is behind.
	if exitCode == 1 && converge.Checked && !converge.Converged {
		notes := []string{
			"",
			"NOTE: the replicas did not converge. Every write refused with 503 during a",
			"      fault window was still applied on the ring-primary, and anti-entropy is",
			"      what is supposed to replay those to the replica once it is healthy again.",
			"      A divergent result means that repair did not happen or did not finish.",
			"      Check the primaries' logs for \"catch-up\" and \"replica cursor is older\",",
			"      and /metrics for anti_entropy_passes, anti_entropy_entries and",
			"      anti_entropy_stale. Raising --convergence-grace only helps if repair was",
			"      still in progress.",
		}
		if len(converge.Unreachable) > 0 {
			notes = append(notes,
				"      Some nodes could not be read at all, so this is 'unverified' rather",
				"      than 'proven divergent' — bring every node up and re-run.")
		}
		return notes
	}

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
				"      the search did not finish. Raising the budget does not help much: a few",
				"      hundred pending operations multiply the search space past any timeout.",
				"      Read the write-failure breakdown above. A large \"sent, no answer\" count",
				"      with few workload pauses means --fail-fast-after is not engaging while the",
				"      target is unreachable; see docs/chaos-harness.md.")
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

	// Skipped is the reason no victim was disrupted in this window, present only
	// for a window that was skipped. Its victim is empty, because none was
	// chosen.
	Skipped string `json:"skipped,omitempty"`
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
			Skipped:        w.Skipped,
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
//
// The victim is printed per window rather than taken from the flags, so a mode
// that resolves its victim at strike time — leader-kill — shows which node it
// actually took down each time instead of a static name.
func formatFaultWindows(windows []FaultWindow, start time.Time) []string {
	lines := make([]string, 0, len(windows))
	for i, w := range windows {
		offset := func(t time.Time) string {
			return fmt.Sprintf("+%s", t.Sub(start).Round(100*time.Millisecond))
		}
		if w.Skipped != "" {
			lines = append(lines, fmt.Sprintf("#%-3d %-12s at %-8s SKIPPED — %s",
				i+1, "(none)", offset(w.DownAt), w.Skipped))
			continue
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
	// kinds counts *write* failures by taxonomy bucket, indexed by failureKind.
	// It is what turns "739 indeterminate writes" from a number into a diagnosis:
	// the two transport buckets separate the requests that provably never left
	// from the ones that may have been read, and those have entirely different
	// fixes. Reads are deliberately excluded — the indeterminate count they sit
	// beside is about writes. Writes cancelled by the runner's own shutdown are
	// excluded for the same reason they are excluded from `errors`: they describe
	// the run ending, not the cluster.
	kinds [kindCount]atomic.Int64
}

// recordKind accumulates one write failure into the taxonomy.
func (c *counters) recordKind(k failureKind) {
	if k == kindNone || int(k) >= len(c.kinds) {
		return
	}
	c.kinds[k].Add(1)
}

// failureBreakdown renders the per-kind write-failure tally, in report order,
// omitting buckets that never fired. Empty when no write failed.
func (c *counters) failureBreakdown() []string {
	var lines []string
	for _, k := range writeFailureKinds {
		n := c.kinds[k].Load()
		if n == 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("%-26s %8d  → %s", k.String()+":", n, k.effect()))
	}
	return lines
}

// failureBreakdownMap is the JSON shape of the same tally.
func (c *counters) failureBreakdownMap() map[string]int64 {
	out := map[string]int64{}
	for _, k := range writeFailureKinds {
		if n := c.kinds[k].Load(); n > 0 {
			out[k.String()] = n
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// runWorkers starts `n` goroutines that each issue operations until ctx is
// cancelled.  If rec is nil, operations are executed but not recorded
// (warmup mode).
//
// br may be nil, which disables fail-fast entirely. When it is not nil, a worker
// asks it before every operation: a refused operation is not attempted, so it
// produces no history event and is not counted as an op. Every operation that is
// attempted is recorded and classified exactly as it would be without the
// breaker — see breaker.go.
func runWorkers(
	ctx context.Context,
	n int,
	keys []string,
	putPct, delPct int,
	client *clientpkg.Client,
	rec *linearizability.Recorder,
	c *counters,
	br *targetBreaker,
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

				if !br.Allow() {
					// The target is not answering. Wait — or, if elected, probe
					// it — and re-check rather than adding another ambiguous
					// request to the history.
					br.Wait(ctx)
					continue
				}

				key := keys[rng.Intn(len(keys))]
				roll := rng.Intn(100)

				c.ops.Add(1)

				switch {
				case roll < putPct:
					val := fmt.Sprintf("w%d-t%d", id, time.Now().UnixNano())
					var cid int
					if rec != nil {
						cid = rec.BeginWorker(id, linearizability.Input{Op: "put", Key: key, Value: val})
					}
					err := client.Put(ctx, key, val)
					br.Record(finishWrite(ctx, rec, cid, err, c))

				case roll < putPct+delPct:
					var cid int
					if rec != nil {
						cid = rec.BeginWorker(id, linearizability.Input{Op: "delete", Key: key})
					}
					err := classifyDeleteErr(client.Delete(ctx, key))
					br.Record(finishWrite(ctx, rec, cid, err, c))

				default: // get
					var cid int
					if rec != nil {
						cid = rec.BeginWorker(id, linearizability.Input{Op: "get", Key: key})
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
					// Reads feed the breaker too — a read that cannot reach the
					// target is the same evidence about the target as a write
					// that cannot — but they are left out of the write taxonomy.
					br.Record(classifyFailure(err))
					if err != nil && !isNotFound(err) && ctx.Err() == nil {
						c.errors.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// finishWrite records the outcome of a put or delete, and returns the taxonomy
// bucket it fell into so the caller can feed the breaker.
//
// How the operation enters the history is decided by classifyFailure: a
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
) failureKind {
	kind := classifyFailure(err)
	effect := kind.effect()

	if err != nil && ctx.Err() == nil {
		c.errors.Add(1)
		c.recordKind(kind)
		switch effect {
		case effectApplied:
			c.refusedWrites.Add(1)
		case effectUnknown:
			c.indeterminateWrites.Add(1)
		}
	}

	if rec == nil {
		return kind
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
	return kind
}
