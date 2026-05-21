// Command bench is distrikv's quantified-perf load generator.
//
// It drives a running cluster (started separately via docker-compose or
// `go run ./cmd/node`) with open-loop, rate-controlled traffic and reports
// latency percentiles via HDR histogram plus engine-side counters scraped
// from the target node's /metrics endpoint.
//
// Open-loop = arrivals follow a Poisson process at the configured QPS,
// independent of how fast workers complete prior ops. Each recorded latency
// includes any queue wait, eliminating coordinated omission.
//
// Example:
//
//	go run ./cmd/bench --target localhost:8001 --qps 5000 --duration 60s
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

func main() {
	var (
		targets    = flag.String("target", "localhost:8001", "comma-separated host:port list of cluster nodes (HTTP)")
		qps        = flag.Float64("qps", 0, "target ops/sec, open-loop (required)")
		duration   = flag.Duration("duration", 60*time.Second, "measurement window after warmup")
		warmup     = flag.Duration("warmup", 10*time.Second, "warmup window, results discarded")
		mix        = flag.String("mix", "20:80:0", "put:get:delete ratio")
		keyspace   = flag.Int("keyspace", 100_000, "number of distinct keys")
		keyDist    = flag.String("keydist", "uniform", "uniform|zipf|sequential")
		valueSize  = flag.Int("valuesize", 256, "bytes per value")
		workers    = flag.Int("workers", 256, "concurrent worker pool size")
		queueCap   = flag.Int("queue-cap", 0, "arrival queue capacity (default: workers*4)")
		output     = flag.String("output", "table", "table|json")
		clientTO   = flag.Duration("client-timeout", 10*time.Second, "per-request HTTP timeout")
	)
	flag.Parse()

	if *qps <= 0 {
		fmt.Fprintln(os.Stderr, "bench: --qps is required and must be > 0")
		flag.Usage()
		os.Exit(2)
	}
	if *queueCap == 0 {
		*queueCap = *workers * 4
	}

	hostList := strings.Split(*targets, ",")
	for i := range hostList {
		hostList[i] = strings.TrimSpace(hostList[i])
	}

	// One Transport, shared by all client objects. The pool lives on the
	// Transport, so right-sizing MaxIdleConnsPerHost is what prevents the
	// "256 workers stomp 2 pooled conns → TIME_WAIT storm" pathology.
	perHost := *workers + 64
	transport := &http.Transport{
		MaxIdleConns:          perHost * len(hostList),
		MaxIdleConnsPerHost:   perHost,
		MaxConnsPerHost:       perHost,
		IdleConnTimeout:       90 * time.Second,
		DisableKeepAlives:     false,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: 5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	clients := make([]*client.Client, len(hostList))
	for i, h := range hostList {
		clients[i] = client.NewWithTransport(client.Config{Host: h, Timeout: *clientTO}, transport)
	}

	wl, err := newWorkload(*keyspace, *keyDist, *mix, *valueSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: workload init: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Probe the first target so we fail fast on an unreachable cluster.
	probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
	if _, err := clients[0].Status(probeCtx); err != nil {
		probeCancel()
		fmt.Fprintf(os.Stderr, "bench: cannot reach %s: %v\n", hostList[0], err)
		os.Exit(1)
	}
	probeCancel()

	baseCfg := runConfig{
		qps:      *qps,
		workers:  *workers,
		queueCap: *queueCap,
		wl:       wl,
		clients:  clients,
	}

	// ---- Warmup phase --------------------------------------------------------
	if *warmup > 0 {
		fmt.Fprintf(os.Stderr, "bench: warmup %s @ %.0f qps ...\n", *warmup, *qps)
		warmCfg := baseCfg
		warmCfg.duration = *warmup
		_ = run(ctx, warmCfg)
	}

	// Snapshot metrics at the start of the measurement window.
	startMetrics, err := scrapeMetrics(ctx, clients[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: scrape start metrics: %v\n", err)
		startMetrics = map[string]uint64{}
	}

	// ---- Measurement phase ---------------------------------------------------
	fmt.Fprintf(os.Stderr, "bench: measuring %s @ %.0f qps ...\n", *duration, *qps)
	measureCfg := baseCfg
	measureCfg.duration = *duration
	phase := run(ctx, measureCfg)

	endMetrics, err := scrapeMetrics(ctx, clients[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: scrape end metrics: %v\n", err)
		endMetrics = map[string]uint64{}
	}

	rep := buildReport(measureCfg, phase, startMetrics, endMetrics, hostList[0], *valueSize, *mix)

	switch *output {
	case "json":
		if err := rep.writeJSON(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "bench: write json: %v\n", err)
			os.Exit(1)
		}
	default:
		rep.writeTable(os.Stdout)
	}
}

// scrapeMetrics polls the target's /metrics endpoint via the supplied client.
func scrapeMetrics(ctx context.Context, c *client.Client) (map[string]uint64, error) {
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return c.Metrics(pollCtx)
}
