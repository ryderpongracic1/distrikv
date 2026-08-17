package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

// defaultReadyTimeout bounds how long startCluster waits for a leader. A cold
// 3-member loopback cluster elects in well under a second; 30s only matters when
// something is wrong (a port already bound, a stale data dir), and in that case
// the error carries the member's own output.
const defaultReadyTimeout = 30 * time.Second

// benchConfig is the fully validated CLI configuration.
type benchConfig struct {
	qps          float64
	duration     time.Duration
	warmup       time.Duration
	workers      int
	valueSize    int
	keyspace     int
	keyDist      string
	mix          string
	endpoints    []string
	noCluster    bool
	etcdBin      string
	readyTimeout time.Duration
}

// parseFlags parses argv (without the program name).
//
// Defaults are copied from cmd/bench so that running both harnesses with the
// same explicit flags — or with none — compares like with like.
func parseFlags(args []string, out io.Writer) (benchConfig, error) {
	fs := flag.NewFlagSet("etcd-ceiling", flag.ContinueOnError)
	fs.SetOutput(out)

	var (
		qps       = fs.Float64("qps", 0, "target ops/sec, open-loop (required)")
		duration  = fs.Duration("duration", 60*time.Second, "measurement window after warmup")
		warmup    = fs.Duration("warmup", 10*time.Second, "warmup window, results discarded")
		workers   = fs.Int("workers", 256, "concurrent worker pool size")
		valueSize = fs.Int("valuesize", 256, "bytes per value")
		keyspace  = fs.Int("keyspace", 100_000, "number of distinct keys")
		keyDist   = fs.String("keydist", "uniform", "uniform|zipf|sequential")
		mix       = fs.String("mix", "20:80:0", "put:get:delete ratio")
		endpoints = fs.String("endpoints", "", "comma-separated etcd client endpoints; requires --no-cluster")
		noCluster = fs.Bool("no-cluster", false, "do not launch etcd; drive an already-running cluster")
		etcdBin   = fs.String("etcd-bin", "etcd", "etcd executable to launch (ignored with --no-cluster)")
	)

	if err := fs.Parse(args); err != nil {
		return benchConfig{}, usageError{err}
	}

	cfg := benchConfig{
		qps:          *qps,
		duration:     *duration,
		warmup:       *warmup,
		workers:      *workers,
		valueSize:    *valueSize,
		keyspace:     *keyspace,
		keyDist:      *keyDist,
		mix:          *mix,
		noCluster:    *noCluster,
		etcdBin:      *etcdBin,
		readyTimeout: defaultReadyTimeout,
	}

	if cfg.qps <= 0 {
		return benchConfig{}, usageError{errors.New("--qps is required and must be > 0")}
	}
	if cfg.workers <= 0 {
		return benchConfig{}, usageError{errors.New("--workers must be > 0")}
	}
	if cfg.duration <= 0 {
		return benchConfig{}, usageError{errors.New("--duration must be > 0")}
	}
	if cfg.warmup < 0 {
		return benchConfig{}, usageError{errors.New("--warmup must be >= 0")}
	}

	switch {
	case *endpoints != "" && !cfg.noCluster:
		// Silently ignoring --endpoints would be the worst outcome: the user
		// would read numbers from a freshly launched local cluster while
		// believing they came from the cluster they named.
		return benchConfig{}, usageError{errors.New("--endpoints requires --no-cluster " +
			"(the launched cluster's endpoints are fixed at 127.0.0.1:2379/2381/2383)")}
	case *endpoints != "":
		eps, err := splitEndpoints(*endpoints)
		if err != nil {
			return benchConfig{}, usageError{fmt.Errorf("--endpoints: %w", err)}
		}
		cfg.endpoints = eps
	case cfg.noCluster:
		// Default to the topology this harness would have launched, so
		// --no-cluster works unadorned against a cluster started by hand.
		cfg.endpoints = endpointsFor(defaultNodes())
	}

	return cfg, nil
}
