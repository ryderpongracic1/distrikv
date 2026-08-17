// Command etcd-ceiling measures etcd under distrikv's own benchmark workload,
// giving distrikv's throughput and latency numbers a state-of-the-art reference
// point.
//
// This is a ceiling measurement, not a competition. etcd and distrikv are
// different classes of CP key-value store: etcd serialises every write through
// a single Raft leader and answers reads linearizably, while distrikv places
// keys on a consistent hash ring and commits a write once its R=2 replicas ACK,
// refusing the write when a replica is unreachable. The interesting output is
// therefore not "who wins" but the shape of the gap — see the "Ceiling — vs
// etcd" section of the repository README.
//
// The harness launches a 3-member etcd cluster on loopback (client ports
// 2379/2381/2383, peer ports 2380/2382/2384), drives it with the exact workload
// semantics of cmd/bench — Zipfian keys, open-loop Poisson arrivals, identical
// key format and value bytes — and reports achieved QPS plus exact latency
// percentiles per operation type.
//
// Example:
//
//	go run . --qps 1200 --duration 60s --mix 100:0:0 --keydist zipf
//
// Point it at an already-running cluster instead with:
//
//	go run . --no-cluster --endpoints 127.0.0.1:2379 --qps 1200
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintf(os.Stderr, "etcd-ceiling: %v\n", err)
		os.Exit(1)
	}
}

// usageError marks a flag/validation problem, which exits 2 rather than 1 —
// same convention as cmd/bench.
type usageError struct{ err error }

func (u usageError) Error() string { return u.err.Error() }

func realMain() error {
	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if _, ok := err.(usageError); ok {
			fmt.Fprintf(os.Stderr, "etcd-ceiling: %v\n", err)
			os.Exit(2)
		}
		return err
	}

	wl, err := newWorkload(cfg.keyspace, cfg.keyDist, cfg.mix, cfg.valueSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "etcd-ceiling: workload init: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	endpoints := cfg.endpoints
	if !cfg.noCluster {
		fmt.Fprintf(os.Stderr, "etcd-ceiling: launching 3-member etcd cluster (%s) ...\n", cfg.etcdBin)
		c, err := startCluster(ctx, clusterConfig{
			binary:       cfg.etcdBin,
			nodes:        defaultNodes(),
			readyTimeout: cfg.readyTimeout,
		})
		if err != nil {
			return err
		}
		defer c.Shutdown()
		endpoints = c.endpoints
		fmt.Fprintf(os.Stderr, "etcd-ceiling: cluster healthy on %v\n", endpoints)
	}

	clients := make([]*clientv3.Client, 0, len(endpoints))
	defer func() {
		for _, cli := range clients {
			_ = cli.Close()
		}
	}()
	for _, ep := range endpoints {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			DialTimeout: 5 * time.Second,
			Context:     ctx,
		})
		if err != nil {
			return fmt.Errorf("create client for %s: %w", ep, err)
		}
		clients = append(clients, cli)
	}

	// Probe the first endpoint so an unreachable cluster fails fast instead of
	// producing a run full of timeouts. Matters most under --no-cluster, where
	// nothing else has verified the target.
	probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = clients[0].Status(probeCtx, endpoints[0])
	probeCancel()
	if err != nil {
		return fmt.Errorf("cannot reach %s: %w", endpoints[0], err)
	}

	baseCfg := runConfig{
		qps:      cfg.qps,
		workers:  cfg.workers,
		queueCap: cfg.workers * 4,
		wl:       wl,
		// Workers are pinned to an endpoint by index, matching cmd/bench's
		// clients[idx%len(clients)] assignment — with 256 workers over 3
		// endpoints that spreads load evenly without adding a balancer whose
		// behaviour would differ between the two harnesses.
		dispatch: func(ctx context.Context, idx int, op opKind, key string) error {
			return wl.dispatch(ctx, clients[idx%len(clients)], op, key)
		},
	}

	// ---- Warmup phase --------------------------------------------------------
	// Discarded, and load-bearing: it populates the keyspace so the measurement
	// window's Gets hit existing keys, and lets etcd's Raft log and page cache
	// reach steady state.
	if cfg.warmup > 0 {
		fmt.Fprintf(os.Stderr, "etcd-ceiling: warmup %s @ %.0f qps ...\n", cfg.warmup, cfg.qps)
		warmCfg := baseCfg
		warmCfg.duration = cfg.warmup
		_ = run(ctx, warmCfg)
	}

	// ---- Measurement phase ---------------------------------------------------
	fmt.Fprintf(os.Stderr, "etcd-ceiling: measuring %s @ %.0f qps ...\n", cfg.duration, cfg.qps)
	measureCfg := baseCfg
	measureCfg.duration = cfg.duration
	phase := run(ctx, measureCfg)

	rep := buildReport(measureCfg, phase, endpoints, cfg.valueSize, cfg.mix)
	rep.writeTable(os.Stdout)
	return nil
}
