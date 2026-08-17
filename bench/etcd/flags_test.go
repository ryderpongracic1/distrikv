package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlagsDefaultsMatchDistrikvBench(t *testing.T) {
	cfg, err := parseFlags([]string{"--qps", "1200"}, discardWriter{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	// These defaults are copied from cmd/bench; drifting from them would make
	// an unadorned run of each harness measure different workloads.
	if cfg.duration != 60*time.Second {
		t.Errorf("duration = %s, want 60s", cfg.duration)
	}
	if cfg.warmup != 10*time.Second {
		t.Errorf("warmup = %s, want 10s", cfg.warmup)
	}
	if cfg.workers != 256 {
		t.Errorf("workers = %d, want 256", cfg.workers)
	}
	if cfg.valueSize != 256 {
		t.Errorf("valuesize = %d, want 256", cfg.valueSize)
	}
	if cfg.keyspace != 100_000 {
		t.Errorf("keyspace = %d, want 100000", cfg.keyspace)
	}
	if cfg.keyDist != "uniform" {
		t.Errorf("keydist = %q, want uniform", cfg.keyDist)
	}
	if cfg.mix != "20:80:0" {
		t.Errorf("mix = %q, want 20:80:0", cfg.mix)
	}
	if cfg.noCluster {
		t.Error("no-cluster should default to false (launch our own cluster)")
	}
	if cfg.etcdBin != "etcd" {
		t.Errorf("etcd-bin = %q, want etcd", cfg.etcdBin)
	}
	if len(cfg.endpoints) != 0 {
		t.Errorf("endpoints = %v, want empty (filled in from the launched cluster)", cfg.endpoints)
	}
}

func TestParseFlagsRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing qps", args: nil, want: "--qps"},
		{name: "zero qps", args: []string{"--qps", "0"}, want: "--qps"},
		{name: "negative qps", args: []string{"--qps", "-3"}, want: "--qps"},
		{name: "zero workers", args: []string{"--qps", "10", "--workers", "0"}, want: "--workers"},
		{name: "zero duration", args: []string{"--qps", "10", "--duration", "0s"}, want: "--duration"},
		{name: "negative warmup", args: []string{"--qps", "10", "--warmup", "-1s"}, want: "--warmup"},
		{
			name: "endpoints without no-cluster",
			args: []string{"--qps", "10", "--endpoints", "127.0.0.1:2379"},
			want: "--no-cluster",
		},
		{
			name: "empty endpoints with no-cluster",
			args: []string{"--qps", "10", "--no-cluster", "--endpoints", " , "},
			want: "--endpoints",
		},
		{name: "unknown flag", args: []string{"--qps", "10", "--nope"}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFlags(tc.args, discardWriter{})
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if _, ok := err.(usageError); !ok {
				t.Errorf("want usageError (exit 2), got %T", err)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q should mention %q", err, tc.want)
			}
		})
	}
}

func TestParseFlagsNoClusterDefaultsToLaunchTopology(t *testing.T) {
	cfg, err := parseFlags([]string{"--qps", "10", "--no-cluster"}, discardWriter{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	want := endpointsFor(defaultNodes())
	if len(cfg.endpoints) != len(want) {
		t.Fatalf("endpoints = %v, want %v", cfg.endpoints, want)
	}
	for i := range want {
		if cfg.endpoints[i] != want[i] {
			t.Errorf("endpoint %d = %q, want %q", i, cfg.endpoints[i], want[i])
		}
	}
}

func TestParseFlagsExplicitEndpoints(t *testing.T) {
	cfg, err := parseFlags([]string{
		"--qps", "10", "--no-cluster", "--endpoints", "a:1, b:2",
	}, discardWriter{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if len(cfg.endpoints) != 2 || cfg.endpoints[0] != "a:1" || cfg.endpoints[1] != "b:2" {
		t.Errorf("endpoints = %v, want [a:1 b:2]", cfg.endpoints)
	}
}

// discardWriter swallows flag-package usage output so failing cases stay quiet.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
