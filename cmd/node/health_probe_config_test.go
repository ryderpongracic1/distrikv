package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
)

// TestTransportProbeConfig pins the parsing rules, including the two that exist
// to stop a misconfiguration from looking like a working one.
func TestTransportProbeConfig(t *testing.T) {
	const fallback = 75 * time.Millisecond

	cases := []struct {
		raw          string
		wantInterval time.Duration
		wantEnabled  bool
		wantErr      bool
		why          string
	}{
		{raw: "", wantInterval: fallback, wantEnabled: true,
			why: "unset must behave exactly as before this knob existed"},
		{raw: "0", wantEnabled: false, wantInterval: fallback,
			why: "0 disables the probe — the pure-twist gate configuration"},
		{raw: "0s", wantEnabled: false, wantInterval: fallback,
			why: "any spelling of zero disables it"},
		{raw: "500ms", wantInterval: 500 * time.Millisecond, wantEnabled: true,
			why: "a positive duration slows the probe rather than switching it off"},
		{raw: "-1s", wantErr: true,
			why: "a negative interval is a mistake, not a synonym for off"},
		{raw: "sometimes", wantErr: true,
			why: "an unparseable value must not silently fall back to the default"},
	}

	for _, tc := range cases {
		interval, enabled, err := transportProbeConfig(tc.raw, fallback)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s=%q: no error, want one — %s", healthProbeIntervalEnv, tc.raw, tc.why)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s=%q: %v", healthProbeIntervalEnv, tc.raw, err)
			continue
		}
		if enabled != tc.wantEnabled {
			t.Errorf("%s=%q: enabled=%v, want %v — %s",
				healthProbeIntervalEnv, tc.raw, enabled, tc.wantEnabled, tc.why)
		}
		if interval != tc.wantInterval {
			t.Errorf("%s=%q: interval=%s, want %s", healthProbeIntervalEnv, tc.raw, interval, tc.wantInterval)
		}
	}
}

// TestTransportProbeNeverFiresWhenDisabled is the claim the pure-twist gate rests
// on: with the probe off, nothing polls the peers.
//
// It wires PeerHealth the way NewNode does — a disabled probe is not handed over
// at all — and then watches a counting probe that would have been handed over.
// The enabled case is asserted in the same test so that a zero count cannot be
// mistaken for a probe that was never going to fire anyway.
//
// Revert-check: make transportProbeConfig report enabled=true for "0", or hand
// the probe to PeerHealth regardless of that decision, and the disabled case
// fails with a non-zero call count.
func TestTransportProbeNeverFiresWhenDisabled(t *testing.T) {
	const interval = 5 * time.Millisecond
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	run := func(raw string) int64 {
		t.Helper()
		var calls atomic.Int64
		probe := func(string) bool {
			calls.Add(1)
			return true
		}

		gotInterval, enabled, err := transportProbeConfig(raw, interval)
		if err != nil {
			t.Fatalf("%s=%q: %v", healthProbeIntervalEnv, raw, err)
		}
		// Exactly the wiring in NewNode.
		if !enabled {
			probe = nil
		}

		ph := cluster.NewPeerHealth([]string{"node2", "node3"}, cluster.HealthConfig{
			Interval: gotInterval,
			Probe:    probe,
			Logger:   logger,
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); ph.Run(ctx) }()

		// Long enough for many ticks: a probe that is running will be obvious.
		time.Sleep(30 * interval)
		cancel()
		<-done
		return calls.Load()
	}

	if got := run("0"); got != 0 {
		t.Errorf("probe fired %d times with %s=0; the gate configuration must poll nobody",
			got, healthProbeIntervalEnv)
	}
	if got := run(""); got == 0 {
		t.Errorf("probe never fired with %s unset; the disabled assertion above would be vacuous",
			healthProbeIntervalEnv)
	}
}
