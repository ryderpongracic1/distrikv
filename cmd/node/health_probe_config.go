package main

import (
	"fmt"
	"time"
)

// healthProbeIntervalEnv names the environment variable that tunes — or turns
// off — the transport probe.
//
// # Why this exists
//
// distrikv's design intent is that Raft carries node health and the hash ring
// carries data. Today health is merged from four signals, and the transport
// probe is one of the local ones: it is the only signal that tells a
// ring-primary which is *not* the Raft leader that a peer has come back, because
// writes to a down peer are being refused and so the write path never observes
// recovery. Committed health entries now cover that case, which is what makes
// removing the probe conceivable — but conceivable is not measured.
//
// Setting this to 0 disables the probe ticker without deleting anything, so a
// chaos gate can run in the exact post-removal configuration and produce the
// evidence that removal is safe. The probe stays on by default, in the default
// stack, in every existing gate.
//
// It is read here rather than in internal/config deliberately. This is a
// diagnostic for one planned experiment, not part of a node's operating
// configuration, and it lives in the package that owns the probe closure so that
// the supported-configuration surface does not grow a knob nobody should turn in
// production. If the probe is eventually removed, this file goes with it.
const healthProbeIntervalEnv = "HEALTH_PROBE_INTERVAL"

// transportProbeConfig decides whether the transport probe runs, and how often.
//
// The rules, and what each one is for:
//
//   - unset or empty → the probe runs at fallback (the heartbeat interval), which
//     is exactly the behaviour before this knob existed;
//   - "0" (or "0s", or any zero duration) → the probe does not run at all;
//   - a positive duration → the probe runs at that interval, which is how a gate
//     can slow the probe down rather than switching it off;
//   - a negative duration, or anything unparseable → an error, because silently
//     falling back would leave an operator believing they had configured
//     something they had not.
func transportProbeConfig(raw string, fallback time.Duration) (interval time.Duration, enabled bool, err error) {
	if raw == "" {
		return fallback, true, nil
	}
	d, parseErr := time.ParseDuration(raw)
	if parseErr != nil {
		return 0, false, fmt.Errorf("config: invalid %s %q: %w", healthProbeIntervalEnv, raw, parseErr)
	}
	if d < 0 {
		return 0, false, fmt.Errorf("config: %s must be >= 0, got %s (0 disables the probe)",
			healthProbeIntervalEnv, d)
	}
	if d == 0 {
		// The interval is still reported as the fallback so that a caller which
		// logs it, or which later re-enables the probe, has a sane value rather
		// than a zero that would make a ticker panic.
		return fallback, false, nil
	}
	return d, true, nil
}
