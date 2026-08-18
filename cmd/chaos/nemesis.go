package main

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"
)

// Nemesis modes accepted by --nemesis.
const (
	nemesisNone        = "none"
	nemesisKillRestart = "kill-restart"
	nemesisStopRestart = "stop-restart"

	// nemesisLeaderKill stops and restarts whichever node currently holds Raft
	// leadership, re-resolved per fault window. See leaderkill.go for the design
	// and for why it never falls back to a non-leader.
	nemesisLeaderKill = "leader-kill"
)

// nemesisModes lists every accepted --nemesis value, for error messages.
var nemesisModes = []string{nemesisNone, nemesisKillRestart, nemesisStopRestart, nemesisLeaderKill}

// nemesisCommandTimeout bounds a single docker invocation. A `compose kill` is
// near-instant and a `compose start` is a container start, so anything past
// this is a wedged daemon rather than slow work.
const nemesisCommandTimeout = 30 * time.Second

// A Nemesis injects and repairs faults on named cluster members.
//
// Implementations only need to be safe for a single scheduler goroutine.
//
// Heal must be idempotent: the scheduler calls it on the shutdown path even
// when the victim may already be healthy, so that a cancelled run can never
// leave a node down.
type Nemesis interface {
	// Preflight reports whether this nemesis can operate on the given victims
	// in this environment. It runs once, before the measurement phase, so that
	// a nemesis that cannot work (no docker, wrong compose file, misspelled
	// service) fails the run loudly instead of silently degrading it into a
	// no-fault run that trivially passes.
	Preflight(ctx context.Context, victims []string) error

	// Disrupt takes victim out of service.
	Disrupt(ctx context.Context, victim string) error

	// Heal returns victim to service.
	Heal(ctx context.Context, victim string) error

	// Mode is the short label reported for this nemesis.
	Mode() string
}

// A FaultWindow records one disrupt/heal cycle, so that a linearizability
// failure can be correlated with the outage that may have produced it.
type FaultWindow struct {
	Victim     string
	DownAt     time.Time
	UpAt       time.Time // zero if the victim was never successfully healed
	DisruptErr string    // non-empty if Disrupt returned an error
	HealErr    string    // non-empty if Heal returned an error

	// Skipped is non-empty when the scheduler declined to inject this window
	// because it could not choose a victim it was willing to act on — a
	// leaderless cluster, or a leader outside the kill set.
	//
	// A skipped window is recorded rather than dropped so that a run which
	// injected fewer faults than it attempted says so. Silently not injecting is
	// the one failure mode that looks like a passing chaos run.
	Skipped string
}

// Injected reports whether this window is an actual outage: Disrupt was attempted
// and returned success. A window whose Disrupt failed, and a window that was
// skipped before any victim was chosen, are both recorded — the fact is worth
// seeing — but neither is an outage and neither may be counted as one.
func (w FaultWindow) Injected() bool { return w.DisruptErr == "" && w.Skipped == "" }

// Healed reports whether the victim was brought back up. UpAt is only set on a
// successful heal, so an unhealed window carries no up-at time at all rather
// than a time the victim did not actually return.
func (w FaultWindow) Healed() bool { return !w.UpAt.IsZero() }

// Down returns the outage duration, or 0 if the victim never came back.
func (w FaultWindow) Down() time.Duration {
	if w.UpAt.IsZero() {
		return 0
	}
	return w.UpAt.Sub(w.DownAt)
}

// A Scheduler drives a Nemesis on a fixed cycle: pick a random victim, disrupt
// it, leave it down for Downtime, heal it, wait Interval, repeat.
//
// The first strike lands immediately, so a short run still sees a fault.
//
// Run heals its victim on every path it controls — a completed cycle, a
// cancelled context, a failed disrupt, or a panic inside the loop — so an
// orderly exit never leaves the cluster degraded. It cannot cover what it does
// not run through: a SIGKILL of the runner, a panic elsewhere in the process, or
// a Heal that itself fails, which is recorded in the window's HealErr and
// reported.
type Scheduler struct {
	Nemesis  Nemesis
	Victims  []string
	Interval time.Duration
	Downtime time.Duration

	// Rand selects victims. Nil means a time-seeded source.
	Rand *rand.Rand

	// SelectVictim chooses the victim for the next fault window. Nil means a
	// uniform random pick from Victims, which is what the stop-restart and
	// kill-restart modes want.
	//
	// A mode whose victim depends on cluster state — leader-kill — supplies this
	// instead, and is re-consulted for every window so that leadership moving
	// between windows is picked up. Returning an error skips the window: the
	// scheduler records the reason and waits out the cycle rather than picking
	// someone else, because a nemesis that quietly redirects its fault is a
	// nemesis whose report cannot be trusted.
	SelectVictim func(ctx context.Context) (string, error)

	// HealTimeout bounds a single nemesis command (disrupt or heal), each of
	// which runs on a context detached from the run context. Zero means 30s.
	HealTimeout time.Duration

	// Logf receives one line per state change. Nil discards.
	Logf func(format string, args ...any)

	mu      sync.Mutex
	windows []FaultWindow
}

// Run drives the disrupt/heal cycle until ctx is done. It is a no-op if no
// victims are configured.
func (s *Scheduler) Run(ctx context.Context) {
	if s.Nemesis == nil || len(s.Victims) == 0 {
		return
	}
	rng := s.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	// outstanding indexes a window whose victim is still down. The deferred
	// heal is what guarantees the cluster is intact after Run returns, on every
	// path including a panic.
	outstanding := -1
	outstandingVictim := ""
	defer func() {
		if outstanding >= 0 {
			s.heal(ctx, outstanding, outstandingVictim)
		}
	}()

	for {
		// Only begin a strike while the run is still live. Combined with the
		// detached Disrupt below, this keeps DisruptErr meaning "the strike
		// failed" rather than "the strike was interrupted and may or may not
		// have landed" — the same distinction writeIsIndeterminate draws for
		// writes, applied to faults.
		if ctx.Err() != nil {
			return
		}

		victim, err := s.pickVictim(ctx, rng)
		if err != nil {
			// Record the skip and hold the cadence. Waiting out Downtime and
			// Interval anyway keeps the fault schedule of a run with skips
			// comparable to one without: the windows line up at the same offsets,
			// which is what makes two gate runs readable side by side.
			s.recordSkip(err)
			s.logf("nemesis: fault window SKIPPED — %v", err)
			if !sleep(ctx, s.Downtime) {
				return
			}
			if !sleep(ctx, s.Interval) {
				return
			}
			continue
		}

		outstanding = s.beginWindow(victim)
		outstandingVictim = victim
		s.logf("nemesis: %s → %s", s.Nemesis.Mode(), victim)
		if err := s.disrupt(ctx, victim); err != nil {
			s.setDisruptErr(outstanding, err)
			s.logf("nemesis: disrupt %s failed: %v", victim, err)
		}

		// A cancelled downtime still falls through to the heal below, so a
		// victim taken down by the last cycle is always brought back.
		completed := sleep(ctx, s.Downtime)

		s.heal(ctx, outstanding, victim)
		outstanding = -1

		if !completed {
			return
		}
		if !sleep(ctx, s.Interval) {
			return
		}
	}
}

// Windows returns a copy of the fault windows recorded so far.
func (s *Scheduler) Windows() []FaultWindow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FaultWindow, len(s.windows))
	copy(out, s.windows)
	return out
}

// disrupt takes victim down. Like heal, it runs on a context detached from ctx
// and bounded by its own timeout: a docker command killed halfway through the
// run deadline may already have stopped the container, so an interrupted strike
// would report a failure whose real outcome is unknown. Letting the command
// finish means DisruptErr is trustworthy.
func (s *Scheduler) disrupt(ctx context.Context, victim string) error {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.commandTimeout())
	defer cancel()
	return s.Nemesis.Disrupt(dctx, victim)
}

// heal brings victim back up and closes out its window. It always runs on a
// context detached from ctx: on the shutdown path ctx is already cancelled, and
// a heal issued on a cancelled context would fail without doing anything.
func (s *Scheduler) heal(ctx context.Context, idx int, victim string) {
	healCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.commandTimeout())
	defer cancel()

	err := s.Nemesis.Heal(healCtx, victim)
	if err != nil {
		s.logf("nemesis: heal %s FAILED — it may still be down: %v", victim, err)
	} else {
		s.logf("nemesis: heal → %s", victim)
	}
	s.endWindow(idx, err)
}

// commandTimeout bounds a single nemesis command. Zero means 30s.
func (s *Scheduler) commandTimeout() time.Duration {
	if s.HealTimeout > 0 {
		return s.HealTimeout
	}
	return 30 * time.Second
}

func (s *Scheduler) beginWindow(victim string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows = append(s.windows, FaultWindow{Victim: victim, DownAt: time.Now()})
	return len(s.windows) - 1
}

// pickVictim chooses who to disrupt next, deferring to SelectVictim when set.
func (s *Scheduler) pickVictim(ctx context.Context, rng *rand.Rand) (string, error) {
	if s.SelectVictim != nil {
		return s.SelectVictim(ctx)
	}
	return s.Victims[rng.Intn(len(s.Victims))], nil
}

// recordSkip records a window that was never injected, with the reason.
func (s *Scheduler) recordSkip(reason error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows = append(s.windows, FaultWindow{DownAt: time.Now(), Skipped: reason.Error()})
}

func (s *Scheduler) setDisruptErr(idx int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows[idx].DisruptErr = err.Error()
}

func (s *Scheduler) endWindow(idx int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// Leave UpAt zero: the victim did not come back, and recording a time
		// here would report a completed outage for a node that is still down.
		s.windows[idx].HealErr = err.Error()
		return
	}
	s.windows[idx].UpAt = time.Now()
}

func (s *Scheduler) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

// sleep waits for d or until ctx is done, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// composeNemesis takes docker-compose services down and back up, via
// `docker compose -f <file> {kill|stop} <service>` and
// `docker compose -f <file> start <service>`.
//
// Two decisions worth knowing, argued in full in
// docs/chaos-harness.md: compose *service* names are addressed rather than
// container names, which are project- and compose-version-dependent; and
// `start` (not `up`) brings a victim back, so it keeps its named volume and
// recovers from its own WAL rather than starting empty.
type composeNemesis struct {
	mode    string
	file    string
	downCmd string // compose subcommand that takes a service down

	// run executes one docker invocation and returns its combined output.
	// Tests replace it; the real implementation is execDocker.
	run func(ctx context.Context, args ...string) ([]byte, error)
}

// newComposeNemesis builds a nemesis for mode, which must be one of the
// nemesis*Restart constants or nemesisLeaderKill.
func newComposeNemesis(mode, composeFile string) (*composeNemesis, error) {
	var downCmd string
	switch mode {
	case nemesisKillRestart:
		downCmd = "kill"
	case nemesisStopRestart:
		downCmd = "stop"
	case nemesisLeaderKill:
		// SIGTERM, matching stop-restart: the point of this mode is the
		// leaderless window, not a torn connection, and using the graceful stop
		// keeps indeterminate writes down so a linearizability failure is
		// attributable to the election rather than to unknown-outcome writes.
		downCmd = "stop"
	default:
		return nil, fmt.Errorf("unknown nemesis mode %q (want %s)",
			mode, strings.Join(nemesisModes, ", "))
	}
	if strings.TrimSpace(composeFile) == "" {
		return nil, fmt.Errorf("--nemesis-compose-file must not be empty")
	}
	return &composeNemesis{mode: mode, file: composeFile, downCmd: downCmd, run: execDocker}, nil
}

func (n *composeNemesis) Mode() string { return n.mode }

func (n *composeNemesis) Disrupt(ctx context.Context, victim string) error {
	_, err := n.compose(ctx, n.downCmd, victim)
	return err
}

func (n *composeNemesis) Heal(ctx context.Context, victim string) error {
	_, err := n.compose(ctx, "start", victim)
	return err
}

// Preflight checks that the docker daemon answers and that every victim names a
// service the compose file actually defines. A misspelled service would
// otherwise fail on every strike and leave the run reporting PASS against a
// cluster nothing ever touched.
func (n *composeNemesis) Preflight(ctx context.Context, victims []string) error {
	if _, err := n.compose(ctx, "ps"); err != nil {
		return err
	}
	out, err := n.definedServices(ctx)
	if err != nil {
		return err
	}
	defined := out
	var unknown []string
	for _, v := range victims {
		if !slices.Contains(defined, v) {
			unknown = append(unknown, v)
		}
	}
	if len(unknown) > 0 {
		return fmt.Errorf("compose file %s defines no service %s (defined: %s)",
			n.file, strings.Join(unknown, ", "), strings.Join(defined, ", "))
	}
	return nil
}

// definedServices lists the services the compose file declares. Split out of
// Preflight because leader-kill needs the same list to check that the node ID
// the cluster reports as leader is a service this nemesis can address.
func (n *composeNemesis) definedServices(ctx context.Context) ([]string, error) {
	out, err := n.compose(ctx, "config", "--services")
	if err != nil {
		return nil, err
	}
	return parseServiceList(string(out)), nil
}

// compose runs `docker compose -f <file> <args...>`.
func (n *composeNemesis) compose(ctx context.Context, args ...string) ([]byte, error) {
	full := append([]string{"compose", "-f", n.file}, args...)
	cctx, cancel := context.WithTimeout(ctx, nemesisCommandTimeout)
	defer cancel()

	out, err := n.run(cctx, full...)
	if err != nil {
		return out, fmt.Errorf("docker %s: %w: %s",
			strings.Join(full, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// execDocker runs the docker CLI and returns its combined output.
func execDocker(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

// parseServiceList splits `docker compose config --services` output, which is
// one service name per line.
func parseServiceList(out string) []string {
	var names []string
	for _, line := range strings.Split(out, "\n") {
		if s := strings.TrimSpace(line); s != "" {
			names = append(names, s)
		}
	}
	return names
}

// nemesisConfig is the validated result of the --nemesis* flag set.
// Mode == nemesisNone means no fault injection.
type nemesisConfig struct {
	Mode        string
	Services    []string
	Interval    time.Duration
	Downtime    time.Duration
	ComposeFile string
}

// Enabled reports whether fault injection is configured.
func (c nemesisConfig) Enabled() bool { return c.Mode != nemesisNone }

// Describe renders the one-line summary used in the report.
func (c nemesisConfig) Describe() string {
	if !c.Enabled() {
		return nemesisNone
	}
	if c.Mode == nemesisLeaderKill {
		// "among" rather than "on": the victim is the current leader, and the
		// service list is the fence the kill must stay inside, not the set the
		// faults are spread over.
		return fmt.Sprintf("%s among [%s] interval=%s downtime=%s",
			c.Mode, strings.Join(c.Services, ","), c.Interval, c.Downtime)
	}
	return fmt.Sprintf("%s on [%s] interval=%s downtime=%s",
		c.Mode, strings.Join(c.Services, ","), c.Interval, c.Downtime)
}

// parseNemesisFlags validates the --nemesis* flags. Any error it returns is a
// startup misconfiguration and exits 3.
func parseNemesisFlags(mode, services, composeFile string, interval, downtime time.Duration) (nemesisConfig, error) {
	mode = strings.TrimSpace(mode)
	victims := splitServices(services)

	if mode == nemesisNone {
		// Setting victims without enabling a nemesis silently injects nothing,
		// which is the one failure mode that looks like a passing chaos run.
		if len(victims) > 0 {
			return nemesisConfig{}, fmt.Errorf("--nemesis-services is set but --nemesis is %s: no faults would be injected", nemesisNone)
		}
		return nemesisConfig{Mode: nemesisNone}, nil
	}
	if mode != nemesisKillRestart && mode != nemesisStopRestart && mode != nemesisLeaderKill {
		return nemesisConfig{}, fmt.Errorf("unknown --nemesis %q (want %s)",
			mode, strings.Join(nemesisModes, ", "))
	}
	if len(victims) == 0 {
		return nemesisConfig{}, fmt.Errorf("--nemesis=%s requires --nemesis-services (comma-separated compose service names)", mode)
	}
	if interval <= 0 {
		return nemesisConfig{}, fmt.Errorf("--nemesis-interval must be > 0, got %s", interval)
	}
	if downtime <= 0 {
		return nemesisConfig{}, fmt.Errorf("--nemesis-downtime must be > 0, got %s", downtime)
	}
	if strings.TrimSpace(composeFile) == "" {
		return nemesisConfig{}, fmt.Errorf("--nemesis-compose-file must not be empty")
	}
	return nemesisConfig{
		Mode:        mode,
		Services:    victims,
		Interval:    interval,
		Downtime:    downtime,
		ComposeFile: composeFile,
	}, nil
}

// splitServices parses a comma-separated service list, dropping blanks and
// duplicates while preserving order.
func splitServices(csv string) []string {
	var out []string
	for _, part := range strings.Split(csv, ",") {
		s := strings.TrimSpace(part)
		if s == "" || slices.Contains(out, s) {
			continue
		}
		out = append(out, s)
	}
	return out
}
