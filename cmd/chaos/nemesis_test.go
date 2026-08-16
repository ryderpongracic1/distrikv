package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	clientpkg "github.com/ryderpongracic1/distrikv/internal/client"
	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

// ---------------------------------------------------------------------------
// fake nemesis
// ---------------------------------------------------------------------------

// fakeNemesis records the calls made against it so the scheduler's ordering and
// shutdown guarantees can be asserted without docker.
type fakeNemesis struct {
	mu    sync.Mutex
	calls []string

	preflightErr error
	disruptErr   error
	healErr      error

	// onDisrupt runs inside Disrupt, before it returns. Tests use it to cancel
	// the run mid-outage or to panic.
	onDisrupt func()
	// onDisruptCtx is like onDisrupt but receives Disrupt's own context, so a
	// test can observe whether that context is detached from the run context.
	onDisruptCtx func(context.Context)
	// healCtxCancelled records whether any heal ran on an already-cancelled
	// context, which would mean the shutdown heal was not detached from the run.
	healCtxCancelled bool
}

func (f *fakeNemesis) Mode() string { return "fake" }

func (f *fakeNemesis) Preflight(_ context.Context, _ []string) error {
	f.record("preflight")
	return f.preflightErr
}

func (f *fakeNemesis) Disrupt(ctx context.Context, victim string) error {
	f.record("disrupt:" + victim)
	if f.onDisrupt != nil {
		f.onDisrupt()
	}
	if f.onDisruptCtx != nil {
		f.onDisruptCtx(ctx)
	}
	return f.disruptErr
}

func (f *fakeNemesis) Heal(ctx context.Context, victim string) error {
	f.mu.Lock()
	f.calls = append(f.calls, "heal:"+victim)
	f.healCtxCancelled = f.healCtxCancelled || ctx.Err() != nil
	f.mu.Unlock()
	return f.healErr
}

func (f *fakeNemesis) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *fakeNemesis) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

// ---------------------------------------------------------------------------
// scheduler
// ---------------------------------------------------------------------------

func TestSchedulerAlternatesDisruptAndHeal(t *testing.T) {
	fake := &fakeNemesis{}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  []string{"node2", "node3"},
		Downtime: 2 * time.Millisecond,
		Interval: time.Millisecond,
		Rand:     rand.New(rand.NewSource(1)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	calls := fake.snapshot()
	if len(calls) < 4 {
		t.Fatalf("expected several disrupt/heal cycles, got %v", calls)
	}
	if len(calls)%2 != 0 {
		t.Fatalf("calls must pair up disrupt with heal, got %d: %v", len(calls), calls)
	}
	for i, call := range calls {
		wantPrefix := "disrupt:"
		if i%2 == 1 {
			wantPrefix = "heal:"
		}
		if !strings.HasPrefix(call, wantPrefix) {
			t.Fatalf("call %d = %q, want prefix %q (full: %v)", i, call, wantPrefix, calls)
		}
		if i%2 == 1 && strings.TrimPrefix(call, "heal:") != strings.TrimPrefix(calls[i-1], "disrupt:") {
			t.Fatalf("heal at %d does not match preceding disrupt: %v", i, calls)
		}
	}

	for i, w := range s.Windows() {
		if !w.Healed() {
			t.Errorf("window %d (%s) was left unhealed", i, w.Victim)
		}
		if w.Down() <= 0 {
			t.Errorf("window %d has non-positive downtime %s", i, w.Down())
		}
	}
}

func TestSchedulerHealsWhenCancelledMidOutage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fake := &fakeNemesis{onDisrupt: cancel}
	s := &Scheduler{
		Nemesis: fake,
		Victims: []string{"node2"},
		// Long enough that the run can only end via cancellation.
		Downtime: time.Hour,
		Interval: time.Hour,
		Rand:     rand.New(rand.NewSource(1)),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if got, want := fake.snapshot(), []string{"disrupt:node2", "heal:node2"}; !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	windows := s.Windows()
	if len(windows) != 1 {
		t.Fatalf("expected 1 fault window, got %d", len(windows))
	}
	if !windows[0].Healed() {
		t.Error("a run cancelled mid-outage must still heal its victim")
	}
	if fake.healCtxCancelled {
		t.Error("heal ran on a cancelled context; it must be detached from the run context")
	}
}

func TestSchedulerHealsAfterPanic(t *testing.T) {
	fake := &fakeNemesis{onDisrupt: func() { panic("nemesis exploded") }}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  []string{"node2"},
		Downtime: time.Millisecond,
		Interval: time.Millisecond,
		Rand:     rand.New(rand.NewSource(1)),
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected the panic to propagate out of Run")
			}
		}()
		s.Run(context.Background())
	}()

	if got, want := fake.snapshot(), []string{"disrupt:node2", "heal:node2"}; !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v — a panicking run must still heal", got, want)
	}
}

func TestSchedulerHealsEvenWhenDisruptFails(t *testing.T) {
	fake := &fakeNemesis{disruptErr: errors.New("boom")}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  []string{"node2"},
		Downtime: time.Millisecond,
		Interval: time.Hour, // one cycle only
		Rand:     rand.New(rand.NewSource(1)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	windows := s.Windows()
	if len(windows) != 1 {
		t.Fatalf("expected 1 fault window, got %d", len(windows))
	}
	if windows[0].Injected() {
		t.Error("a window whose disrupt failed must not report Injected")
	}
	if windows[0].DisruptErr != "boom" {
		t.Errorf("DisruptErr = %q, want %q", windows[0].DisruptErr, "boom")
	}
	// A failed disrupt may still have partially landed, so heal must run.
	if !slices.Contains(fake.snapshot(), "heal:node2") {
		t.Errorf("heal was not attempted after a failed disrupt: %v", fake.snapshot())
	}
}

func TestSchedulerRecordsHealFailure(t *testing.T) {
	fake := &fakeNemesis{healErr: errors.New("start refused")}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  []string{"node2"},
		Downtime: time.Millisecond,
		Interval: time.Hour,
		Rand:     rand.New(rand.NewSource(1)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	windows := s.Windows()
	if len(windows) != 1 {
		t.Fatalf("expected 1 fault window, got %d", len(windows))
	}
	if windows[0].HealErr != "start refused" {
		t.Errorf("HealErr = %q, want %q", windows[0].HealErr, "start refused")
	}
	if windows[0].Healed() {
		t.Error("a window whose heal failed must not report Healed")
	}
	// The victim did not come back, so there is no up-at time to report. A
	// timestamp here would render as a completed outage for a node still down.
	if !windows[0].UpAt.IsZero() {
		t.Errorf("UpAt = %v, want zero for a failed heal", windows[0].UpAt)
	}
	if windows[0].Down() != 0 {
		t.Errorf("Down() = %s, want 0 for a victim that never returned", windows[0].Down())
	}

	// The JSON and table reports must both say so.
	start := windows[0].DownAt
	r := faultWindowReports(windows, start)[0]
	if r.UpAt != nil || r.UpAtOffsetMs != nil || r.DownMs != nil {
		t.Errorf("failed heal reported up_at=%v offset=%v down_ms=%v, want all null",
			r.UpAt, r.UpAtOffsetMs, r.DownMs)
	}
	if line := formatFaultWindows(windows, start)[0]; !strings.Contains(line, "NEVER HEALED") {
		t.Errorf("table line %q should say NEVER HEALED", line)
	}
}

func TestSchedulerVictimsComeOnlyFromTheConfiguredSet(t *testing.T) {
	fake := &fakeNemesis{}
	victims := []string{"node2", "node3"}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  victims,
		Downtime: time.Millisecond,
		Interval: time.Millisecond,
		Rand:     rand.New(rand.NewSource(7)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	windows := s.Windows()
	if len(windows) == 0 {
		t.Fatal("expected at least one fault window")
	}
	var prev time.Time
	for i, w := range windows {
		if !slices.Contains(victims, w.Victim) {
			t.Errorf("window %d victim %q is not in %v", i, w.Victim, victims)
		}
		if w.DownAt.Before(prev) {
			t.Errorf("window %d starts before the previous window ended", i)
		}
		prev = w.UpAt
	}
}

func TestSchedulerNoopWithoutVictimsOrNemesis(t *testing.T) {
	fake := &fakeNemesis{}
	noVictims := &Scheduler{Nemesis: fake, Downtime: time.Millisecond, Interval: time.Millisecond}
	noVictims.Run(context.Background())
	if calls := fake.snapshot(); len(calls) != 0 {
		t.Errorf("scheduler with no victims made calls: %v", calls)
	}
	if len(noVictims.Windows()) != 0 {
		t.Error("scheduler with no victims recorded fault windows")
	}

	// A nil Nemesis must not panic.
	(&Scheduler{Victims: []string{"node2"}}).Run(context.Background())
}

func TestSchedulerDoesNotStrikeAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fake := &fakeNemesis{}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  []string{"node2"},
		Downtime: time.Millisecond,
		Interval: time.Millisecond,
		Rand:     rand.New(rand.NewSource(1)),
	}
	s.Run(ctx)

	if calls := fake.snapshot(); len(calls) != 0 {
		t.Errorf("an already-cancelled run must not strike, got %v", calls)
	}
	if len(s.Windows()) != 0 {
		t.Error("an already-cancelled run must not record a fault window")
	}
}

// TestSchedulerDisruptIsDetachedFromTheRunContext pins why a strike is not
// cancelled by the run deadline: a docker command killed halfway may already
// have stopped the container, so an interrupted strike would record a failure
// whose real outcome is unknown. Letting it finish keeps DisruptErr meaningful.
func TestSchedulerDisruptIsDetachedFromTheRunContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var disruptCtxCancelled bool
	fake := &fakeNemesis{}
	fake.onDisruptCtx = func(dctx context.Context) {
		// Cancel the run while the strike is in progress, then observe whether
		// the strike's own context was taken down with it.
		cancel()
		disruptCtxCancelled = dctx.Err() != nil
	}
	s := &Scheduler{
		Nemesis:  fake,
		Victims:  []string{"node2"},
		Downtime: time.Hour,
		Interval: time.Hour,
		Rand:     rand.New(rand.NewSource(1)),
	}
	s.Run(ctx)

	if disruptCtxCancelled {
		t.Error("Disrupt ran on the run context; it must be detached so an interrupted strike is not misreported")
	}
	windows := s.Windows()
	if len(windows) != 1 || !windows[0].Injected() {
		t.Fatalf("expected one injected window, got %+v", windows)
	}
	if !windows[0].Healed() {
		t.Error("the victim must still be healed after a cancelled run")
	}
}

func TestSleepReportsCancellation(t *testing.T) {
	if !sleep(context.Background(), time.Millisecond) {
		t.Error("sleep should report true when it completes")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleep(ctx, time.Hour) {
		t.Error("sleep should report false when the context is done")
	}
	if sleep(ctx, 0) {
		t.Error("sleep with a zero duration should still observe a done context")
	}
}

// ---------------------------------------------------------------------------
// docker-compose nemesis
// ---------------------------------------------------------------------------

// recordingDocker captures the argv of each docker invocation.
type recordingDocker struct {
	argv    [][]string
	outputs []string
	err     error
}

func (r *recordingDocker) run(ctx context.Context, args ...string) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("docker invocation must carry a deadline")
	}
	r.argv = append(r.argv, args)
	var out string
	if len(r.outputs) > 0 {
		out, r.outputs = r.outputs[0], r.outputs[1:]
	}
	return []byte(out), r.err
}

func TestComposeNemesisBuildsExpectedArgv(t *testing.T) {
	for _, tc := range []struct {
		mode        string
		wantDisrupt []string
	}{
		{nemesisKillRestart, []string{"compose", "-f", "docker/docker-compose.yml", "kill", "node2"}},
		{nemesisStopRestart, []string{"compose", "-f", "docker/docker-compose.yml", "stop", "node2"}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			nem, err := newComposeNemesis(tc.mode, "docker/docker-compose.yml")
			if err != nil {
				t.Fatalf("newComposeNemesis: %v", err)
			}
			rec := &recordingDocker{}
			nem.run = rec.run

			if err := nem.Disrupt(context.Background(), "node2"); err != nil {
				t.Fatalf("Disrupt: %v", err)
			}
			if err := nem.Heal(context.Background(), "node2"); err != nil {
				t.Fatalf("Heal: %v", err)
			}
			if nem.Mode() != tc.mode {
				t.Errorf("Mode() = %q, want %q", nem.Mode(), tc.mode)
			}
			wantHeal := []string{"compose", "-f", "docker/docker-compose.yml", "start", "node2"}
			if !slices.Equal(rec.argv[0], tc.wantDisrupt) {
				t.Errorf("disrupt argv = %v, want %v", rec.argv[0], tc.wantDisrupt)
			}
			if !slices.Equal(rec.argv[1], wantHeal) {
				t.Errorf("heal argv = %v, want %v", rec.argv[1], wantHeal)
			}
		})
	}
}

func TestComposeNemesisWrapsFailuresWithTheCommand(t *testing.T) {
	nem, err := newComposeNemesis(nemesisKillRestart, "docker/docker-compose.yml")
	if err != nil {
		t.Fatalf("newComposeNemesis: %v", err)
	}
	nem.run = (&recordingDocker{err: errors.New("exit status 1"), outputs: []string{"no such service: nodeX"}}).run

	err = nem.Disrupt(context.Background(), "nodeX")
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"docker compose -f docker/docker-compose.yml kill nodeX", "exit status 1", "no such service"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestComposeNemesisPreflight(t *testing.T) {
	newNem := func(t *testing.T, rec *recordingDocker) *composeNemesis {
		t.Helper()
		nem, err := newComposeNemesis(nemesisKillRestart, "docker/docker-compose.yml")
		if err != nil {
			t.Fatalf("newComposeNemesis: %v", err)
		}
		nem.run = rec.run
		return nem
	}

	t.Run("accepts defined services", func(t *testing.T) {
		rec := &recordingDocker{outputs: []string{"", "node1\nnode2\nnode3\n"}}
		if err := newNem(t, rec).Preflight(context.Background(), []string{"node2", "node3"}); err != nil {
			t.Fatalf("Preflight: %v", err)
		}
		want := [][]string{
			{"compose", "-f", "docker/docker-compose.yml", "ps"},
			{"compose", "-f", "docker/docker-compose.yml", "config", "--services"},
		}
		for i := range want {
			if !slices.Equal(rec.argv[i], want[i]) {
				t.Errorf("argv[%d] = %v, want %v", i, rec.argv[i], want[i])
			}
		}
	})

	t.Run("rejects an undefined service", func(t *testing.T) {
		rec := &recordingDocker{outputs: []string{"", "node1\nnode2\nnode3\n"}}
		err := newNem(t, rec).Preflight(context.Background(), []string{"node2", "nodeX"})
		if err == nil {
			t.Fatal("expected an error for a service the compose file does not define")
		}
		if !strings.Contains(err.Error(), "nodeX") {
			t.Errorf("error %q should name the unknown service", err)
		}
	})

	t.Run("surfaces an unreachable daemon", func(t *testing.T) {
		rec := &recordingDocker{err: errors.New("cannot connect to the docker daemon")}
		err := newNem(t, rec).Preflight(context.Background(), []string{"node2"})
		if err == nil {
			t.Fatal("expected an error when docker is unavailable")
		}
		if len(rec.argv) != 1 {
			t.Errorf("preflight should stop after the first failure, ran %d commands", len(rec.argv))
		}
	})
}

func TestNewComposeNemesisRejectsBadInput(t *testing.T) {
	if _, err := newComposeNemesis("partition", "docker/docker-compose.yml"); err == nil {
		t.Error("expected an error for an unknown mode")
	}
	if _, err := newComposeNemesis(nemesisKillRestart, "  "); err == nil {
		t.Error("expected an error for an empty compose file")
	}
}

func TestParseServiceList(t *testing.T) {
	got := parseServiceList("node1\n node2 \n\nnode3\n")
	if want := []string{"node1", "node2", "node3"}; !slices.Equal(got, want) {
		t.Errorf("parseServiceList = %v, want %v", got, want)
	}
	if got := parseServiceList("\n \n"); len(got) != 0 {
		t.Errorf("parseServiceList of blank input = %v, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// flags
// ---------------------------------------------------------------------------

func TestParseNemesisFlags(t *testing.T) {
	const file = "docker/docker-compose.yml"

	tests := []struct {
		name      string
		mode      string
		services  string
		file      string
		interval  time.Duration
		downtime  time.Duration
		wantErr   string
		wantMode  string
		wantHosts []string
	}{
		{
			name: "default is disabled", mode: nemesisNone, file: file,
			interval: 10 * time.Second, downtime: 5 * time.Second,
			wantMode: nemesisNone,
		},
		{
			name: "kill-restart", mode: nemesisKillRestart, services: "node2,node3", file: file,
			interval: 10 * time.Second, downtime: 5 * time.Second,
			wantMode: nemesisKillRestart, wantHosts: []string{"node2", "node3"},
		},
		{
			name: "stop-restart", mode: nemesisStopRestart, services: " node2 , node2 ,node3 ", file: file,
			interval: time.Second, downtime: time.Second,
			wantMode: nemesisStopRestart, wantHosts: []string{"node2", "node3"},
		},
		{
			name: "services without a nemesis is a misconfiguration",
			mode: nemesisNone, services: "node2", file: file,
			interval: 10 * time.Second, downtime: 5 * time.Second,
			wantErr: "--nemesis-services is set but --nemesis is none",
		},
		{
			name: "unknown mode", mode: "partition", services: "node2", file: file,
			interval: 10 * time.Second, downtime: 5 * time.Second,
			wantErr: "unknown --nemesis",
		},
		{
			name: "enabled without services", mode: nemesisKillRestart, file: file,
			interval: 10 * time.Second, downtime: 5 * time.Second,
			wantErr: "requires --nemesis-services",
		},
		{
			name: "non-positive interval", mode: nemesisKillRestart, services: "node2", file: file,
			interval: 0, downtime: 5 * time.Second,
			wantErr: "--nemesis-interval must be > 0",
		},
		{
			name: "non-positive downtime", mode: nemesisKillRestart, services: "node2", file: file,
			interval: 10 * time.Second, downtime: -time.Second,
			wantErr: "--nemesis-downtime must be > 0",
		},
		{
			name: "empty compose file", mode: nemesisKillRestart, services: "node2", file: "",
			interval: 10 * time.Second, downtime: 5 * time.Second,
			wantErr: "--nemesis-compose-file must not be empty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseNemesisFlags(tc.mode, tc.services, tc.file, tc.interval, tc.downtime)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Mode != tc.wantMode {
				t.Errorf("Mode = %q, want %q", cfg.Mode, tc.wantMode)
			}
			if !slices.Equal(cfg.Services, tc.wantHosts) {
				t.Errorf("Services = %v, want %v", cfg.Services, tc.wantHosts)
			}
			if cfg.Enabled() != (tc.wantMode != nemesisNone) {
				t.Errorf("Enabled() = %v for mode %q", cfg.Enabled(), cfg.Mode)
			}
		})
	}
}

func TestNemesisConfigDescribe(t *testing.T) {
	if got := (nemesisConfig{Mode: nemesisNone}).Describe(); got != nemesisNone {
		t.Errorf("Describe() = %q, want %q", got, nemesisNone)
	}
	cfg := nemesisConfig{
		Mode: nemesisKillRestart, Services: []string{"node2", "node3"},
		Interval: 10 * time.Second, Downtime: 5 * time.Second,
	}
	want := "kill-restart on [node2,node3] interval=10s downtime=5s"
	if got := cfg.Describe(); got != want {
		t.Errorf("Describe() = %q, want %q", got, want)
	}
}

func TestSplitServices(t *testing.T) {
	if got := splitServices(""); len(got) != 0 {
		t.Errorf("splitServices(\"\") = %v, want empty", got)
	}
	if got, want := splitServices("a, b ,,a,c"), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("splitServices = %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

func TestFaultWindowReports(t *testing.T) {
	start := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	windows := []FaultWindow{
		{Victim: "node2", DownAt: start.Add(time.Second), UpAt: start.Add(6 * time.Second)},
		{Victim: "node3", DownAt: start.Add(20 * time.Second), DisruptErr: "boom", HealErr: "start refused"},
	}

	if faultWindowReports(nil, start) != nil {
		t.Error("no windows should produce a nil slice so the JSON field is omitted")
	}

	got := faultWindowReports(windows, start)
	if len(got) != 2 {
		t.Fatalf("expected 2 reports, got %d", len(got))
	}

	landed := got[0]
	if landed.Victim != "node2" || landed.DownAtOffsetMs != 1000 {
		t.Errorf("healed window rendered as %+v", landed)
	}
	if !landed.Injected || !landed.Healed {
		t.Errorf("a landed, healed window reported Injected=%v Healed=%v", landed.Injected, landed.Healed)
	}
	if landed.UpAt == nil || *landed.UpAtOffsetMs != 6000 {
		t.Errorf("UpAtOffsetMs = %v, want 6000", landed.UpAtOffsetMs)
	}
	if landed.DownMs == nil || *landed.DownMs != 5000 {
		t.Errorf("DownMs = %v, want 5000", landed.DownMs)
	}

	phantom := got[1]
	if phantom.Injected {
		t.Error("a window whose disrupt failed must not report Injected")
	}
	if phantom.Healed {
		t.Errorf("unhealed window rendered as %+v", phantom)
	}
	// All three absence fields must be null together — a 0 would read as an
	// instantaneous outage, and a missing up_at with a present down_ms would
	// make a consumer handle two encodings of one fact.
	if phantom.UpAt != nil || phantom.DownMs != nil || phantom.UpAtOffsetMs != nil {
		t.Errorf("unhealed window must report null up_at/offset/duration, got %v/%v/%v",
			phantom.UpAt, phantom.UpAtOffsetMs, phantom.DownMs)
	}
	if phantom.DisruptError != "boom" || phantom.HealError != "start refused" {
		t.Errorf("unhealed window lost its errors: %+v", phantom)
	}
}

func TestCountInjectedExcludesFailedStrikes(t *testing.T) {
	windows := []FaultWindow{
		{Victim: "node2"},
		{Victim: "node3", DisruptErr: "boom"},
		{Victim: "node2"},
	}
	if got, want := countInjected(windows), 2; got != want {
		t.Errorf("countInjected = %d, want %d — a failed disrupt is not an outage", got, want)
	}
	if countInjected(nil) != 0 {
		t.Error("countInjected(nil) should be 0")
	}
}

func TestFormatFaultWindows(t *testing.T) {
	start := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	lines := formatFaultWindows([]FaultWindow{
		{Victim: "node2", DownAt: start.Add(time.Second), UpAt: start.Add(6 * time.Second)},
		{Victim: "node3", DownAt: start.Add(20 * time.Second), DisruptErr: "boom"},
	}, start)

	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{"#1", "node2", "+1s", "+6s", "5s"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("line %q does not contain %q", lines[0], want)
		}
	}
	for _, want := range []string{"#2", "node3", "NEVER HEALED", "disrupt error: boom"} {
		if !strings.Contains(lines[1], want) {
			t.Errorf("line %q does not contain %q", lines[1], want)
		}
	}
	if got := formatFaultWindows(nil, start); len(got) != 0 {
		t.Errorf("formatFaultWindows(nil) = %v, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// write classification
// ---------------------------------------------------------------------------

func TestClassifyDeleteErr(t *testing.T) {
	// A 404 means the key was already absent, which leaves the store in exactly
	// the state a successful delete would — record it as applied.
	if got := classifyDeleteErr(clientpkg.ErrNotFound); got != nil {
		t.Errorf("classifyDeleteErr(404) = %v, want nil", got)
	}
	if got := classifyDeleteErr(fmt.Errorf("delete: %w", clientpkg.ErrNotFound)); got != nil {
		t.Errorf("classifyDeleteErr(wrapped 404) = %v, want nil", got)
	}
	if classifyDeleteErr(nil) != nil {
		t.Error("classifyDeleteErr(nil) should stay nil")
	}
	real := errors.New("node unreachable: EOF")
	if got := classifyDeleteErr(real); got != real {
		t.Errorf("classifyDeleteErr passed through %v as %v; a real failure must survive", real, got)
	}
}

// TestProvablyNeverSentUsesTypedErrors covers the classifier the trustworthiness
// of indeterminate_writes rests on. The typed path is the contract; the
// substring path behind it is a last resort for errors that arrive as text with
// no chain left to inspect.
func TestProvablyNeverSentUsesTypedErrors(t *testing.T) {
	t.Run("typed, chain preserved", func(t *testing.T) {
		refused := &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8002},
			Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
		}
		if !provablyNeverSent(refused) {
			t.Error("a real ECONNREFUSED OpError must classify as never sent")
		}
		if writeIsIndeterminate(refused) {
			t.Error("a refused write is provably a no-op, not indeterminate")
		}

		dnsMiss := &net.DNSError{Err: "server misbehaving", Name: "nodeX", IsNotFound: true}
		if !provablyNeverSent(dnsMiss) {
			t.Error("an unresolvable host must classify as never sent")
		}
		// A DNS error that is not a name miss (a timeout, say) leaves the
		// outcome unknown as far as this classifier is concerned.
		dnsTimeout := &net.DNSError{Err: "i/o timeout", Name: "node2", IsTimeout: true}
		if provablyNeverSent(dnsTimeout) {
			t.Error("a DNS timeout is not evidence the request was never sent")
		}

		sent := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
		if provablyNeverSent(sent) {
			t.Error("a reset mid-request is not evidence the request was never sent")
		}
	})

	t.Run("substring fallback, no chain to inspect", func(t *testing.T) {
		// An error that reached the classifier as text only — one that crossed
		// a process boundary, or came from outside this repo. internal/client
		// no longer produces this shape (see
		// TestProvablyNeverSentClassifiesRealClientErrors), but the fallback
		// still has to carry it.
		textOnly := errors.New("dial tcp 127.0.0.1:8002: connect: connection refused")
		if errors.Is(textOnly, syscall.ECONNREFUSED) {
			t.Fatal("test setup error: this error is supposed to have no chain")
		}
		if !provablyNeverSent(textOnly) {
			t.Error("the substring fallback must still classify a chainless refused dial")
		}
	})
}

// opaqueError keeps err reachable through Unwrap while replacing its message
// with text that matches none of the fallback substrings. A classification that
// survives this wrapper can only have come from the typed path.
type opaqueError struct{ err error }

func (o opaqueError) Error() string { return "write failed" }
func (o opaqueError) Unwrap() error { return o.err }

// TestProvablyNeverSentClassifiesRealClientErrors is the end-to-end proof that
// the typed path now covers client-originated errors.
//
// internal/client used to wrap dial failures as fmt.Errorf("%w: %v",
// ErrUnreachable, urlErr.Err): the %v rendered the transport error as text, so
// ErrUnreachable was the deepest identity in the chain and this classifier could
// only recognise a refused connection by reading the message. Now that the
// client joins the cause with a second %w, errors.Is and errors.As reach through
// to the syscall — asserted here against a real refused dial rather than a
// hand-built error, so the two packages' contracts are checked together.
func TestProvablyNeverSentClassifiesRealClientErrors(t *testing.T) {
	// Reserve a port, then release it, so connections to it are refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	host := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}

	c := clientpkg.New(clientpkg.Config{Host: host, Timeout: 2 * time.Second})
	ctx := context.Background()

	writes := map[string]func() error{
		"put":    func() error { return c.Put(ctx, "k", "v") },
		"delete": func() error { return classifyDeleteErr(c.Delete(ctx, "k")) },
	}

	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			err := write()
			if err == nil {
				t.Fatal("a write to a closed port must fail")
			}

			// The sentinel still classifies it...
			if !errors.Is(err, clientpkg.ErrUnreachable) {
				t.Errorf("errors.Is(err, ErrUnreachable) = false for %v", err)
			}
			// ...and the cause is reachable through the sentinel, by value...
			if !errors.Is(err, syscall.ECONNREFUSED) {
				t.Errorf("errors.Is(err, ECONNREFUSED) = false for %v; the client flattened the chain", err)
			}
			// ...and by type.
			var opErr *net.OpError
			if !errors.As(err, &opErr) {
				t.Errorf("errors.As(err, **net.OpError) = false for %v; the client flattened the chain", err)
			} else if opErr.Op != "dial" {
				t.Errorf("opErr.Op = %q, want \"dial\"", opErr.Op)
			}

			if !provablyNeverSent(err) {
				t.Errorf("a refused client write must classify as never sent, got indeterminate for %v", err)
			}
			if writeIsIndeterminate(err) {
				t.Errorf("a refused client write is provably a no-op, not indeterminate: %v", err)
			}

			// The classification must come from the chain, not the message: hide
			// the text and it still holds.
			hidden := opaqueError{err: err}
			if strings.Contains(hidden.Error(), "refused") {
				t.Fatal("test setup error: the opaque message still names the cause")
			}
			if !provablyNeverSent(hidden) {
				t.Error("classification fell back to substring matching; the typed path is not doing the work")
			}
		})
	}
}

func TestWriteIsIndeterminate(t *testing.T) {
	// Message-only errors, so these rows exercise the substring fallback and the
	// 404/nil short-circuits. The typed path against a real client error is
	// covered by TestProvablyNeverSentClassifiesRealClientErrors.
	tests := []struct {
		err  error
		want bool
		why  string
	}{
		{nil, false, "no error"},
		{clientpkg.ErrNotFound, false, "404 on delete: the key was already absent, which is an outcome, not an unknown"},
		{fmt.Errorf("delete: %w", clientpkg.ErrNotFound), false, "wrapped 404 must classify the same"},
		{fmt.Errorf("node unreachable: dial tcp 127.0.0.1:8002: connect: connection refused"), false, "refused → never reached a server"},
		{fmt.Errorf("node unreachable: dial tcp: lookup nodeX: no such host"), false, "unresolvable → never sent"},
		{fmt.Errorf("node unreachable: dial tcp 10.0.0.1:8002: connect: no route to host"), false, "unroutable → never sent"},
		{fmt.Errorf("node unreachable: EOF"), true, "connection died after the request was sent"},
		{fmt.Errorf("context deadline exceeded (Client.Timeout exceeded while awaiting headers)"), true, "timeout → outcome unknown"},
		{fmt.Errorf("node unreachable: read tcp 127.0.0.1:8002: connection reset by peer"), true, "reset mid-request → outcome unknown"},
		{fmt.Errorf("server error: 500"), true, "5xx → the server may have applied the write"},
	}
	for _, tc := range tests {
		if got := writeIsIndeterminate(tc.err); got != tc.want {
			t.Errorf("writeIsIndeterminate(%v) = %v, want %v (%s)", tc.err, got, tc.want, tc.why)
		}
	}
}

func TestFinishWriteRecordsFailuresAsErrEvenWhenCancelled(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name              string
		ctx               context.Context
		err               error
		wantErrOut        bool
		wantErrors        int64
		wantIndeterminate int64
	}{
		{"success", context.Background(), nil, false, 0, 0},
		{
			name: "refused write", ctx: context.Background(),
			err:        fmt.Errorf("node unreachable: connect: connection refused"),
			wantErrOut: true, wantErrors: 1, wantIndeterminate: 0,
		},
		{
			name: "indeterminate write", ctx: context.Background(),
			err:        fmt.Errorf("node unreachable: EOF"),
			wantErrOut: true, wantErrors: 1, wantIndeterminate: 1,
		},
		{
			// A write cut short by the run's own shutdown must still be recorded
			// as Err — recording it as a success would assert it was applied —
			// but it is a shutdown artefact, not a cluster failure, so it is not
			// counted in the reported statistics.
			name: "cancelled write", ctx: cancelled,
			err:        context.Canceled,
			wantErrOut: true, wantErrors: 0, wantIndeterminate: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &linearizability.Recorder{}
			c := &counters{}
			cid := rec.Begin(linearizability.Input{Op: "put", Key: "k", Value: "v"})
			finishWrite(tc.ctx, rec, cid, tc.err, c)

			if rec.Len() != 2 {
				t.Fatalf("expected a call and a return event, got %d", rec.Len())
			}
			// A write recorded without Err asserts it was applied; the model
			// only tolerates an unknown write when Err is set.
			ok, timedOut := rec.CheckTimeout(5 * time.Second)
			if !ok || timedOut {
				t.Fatalf("recorded history should be checkable: ok=%v timedOut=%v", ok, timedOut)
			}
			if got := c.errors.Load(); got != tc.wantErrors {
				t.Errorf("errors = %d, want %d", got, tc.wantErrors)
			}
			if got := c.indeterminateWrites.Load(); got != tc.wantIndeterminate {
				t.Errorf("indeterminateWrites = %d, want %d", got, tc.wantIndeterminate)
			}
		})
	}
}

func TestFinishWriteToleratesANilRecorder(t *testing.T) {
	c := &counters{}
	finishWrite(context.Background(), nil, 0, fmt.Errorf("node unreachable: EOF"), c)
	if c.errors.Load() != 1 || c.indeterminateWrites.Load() != 1 {
		t.Errorf("warmup-mode counters not updated: errors=%d indeterminate=%d",
			c.errors.Load(), c.indeterminateWrites.Load())
	}
}

// TestMakeKeysAreRunScoped covers the reason the keyspace carries a nonce:
// KVModel.Init is an empty map, so a recorded history that reads a key already
// written by the unrecorded warmup phase — or by a previous run against the same
// persistent volume — is spuriously illegal.
func TestMakeKeysAreRunScoped(t *testing.T) {
	got := makeKeys("chaos-m123", 3)
	want := []string{"chaos-m123-00000", "chaos-m123-00001", "chaos-m123-00002"}
	if !slices.Equal(got, want) {
		t.Errorf("makeKeys = %v, want %v", got, want)
	}
	if len(makeKeys("chaos-m123", 0)) != 0 {
		t.Error("makeKeys with n=0 should be empty")
	}
	warm := makeKeys("chaos-w123", 3)
	for _, k := range warm {
		if slices.Contains(got, k) {
			t.Errorf("warmup key %q collides with the measured keyspace", k)
		}
	}
}

// ---------------------------------------------------------------------------
// model contract
// ---------------------------------------------------------------------------

// TestKVModelTreatsFailedOpsAsNoOps pins the model behaviour the nemesis relies
// on: an operation recorded with Output.Err is a no-op, so ops that fail inside
// a fault window cannot on their own make a history illegal.
func TestKVModelTreatsFailedOpsAsNoOps(t *testing.T) {
	step := linearizability.KVModel.Step
	initial := map[string]string{"k": "v0"}

	stateOf := func(t *testing.T, next any) map[string]string {
		t.Helper()
		m, ok := next.(map[string]string)
		if !ok {
			t.Fatalf("model returned %T, want map[string]string", next)
		}
		return m
	}

	t.Run("failed put does not change state", func(t *testing.T) {
		ok, next := step(initial, linearizability.Input{Op: "put", Key: "k", Value: "v1"}, linearizability.Output{Err: true})
		if !ok {
			t.Fatal("a failed put must be a legal transition")
		}
		if got := stateOf(t, next)["k"]; got != "v0" {
			t.Errorf("state[k] = %q after a failed put, want %q", got, "v0")
		}
	})

	t.Run("failed delete does not remove the key", func(t *testing.T) {
		ok, next := step(initial, linearizability.Input{Op: "delete", Key: "k"}, linearizability.Output{Err: true})
		if !ok {
			t.Fatal("a failed delete must be a legal transition")
		}
		if got := stateOf(t, next)["k"]; got != "v0" {
			t.Errorf("state[k] = %q after a failed delete, want %q", got, "v0")
		}
	})

	t.Run("failed get constrains nothing", func(t *testing.T) {
		ok, _ := step(initial, linearizability.Input{Op: "get", Key: "k"}, linearizability.Output{Err: true, Value: "anything"})
		if !ok {
			t.Fatal("a failed get must be a legal transition regardless of value")
		}
	})

	t.Run("successful ops still apply", func(t *testing.T) {
		ok, next := step(initial, linearizability.Input{Op: "put", Key: "k", Value: "v1"}, linearizability.Output{})
		if !ok {
			t.Fatal("a successful put must be legal")
		}
		if got := stateOf(t, next)["k"]; got != "v1" {
			t.Errorf("state[k] = %q after a successful put, want %q", got, "v1")
		}
		if got := stateOf(t, initial)["k"]; got != "v0" {
			t.Error("the model mutated the caller's state instead of cloning it")
		}
		if ok, _ := step(next, linearizability.Input{Op: "get", Key: "k"}, linearizability.Output{Value: "v0"}); ok {
			t.Error("a stale read must be rejected")
		}
	})
}

// TestKVModelRequiresAReturnForEveryCall documents why finishWrite always calls
// Recorder.End: porcupine treats a call with no matching return as a dead end
// rather than as an operation that may still be pending, so dropping the return
// of an in-flight write would report a false anomaly instead of an unknown one.
func TestKVModelRequiresAReturnForEveryCall(t *testing.T) {
	rec := &linearizability.Recorder{}
	rec.Begin(linearizability.Input{Op: "put", Key: "k", Value: "v"}) // deliberately never ended
	if ok, timedOut := rec.CheckTimeout(5 * time.Second); ok || timedOut {
		t.Fatalf("an unmatched call checked as ok=%v timedOut=%v; expected it to be rejected", ok, timedOut)
	}
}
