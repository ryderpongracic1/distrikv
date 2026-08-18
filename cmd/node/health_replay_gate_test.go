package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// The replay effect gate (defect 14).
//
// commitIndex and lastApplied are volatile (raft deviation 3), so a restarting
// node re-applies every committed entry above the snapshot boundary against a
// view that starts empty. Because absent means healthy, each historical down→up
// pair in that window reads as a fresh failure followed by a fresh recovery — so
// before the gate, a restart emitted an operator-facing "consensus marked peer
// unhealthy" for a peer that was fine, plus a recovery notification that had
// anti-entropy schedule a catch-up pass for a replica that never went anywhere,
// once per pair.
//
// What these tests pin is the distinction the fix rests on: the state a replayed
// entry produces is identical, and the announcements are not emitted. Idempotent
// state is not idempotent effects.

// replayGateSM builds a state machine whose log output the test can read, which
// is how the WARN assertions below work. Debug level so the suppression path's
// own line is visible too — the entry is not silently dropped, it is demoted.
func replayGateSM(t *testing.T, selfID string) (*HealthStateMachine, *metrics.Metrics, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	m := &metrics.Metrics{}
	sm := newHealthStateMachine(selfID, 2, m,
		slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return sm, m, &logs
}

// countUnhealthyWarnings counts the operator-facing "peer unhealthy" warnings in
// captured log output. That line is the expensive half of this defect: a restart
// that emits a burst of them for healthy peers teaches an operator to ignore the
// signal.
func countUnhealthyWarnings(logs *bytes.Buffer) int {
	n := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "consensus marked peer unhealthy") {
			n++
		}
	}
	return n
}

// TestHealthSMReplayDoesNotReannounceHistory is the defect-14 pin.
//
// The two halves are the same three-entry history — down(node2), up(node2),
// down(node3) — applied first as live commits and then, in a fresh state machine,
// as the replay a restart performs. The live half exists so the assertions are
// not vacuous: it proves this setup does observe an announcement when there is
// one to observe.
//
// Revert-check: drop the `if !live` branch from HealthStateMachine.apply, so a
// replayed transition falls through to the WARN and the channel send, and the
// replay half of this test fails on both counts.
func TestHealthSMReplayDoesNotReannounceHistory(t *testing.T) {
	ctx := context.Background()
	history := []raft.LogEntry{
		healthEntry(1, opHealthDown, "node2"),
		healthEntry(2, opHealthUp, "node2"),
		healthEntry(3, opHealthDown, "node3"),
	}

	// --- Live: this is what the history looked like when it happened.
	live, _, liveLogs := replayGateSM(t, "node1")
	for i, e := range history {
		if err := live.Apply(ctx, e); err != nil {
			t.Fatalf("live apply %d: %v", i, err)
		}
	}
	if got := drainRecovered(live.Recovered()); len(got) != 1 || got[0] != "node2" {
		t.Fatalf("live applies announced %v, want exactly one recovery for node2; "+
			"without this the replay assertions below prove nothing", got)
	}
	if got := countUnhealthyWarnings(liveLogs); got != 2 {
		t.Fatalf("live applies emitted %d unhealthy warnings, want 2 (node2 and node3)", got)
	}

	// --- Replay: the restart. A fresh state machine is the honest reproduction of
	// one, because the view is volatile and opens empty.
	replayed, _, replayLogs := replayGateSM(t, "node1")
	for i, e := range history {
		if err := replayed.ReplayApply(ctx, e); err != nil {
			t.Fatalf("replay apply %d: %v", i, err)
		}
	}

	if got := drainRecovered(replayed.Recovered()); len(got) != 0 {
		t.Errorf("replaying the log announced recoveries %v, want none: node2's "+
			"recovery is history, and announcing it schedules a catch-up pass for a "+
			"replica that never went anywhere", got)
	}
	if got := countUnhealthyWarnings(replayLogs); got != 0 {
		t.Errorf("replaying the log emitted %d 'peer unhealthy' warnings, want 0: "+
			"every peer they name is either fine now or will be corrected by a later "+
			"entry in the same replay", got)
	}

	// The whole point of replaying is the state, and it must be identical.
	if !replayed.Healthy("node2") {
		t.Error("replay must rebuild the view: node2 ended the history healthy")
	}
	if replayed.Healthy("node3") {
		t.Error("replay must rebuild the view: node3 ended the history unhealthy")
	}
	if live.Healthy("node2") != replayed.Healthy("node2") ||
		live.Healthy("node3") != replayed.Healthy("node3") {
		t.Error("the replayed view must match the live one exactly; the gate " +
			"suppresses effects, never state")
	}
}

// TestHealthSMLiveTransitionAfterReplayStillAnnounces is the other side of the
// frontier. Suppressing replayed announcements is only correct if crossing back
// into live applies restores them — a gate that stayed shut would trade a burst
// of spurious catch-up passes for a missed real one.
//
// Revert-check for the pair: this test is deliberately not the one that fails
// when the gate is removed (it passes either way). It fails when the gate is
// wrong in the opposite direction — stuck on — which
// TestHealthSMReplayDoesNotReannounceHistory cannot detect.
func TestHealthSMLiveTransitionAfterReplayStillAnnounces(t *testing.T) {
	ctx := context.Background()
	sm, _, logs := replayGateSM(t, "node1")

	// Replay a history in which node2 failed and recovered.
	for i, e := range []raft.LogEntry{
		healthEntry(1, opHealthDown, "node2"),
		healthEntry(2, opHealthUp, "node2"),
	} {
		if err := sm.ReplayApply(ctx, e); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
	}
	if got := drainRecovered(sm.Recovered()); len(got) != 0 {
		t.Fatalf("replay announced %v, want none", got)
	}

	// Now a peer fails and recovers for real, above the frontier.
	if err := sm.Apply(ctx, healthEntry(3, opHealthDown, "node3")); err != nil {
		t.Fatalf("live down: %v", err)
	}
	if got := countUnhealthyWarnings(logs); got != 1 {
		t.Errorf("live unhealthy warnings = %d, want 1: a real failure after a "+
			"replay must still reach an operator", got)
	}
	if err := sm.Apply(ctx, healthEntry(4, opHealthUp, "node3")); err != nil {
		t.Fatalf("live up: %v", err)
	}
	if id := awaitRecovered(sm.Recovered(), time.Second); id != "node3" {
		t.Errorf("recovery notification = %q, want node3: the gate must reopen for "+
			"live entries, or a genuine recovery never schedules its catch-up pass", id)
	}

	// And a live transition for a peer whose replayed history left it down is
	// still a transition — the replay set the state it transitions from.
	if err := sm.ReplayApply(ctx, healthEntry(5, opHealthDown, "node2")); err != nil {
		t.Fatalf("replay down: %v", err)
	}
	if sm.Healthy("node2") {
		t.Fatal("replayed health-down must still move the view")
	}
	if err := sm.Apply(ctx, healthEntry(6, opHealthUp, "node2")); err != nil {
		t.Fatalf("live up after replayed down: %v", err)
	}
	if id := awaitRecovered(sm.Recovered(), time.Second); id != "node2" {
		t.Errorf("recovery notification = %q, want node2: state written by a replay "+
			"is real state, so a live entry that changes it is a real transition", id)
	}
}

// TestHealthSMReplayStillCountsAppliedEntries pins what the gate does *not*
// suppress. The counters describe how far through the log this node is, not what
// it announced, and a restart that replayed a hundred entries must say so — that
// is the reading raft_last_applied_index exists to give, and the reason a silent
// restart is explicable rather than mysterious.
func TestHealthSMReplayStillCountsAppliedEntries(t *testing.T) {
	ctx := context.Background()
	sm, m, _ := replayGateSM(t, "node1")

	for i, e := range []raft.LogEntry{
		healthEntry(7, opHealthDown, "node2"),
		healthEntry(8, opHealthUp, "node2"),
	} {
		if err := sm.ReplayApply(ctx, e); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
	}

	if got := m.HealthTransitionsCommitted.Load(); got != 2 {
		t.Errorf("health_transitions_committed = %d, want 2: a replayed entry is "+
			"still an applied entry", got)
	}
	if got := m.RaftLastAppliedIndex.Load(); got != 8 {
		t.Errorf("raft_last_applied_index = %d, want 8: the gauge tracks the log "+
			"position regardless of origin", got)
	}
}

// TestHealthSMRestoreFromSnapshotStaysSilent is a regression guard on the
// behaviour that was already right, and that the replay gate now matches: a
// snapshot describes a state rather than a transition into it, so restoring one
// announces nothing however many healthy peers the payload names.
func TestHealthSMRestoreFromSnapshotStaysSilent(t *testing.T) {
	ctx := context.Background()
	sm, _, logs := replayGateSM(t, "node1")

	if err := sm.RestoreFromSnapshot(ctx, []byte(`{"node2":false,"node3":true}`)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := drainRecovered(sm.Recovered()); len(got) != 0 {
		t.Errorf("restore announced recoveries %v, want none", got)
	}
	if got := countUnhealthyWarnings(logs); got != 0 {
		t.Errorf("restore emitted %d unhealthy warnings, want 0", got)
	}
	if sm.Healthy("node2") {
		t.Error("restore must install the payload's view")
	}
	if !sm.Healthy("node3") {
		t.Error("restore must install the payload's view")
	}
}
