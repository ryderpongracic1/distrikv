package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// HealthStateMachine tests. The contract these pin is raft.StateMachine's, and
// the one clause that is easy to satisfy by accident and expensive to get wrong
// is idempotence: commitIndex and lastApplied are volatile, so a restart replays
// every entry after the snapshot boundary. An Apply with a non-idempotent side
// effect turns that replay into a burst of spurious catch-up passes on every
// node in the cluster.

func testHealthSM(t *testing.T, selfID string) (*HealthStateMachine, *metrics.Metrics) {
	t.Helper()
	m := &metrics.Metrics{}
	return newHealthStateMachine(selfID, 2, m, slog.New(slog.NewTextHandler(io.Discard, nil))), m
}

// healthEntry builds a committed log entry the way the aggregator does.
func healthEntry(index uint64, op, nodeID string) raft.LogEntry {
	return raft.LogEntry{
		Index: index,
		Term:  1,
		Op:    op,
		Key:   nodeID,
		Value: []byte(time.Now().UTC().Format(time.RFC3339Nano)),
	}
}

// drainRecovered collects everything currently on the recovery channel without
// blocking. Nothing pending is a legitimate answer, so it never fails.
func drainRecovered(ch <-chan string) []string {
	var out []string
	for {
		select {
		case id := <-ch:
			out = append(out, id)
		default:
			return out
		}
	}
}

// awaitRecovered waits briefly for one recovery notification. Returns "" if none
// arrives, so a test can assert on absence as well as presence.
func awaitRecovered(ch <-chan string, within time.Duration) string {
	select {
	case id := <-ch:
		return id
	case <-time.After(within):
		return ""
	}
}

// TestHealthSMAbsentNodeIsHealthy pins the convention the whole design leans on:
// a node no transition has been committed for is reachable. The alternative —
// unknown means down — would have every node open believing its entire replica
// set was unreachable.
func TestHealthSMAbsentNodeIsHealthy(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	if !sm.Healthy("node2") {
		t.Error("a node with no committed transition must report healthy; " +
			"'we have no opinion' is not 'down'")
	}
}

// TestHealthSMApplyMarksDownAndUp is the basic round trip: the two ops the log
// carries move the committed view in both directions.
func TestHealthSMApplyMarksDownAndUp(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	if err := sm.Apply(ctx, healthEntry(1, opHealthDown, "node2")); err != nil {
		t.Fatalf("apply health-down: %v", err)
	}
	if sm.Healthy("node2") {
		t.Error("node2 must be unhealthy after a committed health-down")
	}

	if err := sm.Apply(ctx, healthEntry(2, opHealthUp, "node2")); err != nil {
		t.Fatalf("apply health-up: %v", err)
	}
	if !sm.Healthy("node2") {
		t.Error("node2 must be healthy after a committed health-up")
	}
}

// TestHealthSMApplyIsIdempotent is the raft.StateMachine contract test. Applying
// the same entry repeatedly — what every restart does to the entries after the
// snapshot boundary — must leave the same state and must not re-announce a
// recovery that already happened.
//
// Revert-check: make Apply push to the recovery channel outside the `changed`
// branch and the second half of this test fails immediately.
func TestHealthSMApplyIsIdempotent(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	down := healthEntry(1, opHealthDown, "node2")
	for i := 0; i < 3; i++ {
		if err := sm.Apply(ctx, down); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if sm.Healthy("node2") {
		t.Error("repeated health-down must leave node2 unhealthy")
	}

	up := healthEntry(2, opHealthUp, "node2")
	for i := 0; i < 3; i++ {
		if err := sm.Apply(ctx, up); err != nil {
			t.Fatalf("apply up %d: %v", i, err)
		}
	}
	if !sm.Healthy("node2") {
		t.Error("repeated health-up must leave node2 healthy")
	}

	// Exactly one recovery, from the one genuine transition. A replay of the
	// committed log after a restart must not schedule a catch-up pass per replayed
	// entry.
	got := drainRecovered(sm.Recovered())
	if len(got) != 1 || got[0] != "node2" {
		t.Errorf("recovery notifications = %v, want exactly one for node2: a "+
			"re-applied entry changes nothing and must announce nothing", got)
	}
}

// TestHealthSMRecoveredFiresOnTransitionOnly asserts the channel carries
// transitions, not applies: a health-up for a node already considered healthy is
// not a recovery.
func TestHealthSMRecoveredFiresOnTransitionOnly(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	// Absent means healthy, so this entry restates the view.
	if err := sm.Apply(ctx, healthEntry(1, opHealthUp, "node2")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if id := awaitRecovered(sm.Recovered(), 50*time.Millisecond); id != "" {
		t.Errorf("health-up for an already-healthy node announced a recovery for %q; "+
			"only unhealthy → healthy is a recovery", id)
	}

	if err := sm.Apply(ctx, healthEntry(2, opHealthDown, "node2")); err != nil {
		t.Fatalf("apply down: %v", err)
	}
	if err := sm.Apply(ctx, healthEntry(3, opHealthUp, "node2")); err != nil {
		t.Fatalf("apply up: %v", err)
	}
	if id := awaitRecovered(sm.Recovered(), time.Second); id != "node2" {
		t.Errorf("recovery notification = %q, want node2 after a genuine "+
			"unhealthy → healthy transition", id)
	}
}

// TestHealthSMIgnoresEntriesAboutItself pins the reason a node does not learn its
// own health from the log: a leader can legitimately commit "node2 is down" while
// node2 is alive and applying that entry, and putting its own ID on the recovery
// channel would have anti-entropy schedule a catch-up to a replica that is this
// node.
func TestHealthSMIgnoresEntriesAboutItself(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	if err := sm.Apply(ctx, healthEntry(1, opHealthDown, "node1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !sm.Healthy("node1") {
		t.Error("a node must not mark itself unhealthy from a committed entry: it " +
			"cannot act on the claim and the entry describes reachability from " +
			"elsewhere")
	}
	if err := sm.Apply(ctx, healthEntry(2, opHealthUp, "node1")); err != nil {
		t.Fatalf("apply up: %v", err)
	}
	if got := drainRecovered(sm.Recovered()); len(got) != 0 {
		t.Errorf("recovery notifications = %v, want none: an entry about this node "+
			"must not schedule a catch-up pass to itself", got)
	}
}

// TestHealthSMUnknownOpSucceeds pins the forward-compatibility decision, and the
// reason it is not merely cosmetic: Raft retries a failed Apply rather than
// skipping it, so returning an error for an unrecognised op would wedge the apply
// loop forever and freeze the health view of every node that received the entry.
func TestHealthSMUnknownOpSucceeds(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	if err := sm.Apply(ctx, healthEntry(1, "health-sideways", "node2")); err != nil {
		t.Fatalf("an unrecognised op must not fail Apply (Raft would retry it "+
			"forever and never advance): %v", err)
	}
	if !sm.Healthy("node2") {
		t.Error("an unrecognised op must not change the view")
	}
	if err := sm.Apply(ctx, healthEntry(2, opHealthDown, "")); err != nil {
		t.Fatalf("an entry with an empty node ID must not fail Apply: %v", err)
	}
}

// TestHealthSMSnapshotRoundTrip asserts a snapshot reproduces the exact view it
// was taken from, in a fresh state machine that never saw the entries.
func TestHealthSMSnapshotRoundTrip(t *testing.T) {
	src, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	for i, e := range []raft.LogEntry{
		healthEntry(1, opHealthDown, "node2"),
		healthEntry(2, opHealthDown, "node3"),
		healthEntry(3, opHealthUp, "node3"),
	} {
		if err := src.Apply(ctx, e); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	data, err := src.SnapshotState(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	dst, _ := testHealthSM(t, "node1")
	if err := dst.RestoreFromSnapshot(ctx, data); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if dst.Healthy("node2") {
		t.Error("restored view must keep node2 unhealthy")
	}
	if !dst.Healthy("node3") {
		t.Error("restored view must keep node3 healthy")
	}

	// A snapshot describes a state, not a transition into it. Announcing
	// recoveries for every healthy node in the payload would fire a catch-up pass
	// per replica on every restart that loads a snapshot.
	if got := drainRecovered(dst.Recovered()); len(got) != 0 {
		t.Errorf("restore announced recoveries %v, want none", got)
	}
}

// TestHealthSMRestoreReplacesRatherThanMerges pins the "full replace" clause. A
// merge would keep a pre-snapshot entry alive with no entry left in the log to
// correct it.
func TestHealthSMRestoreReplacesRatherThanMerges(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	if err := sm.Apply(ctx, healthEntry(1, opHealthDown, "node2")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// A payload that says nothing about node2 — the state at an index where node2
	// had no committed transition.
	if err := sm.RestoreFromSnapshot(ctx, []byte(`{"node3":false}`)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !sm.Healthy("node2") {
		t.Error("a node absent from the snapshot payload must be absent from the " +
			"restored view (and so healthy); restore replaces, it does not merge")
	}
	if sm.Healthy("node3") {
		t.Error("restore must apply the payload's contents")
	}
}

// TestHealthSMRestoreEmptyPayload covers the snapshot taken before any transition
// was committed — the common case on a cluster that has never had a fault.
func TestHealthSMRestoreEmptyPayload(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	if err := sm.Apply(ctx, healthEntry(1, opHealthDown, "node2")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := sm.RestoreFromSnapshot(ctx, nil); err != nil {
		t.Fatalf("restore nil payload: %v", err)
	}
	if !sm.Healthy("node2") {
		t.Error("an empty payload restores an empty view")
	}
}

// TestHealthSMCountsCommittedTransitions asserts the counter that distinguishes
// "the consensus signal is live" from "the log is empty and the local signals are
// carrying health on their own" — the state this system was in, unnoticed, for
// three months.
func TestHealthSMCountsCommittedTransitions(t *testing.T) {
	sm, m := testHealthSM(t, "node1")
	ctx := context.Background()

	for i, e := range []raft.LogEntry{
		healthEntry(1, opHealthDown, "node2"),
		healthEntry(2, opHealthDown, "node2"), // redundant, still an applied entry
		healthEntry(3, opHealthUp, "node2"),
		healthEntry(4, "health-sideways", "node2"), // not a health entry
		healthEntry(5, opHealthDown, "node1"),      // about self
	} {
		if err := sm.Apply(ctx, e); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	if got := m.HealthTransitionsCommitted.Load(); got != 3 {
		t.Errorf("health_transitions_committed = %d, want 3 (the three health "+
			"entries about a peer, redundant ones included; not the unrecognised "+
			"op and not the entry about this node)", got)
	}
}

// TestHealthSMConcurrentApplyAndRead runs Apply against Healthy and SnapshotState
// so -race can find an unguarded map access. Raft calls Apply from its apply loop
// while anti-entropy reads Healthy from its own goroutine, so this concurrency is
// production shape, not a test artefact.
func TestHealthSMConcurrentApplyAndRead(t *testing.T) {
	sm, _ := testHealthSM(t, "node1")
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			op := opHealthDown
			if i%2 == 0 {
				op = opHealthUp
			}
			_ = sm.Apply(ctx, healthEntry(uint64(i+1), op, "node2"))
		}
	}()

	for i := 0; i < 200; i++ {
		_ = sm.Healthy("node2")
		if _, err := sm.SnapshotState(ctx); err != nil {
			t.Fatalf("snapshot during apply: %v", err)
		}
		// Keep the channel from filling and turning this into a drop-warning test.
		drainRecovered(sm.Recovered())
	}
	<-done
}
