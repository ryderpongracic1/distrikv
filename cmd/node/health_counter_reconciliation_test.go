package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// Health-transition counter reconciliation.
//
// The question these tests answer came from a live cluster, not from a code
// review. After four consecutive stop-restart chaos gates the sole Raft leader
// reported health_transitions_proposed=16 and health_transitions_committed=32 —
// an exact 2x that looked systematic rather than noisy, and this repo's whole
// claim is that its numbers reconcile.
//
// The reconciliation is measured against the log on disk rather than against the
// other counter, because a counter checked only against another counter proves
// nothing about either. Every assertion below compares a metric to the entries a
// node actually persisted or actually applied.

// persistedRaftLog is the on-disk shape of internal/raft's persisted state,
// re-declared here because the production type is unexported. The json tags are
// what couple them; a rename there fails these tests loudly rather than silently
// reading zero entries, because the entry count would then disagree with
// LastLogIndex (asserted below).
type persistedRaftLog struct {
	CurrentTerm   uint64          `json:"current_term"`
	SnapLastIndex uint64          `json:"snap_last_index"`
	Log           []raft.LogEntry `json:"log,omitempty"`
}

// healthEntryCounts is what a node's persisted log actually contains.
type healthEntryCounts struct {
	total  int            // every entry in the log
	down   int            // op == health-down
	up     int            // op == health-up
	other  int            // anything else — must be 0; the log is health-only
	perKey map[string]int // node ID the entry is about → entries naming it
}

// readPersistedHealthLog counts the entries a node has on disk, by op and by the
// node each one is about.
func readPersistedHealthLog(t *testing.T, dataDir string) healthEntryCounts {
	t.Helper()

	raw, err := os.ReadFile(dataDir + "/raft-state")
	if err != nil {
		t.Fatalf("read persisted raft state in %s: %v", dataDir, err)
	}
	var st persistedRaftLog
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("decode persisted raft state in %s: %v", dataDir, err)
	}

	counts := healthEntryCounts{perKey: make(map[string]int)}
	for _, e := range st.Log {
		counts.total++
		counts.perKey[e.Key]++
		switch e.Op {
		case opHealthDown:
			counts.down++
		case opHealthUp:
			counts.up++
		default:
			counts.other++
		}
	}
	return counts
}

// settleAppliesQuiet waits until every node has applied everything committed and
// nothing has moved for a quiet period.
//
// Both conditions are needed. CommitIndex == lastApplied alone can hold in the
// gap between an append and its replication, and a quiet period alone cannot
// tell a finished cluster from a slow one.
func settleAppliesQuiet(t *testing.T, h *raftHarness, quiet, timeout time.Duration) {
	t.Helper()

	type snap struct {
		commit    uint64
		lastLog   uint64
		committed uint64
		proposed  uint64
	}
	take := func() map[string]snap {
		out := make(map[string]snap, len(h.ids))
		for _, id := range h.ids {
			n := h.nodes[id]
			out[id] = snap{
				commit:    n.raft.CommitIndex(),
				lastLog:   n.raft.LastLogIndex(),
				committed: n.metrics.HealthTransitionsCommitted.Load(),
				proposed:  n.metrics.HealthTransitionsProposed.Load(),
			}
		}
		return out
	}
	same := func(a, b map[string]snap) bool {
		for id, av := range a {
			if b[id] != av {
				return false
			}
		}
		return true
	}

	deadline := time.Now().Add(timeout)
	prev := take()
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		cur := take()
		if !same(prev, cur) {
			prev, stableSince = cur, time.Now()
			continue
		}
		// Stable readings alone are not enough: require that every node has
		// committed up to the highest log index in the cluster, so a follower
		// still catching up is never mistaken for a finished one.
		var highest uint64
		for _, s := range cur {
			if s.lastLog > highest {
				highest = s.lastLog
			}
		}
		drained := true
		for _, s := range cur {
			if s.commit != highest {
				drained = false
			}
		}
		if drained && time.Since(stableSince) >= quiet {
			return
		}
	}
	t.Fatalf("cluster did not go quiet within %s", timeout)
}

// oneFaultCycle isolates a node until the leader's committed view marks it down,
// then heals it until the view marks it up again — the in-process equivalent of
// one stop-restart nemesis fault window.
func oneFaultCycle(t *testing.T, h *raftHarness, leader *healthNode, victimID string) {
	t.Helper()

	h.isolate(victimID)
	awaitHealth(t, leader, victimID, false, 3*time.Second)
	h.heal(victimID)
	awaitHealth(t, leader, victimID, true, 3*time.Second)
}

// TestHealthTransitionCountersReconcileWithTheLog is the pin for what the two
// counters mean.
//
// With one leader for the whole run and no re-election, the invariants are:
//
//   - the leader proposed exactly as many entries as its log holds;
//   - every node applied — and counted — every entry that is not about itself;
//   - the victim counted none of the entries about itself, by design.
//
// Revert-check: drop the `nodeID == h.selfID` guard in HealthStateMachine.Apply
// and the victim's committed count stops being 0; move the
// HealthTransitionsCommitted increment above the self/unknown-op early returns
// and the same assertion fails.
func TestHealthTransitionCountersReconcileWithTheLog(t *testing.T) {
	h := newRaftHarness(t, healthAggregatorConfig{DownAfter: 3, UpAfter: 2})
	leader, followers := h.leader(5 * time.Second)

	victim := followers[0]
	bystander := followers[1]

	const faults = 3
	for i := 0; i < faults; i++ {
		oneFaultCycle(t, h, leader, victim.id)
	}
	settleAppliesQuiet(t, h, 250*time.Millisecond, 10*time.Second)

	// Leadership must not have moved, or "the leader proposed every entry" is not
	// the invariant under test. This is the premise the live-cluster arithmetic
	// assumed and could not check.
	if got := h.nodes[leader.id].raft.IsLeader(); !got {
		t.Fatalf("%s is no longer the leader; this test's invariant does not apply", leader.id)
	}

	onDisk := readPersistedHealthLog(t, leader.dataDir)
	if onDisk.other != 0 {
		t.Errorf("leader log holds %d non-health entries; the Raft log must carry health only", onDisk.other)
	}
	if onDisk.total == 0 {
		t.Fatal("leader log holds no entries; the fault cycles produced nothing to reconcile")
	}
	if onDisk.down != faults || onDisk.up != faults {
		t.Errorf("leader log holds %d down / %d up entries, want %d each — one per fault direction",
			onDisk.down, onDisk.up, faults)
	}
	if entriesAboutVictim := onDisk.perKey[victim.id]; entriesAboutVictim != onDisk.total {
		t.Errorf("%d of %d entries are about %s; only the isolated node should have transitions",
			entriesAboutVictim, onDisk.total, victim.id)
	}

	// Invariant 1: the leader's proposed counter equals what it put in the log.
	proposed := leader.metrics.HealthTransitionsProposed.Load()
	if proposed != uint64(onDisk.total) {
		t.Errorf("leader %s: proposed=%d but its log holds %d entries — the proposed counter "+
			"does not account for what was appended", leader.id, proposed, onDisk.total)
	}

	// Invariant 2: every node's committed counter equals the entries it applied,
	// which is every entry not about itself.
	for _, n := range []*healthNode{leader, bystander} {
		committed := n.metrics.HealthTransitionsCommitted.Load()
		if committed != uint64(onDisk.total) {
			t.Errorf("%s: committed=%d, want %d (every entry in the log, none of which is about it)",
				n.id, committed, onDisk.total)
		}
	}
	if committed := victim.metrics.HealthTransitionsCommitted.Load(); committed != 0 {
		t.Errorf("%s: committed=%d, want 0 — a node does not count transitions about itself",
			victim.id, committed)
	}

	// Invariant 3: no node proposed anything but the leader.
	for _, id := range h.ids {
		if id == leader.id {
			continue
		}
		if p := h.nodes[id].metrics.HealthTransitionsProposed.Load(); p != 0 {
			t.Errorf("%s: proposed=%d, want 0 — only the leader proposes", id, p)
		}
	}

	// Invariant 4: the applied-index gauge is the denominator both counters have
	// to be read against. The log carries health entries only and indexes from 1,
	// so the highest applied index is the number of entries the cluster committed.
	for _, n := range []*healthNode{leader, bystander, victim} {
		if got := n.metrics.RaftLastAppliedIndex.Load(); got != uint64(onDisk.total) {
			t.Errorf("%s: raft_last_applied_index=%d, want %d (the log's length) — "+
				"the gauge that lets committed be checked against the log is wrong",
				n.id, got, onDisk.total)
		}
	}

	t.Logf("reconciled: log=%d entries (%d down, %d up), leader proposed=%d, committed leader/bystander/victim=%d/%d/%d",
		onDisk.total, onDisk.down, onDisk.up, proposed,
		leader.metrics.HealthTransitionsCommitted.Load(),
		bystander.metrics.HealthTransitionsCommitted.Load(),
		victim.metrics.HealthTransitionsCommitted.Load())
}

// TestHealthEntriesApplyExactlyOncePerNode pins the property that makes
// invariant 2 above meaningful: committed counts entries, not apply attempts.
//
// applyCommitted has three callers — a heartbeat response that advanced
// commitIndex, an incoming AppendEntries, and Propose itself (raft.go:692,
// raft.go:954, replication.go:331) — so an entry is racing several triggers. If
// that serialisation stopped holding, committed would drift above the log length
// and every "committed is live" reading would be inflated by an unknown factor.
// That was the first hypothesis for the live 2x, and this test refutes it.
//
// Revert-check: drop the `nodeID == h.selfID` guard in HealthStateMachine.Apply
// (or move the HealthTransitionsCommitted increment above the self/unknown-op
// early returns) and this test fails with committed=8 against want=4.
//
// Tried and rejected as a revert-check: removing `applyMu` from applyCommitted
// does *not* make this test fail. Exactly-once does not rest on applyMu alone —
// the loop re-reads lastApplied under r.mu on every iteration and derives the
// next index from it, so racing callers interleave rather than duplicate. Said
// here because the alternative is a comment claiming a check that does not hold.
func TestHealthEntriesApplyExactlyOncePerNode(t *testing.T) {
	h := newRaftHarness(t, healthAggregatorConfig{DownAfter: 2, UpAfter: 2})
	leader, followers := h.leader(5 * time.Second)

	// Enough cycles that a heartbeat response, an AppendEntries and a Propose are
	// all in flight against overlapping commit advances.
	for i := 0; i < 4; i++ {
		oneFaultCycle(t, h, leader, followers[i%len(followers)].id)
	}
	settleAppliesQuiet(t, h, 250*time.Millisecond, 15*time.Second)

	onDisk := readPersistedHealthLog(t, leader.dataDir)
	for _, id := range h.ids {
		n := h.nodes[id]
		// Entries about this node are ignored, so its ceiling is the log minus
		// those. Equality is asserted in the reconciliation test; here the point
		// is strictly that nothing was counted twice.
		want := onDisk.total - onDisk.perKey[id]
		if got := int(n.metrics.HealthTransitionsCommitted.Load()); got != want {
			t.Errorf("%s: committed=%d, want %d (log has %d entries, %d about itself) — "+
				"a value above want means an entry was applied more than once",
				id, got, want, onDisk.total, onDisk.perKey[id])
		}
	}
}

// TestCommittedCountsEntriesThisNodeDidNotPropose is the explanation for the
// live cluster's 16-vs-32, and the reason the two counters must never be
// compared to each other on a single node.
//
// proposed is scoped to the entries this node appended while it was leader.
// committed is scoped to every entry the cluster committed, whoever wrote it. So
// a node that led for part of a session and followed for the rest ends with a
// committed count that includes entries it never authored — with nothing wrong
// anywhere. Entry terms are what attribute authorship here: an entry whose term
// is above the highest term this node led in cannot have come from it.
//
// The test also pins the counter that was used to rule this out. raft_terms
// counts the elections this node *started* (internal/raft/raft.go:377) and
// leader_elections the ones it *won* (raft.go:509). Neither moves when a node
// steps down on a peer's higher term, so a flat raft_terms is not evidence that
// leadership never moved.
//
// Revert-check: add an `IncRaftTerms()` call to `stepDownLocked` in
// internal/raft and the term-counter assertion fails; drop the self-ID guard in
// HealthStateMachine.Apply and the committed accounting assertion fails.
func TestCommittedCountsEntriesThisNodeDidNotPropose(t *testing.T) {
	h := newRaftHarness(t, healthAggregatorConfig{DownAfter: 2, UpAfter: 2})
	first, _ := h.leader(5 * time.Second)

	firstTerm := first.raft.CurrentTerm()
	termsBefore := first.metrics.RaftTerms.Load()
	electionsBefore := first.metrics.LeaderElections.Load()

	// A transition under the first leader, so it has authored entries of its own.
	victim := ""
	for _, id := range h.ids {
		if id != first.id {
			victim = id
			break
		}
	}
	oneFaultCycle(t, h, first, victim)
	settleAppliesQuiet(t, h, 200*time.Millisecond, 10*time.Second)
	if first.metrics.HealthTransitionsProposed.Load() == 0 {
		t.Fatal("first leader proposed nothing; nothing to attribute")
	}

	// Move leadership by isolating the leader, then heal it so it rejoins as a
	// follower and resumes applying what the new leader commits.
	h.isolate(first.id)
	var second *healthNode
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && second == nil {
		for _, id := range h.ids {
			if id != first.id && h.nodes[id].raft.IsLeader() {
				second = h.nodes[id]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if second == nil {
		t.Fatal("no new leader elected after isolating the first one")
	}
	h.heal(first.id)

	// A transition authored by the new leader, in its own higher term.
	third := ""
	for _, id := range h.ids {
		if id != first.id && id != second.id {
			third = id
			break
		}
	}
	oneFaultCycle(t, h, second, third)
	settleAppliesQuiet(t, h, 200*time.Millisecond, 10*time.Second)

	if h.nodes[first.id].raft.IsLeader() {
		t.Skip("the first node regained leadership; this test needs it to stay a follower")
	}

	// The committed log, read from the node that owns it.
	raw, err := os.ReadFile(second.dataDir + "/raft-state")
	if err != nil {
		t.Fatalf("read persisted raft state: %v", err)
	}
	var st persistedRaftLog
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("decode persisted raft state: %v", err)
	}

	var foreign, aboutFirst int
	for _, e := range st.Log {
		if e.Term > firstTerm {
			foreign++
		}
		if e.Key == first.id {
			aboutFirst++
		}
	}
	if foreign == 0 {
		t.Fatalf("no entry in the log was authored after term %d; leadership did not produce one", firstTerm)
	}

	// What the demoted node counted, and what it could possibly have authored.
	committed := first.metrics.HealthTransitionsCommitted.Load()
	appliable := len(st.Log) - aboutFirst
	if committed != uint64(appliable) {
		t.Errorf("%s: committed=%d, want %d (log holds %d entries, %d of them about itself)",
			first.id, committed, appliable, len(st.Log), aboutFirst)
	}
	if committed < uint64(foreign) {
		t.Errorf("%s: committed=%d but %d entries were authored in a later term; it must have "+
			"counted entries it did not propose", first.id, committed, foreign)
	}

	// The refutation: its term counters did not move, though its term did.
	if now := first.raft.CurrentTerm(); now <= firstTerm {
		t.Fatalf("%s is still in term %d; it never learned of the new leader", first.id, now)
	}
	if got := first.metrics.RaftTerms.Load(); got != termsBefore {
		t.Errorf("%s: raft_terms moved %d → %d across a leadership loss; the refutation this "+
			"test records no longer holds", first.id, termsBefore, got)
	}
	if got := first.metrics.LeaderElections.Load(); got != electionsBefore {
		t.Errorf("%s: leader_elections moved %d → %d without this node winning one",
			first.id, electionsBefore, got)
	}

	// And the cluster-wide identity: proposed counts local appends, some of which
	// an isolated leader writes and a later leader truncates. So the proposed
	// counters sum to at least the log length, never less.
	var totalProposed uint64
	for _, id := range h.ids {
		totalProposed += h.nodes[id].metrics.HealthTransitionsProposed.Load()
	}
	if totalProposed < uint64(len(st.Log)) {
		t.Errorf("proposed counters sum to %d across the cluster but the log holds %d entries; "+
			"every committed entry must have been proposed by someone", totalProposed, len(st.Log))
	}

	t.Logf("after leadership moved: %s proposed=%d committed=%d (log=%d entries, %d authored later, "+
		"%d about itself); cluster proposed=%d; %s raft_terms=%d unchanged",
		first.id, first.metrics.HealthTransitionsProposed.Load(), committed,
		len(st.Log), foreign, aboutFirst, totalProposed, first.id, first.metrics.RaftTerms.Load())
}

// TestCommittedRecountsTheLogAfterARestart pins the fourth reason the two
// counters diverge, and the one that produces the cleanest multiple.
//
// commitIndex and lastApplied are volatile by design (see internal/raft/log.go),
// so a node that restarts re-applies every committed entry above the snapshot
// boundary. Apply is idempotent, so the health view is unaffected — but
// health_transitions_committed counts applies, and the metrics are in memory. A
// process that comes up to an existing log therefore reports a committed count
// covering the whole log while having authored none of it.
//
// This is exactly how a committed count can be an integer multiple of a proposed
// count with no missing increment anywhere.
//
// Revert-check: seed an empty log instead and committed stays at the single entry
// this process authored.
func TestCommittedRecountsTheLogAfterARestart(t *testing.T) {
	dir := t.TempDir()

	// A log this process did not write, left behind by an earlier incarnation.
	const seeded = 3
	entries := make([]raft.LogEntry, 0, seeded)
	for i := 1; i <= seeded; i++ {
		op := opHealthDown
		if i%2 == 0 {
			op = opHealthUp
		}
		entries = append(entries, raft.LogEntry{Index: uint64(i), Term: 1, Op: op, Key: "node2"})
	}
	seedPersistedRaftLog(t, dir, 1, entries)

	m := &metrics.Metrics{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sm := newHealthStateMachine("node1", 1, m, logger)

	// A single-node cluster, so majority is itself and commitment needs no peer.
	rn, err := raft.New(raft.Config{
		NodeID:             "node1",
		DataDir:            dir,
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
		SnapshotThreshold:  1000,
	}, nil, sm, &metricsAdapter{m}, logger)
	if err != nil {
		t.Fatalf("init raft: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); rn.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(5 * time.Second)
	for !rn.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !rn.IsLeader() {
		t.Fatal("single-node cluster never elected itself")
	}

	// One entry in the current term. §5.4.2 forbids committing a previous term's
	// entries on the majority rule alone, so the replay lands the moment an entry
	// of this term does — which is what a restarted node's first health
	// transition does in production.
	if _, err := rn.Propose(ctx, opHealthDown, "node3", nil); err != nil {
		t.Fatalf("propose: %v", err)
	}

	const wantApplied = seeded + 1
	deadline = time.Now().Add(5 * time.Second)
	for m.RaftLastAppliedIndex.Load() < wantApplied && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := m.RaftLastAppliedIndex.Load(); got != wantApplied {
		t.Fatalf("raft_last_applied_index=%d, want %d", got, wantApplied)
	}
	if got := m.HealthTransitionsCommitted.Load(); got != wantApplied {
		t.Errorf("committed=%d, want %d — a restarted node counts the whole log it replays",
			got, wantApplied)
	}
	// Nothing here drives the aggregator, so this process authored no proposal of
	// its own: rn.Propose is the raft API, and only healthAggregator.propose
	// increments the counter. That is the point — the committed count above is
	// entirely un-attributable to this process's proposals.
	if got := m.HealthTransitionsProposed.Load(); got != 0 {
		t.Errorf("proposed=%d, want 0 — no aggregator ran in this test", got)
	}

	t.Logf("after replaying a %d-entry log and appending 1: committed=%d proposed=%d applied_index=%d",
		seeded, m.HealthTransitionsCommitted.Load(), m.HealthTransitionsProposed.Load(),
		m.RaftLastAppliedIndex.Load())
}

// seedPersistedRaftLog writes a raft-state file the way a previous incarnation of
// a node would have left it.
func seedPersistedRaftLog(t *testing.T, dataDir string, term uint64, entries []raft.LogEntry) {
	t.Helper()
	raw, err := json.Marshal(persistedRaftLog{CurrentTerm: term, Log: entries})
	if err != nil {
		t.Fatalf("encode seeded raft state: %v", err)
	}
	if err := os.WriteFile(dataDir+"/raft-state", raw, 0o600); err != nil {
		t.Fatalf("write seeded raft state: %v", err)
	}
}
