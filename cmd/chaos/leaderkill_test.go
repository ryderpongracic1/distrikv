package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Tests for the leader-kill nemesis.
//
// The property under test throughout is the one that makes the mode worth
// having: it kills the node it says it killed, or it kills nothing and says so.
// Every test here is about resolution and attribution rather than about docker,
// which the composeNemesis tests already cover.

// fakeCluster answers /status for a set of nodes, and can be reconfigured
// mid-test to move leadership or take a node offline.
type fakeCluster struct {
	mu     sync.Mutex
	status map[string]nodeStatus // addr → status
	down   map[string]bool       // addr → unreachable
	calls  int
}

func newFakeCluster(leader string, addrs map[string]string, term uint64) *fakeCluster {
	c := &fakeCluster{
		status: make(map[string]nodeStatus, len(addrs)),
		down:   make(map[string]bool, len(addrs)),
	}
	for addr, nodeID := range addrs {
		role := "follower"
		if nodeID == leader {
			role = raftRoleLeader
		}
		c.status[addr] = nodeStatus{NodeID: nodeID, Leader: leader, Term: term, Role: role}
	}
	return c
}

// setLeader moves leadership to nodeID in the given term.
func (c *fakeCluster) setLeader(nodeID string, term uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, st := range c.status {
		st.Leader = nodeID
		st.Term = term
		st.Role = "follower"
		if st.NodeID == nodeID {
			st.Role = raftRoleLeader
		}
		c.status[addr] = st
	}
}

// setElectionInProgress makes every node report no leader, which is what a
// cluster mid-election looks like over /status.
func (c *fakeCluster) setElectionInProgress() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, st := range c.status {
		st.Leader = ""
		st.Role = "follower"
		c.status[addr] = st
	}
}

func (c *fakeCluster) setDown(addr string, down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down[addr] = down
}

// setRaw overwrites one node's status verbatim, for the split-view cases.
func (c *fakeCluster) setRaw(addr string, st nodeStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status[addr] = st
}

func (c *fakeCluster) fetch(_ context.Context, addr string) (nodeStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.down[addr] {
		return nodeStatus{}, fmt.Errorf("connection refused")
	}
	st, ok := c.status[addr]
	if !ok {
		return nodeStatus{}, fmt.Errorf("no such node %s", addr)
	}
	return st, nil
}

// resolverFor builds a resolver over the fake cluster with retries short enough
// that a test asserting the no-leader path does not wait seconds.
func resolverFor(c *fakeCluster, addrs ...string) *leaderResolver {
	return &leaderResolver{
		addrs:      addrs,
		fetch:      c.fetch,
		retryFor:   60 * time.Millisecond,
		retryEvery: 10 * time.Millisecond,
	}
}

const (
	addr1 = "localhost:8001"
	addr2 = "localhost:8002"
	addr3 = "localhost:8003"
)

func threeNodes() map[string]string {
	return map[string]string{addr1: "node1", addr2: "node2", addr3: "node3"}
}

// TestLeaderResolutionPrefersTheNodesOwnClaim pins the strongest evidence rule:
// the node that will be killed is the node that said it was the leader.
func TestLeaderResolutionPrefersTheNodesOwnClaim(t *testing.T) {
	c := newFakeCluster("node2", threeNodes(), 7)
	r := resolverFor(c, addr1, addr2, addr3)

	id, ev, err := r.resolveLeader(context.Background())
	if err != nil {
		t.Fatalf("resolveLeader: %v", err)
	}
	if id != "node2" {
		t.Errorf("resolved leader %q, want node2", id)
	}
	if ev != evidenceSelf {
		t.Errorf("evidence %q, want %q", ev, evidenceSelf)
	}
}

// TestLeaderResolutionFallsBackToPeerReport covers a leader whose own HTTP
// address was not supplied: the followers agree on who leads, and that is the
// only evidence available.
func TestLeaderResolutionFallsBackToPeerReport(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 3)
	// Ask only the two followers.
	r := resolverFor(c, addr2, addr3)

	id, ev, err := r.resolveLeader(context.Background())
	if err != nil {
		t.Fatalf("resolveLeader: %v", err)
	}
	if id != "node1" {
		t.Errorf("resolved leader %q, want node1", id)
	}
	if ev != evidencePeers {
		t.Errorf("evidence %q, want %q", ev, evidencePeers)
	}
}

// TestLeaderResolutionTakesTheHigherTermOnASplitView pins the tie-break for the
// one case where two nodes both claim leadership: a partition leaves a stale
// leader believing it still leads, and killing that one would leave the real
// leader running.
func TestLeaderResolutionTakesTheHigherTermOnASplitView(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 4)
	// node3 has been elected in a later term; node1 has not learned it yet.
	c.setRaw(addr3, nodeStatus{NodeID: "node3", Leader: "node3", Term: 5, Role: raftRoleLeader})
	r := resolverFor(c, addr1, addr2, addr3)

	id, ev, err := r.resolveLeader(context.Background())
	if err != nil {
		t.Fatalf("resolveLeader: %v", err)
	}
	if id != "node3" {
		t.Errorf("resolved leader %q, want node3 (term 5 beats term 4)", id)
	}
	if ev != evidenceSelf {
		t.Errorf("evidence %q, want %q", ev, evidenceSelf)
	}
}

// TestLeaderResolutionReportsNoLeaderDuringAnElection is the skip path's cause:
// every node answers and none names a leader.
func TestLeaderResolutionReportsNoLeaderDuringAnElection(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 9)
	c.setElectionInProgress()
	r := resolverFor(c, addr1, addr2, addr3)

	id, _, err := r.resolveLeader(context.Background())
	if err == nil {
		t.Fatalf("resolveLeader returned %q, want an error while no leader exists", id)
	}
	if !strings.Contains(err.Error(), "election in progress") {
		t.Errorf("error %q does not say an election is in progress", err)
	}
	if c.calls < len(threeNodes())*2 {
		t.Errorf("only %d status calls: resolution must retry across an election, not give up on the first round", c.calls)
	}
}

// TestLeaderResolutionRetriesUntilALeaderAppears pins that a window is not
// skipped for a cluster that is merely mid-election.
func TestLeaderResolutionRetriesUntilALeaderAppears(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 9)
	c.setElectionInProgress()
	r := resolverFor(c, addr1, addr2, addr3)
	r.retryFor = 2 * time.Second

	go func() {
		time.Sleep(50 * time.Millisecond)
		c.setLeader("node3", 10)
	}()

	id, _, err := r.resolveLeader(context.Background())
	if err != nil {
		t.Fatalf("resolveLeader: %v", err)
	}
	if id != "node3" {
		t.Errorf("resolved leader %q, want node3", id)
	}
}

// TestLeaderResolutionFailsWhenNothingAnswers distinguishes "no leader" from
// "nothing to ask", because the two have different fixes.
func TestLeaderResolutionFailsWhenNothingAnswers(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 1)
	for _, a := range []string{addr1, addr2, addr3} {
		c.setDown(a, true)
	}
	r := resolverFor(c, addr1, addr2, addr3)

	if _, _, err := r.resolveLeader(context.Background()); err == nil {
		t.Fatal("resolveLeader succeeded with every node unreachable")
	} else if !strings.Contains(err.Error(), "answered /status") {
		t.Errorf("error %q does not say the nodes did not answer", err)
	}
}

// TestLeaderVictimSelectorNamesTheLeader is the mode's headline property.
func TestLeaderVictimSelectorNamesTheLeader(t *testing.T) {
	c := newFakeCluster("node2", threeNodes(), 2)
	sel := leaderVictimSelector(resolverFor(c, addr1, addr2, addr3),
		[]string{"node1", "node2", "node3"}, nil)

	victim, err := sel(context.Background())
	if err != nil {
		t.Fatalf("select victim: %v", err)
	}
	if victim != "node2" {
		t.Errorf("selected %q, want the leader node2", victim)
	}
}

// TestLeaderVictimSelectorReResolvesPerWindow is the reason the selector is a
// function rather than a value computed once: leadership moves between windows,
// including because the previous window killed the leader.
func TestLeaderVictimSelectorReResolvesPerWindow(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 1)
	sel := leaderVictimSelector(resolverFor(c, addr1, addr2, addr3),
		[]string{"node1", "node2", "node3"}, nil)

	first, err := sel(context.Background())
	if err != nil {
		t.Fatalf("first select: %v", err)
	}

	// The kill forced an election and node3 won it.
	c.setLeader("node3", 2)

	second, err := sel(context.Background())
	if err != nil {
		t.Fatalf("second select: %v", err)
	}

	if first != "node1" || second != "node3" {
		t.Errorf("selected %q then %q, want node1 then node3 — the selector must "+
			"re-resolve rather than reuse the first answer", first, second)
	}
}

// TestLeaderVictimSelectorRefusesALeaderOutsideTheKillSet is the design decision
// this mode rests on: the kill set is a fence, never a fallback. Redirecting the
// fault at a node that is not the leader would let a full run of green windows
// report success without ever having produced the leaderless window the mode
// exists to create.
func TestLeaderVictimSelectorRefusesALeaderOutsideTheKillSet(t *testing.T) {
	c := newFakeCluster("node1", threeNodes(), 1)
	sel := leaderVictimSelector(resolverFor(c, addr1, addr2, addr3),
		[]string{"node2", "node3"}, nil) // the historic gate's kill set

	victim, err := sel(context.Background())
	if err == nil {
		t.Fatalf("selected %q; the leader node1 is outside the kill set and the window must skip", victim)
	}
	if victim != "" {
		t.Errorf("returned victim %q alongside an error; a refused window must name nobody", victim)
	}
	if !strings.Contains(err.Error(), "node1") || !strings.Contains(err.Error(), "nemesis-services") {
		t.Errorf("error %q should name the leader and the flag that would admit it", err)
	}
}

// recordingNemesis is a Nemesis that records what it was asked to do.
type recordingNemesis struct {
	mu        sync.Mutex
	disrupted []string
	healed    []string
	failOn    map[string]error
}

func (n *recordingNemesis) Preflight(context.Context, []string) error { return nil }
func (n *recordingNemesis) Mode() string                              { return nemesisLeaderKill }

func (n *recordingNemesis) Disrupt(_ context.Context, victim string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disrupted = append(n.disrupted, victim)
	return n.failOn[victim]
}

func (n *recordingNemesis) Heal(_ context.Context, victim string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.healed = append(n.healed, victim)
	return nil
}

func (n *recordingNemesis) took() ([]string, []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.disrupted...), append([]string(nil), n.healed...)
}

// TestSchedulerKillsWhoTheSelectorNames pins the end-to-end attribution: the
// window's recorded victim and the node actually disrupted are the same node.
// This is the property that makes the fault-window table trustworthy for a mode
// whose victim is not known until strike time.
func TestSchedulerKillsWhoTheSelectorNames(t *testing.T) {
	nem := &recordingNemesis{}
	leaders := []string{"node1", "node3"}
	var idx int
	var mu sync.Mutex

	s := &Scheduler{
		Nemesis:  nem,
		Victims:  []string{"node1", "node2", "node3"},
		Interval: time.Millisecond,
		Downtime: time.Millisecond,
		SelectVictim: func(context.Context) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			v := leaders[idx%len(leaders)]
			idx++
			return v, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	disrupted, healed := nem.took()
	windows := s.Windows()
	if len(windows) < 2 {
		t.Fatalf("only %d windows recorded; expected the scheduler to cycle at least twice", len(windows))
	}
	if len(disrupted) < 2 {
		t.Fatalf("only %d disrupts: %v", len(disrupted), disrupted)
	}
	for i, w := range windows {
		if i >= len(disrupted) {
			break
		}
		if w.Victim != disrupted[i] {
			t.Errorf("window %d records victim %q but %q was disrupted", i+1, w.Victim, disrupted[i])
		}
		if w.Victim != leaders[i%len(leaders)] {
			t.Errorf("window %d victim %q, want %q from the selector", i+1, w.Victim, leaders[i%len(leaders)])
		}
	}
	// Every victim taken down must have been healed.
	for _, d := range disrupted {
		if !contains(healed, d) {
			t.Errorf("%q was disrupted but never healed", d)
		}
	}
}

// TestSchedulerRecordsASkippedWindowVisibly pins that a fault the nemesis
// declined to inject appears in the report. A silent skip is the one failure
// mode that looks exactly like a passing chaos run.
func TestSchedulerRecordsASkippedWindowVisibly(t *testing.T) {
	nem := &recordingNemesis{}
	s := &Scheduler{
		Nemesis:  nem,
		Victims:  []string{"node1"},
		Interval: time.Millisecond,
		Downtime: time.Millisecond,
		SelectVictim: func(context.Context) (string, error) {
			return "", errors.New("no leader resolvable: election in progress")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	windows := s.Windows()
	if len(windows) == 0 {
		t.Fatal("no windows recorded; a skipped fault must still be reported")
	}
	for i, w := range windows {
		if w.Skipped == "" {
			t.Errorf("window %d was not marked skipped", i+1)
		}
		if w.Victim != "" {
			t.Errorf("window %d names victim %q; a skipped window disrupted nobody", i+1, w.Victim)
		}
		if w.Injected() {
			t.Errorf("window %d counts as injected; a skipped window is not an outage", i+1)
		}
	}
	if disrupted, _ := nem.took(); len(disrupted) != 0 {
		t.Errorf("nemesis disrupted %v on a skipped window; it must disrupt nobody", disrupted)
	}
	if got := countInjected(windows); got != 0 {
		t.Errorf("countInjected=%d, want 0 — the report would overstate what the run tested", got)
	}

	// And the skip reaches the rendered report rather than only the struct.
	lines := formatFaultWindows(windows, windows[0].DownAt)
	if !strings.Contains(lines[0], "SKIPPED") || !strings.Contains(lines[0], "election in progress") {
		t.Errorf("rendered line %q does not surface the skip and its reason", lines[0])
	}
}

// TestLeaderKillModeIsAcceptedAndUsesAGracefulStop pins the flag plumbing and
// the SIGTERM choice: leader-kill exists to create a leaderless window, and a
// torn connection would add indeterminate writes that make a failure harder to
// attribute to the election.
func TestLeaderKillModeIsAcceptedAndUsesAGracefulStop(t *testing.T) {
	cfg, err := parseNemesisFlags(nemesisLeaderKill, "node1,node2,node3", "docker/docker-compose.yml",
		10*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("parseNemesisFlags: %v", err)
	}
	if cfg.Mode != nemesisLeaderKill {
		t.Errorf("mode %q, want %q", cfg.Mode, nemesisLeaderKill)
	}
	if !strings.Contains(cfg.Describe(), "among [node1,node2,node3]") {
		t.Errorf("Describe()=%q should say the services are the set the kill stays inside, not the set faults rotate over", cfg.Describe())
	}

	nem, err := newComposeNemesis(nemesisLeaderKill, "docker/docker-compose.yml")
	if err != nil {
		t.Fatalf("newComposeNemesis: %v", err)
	}
	if nem.Mode() != nemesisLeaderKill {
		t.Errorf("Mode()=%q, want %q", nem.Mode(), nemesisLeaderKill)
	}
	if nem.downCmd != "stop" {
		t.Errorf("downCmd=%q, want stop (SIGTERM)", nem.downCmd)
	}
}

// TestPreflightLeaderKillRejectsANodeIDThatIsNotAService pins the assumption the
// mode rests on. /status names a Raft node ID and `docker compose stop` takes a
// service name; in this stack they are equal by design. If they were not, every
// window would skip — so this is a startup error instead.
func TestPreflightLeaderKillRejectsANodeIDThatIsNotAService(t *testing.T) {
	c := newFakeCluster("raft-1", map[string]string{addr1: "raft-1", addr2: "raft-2"}, 1)
	r := resolverFor(c, addr1, addr2)

	err := preflightLeaderKill(context.Background(), r, []string{"node1", "node2", "node3"})
	if err == nil {
		t.Fatal("preflight accepted a leader that is not a compose service")
	}
	if !strings.Contains(err.Error(), "raft-1") {
		t.Errorf("error %q should name the leader it could not address", err)
	}

	// And it accepts the real stack's naming.
	ok := newFakeCluster("node2", threeNodes(), 1)
	if err := preflightLeaderKill(context.Background(), resolverFor(ok, addr1, addr2, addr3),
		[]string{"node1", "node2", "node3"}); err != nil {
		t.Errorf("preflight rejected a valid cluster: %v", err)
	}
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
