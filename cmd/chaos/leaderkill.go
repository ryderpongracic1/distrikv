package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

// Leader-kill victim selection.
//
// # What this mode exercises, and why it did not exist before
//
// Every chaos gate up to now killed node2 and node3 while node1 held
// leadership, so a *leaderless window under fault load* had never been tested:
// no run had ever taken the Raft leader out while writes were flowing and a
// replica was behind. That window is the one case where the consensus health
// signal cannot make progress — no leader, no committed health entry — so it is
// the discriminating case for the planned removal of the transport probe. This
// nemesis creates it deliberately.
//
// # The design decision: it kills the leader or it kills nothing
//
// The hard part is that the runner's own --target may be the leader. Two
// behaviours were available: re-resolve and kill whoever leads (accepting that
// the target may die), or restrict the kill to --nemesis-services and fall back
// to some other member when the leader is outside that set.
//
// This implementation takes the first, with the kill set kept as a fence rather
// than a fallback: the victim is always the current leader, and if the leader is
// not in --nemesis-services the fault window is *skipped and reported* instead of
// being redirected at a node that is not the leader.
//
// The reason is that a fallback would let a mode named leader-kill produce a
// full run of green windows without ever taking a leader down — a passing gate
// that tested nothing, which is the same silent-degradation failure the nemesis
// Preflight already exists to prevent. A skipped window is visible in the
// report, in the "faults injected: N of M attempted" line, and in the log; a
// misdirected kill is not visible anywhere.
//
// The cost is real and accepted: when the target is the leader, the runner's
// operations fail for the duration of the outage. That cost is already modelled —
// failed reads and writes are recorded as errors, and a write whose outcome is
// unknown becomes an indeterminate (pending) operation the checker may place
// anywhere. Passing --peers keeps the convergence check working across the
// outage.

// nodeStatus is the part of GET /status this mode needs.
type nodeStatus struct {
	NodeID string `json:"node_id"`
	Leader string `json:"leader"`
	Term   uint64 `json:"term"`
	Role   string `json:"role"`
}

// raftRoleLeader is the role string internal/raft reports for a leader.
const raftRoleLeader = "leader"

// leaderResolver answers "which node is the Raft leader right now" over HTTP.
//
// It is deliberately conservative: it would rather report that it could not tell
// than name a node that is not the leader, because the caller is about to kill
// whatever it names.
type leaderResolver struct {
	// addrs is every node's client HTTP address (host:port), target included.
	addrs []string

	// fetch reads one node's status. Tests replace it; production uses
	// fetchNodeStatus.
	fetch func(ctx context.Context, addr string) (nodeStatus, error)

	// RetryFor bounds how long resolve keeps trying while an election is in
	// progress; RetryEvery is the gap between attempts. An election on the
	// docker-compose timing settles in 0.5–1s, so a couple of seconds of retry
	// distinguishes "mid-election" from "no leader at all".
	retryFor   time.Duration
	retryEvery time.Duration
}

func newLeaderResolver(addrs []string, hc *http.Client) *leaderResolver {
	return &leaderResolver{
		addrs: addrs,
		fetch: func(ctx context.Context, addr string) (nodeStatus, error) {
			return fetchNodeStatus(ctx, hc, addr)
		},
		retryFor:   3 * time.Second,
		retryEvery: 200 * time.Millisecond,
	}
}

// leaderEvidence records how a leader was identified, so the log line can say
// what the claim rests on.
type leaderEvidence string

const (
	// evidenceSelf means the node itself reported role=leader. This is the
	// strongest evidence available over HTTP: the node that will be killed is
	// the node that made the claim.
	evidenceSelf leaderEvidence = "self-reported"

	// evidencePeers means no node reported itself leader, but every reachable
	// node named the same one. Weaker — a follower's view of the leader can lag
	// its actual loss — so it is only used when a self-report is unavailable,
	// which in practice means the leader's own HTTP address was not supplied.
	evidencePeers leaderEvidence = "peer-reported"
)

// resolveLeader names the current Raft leader, retrying briefly across an
// election.
//
// Resolution rules, in order:
//
//  1. If any reachable node reports role=leader, take it. On the split view that
//     a partition can produce — two nodes claiming leadership in different terms
//     — the higher term wins, because the lower one is a leader that has not yet
//     learned it was superseded.
//  2. Otherwise, if every reachable node names the same non-empty leader, take
//     that. This covers a leader whose own HTTP address was not passed in.
//  3. Otherwise report that no leader could be resolved. The caller must skip
//     the window rather than guess.
func (r *leaderResolver) resolveLeader(ctx context.Context) (string, leaderEvidence, error) {
	if len(r.addrs) == 0 {
		return "", "", fmt.Errorf("no node addresses to ask (pass --peers)")
	}

	deadline := time.Now().Add(r.retryFor)
	var lastWhy string
	for {
		id, ev, why := r.resolveOnce(ctx)
		if id != "" {
			return id, ev, nil
		}
		lastWhy = why
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		if !sleep(ctx, r.retryEvery) {
			break
		}
	}
	return "", "", fmt.Errorf("no leader after %s: %s", r.retryFor, lastWhy)
}

// resolveOnce performs one round of status queries. It returns an empty node ID
// and a human-readable reason when the round is inconclusive.
func (r *leaderResolver) resolveOnce(ctx context.Context) (string, leaderEvidence, string) {
	type claim struct {
		id   string
		term uint64
	}
	var selfClaims []claim
	named := make(map[string]struct{})
	reachable, unreachable := 0, 0

	for _, addr := range r.addrs {
		st, err := r.fetch(ctx, addr)
		if err != nil {
			unreachable++
			continue
		}
		reachable++
		if strings.EqualFold(st.Role, raftRoleLeader) && st.NodeID != "" {
			selfClaims = append(selfClaims, claim{id: st.NodeID, term: st.Term})
		}
		if st.Leader != "" {
			named[st.Leader] = struct{}{}
		}
	}

	if reachable == 0 {
		return "", "", fmt.Sprintf("none of the %d node addresses answered /status", len(r.addrs))
	}
	if len(selfClaims) > 0 {
		// Highest term wins; ties broken by node ID so the choice is
		// deterministic rather than dependent on map or slice ordering.
		sort.Slice(selfClaims, func(i, j int) bool {
			if selfClaims[i].term != selfClaims[j].term {
				return selfClaims[i].term > selfClaims[j].term
			}
			return selfClaims[i].id < selfClaims[j].id
		})
		return selfClaims[0].id, evidenceSelf, ""
	}
	if len(named) == 1 {
		for id := range named {
			return id, evidencePeers, ""
		}
	}
	if len(named) == 0 {
		return "", "", fmt.Sprintf("%d node(s) answered and none named a leader — election in progress", reachable)
	}
	return "", "", fmt.Sprintf("reachable nodes disagree on the leader (%s)", strings.Join(sortedKeys(named), ", "))
}

// fetchNodeStatus reads one node's /status.
func fetchNodeStatus(ctx context.Context, hc *http.Client, addr string) (nodeStatus, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/status", nil)
	if err != nil {
		return nodeStatus{}, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nodeStatus{}, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nodeStatus{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	var st nodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nodeStatus{}, err
	}
	if st.NodeID == "" {
		return nodeStatus{}, fmt.Errorf("empty node_id")
	}
	return st, nil
}

// leaderVictimSelector returns a Scheduler victim selector that names the
// current leader, or a skip reason when it must not act.
//
// The two skip reasons are kept distinct in the message because they mean
// different things to whoever reads the report: "no leader resolvable" is the
// cluster mid-election (expected occasionally, and itself informative), while
// "the leader is not in the kill set" is a misconfigured run whose windows will
// keep skipping until the flags change.
func leaderVictimSelector(r *leaderResolver, killSet []string, logf func(string, ...any)) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		leader, evidence, err := r.resolveLeader(ctx)
		if err != nil {
			return "", fmt.Errorf("no leader resolvable: %w", err)
		}
		if !slices.Contains(killSet, leader) {
			return "", fmt.Errorf("leader %s is not in --nemesis-services [%s]; "+
				"pass every member (e.g. --nemesis-services %s) to let this mode do its job",
				leader, strings.Join(killSet, ","), strings.Join(append(slices.Clone(killSet), leader), ","))
		}
		if logf != nil {
			logf("nemesis: resolved leader %s (%s)", leader, evidence)
		}
		return leader, nil
	}
}

// preflightLeaderKill checks the two things that would otherwise make every
// window skip: that a leader can be resolved at all, and that the node IDs the
// cluster reports are the compose service names the nemesis will address.
//
// The second is the assumption this mode rests on — /status names a Raft node ID
// and `docker compose stop` takes a service name — and in this stack they are
// deliberately equal. Checking it here turns a silent run of skipped windows
// into a startup error.
func preflightLeaderKill(ctx context.Context, r *leaderResolver, definedServices []string) error {
	leader, _, err := r.resolveLeader(ctx)
	if err != nil {
		return fmt.Errorf("leader-kill needs a resolvable leader before the run starts: %w", err)
	}
	if !slices.Contains(definedServices, leader) {
		return fmt.Errorf("the cluster reports its leader as %q, which is not a service in the compose file "+
			"(defined: %s) — leader-kill addresses nodes by compose service name and needs those names to "+
			"match the node IDs /status reports", leader, strings.Join(definedServices, ", "))
	}
	return nil
}

// sortedKeys is a deterministic rendering of a set, for messages.
func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
