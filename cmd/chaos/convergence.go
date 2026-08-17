package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
)

// Convergence verification.
//
// distrikv refuses a write whose replica does not ACK, but keeps it locally —
// so every fault window leaves the primary ahead of its replicas for the keys
// written during it. The chaos runner already counts those writes
// (refused-but-applied); this file answers the question that count raises: did
// the cluster ever put them right?
//
// The check reads every measured key from every node the ring says should hold it
// and asserts they all agree. It runs after the nemesis has healed and after a
// grace period, because anti-entropy is deliberately after-the-fact: it converges
// replicas once they are reachable and stable, not while they are down.
//
// Reads use ?local=true so each node answers from its own store. A plain GET
// would be worthless here: a non-owning node forwards to the ring-primary, so
// every node would report the primary's value and every run would "converge".

// convergenceConfig configures the check.
type convergenceConfig struct {
	// Enabled turns the check on. Default is on for the fault-injecting nemeses,
	// where divergence is expected to occur and therefore expected to be repaired.
	Enabled bool

	// Grace is how long to keep re-checking before declaring the cluster
	// divergent. Catch-up is triggered by a peer's health transition and gated on
	// stable health, so a heal is followed by a short delay before repair even
	// begins.
	Grace time.Duration

	// Nodes is every node's client HTTP address (host:port), including the target.
	Nodes []string

	// Replicas is the cluster's replication factor R, so the check asks the same
	// question of the ring that the nodes do.
	Replicas int

	// PollInterval is the delay between attempts within the grace window.
	PollInterval time.Duration
}

// convergenceResult reports what the check found.
type convergenceResult struct {
	Checked   bool   `json:"checked"`
	Skipped   string `json:"skipped,omitempty"`
	Converged bool   `json:"converged"`

	// KeysChecked is the number of keys inspected, and Replicas the number of
	// node-reads per key, so the reader can see how much was actually verified.
	KeysChecked int `json:"keys_checked"`
	NodeReads   int `json:"node_reads"`

	Divergent   int      `json:"divergent_keys"`
	Examples    []string `json:"divergent_examples,omitempty"`
	Unreachable []string `json:"unreachable_nodes,omitempty"`
	Attempts    int      `json:"attempts"`
	Elapsed     string   `json:"elapsed"`
}

// checkConvergence polls the cluster until every replica agrees on every key or
// the grace window expires.
func checkConvergence(ctx context.Context, cfg convergenceConfig, keys []string, hc *http.Client) convergenceResult {
	res := convergenceResult{Checked: true}
	start := time.Now()

	if len(cfg.Nodes) < 2 {
		res.Checked = false
		res.Skipped = "needs every node's HTTP address — pass --peers (e.g. --peers localhost:8002,localhost:8003)"
		return res
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.Replicas < 1 {
		cfg.Replicas = 2
	}

	// Map each node's address to its node ID, then rebuild the same ring the
	// cluster uses. Ring placement is a hash of the node ID alone, so a ring built
	// from HTTP addresses places keys exactly as the nodes' own gRPC-addressed
	// rings do.
	ring := cluster.New()
	addrByNode := make(map[string]string, len(cfg.Nodes))
	for _, addr := range cfg.Nodes {
		id, err := fetchNodeID(ctx, hc, addr)
		if err != nil {
			res.Unreachable = append(res.Unreachable, fmt.Sprintf("%s (%v)", addr, err))
			continue
		}
		ring.AddNode(id, addr)
		addrByNode[id] = addr
	}
	if len(res.Unreachable) > 0 {
		// A node that cannot be asked cannot be shown to have converged. Saying
		// "converged" while a replica was unreachable would be the one failure mode
		// that makes this whole check worthless.
		res.Elapsed = time.Since(start).Round(time.Millisecond).String()
		return res
	}
	if ring.NodeCount() < 2 {
		res.Checked = false
		res.Skipped = "fewer than two nodes answered /status"
		res.Elapsed = time.Since(start).Round(time.Millisecond).String()
		return res
	}

	deadline := time.Now().Add(cfg.Grace)
	for {
		res.Attempts++
		divergent, reads, examples, err := compareReplicas(ctx, hc, ring, addrByNode, keys, cfg.Replicas)
		res.KeysChecked = len(keys)
		res.NodeReads = reads
		res.Divergent = len(divergent)
		res.Examples = examples

		switch {
		case err != nil:
			res.Unreachable = append(res.Unreachable, err.Error())
		case len(divergent) == 0:
			res.Converged = true
			res.Elapsed = time.Since(start).Round(time.Millisecond).String()
			return res
		}

		if !time.Now().Before(deadline) || ctx.Err() != nil {
			res.Elapsed = time.Since(start).Round(time.Millisecond).String()
			return res
		}
		select {
		case <-ctx.Done():
			res.Elapsed = time.Since(start).Round(time.Millisecond).String()
			return res
		case <-time.After(cfg.PollInterval):
		}
	}
}

// compareReplicas reads every key from every node that should hold it and returns
// the keys whose replicas disagree, plus a few worked examples for the report.
func compareReplicas(
	ctx context.Context,
	hc *http.Client,
	ring *cluster.Ring,
	addrByNode map[string]string,
	keys []string,
	replicas int,
) (divergent []string, reads int, examples []string, err error) {
	const maxExamples = 3

	for _, key := range keys {
		owners, ringErr := ring.GetN(key, replicas)
		if ringErr != nil {
			return nil, reads, nil, fmt.Errorf("ring lookup for %q: %w", key, ringErr)
		}

		type observation struct {
			node    string
			present bool
			value   string
		}
		var seen []observation

		for _, vn := range owners {
			addr, ok := addrByNode[vn.NodeID]
			if !ok {
				return nil, reads, nil, fmt.Errorf("no HTTP address for ring member %q", vn.NodeID)
			}
			value, present, readErr := localGet(ctx, hc, addr, key)
			reads++
			if readErr != nil {
				return nil, reads, nil, fmt.Errorf("local read of %q from %s: %w", key, addr, readErr)
			}
			seen = append(seen, observation{node: vn.NodeID, present: present, value: value})
		}

		agreed := true
		for _, o := range seen[1:] {
			if o.present != seen[0].present || o.value != seen[0].value {
				agreed = false
				break
			}
		}
		if agreed {
			continue
		}

		divergent = append(divergent, key)
		if len(examples) < maxExamples {
			parts := make([]string, 0, len(seen))
			for _, o := range seen {
				if !o.present {
					parts = append(parts, o.node+"=<absent>")
					continue
				}
				parts = append(parts, fmt.Sprintf("%s=%q", o.node, truncate(o.value, 24)))
			}
			examples = append(examples, fmt.Sprintf("%s: %s", key, strings.Join(parts, " ")))
		}
	}

	sort.Strings(divergent)
	return divergent, reads, examples, nil
}

// fetchNodeID reads a node's own ID from /status.
func fetchNodeID(ctx context.Context, hc *http.Client, addr string) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/status", nil)
	if err != nil {
		return "", err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var body struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.NodeID == "" {
		return "", fmt.Errorf("empty node_id")
	}
	return body.NodeID, nil
}

// localGet reads one key from one node's own store, without forwarding.
func localGet(ctx context.Context, hc *http.Client, addr, key string) (value string, present bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	url := "http://" + addr + "/keys/" + key + "?local=true"
	req, reqErr := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if reqErr != nil {
		return "", false, reqErr
	}
	resp, doErr := hc.Do(req)
	if doErr != nil {
		return "", false, doErr
	}
	defer drain(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		var body struct {
			Value string `json:"value"`
		}
		if decErr := json.NewDecoder(resp.Body).Decode(&body); decErr != nil {
			return "", false, decErr
		}
		return body.Value, true, nil
	case http.StatusNotFound:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("status %d", resp.StatusCode)
	}
}

// drain reads and closes a response body so the connection returns to the pool.
// Leaving a body unread is what defeated connection reuse in internal/client and
// exhausted the ephemeral port range; the same mistake is not worth repeating in
// the checker.
func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// parseNodeAddrs builds the node list for the check: the target plus whatever
// --peers supplied, de-duplicated and order-preserving.
func parseNodeAddrs(target, peers string) []string {
	out := []string{target}
	seen := map[string]struct{}{target: {}}
	for _, p := range strings.Split(peers, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// convergenceLines renders the check for the table report.
func (r convergenceResult) convergenceLines() []string {
	if !r.Checked {
		if r.Skipped == "" {
			return []string{fmt.Sprintf("  %-24s off", "converged:")}
		}
		return []string{fmt.Sprintf("  %-24s skipped — %s", "converged:", r.Skipped)}
	}

	var lines []string
	summary := fmt.Sprintf("%t", r.Converged)
	if r.Converged {
		summary += fmt.Sprintf(" (after %s, %d keys × %d node reads)", r.Elapsed, r.KeysChecked, r.NodeReads)
	} else if len(r.Unreachable) > 0 {
		summary += " — could not verify every replica"
	} else {
		summary += fmt.Sprintf(" — %d of %d keys still disagree after %s", r.Divergent, r.KeysChecked, r.Elapsed)
	}
	lines = append(lines, fmt.Sprintf("  %-24s %s", "converged:", summary))

	for _, ex := range r.Examples {
		lines = append(lines, "    "+ex)
	}
	for _, u := range r.Unreachable {
		lines = append(lines, "    unreachable: "+u)
	}
	return lines
}
