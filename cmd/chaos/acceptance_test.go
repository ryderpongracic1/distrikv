package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientpkg "github.com/ryderpongracic1/distrikv/internal/client"
	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

// This file is the acceptance measurement for the leader-kill checkability fix,
// run in-process so it needs no docker and no cluster.
//
// # What it models, and why the proxy matters
//
// The runner's --target is a published container port. When the nemesis stops
// that container, the thing listening on the host port does not vanish at the
// same instant the server behind it does, and on restart it starts accepting
// before the server is serving. So for a stretch at each edge of an outage,
// connections are accepted and then closed with nothing behind them — and a
// request written into one of those returns EOF, which cannot prove the request
// was unread. That is what makes the write indeterminate, and at several thousand
// ops per second a short window produces hundreds of them.
//
// gatedProxy reproduces exactly that: a stable front door whose backend can be
// removed, accepting and closing while it is gone. It is the only faithful way to
// get this shape without docker — closing the listener instead would produce
// refused dials, which classify cleanly and are not the problem.
//
// # What it proves
//
// Two arms, identical but for one flag. The disabled arm reproduces the
// pathology; the enabled arm shows it collapsing to the scale of the worker
// count.
//
// And the oracle: the backend is a mutex-guarded map, which is a correct
// linearizable register by construction. So a FAIL from the checker on the
// enabled arm could not be the store's fault — it could only mean the harness
// recorded a write that happened as a write that did not. That makes the PASS the
// soundness assertion, not just a performance one.

// kvBackend is a linearizable key-value register over HTTP, speaking the subset
// of distrikv's API that internal/client uses.
type kvBackend struct {
	mu   sync.Mutex
	data map[string]string
}

func newKVBackend() *kvBackend {
	return &kvBackend{data: map[string]string{}}
}

func (b *kvBackend) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, clientpkg.StatusResponse{
			NodeID: "oracle", Leader: "oracle", Role: "leader", Term: 1,
		})
	})
	mux.HandleFunc("PUT /keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		var req clientpkg.PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad body"})
			return
		}
		b.mu.Lock()
		b.data[r.PathValue("key")] = req.Value
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		v, ok := b.data[r.PathValue("key")]
		b.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, http.StatusOK, clientpkg.GetResponse{Value: v})
	})
	mux.HandleFunc("DELETE /keys/{key}", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		_, ok := b.data[r.PathValue("key")]
		delete(b.data, r.PathValue("key"))
		b.mu.Unlock()
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// gatedProxy is a stable listening address whose backend can be taken away.
//
// Removing the backend does two things, and it needs both to model a container
// going away:
//
//   - every live connection is closed, as the server's own sockets are when its
//     process exits. This is what a client's pooled keep-alive connection sees.
//   - new connections are still accepted, then closed without being read or
//     answered. This is the port-forwarder that outlives the container it points
//     at, and it is the shape that produces unknown-outcome writes at the full
//     offered rate.
//
// Closing the listener instead would produce refused dials, which classify
// cleanly and are not the problem.
type gatedProxy struct {
	ln      net.Listener
	backend atomic.Pointer[string]
	wg      sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newGatedProxy(t *testing.T) *gatedProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &gatedProxy{ln: ln, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.serve(c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		p.setBackend("")
		p.wg.Wait()
	})
	return p
}

func (p *gatedProxy) addr() string { return p.ln.Addr().String() }

// setBackend points the proxy at addr, or removes the backend when addr is "",
// closing every connection currently open.
func (p *gatedProxy) setBackend(addr string) {
	if addr == "" {
		p.backend.Store(nil)
		p.mu.Lock()
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		return
	}
	p.backend.Store(&addr)
}

func (p *gatedProxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

func (p *gatedProxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

func (p *gatedProxy) serve(down net.Conn) {
	p.track(down)
	defer func() {
		p.untrack(down)
		_ = down.Close()
	}()

	target := p.backend.Load()
	if target == nil {
		// Accepted, and nothing behind it. A request written here gets EOF.
		return
	}
	up, err := net.DialTimeout("tcp", *target, time.Second)
	if err != nil {
		return
	}
	p.track(up)
	defer func() {
		p.untrack(up)
		_ = up.Close()
	}()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, down); done <- struct{}{} }()
	go func() { _, _ = io.Copy(down, up); done <- struct{}{} }()
	<-done
}

// armResult is one arm of the acceptance measurement.
type armResult struct {
	name          string
	ops           int64
	errs          int64
	indeterminate int64
	refused       int64
	events        int
	pauses        breakerStats
	breakdown     []string
}

func (r armResult) String() string {
	return fmt.Sprintf("%-20s ops=%-8d errors=%-8d indeterminate=%-6d events=%-8d "+
		"pauses=%d/%s skipped=%d",
		r.name, r.ops, r.errs, r.indeterminate, r.events,
		r.pauses.Episodes, r.pauses.Paused.Round(time.Millisecond), r.pauses.Skipped)
}

// TestLeaderKillShapeCollapsesIndeterminateWrites is the acceptance measurement.
//
// It runs the workload against a target that is taken away twice, once with
// fail-fast disabled (--fail-fast-after=0, the behaviour before this change) and
// once with it enabled, and compares the number of writes whose outcome the runner
// cannot determine. It then checks the enabled arm's history, which is the
// property the whole change exists for: a leader-kill-shaped run has to reach a
// verdict inside the default budget.
func TestLeaderKillShapeCollapsesIndeterminateWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance measurement drives a few seconds of real load")
	}

	arms := []struct {
		name      string
		threshold int
	}{
		{"fail-fast off", 0},
		{"fail-fast on", 5},
	}

	results := map[string]armResult{}
	var enabledRec *linearizability.Recorder

	for _, arm := range arms {
		res, rec := runLeaderKillShape(t, arm.name, arm.threshold)
		results[arm.name] = res
		if arm.threshold > 0 {
			enabledRec = rec
		}
		t.Log(res)
		for _, line := range res.breakdown {
			t.Logf("    %s", line)
		}
	}

	off := results["fail-fast off"]
	on := results["fail-fast on"]

	// 1. The pathology reproduces. Without fail-fast the ambiguous writes are
	//    proportional to how long something accepted connections with nothing
	//    behind it, not to the number of workers.
	if off.indeterminate < 50 {
		t.Fatalf("the disabled arm produced only %d indeterminate writes; the harness "+
			"did not reproduce the shape this change is about", off.indeterminate)
	}

	// 2. Enabling fail-fast collapses it to the scale of the worker count. The
	//    residue is the genuine kill-instant tear: the requests already on a
	//    connection when the backend went away, which no classifier can resolve.
	if on.indeterminate > int64(8*acceptanceWorkers) {
		t.Errorf("indeterminate writes with fail-fast on = %d, want at most %d "+
			"(~worker-count scale); the pause is not confining them to the transitions",
			on.indeterminate, 8*acceptanceWorkers)
	}
	if on.indeterminate*5 > off.indeterminate {
		t.Errorf("indeterminate writes: %d with fail-fast on vs %d off — the reduction "+
			"is under 5×, which is not the order of magnitude the checker needs",
			on.indeterminate, off.indeterminate)
	}

	// 3. The pause is reported, not silent. Both halves matter: an operator
	//    reading the summary has to be able to see that load was withheld, and
	//    how much.
	if on.pauses.Episodes == 0 {
		t.Error("fail-fast never engaged, so the comparison above measured nothing")
	}
	if on.pauses.Skipped == 0 {
		t.Error("no operations were reported as skipped, so the offered-load change is invisible")
	}
	if on.pauses.Paused <= 0 {
		t.Error("no paused time was reported")
	}

	// 4. The verdict lands, and it lands on the right side. The backend is a
	//    mutex-guarded map — a correct linearizable register — so a FAIL here
	//    would mean the runner recorded a write that happened as one that did
	//    not. This is the soundness assertion, and it is the reason the whole
	//    classification change is safe to make.
	start := time.Now()
	ok, timedOut := enabledRec.CheckTimeout(30 * time.Second)
	t.Logf("check of the fail-fast arm: linearizable=%t timedOut=%t in %s over %d events",
		ok, timedOut, time.Since(start).Round(time.Millisecond), enabledRec.Len())
	if timedOut {
		t.Fatalf("the check timed out on %d events with %d indeterminate writes — "+
			"the run is still unverifiable", enabledRec.Len(), on.indeterminate)
	}
	if !ok {
		t.Fatal("the history of a correct linearizable register was reported non-linearizable: " +
			"the runner recorded a write that happened as a write that did not")
	}
}

// TestFollowerOutageShapeNeverPausesTheWorkload is the no-regression half of the
// acceptance measurement.
//
// stop-restart and kill-restart take down a node the runner is not driving. The
// target stays up and answers 503 for every write it cannot replicate — tens of
// thousands of them in a gate run. Those gates were already checkable, and this
// change must not alter their offered load or their classification at all.
//
// So: full rate against a target that refuses every write, and the breaker must
// stay out of the way completely.
func TestFollowerOutageShapeNeverPausesTheWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("drives real load")
	}

	// A target that is emphatically alive and emphatically refusing.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut, http.MethodDelete:
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": "replication failed: connect: connection refused"})
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		}
	}))
	defer backend.Close()

	transport := chaosTransport(&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second})
	client := clientpkg.NewWithTransport(
		clientpkg.Config{Host: strings.TrimPrefix(backend.URL, "http://")}, transport)

	breaker := newTargetBreaker(5, 250*time.Millisecond, func(ctx context.Context) error {
		_, err := client.Status(ctx)
		return err
	}, t.Logf)

	c := &counters{}
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	runWorkers(ctx, acceptanceWorkers, makeKeys("follower-shape", acceptanceKeyspace),
		50, 5, client, nil, c, breaker)

	stats := breaker.Stats()
	t.Logf("follower-outage shape: ops=%d refused=%d indeterminate=%d pauses=%d skipped=%d",
		c.ops.Load(), c.refusedWrites.Load(), c.indeterminateWrites.Load(),
		stats.Episodes, stats.Skipped)

	if c.refusedWrites.Load() == 0 {
		t.Fatal("setup: the target refused no writes, so nothing was exercised")
	}
	if stats.Episodes != 0 {
		t.Errorf("the workload paused %d time(s) against a serving target — stop-restart "+
			"and kill-restart gates would offer less load than before this change",
			stats.Episodes)
	}
	if stats.Skipped != 0 {
		t.Errorf("%d operation(s) were skipped against a serving target", stats.Skipped)
	}
	if got := c.indeterminateWrites.Load(); got != 0 {
		t.Errorf("indeterminate writes = %d, want 0: a 503 is positive evidence the "+
			"primary applied the write", got)
	}
}

// Acceptance-run shape. Small and short: the point is the ratio between the two
// arms, not absolute throughput.
const (
	acceptanceWorkers  = 6
	acceptanceKeyspace = 10
	acceptanceOutage   = 700 * time.Millisecond
	acceptanceUp       = 400 * time.Millisecond
)

// runLeaderKillShape drives one arm: real client, real transport, real HTTP, with
// the backend taken away twice.
func runLeaderKillShape(t *testing.T, name string, threshold int) (armResult, *linearizability.Recorder) {
	t.Helper()

	backend := httptest.NewServer(newKVBackend().handler())
	defer backend.Close()
	backendAddr := strings.TrimPrefix(backend.URL, "http://")

	proxy := newGatedProxy(t)
	proxy.setBackend(backendAddr)

	// The transport main() installs, with a shorter response-header timeout so a
	// stalled request does not dominate a test that runs for a few seconds. The
	// classification of every error shape is unaffected by the value.
	transport := chaosTransport(&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second})
	client := clientpkg.NewWithTransport(clientpkg.Config{Host: proxy.addr()}, transport)

	breaker := newTargetBreaker(threshold, 250*time.Millisecond, func(ctx context.Context) error {
		_, err := client.Status(ctx)
		return err
	}, func(format string, args ...any) { t.Logf("["+name+"] "+format, args...) })

	rec := &linearizability.Recorder{}
	c := &counters{}
	keys := makeKeys(fmt.Sprintf("acc-%s-%d", strings.ReplaceAll(name, " ", "_"), time.Now().UnixNano()), acceptanceKeyspace)

	// Two outages, mirroring a gate run's shape at a tenth of the duration.
	total := 2*acceptanceUp + 2*acceptanceOutage + acceptanceUp
	ctx, cancel := context.WithTimeout(context.Background(), total)
	defer cancel()

	var nemesis sync.WaitGroup
	nemesis.Add(1)
	go func() {
		defer nemesis.Done()
		for i := 0; i < 2; i++ {
			if !sleep(ctx, acceptanceUp) {
				return
			}
			proxy.setBackend("") // accepted, nothing behind it
			if !sleep(ctx, acceptanceOutage) {
				proxy.setBackend(backendAddr)
				return
			}
			proxy.setBackend(backendAddr)
		}
	}()

	runWorkers(ctx, acceptanceWorkers, keys, 50, 5, client, rec, c, breaker)
	nemesis.Wait()

	return armResult{
		name:          name,
		ops:           c.ops.Load(),
		errs:          c.errors.Load(),
		indeterminate: c.indeterminateWrites.Load(),
		refused:       c.refusedWrites.Load(),
		events:        rec.Len(),
		pauses:        breaker.Stats(),
		breakdown:     c.failureBreakdown(),
	}, rec
}
