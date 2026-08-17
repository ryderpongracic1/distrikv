package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

// fakeNode is a minimal stand-in for a distrikv node's HTTP surface: it accepts
// PUT /keys/{key} and records what it was asked to store. failFirstN lets a test
// make the first N attempts on every key fail with 503, mimicking the
// write-stall backpressure a real prefill provokes.
type fakeNode struct {
	mu       sync.Mutex
	stored   map[string]string
	attempts map[string]int

	failFirstN  int  // per-key attempts to reject with 503 before succeeding
	failForever bool // reject every attempt

	srv *httptest.Server
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	f := &fakeNode{
		stored:   make(map[string]string),
		attempts: make(map[string]int),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/keys/", f.handleKey)
	// The bench probes /status before doing anything else.
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"node_id": "fake", "role": "leader"})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeNode) handleKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/keys/")
	body, _ := io.ReadAll(r.Body)

	f.mu.Lock()
	f.attempts[key]++
	n := f.attempts[key]
	fail := f.failForever || n <= f.failFirstN
	if !fail {
		var payload struct {
			Value string `json:"value"`
		}
		_ = json.Unmarshal(body, &payload)
		f.stored[key] = payload.Value
	}
	f.mu.Unlock()

	if fail {
		http.Error(w, "write stalled: L0 compaction backlog", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// host returns the host:port the bench client should target.
func (f *fakeNode) host() string {
	return strings.TrimPrefix(f.srv.URL, "http://")
}

func (f *fakeNode) storedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stored)
}

func (f *fakeNode) attemptsFor(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[key]
}

func newTestClients(t *testing.T, nodes ...*fakeNode) []*client.Client {
	t.Helper()
	cs := make([]*client.Client, len(nodes))
	for i, n := range nodes {
		cs[i] = client.New(client.Config{Host: n.host(), Timeout: 5 * time.Second})
	}
	return cs
}

// TestPrefillCoversEveryKeyExactlyOnce is the property that justifies the flag's
// existence. The existing --mix/--keydist flags can write *a lot* of keys, but
// only exhaustive coverage makes a subsequent read-path hit rate meaningful, so
// coverage is asserted key-by-key rather than by count alone.
func TestPrefillCoversEveryKeyExactlyOnce(t *testing.T) {
	node := newFakeNode(t)
	const keyspace = 500

	res := runPrefill(context.Background(), prefillConfig{
		keyspace: keyspace,
		value:    []byte("payload"),
		workers:  16,
		clients:  newTestClients(t, node),
	})

	if res.failed != 0 {
		t.Fatalf("failed = %d, want 0 (first error: %v)", res.failed, res.firstErr)
	}
	if res.keysWritten != keyspace {
		t.Errorf("keysWritten = %d, want %d", res.keysWritten, keyspace)
	}
	if got := node.storedCount(); got != keyspace {
		t.Errorf("stored %d distinct keys, want %d", got, keyspace)
	}
	for n := uint64(0); n < keyspace; n++ {
		key := keyForIndex(n)
		if got := node.attemptsFor(key); got != 1 {
			t.Errorf("key %s: %d attempts, want exactly 1", key, got)
		}
	}
	if res.elapsed <= 0 {
		t.Error("elapsed not recorded")
	}
}

// TestPrefillKeysMatchWorkloadKeys pins the alignment between what a prefill
// writes and what a later read phase asks for. If these two ever diverge, every
// read becomes a miss on a key that was never written and the measured hit rate
// silently collapses — so the shared formatter is asserted directly.
func TestPrefillKeysMatchWorkloadKeys(t *testing.T) {
	const keyspace = 64
	wl, err := newWorkload(keyspace, "sequential", "0:100:0", 16)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}

	node := newFakeNode(t)
	res := runPrefill(context.Background(), prefillConfig{
		keyspace: keyspace,
		value:    []byte("v"),
		workers:  4,
		clients:  newTestClients(t, node),
	})
	if res.failed != 0 {
		t.Fatalf("prefill failed: %v", res.firstErr)
	}

	// Every key the sequential workload will read must already be stored.
	for seq := uint64(1); seq <= keyspace; seq++ {
		key := wl.nextKey(seq)
		node.mu.Lock()
		_, ok := node.stored[key]
		node.mu.Unlock()
		if !ok {
			t.Fatalf("workload key %q (seq=%d) was not written by prefill", key, seq)
		}
	}
}

// TestPrefillRetriesTransientFailures covers the write-stall case: a prefill is
// the workload most likely to push the engine into L0 backpressure, so a 503 on
// the first attempts must be retried rather than counted as a hole.
func TestPrefillRetriesTransientFailures(t *testing.T) {
	node := newFakeNode(t)
	node.failFirstN = 2 // every key fails twice, then succeeds
	const keyspace = 20

	res := runPrefill(context.Background(), prefillConfig{
		keyspace: keyspace,
		value:    []byte("v"),
		workers:  4,
		clients:  newTestClients(t, node),
	})

	if res.failed != 0 {
		t.Fatalf("failed = %d, want 0 — transient 503s must be retried (first error: %v)",
			res.failed, res.firstErr)
	}
	if res.keysWritten != keyspace {
		t.Errorf("keysWritten = %d, want %d", res.keysWritten, keyspace)
	}
	if res.retries != keyspace*2 {
		t.Errorf("retries = %d, want %d (2 per key)", res.retries, keyspace*2)
	}
	if got := node.storedCount(); got != keyspace {
		t.Errorf("stored %d keys, want %d", got, keyspace)
	}
}

// TestPrefillReportsUnrecoverableFailures asserts the honest-failure path: when
// a key cannot be written at all, prefill must say so, because main exits rather
// than publishing a hit rate over a keyspace with holes.
func TestPrefillReportsUnrecoverableFailures(t *testing.T) {
	node := newFakeNode(t)
	node.failForever = true
	const keyspace = 3

	res := runPrefill(context.Background(), prefillConfig{
		keyspace: keyspace,
		value:    []byte("v"),
		// One worker per key so all three retry ladders run concurrently: the
		// real backoff constants are exercised, but the test pays for one ladder
		// (~5s) rather than two rounds of them.
		workers: keyspace,
		clients: newTestClients(t, node),
	})

	if res.failed != keyspace {
		t.Errorf("failed = %d, want %d", res.failed, keyspace)
	}
	if res.keysWritten != 0 {
		t.Errorf("keysWritten = %d, want 0", res.keysWritten)
	}
	if res.firstErr == nil {
		t.Error("firstErr is nil; want the underlying write failure for diagnostics")
	}
	// Each key must have exhausted its attempt budget, not given up early.
	for n := uint64(0); n < keyspace; n++ {
		if got := node.attemptsFor(keyForIndex(n)); got != prefillMaxAttempts {
			t.Errorf("key %s: %d attempts, want %d", keyForIndex(n), got, prefillMaxAttempts)
		}
	}
}

// TestPrefillHonorsContextCancellation keeps a Ctrl-C responsive during the
// multi-minute prefill a large keyspace requires.
func TestPrefillHonorsContextCancellation(t *testing.T) {
	node := newFakeNode(t)
	node.failForever = true // forces the backoff path, where cancellation matters

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	done := make(chan *prefillResult, 1)
	go func() {
		done <- runPrefill(ctx, prefillConfig{
			keyspace: 100_000,
			value:    []byte("v"),
			workers:  4,
			clients:  newTestClients(t, node),
		})
	}()

	select {
	case res := <-done:
		if res.keysWritten != 0 {
			t.Errorf("keysWritten = %d, want 0", res.keysWritten)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runPrefill did not return after context cancellation")
	}
}

// TestPrefillRoundRobinsClients checks that a multi-target prefill spreads work
// across every endpoint rather than hammering the first one.
func TestPrefillRoundRobinsClients(t *testing.T) {
	a, b := newFakeNode(t), newFakeNode(t)

	res := runPrefill(context.Background(), prefillConfig{
		keyspace: 200,
		value:    []byte("v"),
		workers:  4, // even worker count → both clients used
		clients:  newTestClients(t, a, b),
	})
	if res.failed != 0 {
		t.Fatalf("prefill failed: %v", res.firstErr)
	}
	if a.storedCount() == 0 || b.storedCount() == 0 {
		t.Errorf("work not spread across targets: a=%d b=%d", a.storedCount(), b.storedCount())
	}
	if total := a.storedCount() + b.storedCount(); total != 200 {
		t.Errorf("total stored = %d, want 200", total)
	}
}

// TestPrefillProgressWritesLines covers the progress output a long prefill needs
// to avoid looking hung, and pins that a decile boundary is reported once.
func TestPrefillProgressWritesLines(t *testing.T) {
	node := newFakeNode(t)
	var buf strings.Builder

	res := runPrefill(context.Background(), prefillConfig{
		keyspace: 100,
		value:    []byte("v"),
		workers:  1, // deterministic ordering
		clients:  newTestClients(t, node),
		progress: &buf,
	})
	if res.failed != 0 {
		t.Fatalf("prefill failed: %v", res.firstErr)
	}

	out := buf.String()
	if !strings.Contains(out, "prefill") {
		t.Errorf("no progress output; got %q", out)
	}
	if got := strings.Count(out, "\n"); got < 5 || got > 10 {
		t.Errorf("progress lines = %d, want one per decile (5..10); got:\n%s", got, out)
	}
}

// TestReportOmitsPrefillWhenAbsent guards the promise that adding --prefill did
// not change existing output: without it, neither the table nor the JSON gains a
// prefill field.
func TestReportOmitsPrefillWhenAbsent(t *testing.T) {
	rep := testReport(t, nil)

	var table strings.Builder
	rep.writeTable(&table)
	if strings.Contains(table.String(), "prefill") {
		t.Errorf("table output mentions prefill without --prefill:\n%s", table.String())
	}

	var jsonBuf strings.Builder
	if err := rep.writeJSON(&jsonBuf); err != nil {
		t.Fatalf("writeJSON: %v", err)
	}
	if strings.Contains(jsonBuf.String(), "prefill") {
		t.Errorf("JSON output includes prefill key without --prefill:\n%s", jsonBuf.String())
	}
}

// TestReportIncludesPrefillWhenPresent asserts the audit trail: a read-path
// measurement is only trustworthy alongside the prefill that produced it, so the
// prefill stats travel with the numbers in both output formats.
func TestReportIncludesPrefillWhenPresent(t *testing.T) {
	pf := &prefillResult{
		keysWritten: 500_000,
		retries:     42,
		failed:      0,
		elapsed:     250 * time.Second,
	}
	rep := testReport(t, pf)

	if rep.Prefill == nil {
		t.Fatal("report.Prefill is nil")
	}
	if rep.Prefill.KeysWritten != 500_000 || rep.Prefill.Retries != 42 {
		t.Errorf("prefill summary = %+v, want keys=500000 retries=42", *rep.Prefill)
	}
	if got := rep.Prefill.KeysPerSec; got < 1999 || got > 2001 {
		t.Errorf("KeysPerSec = %.2f, want ~2000", got)
	}

	var table strings.Builder
	rep.writeTable(&table)
	if !strings.Contains(table.String(), "prefill:") {
		t.Errorf("table output missing prefill line:\n%s", table.String())
	}

	var jsonBuf strings.Builder
	if err := rep.writeJSON(&jsonBuf); err != nil {
		t.Fatalf("writeJSON: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(jsonBuf.String()), &decoded); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if _, ok := decoded["prefill"]; !ok {
		t.Errorf("JSON output missing prefill object:\n%s", jsonBuf.String())
	}
}

// testReport builds a minimal report for output assertions.
func testReport(t *testing.T, pf *prefillResult) *report {
	t.Helper()
	wl, err := newWorkload(1000, "zipf", "0:100:0", 256)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	cfg := runConfig{qps: 100, duration: time.Second, workers: 4, queueCap: 16, wl: wl}
	phase := run(context.Background(), runConfig{
		qps: 1, duration: time.Millisecond, workers: 1, queueCap: 1, wl: wl,
		clients: []*client.Client{client.New(client.Config{Host: "127.0.0.1:1", Timeout: time.Millisecond})},
	})
	return buildReport(cfg, phase, map[string]uint64{}, map[string]uint64{}, "fake:8001", 256, "0:100:0", pf)
}

// TestKeyForIndexFormat pins the on-the-wire key format. The LSM relies on keys
// sorting lexicographically in the same order as their numeric index, which
// zero-padding is what provides.
func TestKeyForIndexFormat(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "k000000000000000"},
		{1, "k000000000000001"},
		{999, "k000000000000999"},
		{500_000, "k000000000500000"},
	}
	for _, c := range cases {
		if got := keyForIndex(c.n); got != c.want {
			t.Errorf("keyForIndex(%d) = %q, want %q", c.n, got, c.want)
		}
	}
	// Lexicographic order must match numeric order.
	prev := keyForIndex(0)
	for n := uint64(1); n < 2000; n++ {
		cur := keyForIndex(n)
		if !(prev < cur) {
			t.Fatalf("key order broken: %q !< %q", prev, cur)
		}
		prev = cur
	}
	if got, want := len(keyForIndex(7)), len(fmt.Sprintf("k%015d", 7)); got != want {
		t.Errorf("key length = %d, want %d", got, want)
	}
}

// TestMetricsDelta covers the counter-subtraction helper, including the restart
// case: a node that restarts mid-run resets its counters, and a negative delta
// is the only signal of that, so it must survive rather than be clamped.
func TestMetricsDelta(t *testing.T) {
	start := map[string]uint64{"bloom_hits": 100, "compactions_total": 4, "gone": 7}
	end := map[string]uint64{"bloom_hits": 350, "compactions_total": 4, "new_key": 9}

	got := metricsDelta(start, end)

	if got["bloom_hits"] != 250 {
		t.Errorf("bloom_hits delta = %d, want 250", got["bloom_hits"])
	}
	if got["compactions_total"] != 0 {
		t.Errorf("compactions_total delta = %d, want 0", got["compactions_total"])
	}
	if got["new_key"] != 9 {
		t.Errorf("new_key delta = %d, want 9 (absent from start = zero baseline)", got["new_key"])
	}
	if _, ok := got["gone"]; ok {
		t.Error("delta contains a key absent from the end snapshot")
	}

	// Restart: end below start must report negative, not zero.
	restarted := metricsDelta(map[string]uint64{"wal_writes": 5000}, map[string]uint64{"wal_writes": 12})
	if restarted["wal_writes"] >= 0 {
		t.Errorf("wal_writes delta = %d, want negative to expose the counter reset",
			restarted["wal_writes"])
	}
}

// TestPrefillEngineDeltaReported asserts the write-path evidence reaches the
// output. A read-only measurement window cannot show flush_bytes or compactions
// — they are earned during prefill — so losing this line would leave the run
// looking like it never touched disk.
func TestPrefillEngineDeltaReported(t *testing.T) {
	pf := &prefillResult{
		keysWritten: 1000,
		elapsed:     time.Second,
		engineDelta: map[string]int64{
			"flush_bytes":              12 << 20,
			"compactions_total":        3,
			"compaction_bytes_written": 30 << 20,
			"write_stall_count":        17,
		},
	}
	rep := testReport(t, pf)

	var table strings.Builder
	rep.writeTable(&table)
	out := table.String()
	for _, want := range []string{"during prefill", "compactions=3", "write_stalls=17", "12.0MB"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}

	var jsonBuf strings.Builder
	if err := rep.writeJSON(&jsonBuf); err != nil {
		t.Fatalf("writeJSON: %v", err)
	}
	if !strings.Contains(jsonBuf.String(), "engine_delta") {
		t.Errorf("JSON missing prefill engine_delta:\n%s", jsonBuf.String())
	}
}
