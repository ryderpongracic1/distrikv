package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The workload is a port of cmd/bench/workload.go. These tests pin the
// properties that make the two harnesses comparable — key format, value bytes,
// Zipf determinism, mix thresholds — because a silent divergence in any of them
// would invalidate every number the harness produces without failing anything.

func TestParseMix(t *testing.T) {
	tests := []struct {
		in                        string
		wantPut, wantGet, wantDel int
		wantErr                   bool
	}{
		{in: "20:80:0", wantPut: 20, wantGet: 100, wantDel: 100},
		{in: "100:0:0", wantPut: 100, wantGet: 100, wantDel: 100},
		{in: "0:100:0", wantPut: 0, wantGet: 100, wantDel: 100},
		{in: "50:45:5", wantPut: 50, wantGet: 95, wantDel: 100},
		{in: "0:0:0", wantErr: true},
		{in: "-1:100:0", wantErr: true},
		{in: "nonsense", wantErr: true},
		{in: "20:80", wantErr: true},
	}
	for _, tc := range tests {
		p, g, d, err := parseMix(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseMix(%q): want error, got (%d,%d,%d)", tc.in, p, g, d)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseMix(%q): unexpected error %v", tc.in, err)
			continue
		}
		if p != tc.wantPut || g != tc.wantGet || d != tc.wantDel {
			t.Errorf("parseMix(%q) = (%d,%d,%d), want (%d,%d,%d)",
				tc.in, p, g, d, tc.wantPut, tc.wantGet, tc.wantDel)
		}
	}
}

func TestNewWorkloadRejectsBadInput(t *testing.T) {
	tests := []struct {
		name      string
		keyspace  int
		keyDist   string
		mix       string
		valueSize int
	}{
		{name: "zero keyspace", keyspace: 0, keyDist: "zipf", mix: "20:80:0", valueSize: 256},
		{name: "negative keyspace", keyspace: -5, keyDist: "zipf", mix: "20:80:0", valueSize: 256},
		{name: "unknown keydist", keyspace: 100, keyDist: "gaussian", mix: "20:80:0", valueSize: 256},
		{name: "empty mix", keyspace: 100, keyDist: "zipf", mix: "", valueSize: 256},
		{name: "zero valuesize", keyspace: 100, keyDist: "zipf", mix: "20:80:0", valueSize: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newWorkload(tc.keyspace, tc.keyDist, tc.mix, tc.valueSize); err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}

func TestKeyFormatMatchesDistrikvBench(t *testing.T) {
	w, err := newWorkload(100_000, "sequential", "100:0:0", 256)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	// cmd/bench emits "k" + 15 zero-padded digits so keys sort
	// lexicographically. Both stores must see identical key strings.
	got := w.nextKey(42)
	if want := "k000000000000042"; got != want {
		t.Errorf("nextKey(42) = %q, want %q", got, want)
	}
	if len(got) != 16 {
		t.Errorf("key length = %d, want 16", len(got))
	}
	// sequential wraps at keyspace.
	w2, err := newWorkload(10, "sequential", "100:0:0", 256)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	if got, want := w2.nextKey(12), "k000000000000002"; got != want {
		t.Errorf("nextKey(12) with keyspace=10 = %q, want %q", got, want)
	}
}

func TestValueBytesMatchDistrikvBench(t *testing.T) {
	const size = 30
	w, err := newWorkload(10, "uniform", "100:0:0", size)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	if len(w.value) != size {
		t.Fatalf("value length = %d, want %d", len(w.value), size)
	}
	// 'a' + i%26 → "abc...xyzabcd" for size 30.
	if want := "abcdefghijklmnopqrstuvwxyzabcd"; string(w.value) != want {
		t.Errorf("value = %q, want %q", w.value, want)
	}
}

func TestZipfIsDeterministicAndSkewed(t *testing.T) {
	const keyspace = 1000
	draw := func() []string {
		w, err := newWorkload(keyspace, "zipf", "100:0:0", 8)
		if err != nil {
			t.Fatalf("newWorkload: %v", err)
		}
		out := make([]string, 200)
		for i := range out {
			out[i] = w.nextKey(uint64(i))
		}
		return out
	}
	// Seeded with source(1) in both harnesses, so the key sequence is
	// reproducible run to run — a prerequisite for comparing two stores.
	a, b := draw(), draw()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("zipf draw %d differs across instances: %q vs %q", i, a[i], b[i])
		}
	}

	// s=1.1 skew: the low end of the keyspace should dominate.
	hot := 0
	for _, k := range a {
		if strings.HasSuffix(k, "000000000000000") || k < "k000000000000010" {
			hot++
		}
	}
	if hot < len(a)/4 {
		t.Errorf("zipf(s=1.1) drew only %d/%d keys from the first 10 of %d — not skewed",
			hot, len(a), keyspace)
	}
}

func TestNextKeyWithinKeyspace(t *testing.T) {
	for _, dist := range []string{"uniform", "zipf", "sequential"} {
		w, err := newWorkload(50, dist, "100:0:0", 8)
		if err != nil {
			t.Fatalf("newWorkload(%s): %v", dist, err)
		}
		for i := 0; i < 500; i++ {
			k := w.nextKey(uint64(i))
			if len(k) != 16 || k[0] != 'k' {
				t.Fatalf("%s: malformed key %q", dist, k)
			}
			if k > "k000000000000049" {
				t.Fatalf("%s: key %q outside keyspace 0..49", dist, k)
			}
		}
	}
}

func TestNextOpHonoursMix(t *testing.T) {
	tests := []struct {
		mix                       string
		wantPut, wantGet, wantDel bool
	}{
		{mix: "100:0:0", wantPut: true},
		{mix: "0:100:0", wantGet: true},
		{mix: "0:0:100", wantDel: true},
		{mix: "20:80:0", wantPut: true, wantGet: true},
	}
	for _, tc := range tests {
		w, err := newWorkload(100, "uniform", tc.mix, 8)
		if err != nil {
			t.Fatalf("newWorkload(%s): %v", tc.mix, err)
		}
		seen := map[opKind]bool{}
		for i := 0; i < 3000; i++ {
			seen[w.nextOp()] = true
		}
		if seen[opPut] != tc.wantPut {
			t.Errorf("mix %s: put seen=%v want=%v", tc.mix, seen[opPut], tc.wantPut)
		}
		if seen[opGet] != tc.wantGet {
			t.Errorf("mix %s: get seen=%v want=%v", tc.mix, seen[opGet], tc.wantGet)
		}
		if seen[opDelete] != tc.wantDel {
			t.Errorf("mix %s: delete seen=%v want=%v", tc.mix, seen[opDelete], tc.wantDel)
		}
	}
}

func TestSharesFromMix(t *testing.T) {
	w, err := newWorkload(100, "uniform", "20:80:0", 8)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	got := w.shares()
	if got[opPut] != 0.2 || got[opGet] != 0.8 || got[opDelete] != 0 {
		t.Errorf("shares() = %v, want [0.2 0.8 0]", got)
	}
}

func TestDispatchRejectsUnknownOp(t *testing.T) {
	w, err := newWorkload(10, "uniform", "100:0:0", 8)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	// nil client is never reached: the unknown op falls through the switch.
	if err := w.dispatch(context.Background(), nil, opKind(99), "k"); err == nil {
		t.Fatal("want error for unknown op, got nil")
	}
}

func TestConcurrentWorkloadUse(t *testing.T) {
	// One workload instance is shared by every worker, so both generators must
	// be race-free. Run under -race to make this meaningful.
	w, err := newWorkload(1000, "zipf", "20:80:0", 64)
	if err != nil {
		t.Fatalf("newWorkload: %v", err)
	}
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			deadline := time.Now().Add(50 * time.Millisecond)
			var i uint64
			for time.Now().Before(deadline) {
				i++
				_ = w.nextKey(i)
				_ = w.nextOp()
			}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
}
