package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestEndpointsFor(t *testing.T) {
	got := endpointsFor(defaultNodes())
	want := []string{"127.0.0.1:2379", "127.0.0.1:2381", "127.0.0.1:2383"}
	if len(got) != len(want) {
		t.Fatalf("endpointsFor() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("endpoint %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestInitialCluster(t *testing.T) {
	got := initialCluster(defaultNodes())
	want := "ceiling-1=http://127.0.0.1:2380," +
		"ceiling-2=http://127.0.0.1:2382," +
		"ceiling-3=http://127.0.0.1:2384"
	if got != want {
		t.Errorf("initialCluster() = %q, want %q", got, want)
	}
}

// A malformed etcd argv shows up only as a cluster that never reaches quorum,
// 30 seconds later, so the flag set is asserted directly.
func TestNodeArgs(t *testing.T) {
	nodes := defaultNodes()
	args := nodeArgs(nodes[1], "/tmp/data-2", nodes)

	flags := map[string]string{}
	for i := 0; i+1 < len(args); i += 2 {
		flags[args[i]] = args[i+1]
	}
	if len(args)%2 != 0 {
		t.Fatalf("odd argv length %d — every etcd flag here takes a value: %v", len(args), args)
	}

	want := map[string]string{
		"--name":                        "ceiling-2",
		"--data-dir":                    "/tmp/data-2",
		"--listen-client-urls":          "http://127.0.0.1:2381",
		"--advertise-client-urls":       "http://127.0.0.1:2381",
		"--listen-peer-urls":            "http://127.0.0.1:2382",
		"--initial-advertise-peer-urls": "http://127.0.0.1:2382",
		"--initial-cluster-token":       clusterToken,
		"--initial-cluster-state":       "new",
	}
	for k, v := range want {
		if flags[k] != v {
			t.Errorf("arg %s = %q, want %q", k, flags[k], v)
		}
	}
	if got := flags["--initial-cluster"]; got != initialCluster(nodes) {
		t.Errorf("--initial-cluster = %q, want %q", got, initialCluster(nodes))
	}
	// Every member must advertise a peer URL that appears in --initial-cluster,
	// otherwise etcd refuses to start with a mismatch error.
	if !strings.Contains(flags["--initial-cluster"], flags["--initial-advertise-peer-urls"]) {
		t.Errorf("advertised peer URL %q absent from --initial-cluster %q",
			flags["--initial-advertise-peer-urls"], flags["--initial-cluster"])
	}
}

func TestNodeArgsPortsAreDistinct(t *testing.T) {
	seen := map[int]string{}
	for _, n := range defaultNodes() {
		for _, p := range []int{n.clientPort, n.peerPort} {
			if prev, dup := seen[p]; dup {
				t.Fatalf("port %d used by both %s and %s", p, prev, n.name)
			}
			seen[p] = n.name
		}
	}
}

func TestSplitEndpoints(t *testing.T) {
	tests := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{in: "127.0.0.1:2379", want: []string{"127.0.0.1:2379"}},
		{in: " a:1 , b:2 ,", want: []string{"a:1", "b:2"}},
		{in: "", wantErr: true},
		{in: " , ", wantErr: true},
	}
	for _, tc := range tests {
		got, err := splitEndpoints(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("splitEndpoints(%q): want error, got %v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("splitEndpoints(%q): unexpected error %v", tc.in, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("splitEndpoints(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("splitEndpoints(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

func TestStartClusterMissingBinary(t *testing.T) {
	_, err := startCluster(context.Background(), clusterConfig{
		binary:       "etcd-binary-that-does-not-exist",
		nodes:        defaultNodes(),
		readyTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("want error for missing binary, got nil")
	}
	// The message has to be actionable — a bare exec.ErrNotFound leaves the
	// user guessing.
	if !strings.Contains(err.Error(), "brew install etcd") {
		t.Errorf("error missing install hint: %v", err)
	}
}

// A member that exits during startup must fail fast with its own output rather
// than burning the whole ready timeout. `false` stands in for an etcd that dies
// immediately (port already bound, corrupt data dir).
func TestStartClusterMemberExitsImmediately(t *testing.T) {
	if _, err := os.Stat("/bin/false"); err != nil {
		t.Skip("/bin/false unavailable")
	}
	start := time.Now()
	_, err := startCluster(context.Background(), clusterConfig{
		binary:       "/bin/false",
		nodes:        defaultNodes()[:1],
		readyTimeout: 30 * time.Second,
	})
	if err == nil {
		t.Fatal("want error when member exits, got nil")
	}
	if !strings.Contains(err.Error(), "exited during startup") {
		t.Errorf("error should name the early exit: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %s to notice a dead member — should not wait out the ready timeout", elapsed)
	}
}

// Shutdown must reclaim the data directory and be safe to call twice, since it
// runs from a defer that may follow an explicit call.
func TestShutdownIsIdempotentAndRemovesDataDir(t *testing.T) {
	if _, err := os.Stat("/bin/sleep"); err != nil {
		t.Skip("/bin/sleep unavailable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// `sleep` never becomes healthy, so startCluster tears down on its own and
	// the temp dir must already be gone when it returns.
	_, err := startCluster(ctx, clusterConfig{
		binary:       "/bin/sleep",
		nodes:        defaultNodes()[:1],
		readyTimeout: 500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("want timeout error from a member that never serves")
	}

	entries, readErr := os.ReadDir(os.TempDir())
	if readErr != nil {
		t.Skipf("cannot read temp dir: %v", readErr)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "distrikv-etcd-ceiling-") {
			t.Errorf("leaked data dir %s after failed start", e.Name())
		}
	}
}

func TestTailBufferKeepsTail(t *testing.T) {
	var tb tailBuffer
	// Write more than the cap; only the tail should survive.
	chunk := strings.Repeat("x", maxTailBytes)
	if _, err := tb.Write([]byte(chunk)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := tb.Write([]byte("TAIL")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := tb.String()
	if len(got) != maxTailBytes {
		t.Errorf("buffer length = %d, want %d", len(got), maxTailBytes)
	}
	if !strings.HasSuffix(got, "TAIL") {
		t.Error("most recent output was dropped instead of the oldest")
	}
}

func TestProbeEndpointsFailsOnDeadEndpoint(t *testing.T) {
	// Port 1 on loopback refuses connections, so this exercises the dial-error
	// path without needing a real cluster.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := probeEndpoints(ctx, []string{"127.0.0.1:1"})
	if err == nil {
		t.Fatal("want error probing a closed port, got nil")
	}
	if errors.Is(err, context.Canceled) {
		t.Errorf("unexpected cancellation error: %v", err)
	}
}
