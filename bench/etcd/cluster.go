package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// nodeSpec describes one member of the embedded etcd cluster.
//
// Ports are chosen so the three members occupy a contiguous block starting at
// etcd's default 2379/2380 pair: client ports 2379/2381/2383, peer ports
// 2380/2382/2384.
type nodeSpec struct {
	name       string
	clientPort int
	peerPort   int
}

// defaultNodes is the 3-member topology used unless --no-cluster is passed.
func defaultNodes() []nodeSpec {
	return []nodeSpec{
		{name: "ceiling-1", clientPort: 2379, peerPort: 2380},
		{name: "ceiling-2", clientPort: 2381, peerPort: 2382},
		{name: "ceiling-3", clientPort: 2383, peerPort: 2384},
	}
}

const (
	// clusterHost is the loopback address every member binds. The comparison is
	// deliberately single-host: distrikv's published numbers come from three
	// containers on one machine, so etcd gets the same physical topology.
	clusterHost = "127.0.0.1"

	// clusterToken scopes this cluster so a stray etcd from another run cannot
	// accidentally join it.
	clusterToken = "distrikv-ceiling"
)

// clusterConfig parameterises startCluster.
type clusterConfig struct {
	binary       string        // etcd executable, resolved via PATH if not absolute
	nodes        []nodeSpec    // members to launch
	readyTimeout time.Duration // how long to wait for a healthy quorum
}

// endpointsFor returns the client endpoints (host:port) for the given members,
// in declaration order.
func endpointsFor(nodes []nodeSpec) []string {
	eps := make([]string, len(nodes))
	for i, n := range nodes {
		eps[i] = fmt.Sprintf("%s:%d", clusterHost, n.clientPort)
	}
	return eps
}

// initialCluster renders etcd's --initial-cluster value: the peer URL of every
// member, keyed by member name.
func initialCluster(nodes []nodeSpec) string {
	parts := make([]string, len(nodes))
	for i, n := range nodes {
		parts[i] = fmt.Sprintf("%s=http://%s:%d", n.name, clusterHost, n.peerPort)
	}
	return strings.Join(parts, ",")
}

// nodeArgs builds the argv (excluding the binary itself) for one member.
//
// Split out as a pure function so the flag set is unit-testable without an etcd
// binary present: a typo here is otherwise only visible as a cluster that never
// reaches quorum.
func nodeArgs(n nodeSpec, dataDir string, all []nodeSpec) []string {
	clientURL := fmt.Sprintf("http://%s:%d", clusterHost, n.clientPort)
	peerURL := fmt.Sprintf("http://%s:%d", clusterHost, n.peerPort)
	return []string{
		"--name", n.name,
		"--data-dir", dataDir,
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", initialCluster(all),
		"--initial-cluster-token", clusterToken,
		"--initial-cluster-state", "new",
		// Quiet the per-request logging that would otherwise dominate the run's
		// wall time on a 6k-qps workload.
		"--log-level", "warn",
	}
}

// tailBuffer collects the last maxTailBytes of a child process's stderr.
//
// exec.Cmd writes to this from its own goroutine while the parent may read it
// to build an error message, so every access is mutex-guarded.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

const maxTailBytes = 8 << 10

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > maxTailBytes {
		t.buf = t.buf[len(t.buf)-maxTailBytes:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// member is one running etcd process.
type member struct {
	spec    nodeSpec
	cmd     *exec.Cmd
	stderr  *tailBuffer
	exited  chan struct{} // closed once the process has been reaped
	exitErr error         // set before exited is closed
}

// cluster is a running embedded etcd cluster owned by this process.
type cluster struct {
	members   []*member
	endpoints []string
	baseDir   string
	once      sync.Once
}

// startCluster launches every member, waits for a healthy quorum, and returns a
// handle whose Shutdown reclaims both the processes and their data directories.
//
// On any failure the partially-started cluster is torn down before returning,
// so a caller that gets an error owns nothing.
func startCluster(ctx context.Context, cfg clusterConfig) (*cluster, error) {
	bin, err := exec.LookPath(cfg.binary)
	if err != nil {
		return nil, fmt.Errorf("etcd binary %q not found on PATH (install it with `brew install etcd`, "+
			"or point --etcd-bin at it): %w", cfg.binary, err)
	}

	baseDir, err := os.MkdirTemp("", "distrikv-etcd-ceiling-")
	if err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	c := &cluster{
		endpoints: endpointsFor(cfg.nodes),
		baseDir:   baseDir,
	}

	for _, spec := range cfg.nodes {
		dataDir := filepath.Join(baseDir, spec.name)
		m := &member{
			spec:   spec,
			stderr: &tailBuffer{},
			exited: make(chan struct{}),
		}
		// Deliberately not exec.CommandContext: teardown is explicit via
		// Shutdown so members get SIGTERM (a clean etcd stop) rather than the
		// SIGKILL CommandContext would deliver on ctx cancellation.
		m.cmd = exec.Command(bin, nodeArgs(spec, dataDir, cfg.nodes)...)
		m.cmd.Stderr = m.stderr
		m.cmd.Stdout = m.stderr

		if err := m.cmd.Start(); err != nil {
			c.Shutdown()
			return nil, fmt.Errorf("start member %s: %w", spec.name, err)
		}
		c.members = append(c.members, m)

		go func(m *member) {
			m.exitErr = m.cmd.Wait()
			close(m.exited)
		}(m)
	}

	if err := c.waitHealthy(ctx, cfg.readyTimeout); err != nil {
		c.Shutdown()
		return nil, err
	}
	return c, nil
}

// waitHealthy polls every endpoint until all of them report a status with a
// known leader, meaning the cluster has both quorum and an elected leader.
func (c *cluster) waitHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		// A member that has already exited will never become healthy — fail
		// immediately with its output rather than burning the whole timeout.
		for _, m := range c.members {
			select {
			case <-m.exited:
				return fmt.Errorf("member %s exited during startup (%v); output:\n%s",
					m.spec.name, m.exitErr, m.stderr.String())
			default:
			}
		}

		lastErr = probeEndpoints(ctx, c.endpoints)
		if lastErr == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster not healthy within %s: %w; last member output:\n%s",
				timeout, lastErr, c.members[len(c.members)-1].stderr.String())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// probeEndpoints returns nil only when every endpoint answers Status with a
// non-zero leader.
func probeEndpoints(ctx context.Context, endpoints []string) error {
	for _, ep := range endpoints {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			DialTimeout: 2 * time.Second,
			Context:     ctx,
			// Silenced deliberately, and only here: a member that has not
			// finished binding its client port yet makes the retry interceptor
			// log a connection-refused warning on every poll, which is expected
			// during startup and would bury the harness's own output. The
			// workload clients keep their default logger so a cluster that goes
			// sick mid-run still says so.
			Logger: zap.NewNop(),
		})
		if err != nil {
			return fmt.Errorf("dial %s: %w", ep, err)
		}
		statusCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := cli.Status(statusCtx, ep)
		cancel()
		closeErr := cli.Close()
		if err != nil {
			return fmt.Errorf("status %s: %w", ep, err)
		}
		if st.Leader == 0 {
			return fmt.Errorf("status %s: no leader elected yet", ep)
		}
		if closeErr != nil {
			return fmt.Errorf("close probe client %s: %w", ep, closeErr)
		}
	}
	return nil
}

// Shutdown stops every member and removes the data directories. It is safe to
// call more than once and safe to call on a partially started cluster.
func (c *cluster) Shutdown() {
	c.once.Do(func() {
		for _, m := range c.members {
			if m.cmd.Process == nil {
				continue
			}
			// SIGTERM first: etcd flushes and closes its WAL, so a rerun of the
			// harness does not pay recovery cost from a torn store.
			if err := m.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				_ = m.cmd.Process.Kill()
			}
		}
		for _, m := range c.members {
			if m.cmd.Process == nil {
				continue
			}
			select {
			case <-m.exited:
			case <-time.After(5 * time.Second):
				_ = m.cmd.Process.Kill()
				<-m.exited
			}
		}
		if c.baseDir != "" {
			_ = os.RemoveAll(c.baseDir)
		}
	})
}

// splitEndpoints parses a comma-separated endpoint list, trimming whitespace and
// rejecting empty entries.
func splitEndpoints(s string) ([]string, error) {
	raw := strings.Split(s, ",")
	eps := make([]string, 0, len(raw))
	for _, r := range raw {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		eps = append(eps, r)
	}
	if len(eps) == 0 {
		return nil, errors.New("no endpoints given")
	}
	return eps, nil
}
