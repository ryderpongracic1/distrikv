package raft

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// ---------------------------------------------------------------------------
// In-process cluster harness
// ---------------------------------------------------------------------------

// inProcPeer is a kvpb.KVServiceClient that dispatches straight into another
// RaftNode's handlers, with an injectable one-way delay in front of each call.
//
// The delay is the latency-injection seam these tests need: it models a network
// hop that is slower than the leader's heartbeat send interval, and — like a
// real gRPC call — it is abandoned if the caller's context deadline expires
// first. That is exactly how a too-tight per-RPC heartbeat deadline manifests in
// production: the follower never sees the heartbeat at all.
type inProcPeer struct {
	kvpb.KVServiceClient
	target  *RaftNode
	latency time.Duration
}

// hop blocks for the injected latency, or fails the call if the caller's
// deadline expires first.
func (p *inProcPeer) hop(ctx context.Context) error {
	if p.latency <= 0 {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.latency):
		return nil
	}
}

func (p *inProcPeer) PreVote(ctx context.Context, req *kvpb.PreVoteRequest, _ ...grpc.CallOption) (*kvpb.PreVoteResponse, error) {
	if err := p.hop(ctx); err != nil {
		return nil, err
	}
	return p.target.HandlePreVote(ctx, req)
}

func (p *inProcPeer) RequestVote(ctx context.Context, req *kvpb.RequestVoteRequest, _ ...grpc.CallOption) (*kvpb.RequestVoteResponse, error) {
	if err := p.hop(ctx); err != nil {
		return nil, err
	}
	return p.target.HandleRequestVote(ctx, req)
}

func (p *inProcPeer) AppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	if err := p.hop(ctx); err != nil {
		return nil, err
	}
	return p.target.HandleAppendEntries(ctx, req)
}

func (p *inProcPeer) InstallSnapshot(ctx context.Context, req *kvpb.InstallSnapshotRequest, _ ...grpc.CallOption) (*kvpb.InstallSnapshotResponse, error) {
	if err := p.hop(ctx); err != nil {
		return nil, err
	}
	return p.target.HandleInstallSnapshot(ctx, req)
}

// testCluster is a set of RaftNodes wired to each other in-process.
type testCluster struct {
	nodes []*RaftNode
}

// newTestCluster builds and starts a 3-node cluster. latencyFor reports the
// one-way delay for an RPC sent from one node to another; a nil function means
// no injected latency anywhere.
func newTestCluster(t *testing.T, timing Config, latencyFor func(from, to string) time.Duration) *testCluster {
	t.Helper()

	// Discard logs: a broken cluster emits thousands of election lines, and a
	// handler bound to the test would also outlive the test via the RPC
	// goroutines still draining after cancellation.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ids := []string{"n1", "n2", "n3"}
	c := &testCluster{}
	for _, id := range ids {
		cfg := timing
		cfg.NodeID = id
		cfg.DataDir = t.TempDir()
		node, err := New(cfg, nil, &noopStore{}, &noopMetrics{}, logger)
		require.NoError(t, err)
		c.nodes = append(c.nodes, node)
	}

	// Wire peers before any node starts, so no lock is needed here.
	for _, node := range c.nodes {
		for _, other := range c.nodes {
			if other == node {
				continue
			}
			var latency time.Duration
			if latencyFor != nil {
				latency = latencyFor(node.nodeID, other.nodeID)
			}
			node.peers = append(node.peers, PeerClient{
				NodeID: other.nodeID,
				Client: &inProcPeer{target: other, latency: latency},
			})
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, node := range c.nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.Run(ctx)
		}()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	return c
}

// node returns the node with the given ID.
func (c *testCluster) node(t *testing.T, id string) *RaftNode {
	t.Helper()
	for _, n := range c.nodes {
		if n.nodeID == id {
			return n
		}
	}
	t.Fatalf("no node %q in cluster", id)
	return nil
}

// maxTerm returns the highest currentTerm across the given nodes.
func maxTerm(nodes ...*RaftNode) uint64 {
	var max uint64
	for _, n := range nodes {
		if term := n.CurrentTerm(); term > max {
			max = term
		}
	}
	return max
}

// leaderCount returns how many of the given nodes currently believe they lead.
func leaderCount(nodes ...*RaftNode) int {
	n := 0
	for _, node := range nodes {
		if node.IsLeader() {
			n++
		}
	}
	return n
}

// waitForLeader blocks until exactly one of the given nodes is leader.
func waitForLeader(t *testing.T, timeout time.Duration, nodes ...*RaftNode) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leaderCount(nodes...) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %v (leaders=%d, term=%d)",
		timeout, leaderCount(nodes...), maxTerm(nodes...))
}

// stormTiming mirrors the shape of the docker-compose configuration — an
// election floor several heartbeat intervals wide — scaled down so a test can
// observe many heartbeat rounds and many would-be elections in a few seconds.
func stormTiming() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
	}
}

// settleAndObserve waits for the cluster to settle on a leader, then asserts the
// term does not climb over the observation window. A leader-election storm
// advances the term once per election timeout — roughly 13 times over 3s at this
// timing — so the tolerance of 1 leaves a wide margin for a single scheduling
// hiccup while still failing decisively on a storm.
func settleAndObserve(t *testing.T, nodes ...*RaftNode) {
	t.Helper()

	waitForLeader(t, 3*time.Second, nodes...)
	time.Sleep(500 * time.Millisecond) // let the initial election fully settle

	before := maxTerm(nodes...)
	time.Sleep(3 * time.Second)
	after := maxTerm(nodes...)

	require.LessOrEqualf(t, after-before, uint64(1),
		"term advanced %d times in 3s (%d → %d): leadership is churning, "+
			"which means heartbeats are not reaching followers",
		after-before, before, after)
	require.Equal(t, 1, leaderCount(nodes...),
		"expected exactly one stable leader at the end of the observation window")
}

// ---------------------------------------------------------------------------
// Term-stability regression tests
// ---------------------------------------------------------------------------

// TestCluster_TermStableOnIdleCluster is the direct regression test for the
// leader-election storm: an idle, fully healthy 3-node cluster must elect once
// and then hold that term.
//
// Before the fix this failed with no injected latency at all. broadcastHeartbeat
// treated a fully-caught-up peer (nextIndex == snapLastIndex+1) as needing a
// snapshot and sent InstallSnapshot *instead of* a heartbeat; with no snapshot
// on disk that send returned early, so not a single heartbeat ever left the
// leader and every follower elected as soon as its timer expired.
func TestCluster_TermStableOnIdleCluster(t *testing.T) {
	c := newTestCluster(t, stormTiming(), nil)
	settleAndObserve(t, c.nodes...)
}

// TestCluster_TermStableUnderHeartbeatLatency injects a one-way delay of 1.5×
// the heartbeat send interval — the exact condition that used to cancel every
// heartbeat by its own deadline, since the per-RPC deadline was the send
// interval itself. The delay is still far inside the election floor, so a
// healthy cluster must ride it out without a single new term.
func TestCluster_TermStableUnderHeartbeatLatency(t *testing.T) {
	timing := stormTiming()
	latency := timing.HeartbeatInterval * 3 / 2
	require.Less(t, latency, timing.ElectionTimeoutMin,
		"injected latency must exceed one send interval but stay inside the election floor")

	c := newTestCluster(t, timing, func(_, _ string) time.Duration { return latency })
	settleAndObserve(t, c.nodes...)
}

// TestCluster_SlowPeerDoesNotStarveOtherPeers partitions n3 off — every RPC to or
// from it stalls past any deadline — while leaving n1↔n2 fast. The heartbeat owed
// to the reachable peer must be neither delayed nor suppressed by the stalled
// one, so n1 and n2 must still settle on a single leader and hold its term.
//
// n3 cannot disturb them: its pre-votes never arrive, so it never increments a
// term, which is precisely what the pre-vote phase is for.
func TestCluster_SlowPeerDoesNotStarveOtherPeers(t *testing.T) {
	timing := stormTiming()
	c := newTestCluster(t, timing, func(from, to string) time.Duration {
		if from == "n3" || to == "n3" {
			return 10 * timing.ElectionTimeoutMax // effectively unreachable
		}
		return 0
	})

	settleAndObserve(t, c.node(t, "n1"), c.node(t, "n2"))
}

// ---------------------------------------------------------------------------
// Snapshot-vs-heartbeat dispatch
// ---------------------------------------------------------------------------

// recordingPeer counts the RPCs a leader sends it.
type recordingPeer struct {
	kvpb.KVServiceClient
	appendEntries   atomic.Int64
	installSnapshot atomic.Int64
}

func (p *recordingPeer) AppendEntries(_ context.Context, _ *kvpb.AppendEntriesRequest, _ ...grpc.CallOption) (*kvpb.AppendEntriesResponse, error) {
	p.appendEntries.Add(1)
	return &kvpb.AppendEntriesResponse{Term: 1, Success: true}, nil
}

func (p *recordingPeer) InstallSnapshot(_ context.Context, _ *kvpb.InstallSnapshotRequest, _ ...grpc.CallOption) (*kvpb.InstallSnapshotResponse, error) {
	p.installSnapshot.Add(1)
	return &kvpb.InstallSnapshotResponse{Term: 1, Success: true}, nil
}

// newSnapshotLeader returns a leader that has compacted its log at index 5 and
// has that snapshot on disk, with one peer whose nextIndex is peerNext.
func newSnapshotLeader(t *testing.T, peerNext uint64) (*RaftNode, *recordingPeer) {
	t.Helper()

	node := newTestNode(t)
	peer := &recordingPeer{}
	node.peers = []PeerClient{{NodeID: "peer", Client: peer}}

	require.NoError(t, node.snapshotStore.Save(Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              map[string][]byte{"k": []byte("v")},
	}))

	node.currentTerm = 1
	node.role = Leader
	node.snapLastIndex = 5
	node.snapLastTerm = 1
	node.nextIndex = map[string]uint64{"peer": peerNext}
	node.matchIndex = map[string]uint64{"peer": 0}

	return node, peer
}

// TestBroadcastHeartbeat_CaughtUpPeerGetsNoSnapshot pins the boundary condition
// that caused the storm. A peer with nextIndex == snapLastIndex+1 is fully
// caught up: PrevLogIndex is snapLastIndex, whose term the leader still holds,
// so a plain heartbeat suffices.
//
// The old condition (nextIndex-1 <= snapLastIndex) called that peer behind,
// which was true of every peer in a cluster that never appends log entries. It
// mattered twice over: the heartbeat was skipped in favour of the snapshot, and
// had a snapshot ever existed on disk the leader would have shipped its entire
// store to a caught-up follower on every heartbeat interval.
func TestBroadcastHeartbeat_CaughtUpPeerGetsNoSnapshot(t *testing.T) {
	node, peer := newSnapshotLeader(t, 6) // snapLastIndex+1 — fully caught up

	node.broadcastHeartbeat(context.Background())
	require.Eventually(t, func() bool { return peer.appendEntries.Load() == 1 },
		time.Second, 5*time.Millisecond, "caught-up peer must receive a heartbeat")

	require.Zero(t, peer.installSnapshot.Load(),
		"a caught-up peer must not be sent a snapshot")
}

// TestBroadcastHeartbeat_BehindPeerGetsSnapshotAndHeartbeat verifies the peer
// that genuinely needs a snapshot gets one — and still gets its heartbeat, since
// snapshot delivery takes as long as it takes and the follower's election timer
// does not wait for it.
func TestBroadcastHeartbeat_BehindPeerGetsSnapshotAndHeartbeat(t *testing.T) {
	node, peer := newSnapshotLeader(t, 3) // asks for an entry compacted away

	node.broadcastHeartbeat(context.Background())
	require.Eventually(t, func() bool {
		return peer.appendEntries.Load() == 1 && peer.installSnapshot.Load() == 1
	}, time.Second, 5*time.Millisecond,
		"a lagging peer must receive both a snapshot and a heartbeat (got %d heartbeats, %d snapshots)",
		peer.appendEntries.Load(), peer.installSnapshot.Load())
}

// TestHeartbeatRPCTimeout_DecoupledFromSendInterval pins the deadline contract:
// a heartbeat must never be cancelled at one send interval, and must never
// outlive the follower's election floor.
func TestHeartbeatRPCTimeout_DecoupledFromSendInterval(t *testing.T) {
	node := newTestNode(t) // 150ms/300ms election window, 75ms heartbeat

	timeout := node.heartbeatRPCTimeout()
	require.Greater(t, timeout, node.heartbeatInterval,
		"a heartbeat cancelled at one send interval cannot survive jitter")
	require.Equal(t, node.electionTimeoutMin, timeout,
		"the deadline should be the election floor: past it, the reply is moot")

	// Floor: with an election window tighter than the send interval, the
	// deadline still stays wider than one interval rather than collapsing.
	node.electionTimeoutMin = 10 * time.Millisecond
	require.Equal(t, 2*node.heartbeatInterval, node.heartbeatRPCTimeout())
}
