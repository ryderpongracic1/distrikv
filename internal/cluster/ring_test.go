package cluster

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRingDistribution verifies that with 3 nodes and 150 virtual nodes each,
// no single physical node owns more than 40% or fewer than 20% of a large
// sample of keys — i.e., the distribution is acceptably balanced.
func TestRingDistribution(t *testing.T) {
	r := New()
	nodes := []string{"node1", "node2", "node3"}
	for _, n := range nodes {
		r.AddNode(n, n+":9000")
	}

	const total = 10_000
	counts := make(map[string]int)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", rng.Int63())
		vn, err := r.Get(key)
		require.NoError(t, err)
		counts[vn.NodeID]++
	}

	for _, n := range nodes {
		pct := float64(counts[n]) / float64(total)
		assert.GreaterOrEqual(t, pct, 0.20, "node %s owns too few keys (%.1f%%)", n, pct*100)
		assert.LessOrEqual(t, pct, 0.40, "node %s owns too many keys (%.1f%%)", n, pct*100)
	}
}

// TestRingGetN_DistinctNodes verifies that GetN always returns the requested
// number of distinct physical nodes (not virtual node duplicates).
func TestRingGetN_DistinctNodes(t *testing.T) {
	r := New()
	r.AddNode("node1", "node1:9001")
	r.AddNode("node2", "node2:9002")
	r.AddNode("node3", "node3:9003")

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	for _, key := range keys {
		vnodes, err := r.GetN(key, 2)
		require.NoError(t, err)
		require.Len(t, vnodes, 2, "key %q: expected 2 nodes", key)

		assert.NotEqual(t, vnodes[0].NodeID, vnodes[1].NodeID,
			"key %q: GetN returned duplicate node %s", key, vnodes[0].NodeID)
	}
}

// TestRingWrap verifies that a key whose hash lands at (or beyond) the maximum
// ring position wraps around and returns the first virtual node.
func TestRingWrap(t *testing.T) {
	r := New()
	r.AddNode("only", "only:9001")

	// Any key must resolve to the single node even at the wrap boundary.
	vn, err := r.Get("any-key-at-all")
	require.NoError(t, err)
	assert.Equal(t, "only", vn.NodeID)
}

// TestRingGetEmpty returns an error when the ring has no nodes.
func TestRingGetEmpty(t *testing.T) {
	r := New()
	_, err := r.Get("key")
	assert.Error(t, err)
}

// TestRingAddNodeIdempotent verifies that calling AddNode twice for the same
// node does not double-count its virtual nodes.
func TestRingAddNodeIdempotent(t *testing.T) {
	r := New()
	r.AddNode("node1", "node1:9001")
	r.AddNode("node1", "node1:9001") // duplicate — should replace, not append
	r.AddNode("node2", "node2:9002")

	// With two physical nodes and 150 vnodes each, total vnodes == 300.
	r.mu.RLock()
	total := len(r.vnodes)
	r.mu.RUnlock()

	assert.Equal(t, 2*VirtualNodesPerPhysical, total,
		"expected exactly 2 * %d vnodes, got %d", VirtualNodesPerPhysical, total)
}

// TestRingRemoveNode verifies that after removing a node its keys route to
// surviving nodes.
func TestRingRemoveNode(t *testing.T) {
	r := New()
	r.AddNode("node1", "node1:9001")
	r.AddNode("node2", "node2:9002")
	r.AddNode("node3", "node3:9003")
	r.RemoveNode("node2")

	assert.Equal(t, 2, r.NodeCount())

	for i := 0; i < 200; i++ {
		vn, err := r.Get(fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		assert.NotEqual(t, "node2", vn.NodeID, "removed node must not own any keys")
	}
}
