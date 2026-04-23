// Package cluster provides consistent hashing for mapping keys to nodes.
package cluster

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// VirtualNodesPerPhysical is the number of virtual ring positions allocated
// for each physical node. 150 virtual nodes produces good key distribution
// across a small cluster (typical variance < ±15% from the ideal 1/N share).
const VirtualNodesPerPhysical = 150

// VNode is a single point on the consistent hash ring.
type VNode struct {
	// Hash is the ring position of this virtual node.
	Hash uint32

	// NodeID is the physical node this virtual node belongs to.
	NodeID string

	// GRPCAddr is the gRPC dial address of the physical node.
	GRPCAddr string
}

// Ring is a thread-safe consistent hash ring. Multiple goroutines may call
// Get and GetN concurrently; AddNode and RemoveNode take a write lock.
type Ring struct {
	mu     sync.RWMutex
	vnodes []VNode // sorted ascending by Hash
}

// New creates an empty Ring. Call AddNode for each cluster member before use.
func New() *Ring {
	return &Ring{}
}

// AddNode adds a physical node by distributing VirtualNodesPerPhysical virtual
// nodes across the ring. Calling AddNode with an already-present nodeID first
// removes the old virtual nodes, making this operation idempotent.
func (r *Ring) AddNode(nodeID, grpcAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove any existing vnodes for this nodeID (idempotency).
	r.removeNodeLocked(nodeID)

	for i := 0; i < VirtualNodesPerPhysical; i++ {
		r.vnodes = append(r.vnodes, VNode{
			Hash:     hashVNode(nodeID, i),
			NodeID:   nodeID,
			GRPCAddr: grpcAddr,
		})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Hash < r.vnodes[j].Hash
	})
}

// RemoveNode removes all virtual nodes for nodeID from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeNodeLocked(nodeID)
}

func (r *Ring) removeNodeLocked(nodeID string) {
	out := r.vnodes[:0]
	for _, v := range r.vnodes {
		if v.NodeID != nodeID {
			out = append(out, v)
		}
	}
	r.vnodes = out
}

// Get returns the node responsible for key — the first virtual node whose ring
// hash is >= hash(key), wrapping around to the start if necessary.
// Returns an error only if the ring is empty.
func (r *Ring) Get(key string) (VNode, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return VNode{}, fmt.Errorf("ring: empty ring, no nodes available")
	}

	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= h
	})
	// Wrap around.
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx], nil
}

// GetN returns up to n distinct physical nodes starting from the node
// responsible for key and walking clockwise. It returns fewer than n nodes
// only when the ring contains fewer than n distinct physical nodes.
//
// This is used to identify replication targets: GetN(key, R) gives the
// primary followed by R-1 replica nodes.
func (r *Ring) GetN(key string, n int) ([]VNode, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil, fmt.Errorf("ring: empty ring, no nodes available")
	}

	h := hashKey(key)
	start := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= h
	})
	if start == len(r.vnodes) {
		start = 0
	}

	seen := make(map[string]struct{})
	var result []VNode

	for i := 0; i < len(r.vnodes) && len(result) < n; i++ {
		idx := (start + i) % len(r.vnodes)
		vn := r.vnodes[idx]
		if _, ok := seen[vn.NodeID]; !ok {
			seen[vn.NodeID] = struct{}{}
			result = append(result, vn)
		}
	}

	return result, nil
}

// NodeCount returns the number of distinct physical nodes in the ring.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{})
	for _, vn := range r.vnodes {
		seen[vn.NodeID] = struct{}{}
	}
	return len(seen)
}

// hashKey maps an arbitrary string key to a uint32 ring position using the
// first four bytes of its MD5 digest (big-endian). MD5 provides deterministic,
// well-distributed output at low CPU cost; cryptographic strength is not needed.
func hashKey(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// hashVNode hashes the string "nodeID-i" to produce the ring position for the
// i-th virtual node of nodeID.
func hashVNode(nodeID string, i int) uint32 {
	return hashKey(fmt.Sprintf("%s-%d", nodeID, i))
}
