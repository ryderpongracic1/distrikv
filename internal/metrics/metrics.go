// Package metrics provides a lightweight, allocation-free metrics collector
// built on sync/atomic counters. All fields are safe for concurrent use
// without additional locking.
//
// Usage: allocate one *Metrics on the Node struct and pass it by pointer to
// subsystems that need to increment counters. Never use a global instance.
package metrics

import (
	"sync/atomic"
)

// Metrics holds observable counters for a single distrikv node.
// Every field is an atomic.Uint64 so any goroutine can increment it without
// acquiring a lock.
type Metrics struct {
	// Client-facing operation totals.
	PutTotal    atomic.Uint64
	GetTotal    atomic.Uint64
	DeleteTotal atomic.Uint64

	// GetMiss counts Get calls that returned ErrNotFound.
	GetMiss atomic.Uint64

	// WALWrites counts the number of fsync'd WAL appends.
	WALWrites atomic.Uint64

	// RaftTerms counts the number of term increments (candidate elections
	// started), giving a rough proxy for election churn.
	RaftTerms atomic.Uint64

	// LeaderElections counts the number of times this node became leader.
	LeaderElections atomic.Uint64

	// ForwardedRequests counts client requests this node forwarded to another
	// node because it was not the ring-primary for the requested key.
	ForwardedRequests atomic.Uint64

	// ReplicationErrors counts failures when replicating a write to a replica.
	ReplicationErrors atomic.Uint64
}

// Snapshot returns a point-in-time copy of all counters as a map suitable for
// JSON serialisation.
func (m *Metrics) Snapshot() map[string]uint64 {
	return map[string]uint64{
		"put_total":           m.PutTotal.Load(),
		"get_total":           m.GetTotal.Load(),
		"delete_total":        m.DeleteTotal.Load(),
		"get_miss":            m.GetMiss.Load(),
		"wal_writes":          m.WALWrites.Load(),
		"raft_terms":          m.RaftTerms.Load(),
		"leader_elections":    m.LeaderElections.Load(),
		"forwarded_requests":  m.ForwardedRequests.Load(),
		"replication_errors":  m.ReplicationErrors.Load(),
	}
}
