package raft

import (
	"context"
	"fmt"

	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// This file holds the log machinery: durability, the §5.3 entry merge, the
// §5.4.2 commit rule, the apply loop, and the proposal entry point. The RPC
// handlers that drive it live in raft.go.

// --- Durability --------------------------------------------------------------

// persistLocked writes the node's persistent state, using log as the log to
// record rather than r.log.
//
// That parameter is the whole point of this helper. Every persistence-critical
// path here computes a *candidate* log, persists it, and only then installs it
// in r.log — so a failed write leaves the in-memory log exactly as durable
// storage has it. Persisting r.log directly would invert that: the entry would
// be in memory before it was on disk, and the retry that followed would find it
// already present, dedup it, and acknowledge an entry no disk ever saw.
//
// Caller must hold r.mu.
func (r *RaftNode) persistLocked(log []LogEntry) error {
	return r.persist.Save(persistedState{
		CurrentTerm:   r.currentTerm,
		VotedFor:      r.votedFor,
		SnapLastIndex: r.snapLastIndex,
		SnapLastTerm:  r.snapLastTerm,
		Log:           log,
	})
}

// --- Log inspection ---------------------------------------------------------

// termAtLocked returns the term of the entry at absolute index idx.
//
// ok is false when the index cannot be resolved: either it was compacted into
// the snapshot (below snapLastIndex, where only the boundary entry's term
// survives) or it is beyond the end of this node's log. Both mean "I cannot
// vouch for that position", which is exactly what the AppendEntries consistency
// check must reject on.
//
// Caller must hold r.mu.
func (r *RaftNode) termAtLocked(idx uint64) (term uint64, ok bool) {
	if idx == r.snapLastIndex {
		// The snapshot boundary is the one compacted index whose term is still
		// known, which is what lets a caught-up follower answer a PrevLogIndex
		// pointing at it without needing the snapshot resent.
		return r.snapLastTerm, true
	}
	if idx < r.snapLastIndex {
		return 0, false
	}
	si := r.logSliceIndex(idx)
	if si >= len(r.log) {
		return 0, false
	}
	return r.log[si].Term, true
}

// entriesFromLocked returns the log entries starting at absolute index from,
// converted to wire form. Caller must hold r.mu.
func (r *RaftNode) entriesFromLocked(from uint64) []*kvpb.LogEntry {
	if from <= r.snapLastIndex {
		return nil // caller should be sending a snapshot instead
	}
	si := r.logSliceIndex(from)
	if si >= len(r.log) {
		return nil
	}
	out := make([]*kvpb.LogEntry, 0, len(r.log)-si)
	for _, e := range r.log[si:] {
		out = append(out, &kvpb.LogEntry{
			Index: e.Index,
			Term:  e.Term,
			Op:    e.Op,
			Key:   e.Key,
			Value: cloneBytes(e.Value),
		})
	}
	return out
}

// --- §5.3 entry merge -------------------------------------------------------

// mergeEntriesLocked merges the leader's entries into a copy of the log,
// following §5.3: an entry that matches an existing index and term is already
// present and is skipped, an entry that conflicts truncates that index and
// everything after it, and anything past the end is appended.
//
// It returns the candidate log and whether it differs from r.log. It never
// mutates r.log — see persistLocked for why that matters.
//
// Caller must hold r.mu.
func (r *RaftNode) mergeEntriesLocked(prevLogIndex uint64, entries []*kvpb.LogEntry) (newLog []LogEntry, changed bool, err error) {
	newLog = r.log

	for i, e := range entries {
		idx := prevLogIndex + 1 + uint64(i)

		if idx <= r.snapLastIndex {
			// Compacted into the snapshot: durable and already applied, so
			// there is nothing to store and no term left to compare against.
			continue
		}

		si := r.logSliceIndex(idx)
		switch {
		case si > len(newLog):
			// Unreachable if the consistency check passed: entries are
			// contiguous from prevLogIndex+1, so si walks up one position at a
			// time from a position no higher than the log's end. Refuse rather
			// than write a hole into the log.
			return nil, false, fmt.Errorf("raft: entry index %d leaves a gap (log ends at %d)", idx, r.lastLogIndex())

		case si < len(newLog):
			if newLog[si].Term == e.Term {
				continue // same index and term ⇒ same entry (§5.3). Idempotent.
			}
			if idx <= r.commitIndex {
				// A committed entry cannot conflict with the current leader's
				// log — the Leader Completeness Property forbids it. Reaching
				// here means an invariant is already broken, so refusing to
				// truncate is the only move that does not turn a detectable
				// bug into silent data loss.
				return nil, false, fmt.Errorf(
					"raft: refusing to truncate committed index %d (commitIndex=%d): "+
						"leader term %d conflicts with local term %d",
					idx, r.commitIndex, e.Term, newLog[si].Term)
			}
			// Conflict: drop this entry and every entry that follows it.
			trunc := make([]LogEntry, si, si+len(entries)-i)
			copy(trunc, newLog[:si])
			newLog = trunc
			changed = true

		default: // si == len(newLog): a genuinely new entry
			if !changed {
				// First mutation of the run — take a private copy so a failed
				// persist leaves r.log untouched.
				cp := make([]LogEntry, len(newLog), len(newLog)+len(entries)-i)
				copy(cp, newLog)
				newLog = cp
			}
		}

		newLog = append(newLog, LogEntry{
			Index: idx,
			Term:  e.Term,
			Op:    e.Op,
			Key:   e.Key,
			Value: cloneBytes(e.Value),
		})
		changed = true
	}

	return newLog, changed, nil
}

// --- §5.4.2 commit rule -----------------------------------------------------

// advanceCommitIndexLocked raises commitIndex to the highest index that is
// stored on a majority of the cluster *and* was created in the current term.
//
// The current-term condition is the Figure-8 fix, and it is the one rule here
// that looks like a needless restriction and is not. An entry from an earlier
// term can sit on a majority of nodes and still be lost: a later leader that
// never received it can be elected (its own log is longer in a higher term) and
// overwrite it. Only once an entry of the leader's own term commits does
// everything before it become safe — at which point those earlier entries
// commit indirectly, on the next pass through this loop.
//
// Cost: O(uncommitted tail × peers) per call, called once per accepted
// AppendEntries reply. That is negligible for a log carrying a handful of
// control-plane entries, and it is stated rather than assumed because it is the
// wrong shape for a burst: a producer that could commit thousands of entries at
// once wants the standard sorted-matchIndex form (O(p log p), independent of log
// length) instead. Phase B should revisit this if health transitions can arrive
// in bursts.
//
// Caller must hold r.mu.
func (r *RaftNode) advanceCommitIndexLocked() {
	if r.role != Leader {
		return
	}
	majority := (len(r.peers)+1)/2 + 1

	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		term, ok := r.termAtLocked(n)
		if !ok {
			continue
		}
		if term != r.currentTerm {
			// Terms never decrease along the log, so every lower index is also
			// from an older term. Kept as a continue rather than a break so the
			// safety of this loop does not rest on that invariant.
			continue
		}
		replicas := 1 // the leader itself: the entry is persisted before Propose returns
		for _, p := range r.peers {
			if r.matchIndex[p.NodeID] >= n {
				replicas++
			}
		}
		if replicas >= majority {
			r.commitIndex = n
			return
		}
	}
}

// --- Apply ------------------------------------------------------------------

// applyCommitted feeds every entry between lastApplied and commitIndex to the
// state machine, in index order.
//
// It must be called with no Raft lock held. applyMu serialises concurrent
// callers — heartbeat responses, incoming AppendEntries, and Propose all
// trigger it — so an entry reaches Apply exactly once per advance of
// lastApplied even though the callers race. r.mu is dropped across Apply so a
// state machine that blocks cannot stall elections.
//
// On an Apply error the loop stops without advancing lastApplied, leaving the
// entry to be retried by the next trigger. Followers get one every heartbeat,
// so a transient failure recovers on its own.
func (r *RaftNode) applyCommitted(ctx context.Context) {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()

	for {
		r.mu.Lock()
		if r.lastApplied >= r.commitIndex {
			r.mu.Unlock()
			return
		}
		next := r.lastApplied + 1
		if next <= r.snapLastIndex {
			// Compaction moved the frontier past us: everything up to the
			// snapshot boundary is already reflected in the state machine.
			r.lastApplied = r.snapLastIndex
			r.mu.Unlock()
			continue
		}
		si := r.logSliceIndex(next)
		if si >= len(r.log) {
			// commitIndex should never exceed what we store; if it somehow
			// does, stop rather than invent an entry.
			r.logger.Error("raft: committed index is not in the log",
				"index", next, "commit_index", r.commitIndex, "last_log_index", r.lastLogIndex())
			r.mu.Unlock()
			return
		}
		entry := r.log[si]
		r.mu.Unlock()

		if err := r.sm.Apply(ctx, entry); err != nil {
			r.logger.Error("raft: state machine apply failed; will retry",
				"index", entry.Index, "term", entry.Term, "op", entry.Op, "error", err)
			return
		}

		r.mu.Lock()
		if r.lastApplied+1 == entry.Index {
			r.lastApplied = entry.Index
		} else {
			// Unreachable while applyMu is doing its job: this loop is the only
			// writer of lastApplied and it holds applyMu throughout. Logged
			// rather than ignored so a broken serialisation shows up as itself
			// instead of as a state machine that quietly missed an entry.
			r.logger.Error("raft: lastApplied moved under the apply loop",
				"applied_index", entry.Index, "last_applied", r.lastApplied)
		}
		needSnapshot := len(r.log) >= r.snapshotThreshold
		r.mu.Unlock()

		if needSnapshot {
			go r.takeSnapshot(context.WithoutCancel(ctx))
		}
	}
}

// --- Proposal ---------------------------------------------------------------

// Propose appends an entry to the leader's log and returns the index it was
// assigned. The entry is durable before Propose returns, which is what lets the
// leader count itself toward the majority that commits it.
//
// Propose does not wait for commitment: replication rides the next heartbeat
// tick. Callers that need to know an entry took effect should watch
// CommitIndex() against the returned index, or observe their own state machine.
//
// Returns ErrNotLeader on a non-leader. Only the leader may propose; a follower
// with an entry to add must route it to Leader().
func (r *RaftNode) Propose(ctx context.Context, op, key string, value []byte) (uint64, error) {
	r.mu.Lock()

	if r.role != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}

	entry := LogEntry{
		Index: r.lastLogIndex() + 1,
		Term:  r.currentTerm,
		Op:    op,
		Key:   key,
		Value: cloneBytes(value),
	}

	// Copy-then-persist-then-install, so a failed write leaves no trace of an
	// entry the caller will be told was rejected.
	newLog := make([]LogEntry, len(r.log), len(r.log)+1)
	copy(newLog, r.log)
	newLog = append(newLog, entry)

	if err := r.persistLocked(newLog); err != nil {
		r.mu.Unlock()
		return 0, fmt.Errorf("raft: persist proposed entry: %w", err)
	}
	r.log = newLog

	// A single-node cluster reaches its own majority immediately, so commit and
	// apply without waiting for a heartbeat tick that has no peer to talk to.
	r.advanceCommitIndexLocked()
	r.mu.Unlock()

	r.applyCommitted(ctx)

	return entry.Index, nil
}

// --- Misc -------------------------------------------------------------------

// cloneBytes copies b so that neither Raft's log nor a caller's buffer can be
// mutated through the other. nil stays nil, which keeps a delete entry's absent
// value distinguishable from an empty one on the wire.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
