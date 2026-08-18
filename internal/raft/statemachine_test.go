package raft

import (
	"context"
	"fmt"
	"sync"
)

// testSM is the StateMachine test double for this package. It records every
// entry Raft applies, in order, which is what the apply-on-commit tests assert
// against, and can be told to fail so the retry path can be exercised.
//
// It also records which entry point each apply arrived through, so the
// replay-frontier tests can assert that Raft classified an entry as replayed or
// live — the distinction defect 14 turns on.
type testSM struct {
	mu       sync.Mutex
	applied  []LogEntry
	replayed []bool // parallel to applied: true if it arrived via ReplayApply
	snapshot []byte
	restores [][]byte

	// applyErr, when non-nil, makes Apply fail without recording the entry.
	applyErr error
	// snapshotErr, when non-nil, makes SnapshotState fail.
	snapshotErr error
}

func newTestSM() *testSM { return &testSM{} }

func (s *testSM) Apply(_ context.Context, entry LogEntry) error {
	return s.record(entry, false)
}

func (s *testSM) ReplayApply(_ context.Context, entry LogEntry) error {
	return s.record(entry, true)
}

func (s *testSM) record(entry LogEntry, replay bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyErr != nil {
		return s.applyErr
	}
	s.applied = append(s.applied, entry)
	s.replayed = append(s.replayed, replay)
	// The snapshot payload is a stand-in for real state: enough to prove the
	// bytes Raft ships are the state machine's own and are handed back intact.
	s.snapshot = []byte(fmt.Sprintf("applied=%d", len(s.applied)))
	return nil
}

func (s *testSM) SnapshotState(_ context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshotErr != nil {
		return nil, s.snapshotErr
	}
	out := make([]byte, len(s.snapshot))
	copy(out, s.snapshot)
	return out, nil
}

func (s *testSM) RestoreFromSnapshot(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.restores = append(s.restores, cp)
	s.snapshot = cp
	return nil
}

// appliedEntries returns a copy of everything applied so far.
func (s *testSM) appliedEntries() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.applied))
	copy(out, s.applied)
	return out
}

// appliedKeys returns the Key of each applied entry, in apply order.
func (s *testSM) appliedKeys() []string {
	out := []string{}
	for _, e := range s.appliedEntries() {
		out = append(out, e.Key)
	}
	return out
}

// replayFlags returns, in apply order, whether each entry arrived through
// ReplayApply rather than Apply.
func (s *testSM) replayFlags() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]bool, len(s.replayed))
	copy(out, s.replayed)
	return out
}

func (s *testSM) restoreCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.restores)
}

func (s *testSM) lastRestore() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.restores) == 0 {
		return nil
	}
	return s.restores[len(s.restores)-1]
}

func (s *testSM) setApplyErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyErr = err
}
