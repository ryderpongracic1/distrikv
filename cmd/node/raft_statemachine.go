package main

import (
	"context"
	"log/slog"

	"github.com/ryderpongracic1/distrikv/internal/raft"
)

// placeholderStateMachine is what Raft applies committed entries to until the
// health state machine exists.
//
// It is deliberately inert rather than absent. The log machinery is complete and
// tested, but nothing in this system proposes an entry yet — client writes go to
// the storage engine through the hash ring, never through Raft (see the raft
// package doc) — so in a running cluster Apply is never called. Wiring a real
// component in now would only invite the reader to believe otherwise.
//
// Phase B replaces this with the state machine that holds committed node-health
// transitions. The seam is exactly the raft.StateMachine interface: nothing else
// in this file or in node.go needs to change.
type placeholderStateMachine struct {
	logger *slog.Logger
}

func newPlaceholderStateMachine(logger *slog.Logger) *placeholderStateMachine {
	return &placeholderStateMachine{logger: logger.With("component", "raft-state-machine")}
}

// Apply logs the entry and succeeds. Debug level, not Warn: reaching here is not
// a fault, it is an entry arriving before its consumer was built.
func (p *placeholderStateMachine) Apply(_ context.Context, entry raft.LogEntry) error {
	p.logger.Debug("placeholder state machine: ignoring committed entry",
		"index", entry.Index, "term", entry.Term, "op", entry.Op, "key", entry.Key)
	return nil
}

// SnapshotState returns an empty payload — there is no state to capture.
func (p *placeholderStateMachine) SnapshotState(_ context.Context) ([]byte, error) {
	return nil, nil
}

// RestoreFromSnapshot accepts any payload and keeps nothing.
func (p *placeholderStateMachine) RestoreFromSnapshot(_ context.Context, _ []byte) error {
	return nil
}
