package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/cluster"
	"github.com/ryderpongracic1/distrikv/internal/store"
)

// Anti-entropy honesty after a snapshot restore.
//
// A restore replaces the store from a payload that never passed through the WAL,
// so there is nothing in the log to replay: a catch-up pass finds zero entries
// and reports no error. The engine's ordinary reading of that — "nothing left to
// ship, therefore converged" — is exactly wrong here, and it is the one claim the
// node must not make, because the chaos harness and any operator read "replica
// caught up" as a convergence statement.
//
// These tests drive repair() directly, like the rest of the anti-entropy suite.

// syncBuffer is a concurrency-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// withAntiEntropyLogging is withAntiEntropy with a capturing logger, plus the
// option to pre-mark the cursor store as needing a full sync (the state a restore
// leaves behind).
func withAntiEntropyLogging(t *testing.T, n *Node, afterRestore bool, peerIDs ...string) (*antiEntropy, *syncBuffer) {
	t.Helper()

	cursors, err := store.OpenCursorStore(n.cfg.DataDir)
	if err != nil {
		t.Fatalf("open cursor store: %v", err)
	}
	if afterRestore {
		if err := cursors.InvalidateAll("store replaced from a snapshot (test)"); err != nil {
			t.Fatalf("invalidate cursors: %v", err)
		}
	}
	n.cursors = cursors

	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	n.health = cluster.NewPeerHealth(peerIDs, cluster.HealthConfig{
		Logger: logger,
	})

	n.antiEntropy = newAntiEntropy(
		n.cfg.NodeID, n.cfg.ReplicaCount, peerIDs,
		n.store, cursors, n.ring, n.peerClients, n.health,
		nil, // no consensus health signal: these tests drive the local signals
		n.raft.CurrentTerm, n.metrics, logger,
		antiEntropyConfig{SettleDelay: time.Millisecond},
	)
	return n.antiEntropy, logs
}

// TestAntiEntropyWithholdsConvergenceClaimAfterRestore is the invariant: an empty
// pass after a restore must not be reported as the replica being caught up.
func TestAntiEntropyWithholdsConvergenceClaimAfterRestore(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae, logs := withAntiEntropyLogging(t, n, true, "node2")

	if got := n.metrics.AntiEntropyFullSyncRequired.Load(); got != 1 {
		t.Errorf("anti_entropy_full_sync_required = %d, want 1: the standing "+
			"inability to converge from the WAL is not being surfaced", got)
	}

	// A pass over the post-restore log: there is nothing in it, so this ships
	// zero entries and returns no error — the shape that used to be read as
	// convergence.
	ae.repair(context.Background(), "node2")

	out := logs.String()
	if strings.Contains(out, "replica caught up") {
		t.Errorf("the node claimed \"replica caught up\" after a snapshot restore, "+
			"but the restored keys were never in the WAL and so were never shipped.\nlogs:\n%s", out)
	}
	if !strings.Contains(out, "is NOT known to agree") {
		t.Errorf("the node did not report that the replica is not known to agree after a "+
			"restore; the empty pass was silent.\nlogs:\n%s", out)
	}
}

// TestAntiEntropyStillClaimsConvergenceNormally is the other half of the same
// branch: without a restore, an empty pass is genuine evidence of convergence and
// must still be reported. Without this, suppressing the claim unconditionally
// would pass the test above.
func TestAntiEntropyStillClaimsConvergenceNormally(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae, logs := withAntiEntropyLogging(t, n, false, "node2")

	if got := n.metrics.AntiEntropyFullSyncRequired.Load(); got != 0 {
		t.Errorf("anti_entropy_full_sync_required = %d on a node that never restored, want 0", got)
	}

	ae.repair(context.Background(), "node2")

	out := logs.String()
	if !strings.Contains(out, "replica caught up") {
		t.Errorf("a node that never restored must still report a caught-up replica.\nlogs:\n%s", out)
	}
	if strings.Contains(out, "is NOT known to agree") {
		t.Errorf("a node that never restored must not warn about full sync.\nlogs:\n%s", out)
	}
}

// TestAntiEntropyDoesNotSeedBehindFromInvalidatedCursors pins the startup half.
// An invalidated (zero) cursor means "no evidence", which must not be read as
// "this replica is behind" — that would make every restored node open by
// replaying its whole retained log to every replica, and after a restore that log
// is empty anyway.
func TestAntiEntropyDoesNotSeedBehindFromInvalidatedCursors(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ae, _ := withAntiEntropyLogging(t, n, true, "node2")

	if behind := ae.behindReplicas(); len(behind) != 0 {
		t.Errorf("replicas seeded as behind from zero cursors: %v", behind)
	}
}
