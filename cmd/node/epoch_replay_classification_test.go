package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"github.com/ryderpongracic1/distrikv/internal/config"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
	"github.com/ryderpongracic1/distrikv/internal/store"
	kvpb "github.com/ryderpongracic1/distrikv/proto/kvpb"
)

// The epoch-regression alarm exists to report a primary whose incarnation went
// backwards — a wiped data directory whose clock could not carry it forward, or a
// stale write from a previous incarnation. It latches a gauge and logs at ERROR,
// so its whole value is specificity: one reachable benign trigger and the signal
// becomes noise.
//
// A catch-up replay is such a trigger, and it needs no clock anomaly at all. The
// interleaving below is the whole mechanism, and every step of it is ordinary:
//
//  1. The primary, in incarnation E1, writes K while a replica is unreachable.
//     The write is refused to the client and kept locally, and the replica's
//     cursor freezes before that entry — the divergence anti-entropy exists for.
//  2. The primary restarts, so its next incarnation is E2 > E1. The retained WAL
//     segments still hold K@(E1,·).
//  3. The replica returns and a pass starts, pinning its range at the WAL tip it
//     reads on entry (see repair) and collecting K@(E1,·).
//  4. A client writes K again → stamped (E2,·) → live replication lands it at the
//     replica first. That write is *past* the pinned range, so the pass's dedup
//     never sees it and the pass ships the E1 entry anyway.
//  5. The replica discards it, correctly: apply-if-newer working exactly as
//     designed, and the data is right on both nodes afterwards.
//
// Nothing there is wrong with the data. What is wrong is the classification: the
// arriving epoch is below the stored one, so a receiver that cannot tell a replay
// from a live write counts an epoch regression, latches the gauge and logs ERROR
// on a healthy cluster doing a routine restart.
//
// These tests exercise that interleaving rather than the two-call shortcut of
// handing an engine a low-epoch write directly, because the shortcut cannot show
// the two things that make the path reachable: the pass pinning a range that
// excludes the newer write, and the epoch difference coming from a restart rather
// than from a fault.

// replicaLink is a kvpb.KVServiceClient that delivers Replicate calls into a real
// replica Node instead of recording them, so the receiving engine's apply-if-newer
// comparison and its classification of a refusal are exercised for real. It
// mirrors GRPCServer.Replicate: every field on the wire is what the replica sees.
type replicaLink struct {
	kvpb.KVServiceClient

	replica *Node

	mu   sync.Mutex
	down error
	reqs []*kvpb.ReplicateRequest
}

func (l *replicaLink) Replicate(
	ctx context.Context, in *kvpb.ReplicateRequest, _ ...grpc.CallOption,
) (*kvpb.ReplicateResponse, error) {
	l.mu.Lock()
	down := l.down
	l.mu.Unlock()
	if down != nil {
		return nil, down
	}

	l.mu.Lock()
	l.reqs = append(l.reqs, in)
	l.mu.Unlock()

	if err := l.replica.ApplyReplica(ctx, in.Op, in.Key, in.Value, in.Seq, in.Replay); err != nil {
		return nil, err
	}
	return &kvpb.ReplicateResponse{Success: true, NodeId: "node2"}, nil
}

func (l *replicaLink) setDown(err error) {
	l.mu.Lock()
	l.down = err
	l.mu.Unlock()
}

func (l *replicaLink) requests() []*kvpb.ReplicateRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*kvpb.ReplicateRequest(nil), l.reqs...)
}

// receivingNode builds the far side of a replication link: a node with a real
// store wired to real metrics and a logger whose output can be inspected, which
// is what lets a test assert on the classification of a discard rather than only
// on the value that survived.
//
// It is deliberately minimal. ApplyReplica needs the store and the logger and
// nothing else, so wiring a ring, a Raft node or peer clients here would only
// suggest this node does more than receive.
func receivingNode(t *testing.T) (*Node, *metrics.Metrics, *bytes.Buffer) {
	t.Helper()

	dir := t.TempDir()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	m := &metrics.Metrics{}
	st, err := store.NewWithMetrics(dir, logger, m)
	if err != nil {
		t.Fatalf("open receiving store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	return &Node{
		cfg:     &config.Config{NodeID: "node2", DataDir: dir},
		store:   st,
		metrics: m,
		logger:  logger,
	}, m, &logs
}

// staleReplayState is what the interleaving above leaves behind: the wire request
// the pass shipped, and the receiving replica's counters and log after it was
// discarded.
type staleReplayState struct {
	replayed *kvpb.ReplicateRequest
	metrics  *metrics.Metrics
	logs     string
	replica  *Node
	key      string

	// discardsBefore is the receiving replica's discard count immediately before
	// the pass ran, so an assertion can be about the discard the pass caused. The
	// healthy cycle earlier in the interleaving also produces one — a same-epoch
	// re-ship of a key the replica already had, which is replay idempotence doing
	// its job — and an absolute count would be asserting on that too.
	discardsBefore uint64
}

// runStaleReplayAfterRestart drives the five-step interleaving and returns what
// the receiving replica made of the stale entry the pass shipped.
func runStaleReplayAfterRestart(t *testing.T) staleReplayState {
	t.Helper()
	ctx := context.Background()

	primary, _ := testNode(t, 2, "node2")
	replica, replicaMetrics, replicaLogs := receivingNode(t)

	// Replace the recording fake with a link into the real replica. Done before the
	// anti-entropy engine is built so the engine ships over the same link.
	link := &replicaLink{replica: replica}
	primary.peerClients["node2"] = link

	ae := withAntiEntropy(t, primary, "node2")
	key := keysOwedTo(t, ae, "node2", 1)[0]

	// A healthy write and a completed cycle, so there is a recorded cursor to
	// freeze — a pass starting from a zero cursor is a different (and separately
	// tested) path.
	if err := primaryPut(t, primary, key, "healthy"); err != nil {
		t.Fatalf("healthy write: %v", err)
	}
	ae.repair(ctx, "node2")
	if primary.cursors.Get("node2").IsZero() {
		t.Fatal("no cursor recorded after a clean catch-up cycle")
	}

	// Step 1: the replica is unreachable, so this write is refused to the client
	// and kept locally in incarnation E1.
	link.setDown(errors.New("connection refused"))
	if err := primaryPut(t, primary, key, "refused-in-E1"); err == nil {
		t.Fatal("a write a replica did not ACK must be refused to the client")
	}

	// Step 2: the primary restarts, so its next incarnation is above E1.
	restarted := restartPrimary(t, primary, "node2")

	// Step 3: the replica is back and a pass pins its range at the tip it reads on
	// entry — before the write in step 4 exists.
	link.setDown(nil)
	from := primary.cursors.Get("node2")
	if from.IsZero() {
		t.Fatal("the cursor recorded before the restart did not survive it")
	}
	pinned := primary.store.WALTip()

	// Step 4: a client writes the same key again. It is stamped in E2 and live
	// replication lands it at the replica first — and it is past the pinned range,
	// so the pass will not see it.
	if err := primaryPut(t, primary, key, "live-in-E2"); err != nil {
		t.Fatalf("post-restart write was refused: %v", err)
	}
	wantValue(t, replica, key, "live-in-E2")

	// Step 5: the pass ships what it collected — the E1 entry — and the replica
	// discards it.
	before := len(link.requests())
	discardsBefore := replicaMetrics.ReplicaWritesDiscarded.Load()
	sent, err := restarted.runPass(ctx, "node2", from, pinned)
	if err != nil {
		t.Fatalf("runPass: %v", err)
	}
	if sent != 1 {
		t.Fatalf("the pass shipped %d entries, want exactly 1 (the E1 write refused "+
			"during the fault); the interleaving under test did not happen", sent)
	}

	shipped := link.requests()[before:]
	if len(shipped) != 1 {
		t.Fatalf("the link saw %d requests from the pass, want 1", len(shipped))
	}
	replayed := shipped[0]
	if string(replayed.Value) != "refused-in-E1" {
		t.Fatalf("the pass shipped %q, want the value refused during the fault; the "+
			"pinned range did not exclude the newer write", replayed.Value)
	}

	return staleReplayState{
		replayed:       replayed,
		metrics:        replicaMetrics,
		logs:           replicaLogs.String(),
		replica:        replica,
		key:            key,
		discardsBefore: discardsBefore,
	}
}

// TestCatchUpReplayAfterARestartIsNotAnEpochRegression is the repro, and the
// revert-check: before the fix it fails on the two classification assertions and
// passes on the discard count and the value, which is exactly the shape of the
// defect — the mechanism was right and the alarm was wrong.
func TestCatchUpReplayAfterARestartIsNotAnEpochRegression(t *testing.T) {
	got := runStaleReplayAfterRestart(t)

	// The premise the whole interleaving rests on: the entry the pass shipped is
	// from a lower incarnation than the one the replica now holds. Without this the
	// test would pass for the wrong reason — there would be no epoch difference to
	// misclassify.
	if got.replayed.Seq == 0 {
		t.Fatal("the replayed entry carries no sequence, so there is no epoch to compare")
	}

	// The discard itself is real and must stay counted: it is how an operator sees
	// that replay is idempotent rather than silently reverting newer values.
	if delta := got.metrics.ReplicaWritesDiscarded.Load() - got.discardsBefore; delta != 1 {
		t.Errorf("the pass caused %d discards, want 1: a discarded replay is still a "+
			"discard and must remain visible", delta)
	}

	// The classification is the defect.
	if n := got.metrics.ReplicaWritesEpochRegressed.Load(); n != 0 {
		t.Errorf("replica_writes_epoch_regressed = %d, want 0: the arriving epoch is "+
			"lower because the primary restarted between the two writes, not because any "+
			"incarnation went backwards", n)
	}
	if n := got.metrics.ReplicaEpochRegressed.Load(); n != 0 {
		t.Errorf("replica_epoch_regressed gauge = %d, want 0: it latches until someone "+
			"reads it, so a routine restart raising it makes the signal unusable", n)
	}
	if strings.Contains(got.logs, `level=ERROR`) {
		t.Errorf("the receiving replica logged an ERROR for a healthy catch-up replay:\n%s",
			got.logs)
	}

	// Nothing was ever wrong with the data, and the fix must not change that.
	wantValue(t, got.replica, got.key, "live-in-E2")
}

// TestCatchUpReplayCarriesTheReplayMarker pins the wire half: the discard above is
// only distinguishable from a live write because the pass says so. A pass that
// stopped setting the marker would silently restore the false positive, and the
// only visible symptom would be an alarm nobody trusts.
func TestCatchUpReplayCarriesTheReplayMarker(t *testing.T) {
	got := runStaleReplayAfterRestart(t)
	if !got.replayed.Replay {
		t.Error("a catch-up pass shipped an entry without the replay marker; the receiver " +
			"cannot then tell a re-shipped WAL entry from a primary whose incarnation " +
			"went backwards")
	}
}

// TestLiveWriteIsNotMarkedAsReplay is the other half of the same pin, and the
// reason the suppression cannot hide a genuine regression: the live path is where
// a wiped or regressed primary's writes arrive, and it must stay classified.
func TestLiveWriteIsNotMarkedAsReplay(t *testing.T) {
	primary, _ := testNode(t, 2, "node2")
	replica, _, _ := receivingNode(t)
	link := &replicaLink{replica: replica}
	primary.peerClients["node2"] = link

	ae := withAntiEntropy(t, primary, "node2")
	key := keysOwedTo(t, ae, "node2", 1)[0]

	if err := primaryPut(t, primary, key, "v"); err != nil {
		t.Fatalf("healthy write: %v", err)
	}

	reqs := link.requests()
	if len(reqs) != 1 {
		t.Fatalf("the link saw %d requests for one write, want 1", len(reqs))
	}
	if reqs[0].Replay {
		t.Error("a live replication RPC was marked as a replay, which would suppress the " +
			"epoch-regression alarm on the one path that still reports it")
	}
}
