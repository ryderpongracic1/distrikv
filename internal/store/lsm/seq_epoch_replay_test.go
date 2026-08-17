package lsm

import (
	"context"
	"testing"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// Engine-level classification of a catch-up replay's refusal.
//
// The epoch-regression alarm latches a gauge and logs at ERROR, so its value is
// entirely in its specificity. A catch-up pass legitimately re-ships entries
// written by an earlier incarnation of the primary — its range is pinned when the
// pass starts, so a newer live write can already be on the replica — and counting
// those refusals as regressions makes the alarm fire on an ordinary restart. The
// cmd/node tests drive that whole interleaving; these pin the engine's half of it
// directly, including the delete path and the fact that the live path is
// unaffected.

// TestReplayRefusalIsNotAnEpochRegression is the engine-level statement of the
// fix: identical arriving and stored sequences, classified two ways depending on
// how the mutation reached the engine.
func TestReplayRefusalIsNotAnEpochRegression(t *testing.T) {
	ctx := context.Background()
	m := &metrics.Metrics{}
	l := openEpochTree(t, t.TempDir(), WithMetrics(m))

	stored := makeSeq(2_000, 10)
	if applied, err := l.PutIfNewer(ctx, "k", []byte("live-in-E2"), stored); err != nil || !applied {
		t.Fatalf("seeding the stored version: applied=%v err=%v", applied, err)
	}

	// A pass re-shipping an entry from the primary's previous incarnation.
	regressed := makeSeq(1_999, 999)
	applied, err := l.ReplayPutIfNewer(ctx, "k", []byte("refused-in-E1"), regressed)
	if err != nil {
		t.Fatalf("ReplayPutIfNewer: %v", err)
	}
	if applied {
		t.Error("a replay below the stored sequence must still be discarded: applying it " +
			"would revert a newer value, which is the inversion the comparison exists for")
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 1 {
		t.Errorf("replica_writes_discarded = %d, want 1: a replay's refusal is still a "+
			"discard, and its rate is what distinguishes idempotent replay from a "+
			"primary whose every write is being dropped", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 0 {
		t.Errorf("replica_writes_epoch_regressed = %d, want 0: the arriving epoch is "+
			"lower because the sender restarted, not because any incarnation went "+
			"backwards", got)
	}
	if got := m.ReplicaEpochRegressed.Load(); got != 0 {
		t.Errorf("replica_epoch_regressed gauge = %d, want 0: it never clears, so a "+
			"routine restart latching it makes the signal unusable", got)
	}

	// A tombstone is ordered like a value, so it must be classified like one —
	// the half a fix applied only to the put path would silently miss.
	applied, err = l.ReplayDeleteIfNewer(ctx, "k", makeSeq(1_999, 1_000))
	if err != nil {
		t.Fatalf("ReplayDeleteIfNewer: %v", err)
	}
	if applied {
		t.Error("a replayed tombstone below the stored sequence must be discarded")
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 2 {
		t.Errorf("replica_writes_discarded after a discarded replayed delete = %d, want 2", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 0 {
		t.Errorf("replica_writes_epoch_regressed after a discarded replayed delete = %d, "+
			"want 0", got)
	}

	// The live path is what the alarm still covers, and it must be untouched: a
	// primary that lost its state does not only replay, it serves clients, and
	// those writes arrive here.
	applied, err = l.PutIfNewer(ctx, "k", []byte("live-from-a-wiped-primary"), regressed)
	if err != nil {
		t.Fatalf("PutIfNewer(regressed, live): %v", err)
	}
	if applied {
		t.Error("a live write below the stored sequence must be discarded")
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 1 {
		t.Errorf("replica_writes_epoch_regressed = %d, want 1: suppressing the "+
			"classification for replays must not suppress it for live writes, which is "+
			"where a wiped primary is discovered", got)
	}
	if got := m.ReplicaEpochRegressed.Load(); got != 1 {
		t.Errorf("replica_epoch_regressed gauge = %d, want 1 (latched)", got)
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 3 {
		t.Errorf("replica_writes_discarded = %d, want 3", got)
	}
}

// TestReplayAppliesWhenNewer pins that the marker changes classification only.
// A replay is how a replica converges after a fault, so an entry it has not seen
// must still be taken.
func TestReplayAppliesWhenNewer(t *testing.T) {
	ctx := context.Background()
	m := &metrics.Metrics{}
	l := openEpochTree(t, t.TempDir(), WithMetrics(m))

	if applied, err := l.ReplayPutIfNewer(ctx, "k", []byte("replayed"), makeSeq(2_000, 5)); err != nil || !applied {
		t.Fatalf("a replay of an unseen key must apply: applied=%v err=%v", applied, err)
	}
	if applied, err := l.ReplayPutIfNewer(ctx, "k", []byte("newer"), makeSeq(2_001, 1)); err != nil || !applied {
		t.Fatalf("a replay above the stored sequence must apply: applied=%v err=%v", applied, err)
	}
	if got, err := l.Get(ctx, "k"); err != nil || string(got) != "newer" {
		t.Errorf("stored value = (%q, %v), want (\"newer\", nil)", got, err)
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 0 {
		t.Errorf("replica_writes_discarded = %d, want 0: nothing here was refused", got)
	}

	// A replayed tombstone above the stored sequence is applied for the same
	// reason: it is how a delete refused during a fault reaches the replica.
	if applied, err := l.ReplayDeleteIfNewer(ctx, "k", makeSeq(2_002, 1)); err != nil || !applied {
		t.Fatalf("a replayed tombstone above the stored sequence must apply: applied=%v err=%v",
			applied, err)
	}
}
