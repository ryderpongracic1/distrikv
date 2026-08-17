package main

import (
	"context"
	"errors"
	"testing"
)

// A catch-up pass replays entries out of this node's WAL, so the sequence it puts
// on the wire has to be the one each entry was written with. Reading it back out
// of the log is what makes a pass idempotent and safe to race against live
// replication: an entry the replica already has arrives with a sequence it
// already holds and is discarded, and an entry superseded by a write that landed
// after the pass's range was pinned loses to that write instead of reverting it.
//
// Assigning a fresh sequence at replay time would invert exactly that case — the
// replay would look newer than the newer write — which is why this is asserted
// rather than left to the fan-out tests.

// TestCatchUpReplayCarriesTheOriginalSequence pins that a replayed entry's
// sequence matches the sequence its local write was assigned.
func TestCatchUpReplayCarriesTheOriginalSequence(t *testing.T) {
	n, peers := testNode(t, 2, "node2")
	ae := withAntiEntropy(t, n, "node2")
	replica := peers["node2"]
	keys := keysOwedTo(t, ae, "node2", 2)

	// The replica is unreachable, so these writes are refused and kept locally —
	// the divergence a catch-up pass exists to repair.
	replica.setErr(errors.New("replica down"))
	seqByKey := make(map[string]uint64, len(keys))
	for _, key := range keys {
		seq, err := n.store.Put(context.Background(), key, []byte("v"))
		if err != nil {
			t.Fatalf("local put %q: %v", key, err)
		}
		if seq == 0 {
			t.Fatal("local write was assigned sequence 0")
		}
		seqByKey[key] = seq
	}

	// The replica comes back and the pass replays what it missed.
	replica.setErr(nil)
	replica.reset()
	ae.repair(context.Background(), "node2")

	got := make(map[string]uint64)
	for _, r := range replica.requests() {
		got[r.Key] = r.Seq
	}
	for key, want := range seqByKey {
		seq, ok := got[key]
		if !ok {
			t.Errorf("catch-up did not replay %q", key)
			continue
		}
		if seq != want {
			t.Errorf("catch-up replayed %q with seq %d, want %d (the sequence the write was given)",
				key, seq, want)
		}
	}
}

// TestCatchUpReplayLosesToANewerLiveWrite is the property the sequence buys.
//
// A pass covers a range of the log that was pinned when it started, so a write
// that lands after that point is not in the range and the pass ships the older
// value for that key. Delivering that pass output to a replica that already took
// the newer write must not roll it back — which holds because the newer write has
// the higher sequence, and would not if replay stamped its entries with a
// sequence read at replay time.
func TestCatchUpReplayLosesToANewerLiveWrite(t *testing.T) {
	n, _ := testNode(t, 2, "node2")
	ctx := context.Background()
	const key = "raced"

	// What a catch-up pass would ship: the older value, with its own sequence.
	oldSeq, err := n.store.Put(ctx, key, []byte("from-replay"))
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	// The newer live write, which reached the replica first.
	newSeq, err := n.store.Put(ctx, key, []byte("from-live"))
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if newSeq <= oldSeq {
		t.Fatalf("precondition: sequences must increase, got %d then %d", oldSeq, newSeq)
	}

	// Replay the two against a replica in the order that used to lose data.
	replica, _ := testNode(t, 2, "node2")
	deliverPut(t, replica, key, "from-live", newSeq)
	deliverPut(t, replica, key, "from-replay", oldSeq)
	wantValue(t, replica, key, "from-live")
}
