package linearizability_test

import (
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

const checkBudget = 10 * time.Second

// check runs the recorded history and fails the test if the checker times out —
// a timeout is neither a PASS nor a FAIL and must never be silently read as one.
func check(t *testing.T, rec *linearizability.Recorder) bool {
	t.Helper()
	ok, timedOut := rec.CheckTimeout(checkBudget)
	if timedOut {
		t.Fatalf("linearizability check timed out after %s", checkBudget)
	}
	return ok
}

// op records one complete, non-overlapping operation.
func op(rec *linearizability.Recorder, in linearizability.Input, out linearizability.Output) {
	rec.End(rec.Begin(in), out)
}

func put(key, value string) linearizability.Input {
	return linearizability.Input{Op: "put", Key: key, Value: value}
}

func get(key string) linearizability.Input {
	return linearizability.Input{Op: "get", Key: key}
}

func del(key string) linearizability.Input {
	return linearizability.Input{Op: "delete", Key: key}
}

func read(value string) linearizability.Output {
	return linearizability.Output{Value: value}
}

var (
	ok        = linearizability.Output{}
	failed    = linearizability.Output{Err: true}
	appliedNK = linearizability.Output{Err: true, Applied: true}
	absent    = linearizability.Output{}
)

// TestRefusedButAppliedWriteIsNotAnAnomaly is the shape the chaos runner
// observed against a live 3-node cluster on 2026-08-16: while a replica was
// down, the ring-primary answered 503 "replication to replicas failed" for
// writes it had already applied to its own store, and because reads are served
// by the primary, later reads returned those refused values. Encoding the write
// as a no-op made every one of those correct reads an anomaly.
func TestRefusedButAppliedWriteIsNotAnAnomaly(t *testing.T) {
	t.Run("read-back of a refused put", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), appliedNK)
		op(rec, get("k"), read("v1"))

		if !check(t, rec) {
			t.Error("a read of a refused-but-applied write must be legal; " +
				"Output.Applied is not reaching the model")
		}
	})

	t.Run("read-back of a refused delete", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), ok)
		op(rec, del("k"), appliedNK)
		op(rec, get("k"), absent)

		if !check(t, rec) {
			t.Error("a refused-but-applied delete removes the key; reading it as absent must be legal")
		}
	})

	t.Run("the value may also still be read before the refusal is applied", func(t *testing.T) {
		// The primary applies the mutation before it attempts replication, but
		// the model does not need to know *when* inside the call it landed: a
		// read concurrent with the refused write may legally see either value.
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), ok)

		cid := rec.Begin(put("k", "v2"))
		op(rec, get("k"), read("v1"))
		rec.End(cid, appliedNK)
		op(rec, get("k"), read("v2"))

		if !check(t, rec) {
			t.Error("a concurrent read of the pre-refusal value must be legal")
		}
	})
}

// TestNeverSentWriteStaysANoOp pins the other half of the distinction: a write
// that provably never reached a server did not happen, so a value appearing out
// of nowhere is still an anomaly. If Applied leaked onto this class, the harness
// would stop detecting fabricated reads.
func TestNeverSentWriteStaysANoOp(t *testing.T) {
	rec := &linearizability.Recorder{}
	op(rec, put("k", "v1"), failed)
	op(rec, get("k"), read("v1"))

	if check(t, rec) {
		t.Error("a read of a write that never reached the store must be illegal")
	}
}

// TestLostAcknowledgedWriteStillFails is the regression the whole harness exists
// for. Relaxing the failed-write encoding must not relax the successful one: a
// write the server acknowledged and then lost is a real consistency bug and has
// to keep failing the check.
func TestLostAcknowledgedWriteStillFails(t *testing.T) {
	t.Run("acknowledged put disappears", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), ok)
		op(rec, get("k"), absent)

		if check(t, rec) {
			t.Error("an acknowledged write that a later read cannot see must FAIL")
		}
	})

	t.Run("stale read after a refused-but-applied write", func(t *testing.T) {
		// The refused write is known applied, so a read that comes back with the
		// *previous* value after it is a stale read, not a tolerated unknown.
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), ok)
		op(rec, put("k", "v2"), appliedNK)
		op(rec, get("k"), read("v1"))

		if check(t, rec) {
			t.Error("a stale read after a known-applied write must FAIL")
		}
	})
}

// TestPendingWriteIsUnconstrained covers the third outcome: an unknown effect,
// recorded through EndUnknown. Both readings of the history have to be legal,
// because the caller genuinely does not know which happened.
func TestPendingWriteIsUnconstrained(t *testing.T) {
	t.Run("read-back: the write did land", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		rec.EndUnknown(rec.Begin(put("k", "v1")))
		op(rec, get("k"), read("v1"))

		if !check(t, rec) {
			t.Error("a pending write observed by a later read must be legal")
		}
	})

	t.Run("never observed: the write did not land", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		rec.EndUnknown(rec.Begin(put("k", "v1")))
		op(rec, get("k"), absent)

		if !check(t, rec) {
			t.Error("a pending write no read ever observes must be legal — it may " +
				"linearize after the whole history, which is indistinguishable " +
				"from never happening")
		}
	})

	t.Run("observed late: absent, then present", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), ok)
		rec.EndUnknown(rec.Begin(put("k", "v2")))
		op(rec, get("k"), read("v1"))
		op(rec, get("k"), read("v2"))

		if !check(t, rec) {
			t.Error("a pending write may linearize between two reads")
		}
	})

	t.Run("a pending delete is unconstrained too", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		op(rec, put("k", "v1"), ok)
		rec.EndUnknown(rec.Begin(del("k")))
		op(rec, get("k"), read("v1"))

		if !check(t, rec) {
			t.Error("a pending delete that no read observes must be legal")
		}
	})

	t.Run("pending ops do not mask a genuine anomaly on another key", func(t *testing.T) {
		rec := &linearizability.Recorder{}
		rec.EndUnknown(rec.Begin(put("pending", "v1")))
		op(rec, put("k", "v1"), ok)
		op(rec, get("k"), absent)

		if check(t, rec) {
			t.Error("a pending operation must not license a lost write elsewhere")
		}
	})
}

// TestPendingReturnsComeAfterEveryRecordedEvent is the mechanism behind
// EndUnknown: the event API uses each event's index as its timestamp, so a
// pending operation's return has to sit past every observed event for the
// operation to be placeable anywhere. Asserted through behaviour, since the
// history itself is unexported: a pending write recorded *first* must still be
// linearizable after a read that did not observe it.
func TestPendingReturnsComeAfterEveryRecordedEvent(t *testing.T) {
	rec := &linearizability.Recorder{}
	pending := rec.Begin(put("k", "v1"))
	rec.EndUnknown(pending)
	for i := 0; i < 20; i++ {
		op(rec, get("k"), absent)
	}

	if !check(t, rec) {
		t.Error("20 reads that never observe a pending write must all be legal")
	}
	if got, want := rec.Len(), 2+2*20; got != want {
		t.Errorf("Len() = %d, want %d — a pending op must still count two events", got, want)
	}
	if got := rec.Pending(); got != 1 {
		t.Errorf("Pending() = %d, want 1", got)
	}
}

// TestDescribeOperationLabelsEachOutcome keeps the three write encodings
// distinguishable in a failing history's output. An operator reading a FAIL has
// to be able to tell "the store confirmed this" from "we never found out".
func TestDescribeOperationLabelsEachOutcome(t *testing.T) {
	describe := linearizability.KVModel.DescribeOperation
	tests := []struct {
		name string
		in   linearizability.Input
		out  linearizability.Output
		want string
	}{
		{"successful put", put("k", "v"), ok, `put("k", "v") -> ok`},
		{"successful get", get("k"), read("v"), `get("k") -> "v"`},
		{"absent get", get("k"), absent, `get("k") -> <absent>`},
		{"failed write", put("k", "v"), failed, `put("k") -> ERR`},
		{"refused but applied", put("k", "v"), appliedNK, `put("k", "v") -> ERR but applied`},
		{
			"pending",
			put("k", "v"),
			linearizability.Output{Err: true, Applied: true, Deferred: true},
			`put("k") -> UNKNOWN (pending: may be linearized anywhere)`,
		},
	}
	for _, tc := range tests {
		if got := describe(tc.in, tc.out); got != tc.want {
			t.Errorf("%s: DescribeOperation = %q, want %q", tc.name, got, tc.want)
		}
	}
}
