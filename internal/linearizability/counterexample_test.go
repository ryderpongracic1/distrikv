package linearizability

import (
	"testing"
	"time"
)

// TestCounterexampleLocalizesTheFailingKey is the core contract: a history that
// is illegal on exactly one key must name that key, and must not accuse any of
// the keys whose sub-history is fine.
//
// Revert check: with the per-key scan removed (returning nil, or the whole
// history's verdict without localisation), this test fails — which is the point,
// because a FAIL with no key named is the state this tooling exists to end.
func TestCounterexampleLocalizesTheFailingKey(t *testing.T) {
	rec := &Recorder{}

	// Two keys behave correctly.
	for _, k := range []string{"clean-a", "clean-b"} {
		id := rec.BeginWorker(0, Input{Op: "put", Key: k, Value: "v1"})
		rec.End(id, Output{})
		id = rec.BeginWorker(1, Input{Op: "get", Key: k})
		rec.End(id, Output{Value: "v1"})
	}

	// One key loses an acknowledged write: the put completed before the read
	// was issued, so no ordering explains the read.
	put := rec.BeginWorker(2, Input{Op: "put", Key: "broken", Value: "kept"})
	rec.End(put, Output{})
	get := rec.BeginWorker(3, Input{Op: "get", Key: "broken"})
	rec.End(get, Output{}) // absent

	if ok := rec.Check(); ok {
		t.Fatal("history should not be linearizable — the test fixture is wrong")
	}

	cx := rec.Counterexample(30 * time.Second)
	if cx == nil {
		t.Fatal("Counterexample returned nil for an illegal history")
	}
	if cx.Key != "broken" {
		t.Errorf("failing key = %q, want %q", cx.Key, "broken")
	}
	if len(cx.FailingKeys) != 1 || cx.FailingKeys[0] != "broken" {
		t.Errorf("FailingKeys = %v, want exactly [broken]", cx.FailingKeys)
	}
	if cx.KeysChecked != 3 || cx.KeysTotal != 3 {
		t.Errorf("scanned %d of %d keys, want 3 of 3", cx.KeysChecked, cx.KeysTotal)
	}
	if cx.BudgetExhausted {
		t.Error("BudgetExhausted set on a 30s budget for a 6-operation history")
	}
	if len(cx.UnresolvedKeys) != 0 {
		t.Errorf("UnresolvedKeys = %v, want none", cx.UnresolvedKeys)
	}

	if len(cx.Ops) != 2 {
		t.Fatalf("reported %d ops on the failing key, want 2: %+v", len(cx.Ops), cx.Ops)
	}
	if cx.Ops[0].Op != "put" || cx.Ops[0].Value != "kept" || cx.Ops[0].Worker != 2 {
		t.Errorf("first op = %+v, want the put of \"kept\" by worker 2", cx.Ops[0])
	}
	if cx.Ops[1].Op != "get" || !cx.Ops[1].Absent || cx.Ops[1].Worker != 3 {
		t.Errorf("second op = %+v, want the absent read by worker 3", cx.Ops[1])
	}
	// The read is the operation that cannot be placed: the put linearizes, and
	// nothing after it explains an absent read.
	if cx.FirstStuck != 1 {
		t.Errorf("FirstStuck = %d, want 1 (the read)", cx.FirstStuck)
	}
	if cx.LinearizedOps != 1 {
		t.Errorf("LinearizedOps = %d, want 1", cx.LinearizedOps)
	}
	if !cx.Ops[0].Linearized || cx.Ops[1].Linearized {
		t.Errorf("linearized flags = [%t %t], want [true false]", cx.Ops[0].Linearized, cx.Ops[1].Linearized)
	}
	for i, op := range cx.Ops {
		if op.Call.IsZero() || op.Return.IsZero() {
			t.Errorf("op %d has no timestamps: call=%v return=%v", i, op.Call, op.Return)
		}
	}
}

// TestCounterexampleIsNilForALegalHistory keeps the localisation from inventing
// a culprit: nothing to localise must read as nothing, not as the first key.
func TestCounterexampleIsNilForALegalHistory(t *testing.T) {
	rec := &Recorder{}
	id := rec.BeginWorker(0, Input{Op: "put", Key: "k", Value: "v"})
	rec.End(id, Output{})
	id = rec.BeginWorker(0, Input{Op: "get", Key: "k"})
	rec.End(id, Output{Value: "v"})

	if !rec.Check() {
		t.Fatal("fixture history should be linearizable")
	}
	if cx := rec.Counterexample(10 * time.Second); cx != nil {
		t.Errorf("Counterexample = %+v for a legal history, want nil", cx)
	}
}

// TestCounterexampleLabelsHowEachOpWasModelled is what makes a printed window
// readable. A refused-but-applied write next to a failing read means something
// different from a no-op write next to the same read, and an operator reading a
// FAIL has to be able to tell them apart without reading the runner's source.
func TestCounterexampleLabelsHowEachOpWasModelled(t *testing.T) {
	rec := &Recorder{}

	okPut := rec.BeginWorker(0, Input{Op: "put", Key: "k", Value: "v1"})
	rec.End(okPut, Output{})

	refused := rec.BeginWorker(1, Input{Op: "put", Key: "k", Value: "refused"})
	rec.End(refused, Output{Err: true, Applied: true})

	noop := rec.BeginWorker(2, Input{Op: "put", Key: "k", Value: "never-sent"})
	rec.End(noop, Output{Err: true})

	pending := rec.BeginWorker(3, Input{Op: "put", Key: "k", Value: "unknown"})
	rec.EndUnknown(pending)

	failedRead := rec.BeginWorker(4, Input{Op: "get", Key: "k"})
	rec.End(failedRead, Output{Err: true})

	// The anomaly: an absent read after an acknowledged and a known-applied
	// write, neither of which was deleted.
	badRead := rec.BeginWorker(5, Input{Op: "get", Key: "k"})
	rec.End(badRead, Output{})

	cx := rec.Counterexample(30 * time.Second)
	if cx == nil {
		t.Fatal("Counterexample returned nil for an illegal history")
	}

	want := map[int]Modeling{
		okPut:      ModelingOK,
		refused:    ModelingRefusedApplied,
		noop:       ModelingNoOp,
		pending:    ModelingPending,
		failedRead: ModelingFailedRead,
		badRead:    ModelingOK,
	}
	got := make(map[int]Modeling, len(cx.Ops))
	for _, op := range cx.Ops {
		got[op.ID] = op.Modeling
	}
	for id, m := range want {
		if got[id] != m {
			t.Errorf("op %d modelled as %q, want %q", id, got[id], m)
		}
	}
	for _, op := range cx.Ops {
		if op.Desc == "" {
			t.Errorf("op %d has no description", op.ID)
		}
	}
}

// TestCounterexampleWindowIsBounded pins the output bound. A 60s run on a
// 20-key space records tens of thousands of operations per key; a report that
// printed them all would bury the anomaly.
func TestCounterexampleWindowIsBounded(t *testing.T) {
	rec := &Recorder{}
	// 200 legal writes, then a lost-write anomaly at the end.
	for i := 0; i < 200; i++ {
		id := rec.BeginWorker(i%4, Input{Op: "put", Key: "k", Value: "v"})
		rec.End(id, Output{})
	}
	last := rec.BeginWorker(0, Input{Op: "put", Key: "k", Value: "final"})
	rec.End(last, Output{})
	bad := rec.BeginWorker(1, Input{Op: "get", Key: "k"})
	rec.End(bad, Output{}) // absent, though "final" was acknowledged

	cx := rec.Counterexample(60 * time.Second)
	if cx == nil {
		t.Fatal("Counterexample returned nil for an illegal history")
	}
	if len(cx.Ops) != 202 {
		t.Fatalf("recorded %d ops on the key, want 202", len(cx.Ops))
	}

	ops, before, after := cx.Window(5, 2)
	if len(ops) > 8 {
		t.Errorf("window returned %d ops, want at most 8", len(ops))
	}
	if before+len(ops)+after != len(cx.Ops) {
		t.Errorf("window accounting: %d omitted before + %d shown + %d omitted after ≠ %d total",
			before, len(ops), after, len(cx.Ops))
	}
	if before == 0 {
		t.Error("window omitted nothing before the frontier on a 202-op history")
	}
	// The frontier must be inside the window — that is the point of centring on it.
	foundStuck := false
	for _, op := range ops {
		if op.ID == bad {
			foundStuck = true
		}
	}
	if !foundStuck {
		t.Error("the operation that could not be placed is not inside the printed window")
	}
}

// TestCounterexampleLabelsAnUnreturnedCall covers the defensive class: a call the
// runner never ended. It should not happen — every Begin is paired — but if it
// ever does, the report must say the return is missing rather than describe the
// operation as having returned successfully at offset zero.
func TestCounterexampleLabelsAnUnreturnedCall(t *testing.T) {
	rec := &Recorder{}
	dangling := rec.BeginWorker(7, Input{Op: "put", Key: "k", Value: "never-ended"})

	op := describeOp(dangling, snapshotMeta(rec))
	if op.Modeling != ModelingIncomplete {
		t.Errorf("modelling = %q, want %q", op.Modeling, ModelingIncomplete)
	}
	if !op.Return.IsZero() {
		t.Errorf("Return = %v, want the zero time for an operation that never returned", op.Return)
	}
	if op.Worker != 7 {
		t.Errorf("Worker = %d, want 7", op.Worker)
	}
}

// snapshotMeta exposes the recorder's metadata to the tests in this package.
func snapshotMeta(r *Recorder) []opMeta {
	_, meta := r.snapshot()
	return meta
}

// TestCounterexampleWindowHandlesAnEmptyReport keeps the formatter safe on the
// paths where localisation produced nothing.
func TestCounterexampleWindowHandlesAnEmptyReport(t *testing.T) {
	var nilCx *Counterexample
	if ops, b, a := nilCx.Window(5, 5); ops != nil || b != 0 || a != 0 {
		t.Errorf("Window on a nil counterexample = (%v, %d, %d), want (nil, 0, 0)", ops, b, a)
	}
	empty := &Counterexample{FirstStuck: -1}
	if ops, _, _ := empty.Window(5, 5); ops != nil {
		t.Errorf("Window on an op-less counterexample = %v, want nil", ops)
	}
}
