package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

// lostWriteHistory builds a recorder whose history is illegal on exactly one
// key: an acknowledged write disappears, which is the shape of the anomaly a
// FAIL is supposed to describe. Operations are stamped by hand so the report's
// offsets and fault correlation are testable.
//
// It returns the recorder, the measurement start the offsets are relative to,
// and the ids of the acknowledged write and the read that lost it.
func lostWriteHistory(t *testing.T) (rec *linearizability.Recorder, badKey string, put, get int) {
	t.Helper()
	rec = &linearizability.Recorder{}

	// A clean key, so the report has to pick the right one out of two.
	id := rec.BeginWorker(0, linearizability.Input{Op: "put", Key: "clean", Value: "v"})
	rec.End(id, linearizability.Output{})
	id = rec.BeginWorker(0, linearizability.Input{Op: "get", Key: "clean"})
	rec.End(id, linearizability.Output{Value: "v"})

	badKey = "hot-key"
	// A refused-but-applied write, so the printed window has to label it.
	id = rec.BeginWorker(2, linearizability.Input{Op: "put", Key: badKey, Value: "refused"})
	rec.End(id, linearizability.Output{Err: true, Applied: true})
	put = rec.BeginWorker(3, linearizability.Input{Op: "put", Key: badKey, Value: "acked"})
	rec.End(put, linearizability.Output{})
	get = rec.BeginWorker(1, linearizability.Input{Op: "get", Key: badKey})
	rec.End(get, linearizability.Output{}) // absent — the acknowledged write is gone

	if rec.Check() {
		t.Fatal("fixture history is linearizable; the test cannot exercise a FAIL")
	}
	return rec, badKey, put, get
}

// TestCounterexampleReportsTheOffendingWindow is the revert check for Part 1: a
// synthetic non-linearizable history must produce a printed window that names
// the key, marks the operation the checker could not place, and labels how each
// operation was modelled.
//
// Reverting the extraction (returning nil from Recorder.Counterexample, or
// dropping the formatter) fails this test, which is the whole point — before it,
// a FAIL printed one word and the operator had nothing to correlate.
func TestCounterexampleReportsTheOffendingWindow(t *testing.T) {
	rec, badKey, _, get := lostWriteHistory(t)

	cx := rec.Counterexample(30 * time.Second)
	if cx == nil {
		t.Fatal("no counterexample extracted from an illegal history")
	}
	if cx.Key != badKey {
		t.Fatalf("localised to key %q, want %q", cx.Key, badKey)
	}

	start := time.Now().Add(-time.Minute)
	lines := formatCounterexample(cx, start, nil, "chaos-counterexample-test.json")
	out := strings.Join(lines, "\n")

	for _, want := range []string{
		badKey,                    // the key is named
		"✗",                       // the frontier is marked
		"refused-applied",         // modelling labels are present
		"could not place",         // and the report says what the mark means
		"chaos-counterexample-te", // the file is pointed at
	} {
		if !strings.Contains(out, want) {
			t.Errorf("counterexample output does not mention %q:\n%s", want, out)
		}
	}

	// The marked operation must be the read that lost the write, on the line
	// carrying the ✗ — not merely somewhere in the block.
	var marked string
	for _, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "✗") {
			marked = l
		}
	}
	if marked == "" {
		t.Fatalf("no operation was marked as unplaceable:\n%s", out)
	}
	if !strings.Contains(marked, "get") || !strings.Contains(marked, "absent") {
		t.Errorf("marked line does not describe the absent read: %q", marked)
	}
	if !strings.Contains(marked, "w1") {
		t.Errorf("marked line does not name the worker that issued it: %q", marked)
	}
	_ = get

	// Every clean key must stay out of the accusation.
	if strings.Contains(out, "clean") {
		t.Errorf("output implicates the legal key:\n%s", out)
	}
}

// TestCounterexampleCorrelatesWithFaultWindows checks the annotation an operator
// actually reads first: which fault, if any, was in progress.
func TestCounterexampleCorrelatesWithFaultWindows(t *testing.T) {
	rec, _, _, _ := lostWriteHistory(t)
	cx := rec.Counterexample(30 * time.Second)
	if cx == nil {
		t.Fatal("no counterexample extracted from an illegal history")
	}

	// A window that spans the whole (sub-millisecond) fixture history, and one
	// that does not overlap it at all.
	start := cx.Ops[0].Call.Add(-time.Second)
	windows := []FaultWindow{
		{Victim: "node9", DownAt: start, UpAt: start.Add(500 * time.Millisecond)},
		{Victim: "node3", DownAt: cx.Ops[0].Call.Add(-time.Millisecond), UpAt: cx.Ops[len(cx.Ops)-1].Return.Add(time.Millisecond)},
	}

	out := strings.Join(formatCounterexample(cx, start, windows, ""), "\n")
	if !strings.Contains(out, "[fault #2 node3]") {
		t.Errorf("operations inside the outage are not annotated with it:\n%s", out)
	}
	if strings.Contains(out, "node9") {
		t.Errorf("operations are attributed to an outage they did not overlap:\n%s", out)
	}
}

// TestOverlappingWindowBoundaries pins the overlap relation, because the whole
// value of the annotation is that it is not approximate. An operation whose
// interval touches an outage counts; one that finished before it started does
// not; and a strike that never landed is not an outage at all.
func TestOverlappingWindowBoundaries(t *testing.T) {
	base := time.Now()
	op := func(call, ret time.Time) linearizability.Op {
		return linearizability.Op{Call: call, Return: ret}
	}
	down := base.Add(10 * time.Second)
	up := base.Add(20 * time.Second)

	cases := []struct {
		name    string
		op      linearizability.Op
		windows []FaultWindow
		want    int
	}{
		{
			name:    "wholly before the outage",
			op:      op(base, base.Add(time.Second)),
			windows: []FaultWindow{{Victim: "n", DownAt: down, UpAt: up}},
			want:    -1,
		},
		{
			name:    "wholly after the outage",
			op:      op(base.Add(30*time.Second), base.Add(31*time.Second)),
			windows: []FaultWindow{{Victim: "n", DownAt: down, UpAt: up}},
			want:    -1,
		},
		{
			name:    "issued before, answered during",
			op:      op(base.Add(9*time.Second), base.Add(11*time.Second)),
			windows: []FaultWindow{{Victim: "n", DownAt: down, UpAt: up}},
			want:    0,
		},
		{
			name:    "issued during, answered after",
			op:      op(base.Add(19*time.Second), base.Add(21*time.Second)),
			windows: []FaultWindow{{Victim: "n", DownAt: down, UpAt: up}},
			want:    0,
		},
		{
			name:    "a strike that failed is not an outage",
			op:      op(base.Add(11*time.Second), base.Add(12*time.Second)),
			windows: []FaultWindow{{Victim: "n", DownAt: down, UpAt: up, DisruptErr: "boom"}},
			want:    -1,
		},
		{
			name:    "an unhealed window stays open",
			op:      op(base.Add(1*time.Hour), base.Add(1*time.Hour)),
			windows: []FaultWindow{{Victim: "n", DownAt: down}},
			want:    0,
		},
		{
			name:    "an incomplete operation is placed by its call",
			op:      op(base.Add(11*time.Second), time.Time{}),
			windows: []FaultWindow{{Victim: "n", DownAt: down, UpAt: up}},
			want:    0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := overlappingWindow(tc.op, tc.windows); got != tc.want {
				t.Errorf("overlappingWindow = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestCounterexampleFileIsWrittenAndComparable covers the artefact: runs are
// compared by diffing these files, so the file must be valid JSON carrying the
// key, the frontier, and the omission accounting.
func TestCounterexampleFileIsWrittenAndComparable(t *testing.T) {
	rec, badKey, _, get := lostWriteHistory(t)
	cx := rec.Counterexample(30 * time.Second)
	if cx == nil {
		t.Fatal("no counterexample extracted from an illegal history")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "cx.json")
	start := cx.Ops[0].Call.Add(-time.Second)
	meta := counterexampleMeta{
		Verdict:          "FAIL",
		Target:           "localhost:8001",
		Nemesis:          "stop-restart on [node2]",
		MeasurementStart: start,
		MeasuredFor:      time.Minute,
		Windows:          []FaultWindow{{Victim: "node3", DownAt: start, UpAt: start.Add(5 * time.Second)}},
	}

	written, err := writeCounterexampleFile(path, cx, meta)
	if err != nil {
		t.Fatalf("writeCounterexampleFile: %v", err)
	}
	if written != path {
		t.Fatalf("wrote %q, want %q", written, path)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var rep counterexampleReport
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("counterexample file is not valid JSON: %v", err)
	}
	if rep.Key != badKey {
		t.Errorf("file names key %q, want %q", rep.Key, badKey)
	}
	if rep.Verdict != "FAIL" || rep.Target != "localhost:8001" {
		t.Errorf("run context lost: %+v", rep)
	}
	if rep.OpsOnKey != len(cx.Ops) {
		t.Errorf("ops_on_key = %d, want %d", rep.OpsOnKey, len(cx.Ops))
	}
	if rep.OmittedBefore+len(rep.Ops)+rep.OmittedAfter != rep.OpsOnKey {
		t.Errorf("omission accounting does not add up: %d + %d + %d ≠ %d",
			rep.OmittedBefore, len(rep.Ops), rep.OmittedAfter, rep.OpsOnKey)
	}
	if len(rep.FaultWindows) != 1 {
		t.Errorf("fault windows not carried into the file: %+v", rep.FaultWindows)
	}

	var stuck *counterexampleOp
	for i := range rep.Ops {
		if rep.Ops[i].Stuck {
			stuck = &rep.Ops[i]
		}
	}
	if stuck == nil {
		t.Fatal("no operation marked stuck in the file")
	}
	if stuck.ID != get {
		t.Errorf("stuck op id = %d, want %d (the absent read)", stuck.ID, get)
	}
	if stuck.FaultWindow == nil || *stuck.FaultWindow != 1 {
		t.Errorf("stuck op is not correlated with the outage it ran inside: %+v", stuck)
	}
}

// TestCounterexampleFileCanBeDisabled keeps the runner usable in environments
// where writing next to the binary is unwelcome.
func TestCounterexampleFileCanBeDisabled(t *testing.T) {
	rec, _, _, _ := lostWriteHistory(t)
	cx := rec.Counterexample(30 * time.Second)
	if cx == nil {
		t.Fatal("no counterexample extracted from an illegal history")
	}
	written, err := writeCounterexampleFile(counterexampleOff, cx, counterexampleMeta{MeasurementStart: time.Now()})
	if err != nil {
		t.Fatalf("writeCounterexampleFile: %v", err)
	}
	if written != "" {
		t.Errorf("wrote %q with output disabled", written)
	}
}

// TestCounterexampleNoteWhenNotLocalisable keeps the runner from implying the
// history was fine when only the localisation failed. The verdict and the
// localisation are separate claims and the report must not conflate them.
func TestCounterexampleNoteWhenNotLocalisable(t *testing.T) {
	out := strings.Join(formatCounterexample(nil, time.Now(), nil, ""), "\n")
	if !strings.Contains(out, "could not be localised") {
		t.Errorf("missing the honest note:\n%s", out)
	}
	if !strings.Contains(out, "verdict above is the authority") {
		t.Errorf("note does not preserve the whole-history verdict's authority:\n%s", out)
	}

	written, err := writeCounterexampleFile("", nil, counterexampleMeta{MeasurementStart: time.Now()})
	if err != nil {
		t.Fatalf("writeCounterexampleFile with no counterexample: %v", err)
	}
	if written != "" {
		t.Errorf("wrote %q for a run with nothing to report", written)
	}
}
