package main

// Counterexample reporting.
//
// A FAIL used to be a single word. The history that produced it was 580,000
// events long, the anomaly lived on one of 20 keys, and the runner said nothing
// about which — so diagnosing a 1-in-4 race meant re-running until it happened
// again and guessing. This file turns that verdict into evidence: the key whose
// sub-history the checker rejected, the operations around the point where it got
// stuck, how each one was modelled, which worker issued it, and which fault
// window (if any) it overlapped.
//
// Two bounds are deliberate. The window is capped, because a 60s run records
// tens of thousands of operations per key and printing them all would bury the
// anomaly. And the localisation is honest about its limits: it names the
// operations the checker could not place, not "the bug" — an anomaly is a
// property of a set of operations, and the checker's frontier is where the set
// stopped being satisfiable, not necessarily where the store went wrong.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/linearizability"
)

// Window sizes. The terminal gets a tight window centred on the frontier; the
// file gets a wider one, because comparing two runs benefits from context that
// would be noise on stdout.
const (
	cxTerminalBefore = 12
	cxTerminalAfter  = 6
	cxFileBefore     = 120
	cxFileAfter      = 40
)

// counterexampleOff disables writing the counterexample file.
const counterexampleOff = "none"

// counterexampleOp is one operation as written to the counterexample file.
type counterexampleOp struct {
	ID     int    `json:"id"`
	Worker int    `json:"worker"`
	Op     string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	Absent bool   `json:"absent"`
	// ReturnOffsetMs is null for an operation with no recorded return, so that
	// "never returned" is not reported as "returned at offset 0" — the same
	// encoding faultWindowReport uses for a window whose victim never came back.
	CallOffsetMs   int64  `json:"call_offset_ms"`
	ReturnOffsetMs *int64 `json:"return_offset_ms"`
	Modeling       string `json:"modeling"`
	Desc           string `json:"desc"`
	Linearized     bool   `json:"linearized"`
	Stuck          bool   `json:"stuck"`
	// FaultWindow is the 1-based index of the fault window this operation
	// overlapped, or null when it ran while the cluster was whole.
	FaultWindow *int   `json:"fault_window"`
	FaultVictim string `json:"fault_victim,omitempty"`
}

// counterexampleReport is the file format. It carries enough run context to be
// read on its own, months later, next to another run's file.
type counterexampleReport struct {
	Verdict          string              `json:"verdict"`
	Target           string              `json:"target"`
	Nemesis          string              `json:"nemesis"`
	MeasurementStart string              `json:"measurement_start"`
	MeasuredDuration string              `json:"measured_duration"`
	Key              string              `json:"key"`
	FailingKeys      []string            `json:"failing_keys"`
	KeysChecked      int                 `json:"keys_checked"`
	KeysTotal        int                 `json:"keys_total"`
	UnresolvedKeys   []string            `json:"unresolved_keys,omitempty"`
	BudgetExhausted  bool                `json:"budget_exhausted"`
	OpsOnKey         int                 `json:"ops_on_key"`
	LinearizedOps    int                 `json:"linearized_ops"`
	OmittedBefore    int                 `json:"omitted_before"`
	OmittedAfter     int                 `json:"omitted_after"`
	FaultWindows     []faultWindowReport `json:"fault_windows,omitempty"`
	Ops              []counterexampleOp  `json:"ops"`
}

// overlappingWindow returns the index of the fault window an operation ran
// inside, or -1. An operation counts as overlapping when its call/return
// interval intersects the outage: that is the relation an operator wants, since
// a read issued before a node went down but answered after it did is exactly the
// kind of operation a fault can corrupt.
//
// A window whose victim never came back is treated as open-ended rather than
// instantaneous, because that is what it was.
func overlappingWindow(op linearizability.Op, windows []FaultWindow) int {
	end := op.Return
	if end.IsZero() {
		end = op.Call
	}
	for i, w := range windows {
		if !w.Injected() {
			// A strike that failed is not an outage; attributing an operation to
			// it would point the reader at a fault that never happened.
			continue
		}
		up := w.UpAt
		if up.IsZero() {
			if !end.Before(w.DownAt) {
				return i
			}
			continue
		}
		if !end.Before(w.DownAt) && !op.Call.After(up) {
			return i
		}
	}
	return -1
}

// counterexampleOps converts a bounded window of operations to their file shape.
func counterexampleOps(ops []linearizability.Op, stuckID int, start time.Time, windows []FaultWindow) []counterexampleOp {
	out := make([]counterexampleOp, 0, len(ops))
	for _, op := range ops {
		c := counterexampleOp{
			ID:           op.ID,
			Worker:       op.Worker,
			Op:           op.Op,
			Key:          op.Key,
			Value:        op.Value,
			Absent:       op.Absent,
			Modeling:     string(op.Modeling),
			Desc:         op.Desc,
			CallOffsetMs: op.Call.Sub(start).Milliseconds(),
			Linearized:   op.Linearized,
			Stuck:        op.ID == stuckID,
		}
		if !op.Return.IsZero() {
			ret := op.Return.Sub(start).Milliseconds()
			c.ReturnOffsetMs = &ret
		}
		if i := overlappingWindow(op, windows); i >= 0 {
			idx := i + 1
			c.FaultWindow = &idx
			c.FaultVictim = windows[i].Victim
		}
		out = append(out, c)
	}
	return out
}

// stuckOpID returns the id of the earliest operation the checker could not
// place, or -1 when the frontier is not localisable.
func stuckOpID(cx *linearizability.Counterexample) int {
	if cx == nil || cx.FirstStuck < 0 || cx.FirstStuck >= len(cx.Ops) {
		return -1
	}
	return cx.Ops[cx.FirstStuck].ID
}

// formatCounterexample renders the terminal block. An empty string is a
// separator line, matching verdictNotes.
func formatCounterexample(cx *linearizability.Counterexample, start time.Time, windows []FaultWindow, file string) []string {
	if cx == nil {
		return []string{
			"",
			"NOTE: the anomaly could not be localised to a single key. The whole-history",
			"      verdict above is the authority on legality; this only means the per-key",
			"      re-check did not reproduce it on any one key inside its budget. Raise",
			"      --check-timeout and re-run to get the operation window.",
		}
	}

	lines := []string{
		"",
		fmt.Sprintf("counterexample — key %s", cx.Key),
	}

	scope := fmt.Sprintf("%d of %d key(s) failed to linearize", len(cx.FailingKeys), cx.KeysTotal)
	if cx.KeysChecked < cx.KeysTotal {
		scope += fmt.Sprintf("; only %d were checked before the budget ran out", cx.KeysChecked)
	}
	lines = append(lines, "  "+scope)
	if len(cx.FailingKeys) > 1 {
		lines = append(lines, "  also failing: "+strings.Join(cx.FailingKeys[1:], ", "))
	}
	if len(cx.UnresolvedKeys) > 0 {
		lines = append(lines, fmt.Sprintf("  %d key(s) timed out and are neither proven legal nor illegal: %s",
			len(cx.UnresolvedKeys), strings.Join(cx.UnresolvedKeys, ", ")))
	}

	stuck := stuckOpID(cx)
	if stuck >= 0 {
		lines = append(lines, fmt.Sprintf("  the checker ordered %d of %d operation(s) on this key; the earliest one it",
			cx.LinearizedOps, len(cx.Ops)))
		lines = append(lines, "  could not place is marked ✗ — that is where the history stopped being")
		lines = append(lines, "  satisfiable, not a proof that this one operation is the defect")
	} else {
		lines = append(lines, fmt.Sprintf("  the checker ordered %d of %d operation(s) on this key but no single one",
			cx.LinearizedOps, len(cx.Ops)))
		lines = append(lines, "  stands out as unplaceable; the tail of the history is shown instead")
	}
	lines = append(lines, "  offsets are from measurement start, so they line up with the fault windows above")
	lines = append(lines, "")

	ops, before, after := cx.Window(cxTerminalBefore, cxTerminalAfter)
	if before > 0 {
		lines = append(lines, fmt.Sprintf("  … %d earlier operation(s) on this key omitted", before))
	}
	for _, op := range ops {
		lines = append(lines, "  "+formatCounterexampleOp(op, stuck, start, windows))
	}
	if after > 0 {
		lines = append(lines, fmt.Sprintf("  … %d later operation(s) on this key omitted", after))
	}

	if file != "" {
		lines = append(lines, "  full window written to "+file)
	}
	return lines
}

// formatCounterexampleOp renders one operation line.
//
// The key is deliberately absent: it is stated once in the header, and repeating
// a 32-character key on every line pushes the part that varies — the value, the
// modelling, the fault — off the right edge of a terminal.
func formatCounterexampleOp(op linearizability.Op, stuckID int, start time.Time, windows []FaultWindow) string {
	mark := "  "
	if op.ID == stuckID {
		mark = "✗ "
	}
	worker := "w?"
	if op.Worker >= 0 {
		worker = fmt.Sprintf("w%d", op.Worker)
	}
	ret := "—"
	if !op.Return.IsZero() {
		ret = offsetString(op.Return, start)
	}
	line := fmt.Sprintf("%s%10s→%-10s %-4s %-15s %s",
		mark, offsetString(op.Call, start), ret, worker, op.Modeling, compactOpDesc(op))
	if i := overlappingWindow(op, windows); i >= 0 {
		line += fmt.Sprintf("   [fault #%d %s]", i+1, windows[i].Victim)
	}
	return line
}

// compactOpDesc describes an operation without its key. The outcome of a write
// is not repeated here either — the modelling column already carries it, and
// that is the more precise statement of the two.
func compactOpDesc(op linearizability.Op) string {
	switch op.Op {
	case "put":
		return "put " + truncQuote(op.Value)
	case "delete":
		return "delete"
	case "get":
		switch {
		case op.Modeling == linearizability.ModelingFailedRead:
			return "get -> ERR"
		case op.Absent:
			return "get -> <absent>"
		default:
			return "get -> " + truncQuote(op.Value)
		}
	}
	return op.Op
}

// truncQuote quotes a value, shortening one long enough to wrap the line. The
// full value is in the counterexample file.
func truncQuote(v string) string {
	const max = 32
	if len(v) <= max {
		return fmt.Sprintf("%q", v)
	}
	return fmt.Sprintf("%q…", v[:max])
}

// offsetString renders a wall-clock instant as an offset from the measurement
// start, in the same shape the fault-window table uses.
func offsetString(t, start time.Time) string {
	return fmt.Sprintf("+%s", t.Sub(start).Round(time.Millisecond))
}

// buildCounterexampleReport assembles the machine-readable counterexample: the
// bounded operation window plus enough run context to be read on its own,
// months later, next to another run's file.
func buildCounterexampleReport(cx *linearizability.Counterexample, meta counterexampleMeta) *counterexampleReport {
	if cx == nil {
		return nil
	}
	ops, before, after := cx.Window(cxFileBefore, cxFileAfter)
	return &counterexampleReport{
		Verdict:          meta.Verdict,
		Target:           meta.Target,
		Nemesis:          meta.Nemesis,
		MeasurementStart: meta.MeasurementStart.Format(time.RFC3339Nano),
		MeasuredDuration: meta.MeasuredFor.Round(time.Millisecond).String(),
		Key:              cx.Key,
		FailingKeys:      cx.FailingKeys,
		KeysChecked:      cx.KeysChecked,
		KeysTotal:        cx.KeysTotal,
		UnresolvedKeys:   cx.UnresolvedKeys,
		BudgetExhausted:  cx.BudgetExhausted,
		OpsOnKey:         len(cx.Ops),
		LinearizedOps:    cx.LinearizedOps,
		OmittedBefore:    before,
		OmittedAfter:     after,
		FaultWindows:     faultWindowReports(meta.Windows, meta.MeasurementStart),
		Ops:              counterexampleOps(ops, stuckOpID(cx), meta.MeasurementStart, meta.Windows),
	}
}

// writeCounterexampleFile writes the counterexample to path (auto-naming it when
// path is empty) and returns the name written. It returns "" when writing is
// disabled or there is nothing to write; an I/O failure is returned as an error
// rather than swallowed, because a report the operator was told exists and does
// not is worse than no report.
func writeCounterexampleFile(
	path string,
	cx *linearizability.Counterexample,
	meta counterexampleMeta,
) (string, error) {
	rep := buildCounterexampleReport(cx, meta)
	if path == counterexampleOff || rep == nil {
		return "", nil
	}
	if path == "" {
		path = fmt.Sprintf("chaos-counterexample-%s.json", meta.MeasurementStart.Format("20060102-150405"))
	}

	body, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode counterexample: %w", err)
	}
	body = append(body, '\n')
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", fmt.Errorf("write counterexample %s: %w", path, err)
	}
	return path, nil
}

// counterexampleMeta is the run context stamped into the counterexample file.
type counterexampleMeta struct {
	Verdict          string
	Target           string
	Nemesis          string
	MeasurementStart time.Time
	MeasuredFor      time.Duration
	Windows          []FaultWindow
}
