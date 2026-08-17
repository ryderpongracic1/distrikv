package linearizability

// Counterexample extraction: turning "FAIL" into "which key, which operations".
//
// Porcupine reports a verdict over the whole history and, with the verbose
// check, the *maximal partial linearizations* it found per partition — the
// longest prefixes it could build before getting stuck. It does not report
// which partition failed, and LinearizationInfo's internals are unexported, so
// there is no public way to ask.
//
// The model already partitions by key, and KV keys are independent registers,
// so the localisation is available another way: re-check each key's
// sub-history on its own. A whole-history FAIL means at least one key's
// sub-history is illegal, and checking them one at a time says which. Each
// sub-history is a fraction of the whole, so the scan is cheap relative to the
// check that already ran.
//
// Within the failing key, the longest partial linearization is the frontier:
// the operations in it are ones the checker could order consistently, and the
// earliest operation that is *not* in it is where the history stopped making
// sense. That is not a proof that the marked operation is the culprit — an
// anomaly is a property of a set of operations, not of one — so the report says
// "could not be placed", never "this is the bug".

import (
	"fmt"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

// Modeling labels how an operation entered the recorded history. A
// counterexample is unreadable without it: the same failing read means
// different things depending on whether the write next to it was confirmed,
// refused-but-kept, or never delivered.
type Modeling string

const (
	// ModelingOK: the operation succeeded and constrains the model normally.
	ModelingOK Modeling = "ok"
	// ModelingRefusedApplied: a write that returned an error with positive
	// evidence it took effect anyway (distrikv's 503). The model applies it.
	ModelingRefusedApplied Modeling = "refused-applied"
	// ModelingNoOp: a write that provably never reached the store. The model
	// treats it as having no effect.
	ModelingNoOp Modeling = "no-op"
	// ModelingPending: a write whose effect is unknown, recorded through
	// EndUnknown. The checker may place it anywhere or nowhere.
	ModelingPending Modeling = "pending"
	// ModelingFailedRead: a read that returned an error. It asserts nothing.
	ModelingFailedRead Modeling = "failed-read"
	// ModelingIncomplete: a call with no recorded return. The checker cannot
	// use it; it appears in a report only because dropping it silently would
	// hide a recording bug.
	ModelingIncomplete Modeling = "incomplete"
)

// Op is one operation as it appears in a counterexample: what was asked, what
// came back, how it was modelled, who issued it, and when.
type Op struct {
	ID       int      `json:"id"`
	Worker   int      `json:"worker"` // -1 when the caller did not attribute it
	Op       string   `json:"op"`     // "put" | "get" | "delete"
	Key      string   `json:"key"`
	Value    string   `json:"value"`    // put: value written; get: value read
	Absent   bool     `json:"absent"`   // get only: the key was reported absent
	Modeling Modeling `json:"modeling"` // how the operation entered the history
	// Desc is the model's own rendering of the operation, so a report and a
	// porcupine visualization describe it the same way.
	Desc string `json:"desc"`
	// Call and Return are wall-clock; the report converts them to offsets from
	// the measurement start. Return is zero for an incomplete operation.
	Call   time.Time `json:"call"`
	Return time.Time `json:"return"`
	// Linearized reports whether the operation appears in the longest partial
	// linearization the checker found for this key.
	Linearized bool `json:"linearized"`
}

// Counterexample localises a non-linearizable history to one key and the
// operations recorded against it.
type Counterexample struct {
	// Key is the key whose sub-history the checker rejected. When more than one
	// key failed, this is the first in sorted order and FailingKeys lists them
	// all.
	Key         string   `json:"key"`
	FailingKeys []string `json:"failing_keys"`
	// KeysChecked and KeysTotal say how much of the keyspace the scan covered.
	// They differ when the budget ran out mid-scan.
	KeysChecked int `json:"keys_checked"`
	KeysTotal   int `json:"keys_total"`
	// UnresolvedKeys are keys whose own sub-history check timed out. They are
	// neither proven legal nor proven illegal, so a report must not present
	// them as clean.
	UnresolvedKeys []string `json:"unresolved_keys,omitempty"`
	// BudgetExhausted reports that the scan stopped before covering every key.
	BudgetExhausted bool `json:"budget_exhausted"`

	// Ops holds every operation recorded against Key, in call order.
	Ops []Op `json:"ops"`
	// LinearizedOps is the size of the longest partial linearization the
	// checker built for this key, out of len(Ops).
	LinearizedOps int `json:"linearized_ops"`
	// FirstStuck indexes the earliest operation in Ops that the checker could
	// not fit into that linearization, or -1 if there is none (which would mean
	// the frontier is not localisable — see the package comment).
	FirstStuck int `json:"first_stuck"`
}

// Counterexample localises a failing history. It re-checks each key's
// sub-history independently, within the given total budget, and returns the
// detail for the first key found illegal.
//
// It returns nil when no single key's sub-history could be shown illegal:
// either the history is in fact linearizable, or the budget ran out before a
// failing key was reached. Callers report that honestly rather than treating it
// as "no anomaly" — the whole-history verdict is the authority on whether the
// history is legal, and this is only the localisation of one.
func (r *Recorder) Counterexample(budget time.Duration) *Counterexample {
	events, meta := r.snapshot()
	if len(events) == 0 {
		return nil
	}

	keyOf := func(id int) string {
		if id >= 0 && id < len(meta) {
			return meta[id].input.Key
		}
		return ""
	}

	// Group event positions by key, preserving relative order. Order is
	// load-bearing: porcupine's event API uses each event's index as its
	// timestamp, so a sub-history must keep the ordering of the history it came
	// from — including the synthesized returns of pending operations, which sit
	// after everything else.
	byKey := make(map[string][]porcupine.Event)
	for _, e := range events {
		k := keyOf(e.Id)
		byKey[k] = append(byKey[k], e)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cx := &Counterexample{KeysTotal: len(keys), FirstStuck: -1}
	deadline := time.Now().Add(budget)

	for _, k := range keys {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			cx.BudgetExhausted = true
			break
		}
		cx.KeysChecked++
		res, info := porcupine.CheckEventsVerbose(KVModel, byKey[k], remaining)
		switch res {
		case porcupine.Illegal:
			cx.FailingKeys = append(cx.FailingKeys, k)
			if cx.Key == "" {
				cx.Key = k
				cx.Ops, cx.LinearizedOps, cx.FirstStuck = describeFailure(byKey[k], meta, &info)
			}
		case porcupine.Unknown:
			cx.UnresolvedKeys = append(cx.UnresolvedKeys, k)
		}
	}

	if cx.Key == "" {
		return nil
	}
	return cx
}

// snapshot returns the history that would be checked together with a consistent
// copy of the per-operation metadata, so a counterexample cannot describe an
// operation the checked history did not contain.
func (r *Recorder) snapshot() ([]porcupine.Event, []opMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := make([]porcupine.Event, 0, len(r.events)+len(r.deferredReturns))
	h = append(h, r.events...)
	h = append(h, r.deferredReturns...)
	m := make([]opMeta, len(r.meta))
	copy(m, r.meta)
	return h, m
}

// describeFailure renders one key's sub-history as an ordered operation list and
// marks how far the checker got.
//
// sub must be the exact event slice that was checked, because the mapping back
// to recorded operations goes through position: porcupine renumbers ids per
// partition in first-appearance order and timestamps each event with its index,
// so a returned Operation's Call is the index of its call event in sub.
func describeFailure(sub []porcupine.Event, meta []opMeta, info *porcupine.LinearizationInfo) (ops []Op, linearized int, firstStuck int) {
	// Operations in call order, deduplicated by id.
	seen := make(map[int]bool, len(sub)/2)
	order := make([]int, 0, len(sub)/2)
	for _, e := range sub {
		if e.Kind == porcupine.CallEvent && !seen[e.Id] {
			seen[e.Id] = true
			order = append(order, e.Id)
		}
	}

	placed := longestLinearization(sub, info)
	ops = make([]Op, 0, len(order))
	firstStuck = -1
	for _, id := range order {
		op := describeOp(id, meta)
		op.Linearized = placed[id]
		if op.Linearized {
			linearized++
		} else if firstStuck < 0 {
			firstStuck = len(ops)
		}
		ops = append(ops, op)
	}
	return ops, linearized, firstStuck
}

// longestLinearization returns the set of recorded operation ids in the longest
// partial linearization the checker found for this sub-history.
//
// Ties are broken deterministically: partial linearizations come out of a map,
// so without an ordering the same failing history would print a different
// frontier from one run to the next.
func longestLinearization(sub []porcupine.Event, info *porcupine.LinearizationInfo) map[int]bool {
	// Position of each call event in sub — the key to porcupine's renumbering.
	idAt := make(map[int64]int, len(sub)/2)
	for i, e := range sub {
		if e.Kind == porcupine.CallEvent {
			idAt[int64(i)] = e.Id
		}
	}

	var best []porcupine.Operation
	for _, partition := range info.PartialLinearizationsOperations() {
		for _, partial := range partition {
			if len(partial) > len(best) || (len(partial) == len(best) && earlierSequence(partial, best)) {
				best = partial
			}
		}
	}

	placed := make(map[int]bool, len(best))
	for _, op := range best {
		if id, ok := idAt[op.Call]; ok {
			placed[id] = true
		}
	}
	return placed
}

// earlierSequence orders two equal-length linearizations by their call
// positions, so tie-breaking is stable.
func earlierSequence(a, b []porcupine.Operation) bool {
	for i := range a {
		if a[i].Call != b[i].Call {
			return a[i].Call < b[i].Call
		}
	}
	return false
}

// describeOp renders one recorded operation, including how it was modelled.
func describeOp(id int, meta []opMeta) Op {
	if id < 0 || id >= len(meta) {
		return Op{ID: id, Worker: -1, Modeling: ModelingIncomplete, Desc: fmt.Sprintf("op(id=%d) -> <not recorded>", id)}
	}
	m := meta[id]
	op := Op{
		ID:     id,
		Worker: m.worker,
		Op:     m.input.Op,
		Key:    m.input.Key,
		Value:  m.input.Value,
		Call:   m.call,
		Return: m.ret,
		Desc:   KVModel.DescribeOperation(m.input, m.output),
	}
	if m.input.Op == "get" {
		op.Value = m.output.Value
		op.Absent = !m.output.Err && m.output.Value == ""
	}
	switch {
	case !m.returned:
		op.Modeling = ModelingIncomplete
		op.Desc = fmt.Sprintf("%s(%q) -> <no return recorded>", m.input.Op, m.input.Key)
	case !m.output.Err:
		op.Modeling = ModelingOK
	case m.output.Deferred:
		op.Modeling = ModelingPending
	case m.input.Op == "get":
		op.Modeling = ModelingFailedRead
	case m.output.Applied:
		op.Modeling = ModelingRefusedApplied
	default:
		op.Modeling = ModelingNoOp
	}
	return op
}

// Window returns a bounded slice of Ops centred on the frontier: up to `before`
// operations preceding it and `after` following it, plus how many were omitted
// on each side. A run against a 20-key space records tens of thousands of
// operations per key, so an unbounded dump would bury the anomaly it exists to
// show.
func (c *Counterexample) Window(before, after int) (ops []Op, omittedBefore, omittedAfter int) {
	if c == nil || len(c.Ops) == 0 {
		return nil, 0, 0
	}
	centre := c.FirstStuck
	if centre < 0 {
		// No localisable frontier: show the tail, where a history that ran out
		// of legal orderings most often ends up.
		centre = len(c.Ops) - 1
	}
	lo := centre - before
	if lo < 0 {
		lo = 0
	}
	hi := centre + after + 1
	if hi > len(c.Ops) {
		hi = len(c.Ops)
	}
	return c.Ops[lo:hi], lo, len(c.Ops) - hi
}
