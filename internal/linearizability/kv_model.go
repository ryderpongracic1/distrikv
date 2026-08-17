// Package linearizability provides a Jepsen-style linearizability harness
// for the distrikv KV store.
//
// # Model
//
// The KV store is modelled as a collection of independent string registers
// (one per key). An absent key and a deleted key are both represented as the
// empty string ""; the distinction is captured in the operation result.
//
// # Recorder
//
// Recorder is a thread-safe Porcupine event collector. Callers bracket each
// operation with Begin/End:
//
//	id := rec.Begin(linearizability.Input{Op: "put", Key: "k", Value: "v"})
//	err := store.Put(ctx, "k", []byte("v"))
//	out := linearizability.Output{}
//	if err != nil { out.Err = true }
//	rec.End(id, out)
//
// After all operations complete, call rec.Check() or rec.CheckTimeout() to
// verify the recorded history is linearizable.
//
// # The three outcomes of a failed write
//
// A write that returns an error has three possible effects on the store, and
// the harness encodes each one differently. Collapsing them into a single
// "failed" case is what makes a correct store look broken:
//
//   - It provably did not take effect (a refused connection: nothing was ever
//     delivered). Record Output{Err: true}. The model treats it as a no-op.
//   - It provably did take effect (distrikv answers HTTP 503 when the primary
//     applied a mutation locally and then could not replicate it; there is no
//     rollback). Record Output{Err: true, Applied: true}. The model applies it.
//   - Its effect is unknown (the connection died mid-request; a deadline
//     expired). Call Recorder.EndUnknown, which makes it a *pending*
//     operation — see that method for how, and why a no-op is unsound here.
//
// # Per-key partitioning
//
// The model uses porcupine's PartitionEvent hook to split the history by key
// before checking. For a KV store with K independent keys, this reduces
// checking complexity from O(exp(N)) to O(K × exp(N/K)), which is essential
// for large histories.
package linearizability

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
)

// Input is the call-side descriptor of one KV operation.
type Input struct {
	Op    string // "put", "get", or "delete"
	Key   string
	Value string // put: value being written; get/delete: empty
}

// Output is the return-side descriptor of one KV operation.
type Output struct {
	Value string // get: returned value; "" means key absent or non-get op
	Err   bool   // true → the operation returned an error

	// Applied records that a *write* took effect even though it returned an
	// error, so the model must apply it rather than treat it as a no-op. It is
	// only consulted when Err is set, and only for put and delete.
	//
	// This is not a hedge — it is a positive claim, and the caller needs
	// evidence for it. distrikv's HTTP 503 is exactly that evidence: the
	// ring-primary writes to its own store first and only then fans the
	// mutation out to the replicas, so a 503 ("replication to replicas failed")
	// is reported over a mutation that is already durable on the primary, and
	// there is no rollback. Since reads are served by the primary too, a later
	// read of that key returning the refused value is correct behaviour. See
	// the README's "CAP Position" — "a refused write is not an undone write".
	Applied bool

	// Deferred marks a return synthesized by Recorder.EndUnknown. It carries no
	// meaning for the model's Step function (EndUnknown also sets Applied); it
	// exists so DescribeOperation can label the operation honestly in a failing
	// history rather than claiming the store confirmed anything.
	Deferred bool
}

// KVModel is a Porcupine model for a multi-key KV store that operates on
// string values. State is map[string]string; an absent key means no value.
//
// A write recorded with Output.Err is a no-op *unless* Output.Applied is set,
// in which case the model applies it — see Output.Applied for why an errored
// write can be known-applied, and Recorder.EndUnknown for the third case,
// where the effect is unknown and neither encoding is sound.
//
// A failed read constrains nothing: the value it returned is not asserted.
//
// PartitionEvent splits the history by key so Porcupine checks each key's
// sub-history independently. This is sound because each KV key is an
// independent register.
var KVModel = porcupine.Model{
	Init: func() interface{} { return map[string]string{} },

	Step: func(state, inp, outp interface{}) (bool, interface{}) {
		s := state.(map[string]string)
		in := inp.(Input)
		out := outp.(Output)

		clone := func() map[string]string {
			ns := make(map[string]string, len(s)+1)
			for k, v := range s {
				ns[k] = v
			}
			return ns
		}

		// writeFailed reports whether a write should be modelled as having had
		// no effect: it returned an error and the caller has no evidence the
		// mutation landed.
		writeFailed := out.Err && !out.Applied

		switch in.Op {
		case "put":
			if writeFailed {
				return true, s
			}
			ns := clone()
			ns[in.Key] = in.Value
			return true, ns
		case "get":
			if out.Err {
				return true, s
			}
			v, ok := s[in.Key]
			if !ok {
				return out.Value == "", s
			}
			return out.Value == v, s
		case "delete":
			if writeFailed {
				return true, s
			}
			ns := clone()
			delete(ns, in.Key)
			return true, ns
		}
		return false, state
	},

	Equal: func(s1, s2 interface{}) bool {
		m1, m2 := s1.(map[string]string), s2.(map[string]string)
		if len(m1) != len(m2) {
			return false
		}
		for k, v := range m1 {
			if m2[k] != v {
				return false
			}
		}
		return true
	},

	// PartitionEvent groups events by key so each key is checked independently.
	// ReturnEvents don't carry the key; we look up the call-side key via Id.
	PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
		// First pass: map call Id → key.
		callKey := make(map[int]string, len(history)/2)
		for _, e := range history {
			if e.Kind == porcupine.CallEvent {
				callKey[e.Id] = e.Value.(Input).Key
			}
		}
		// Second pass: bucket events by key.
		buckets := make(map[string][]porcupine.Event)
		for _, e := range history {
			var key string
			if e.Kind == porcupine.CallEvent {
				key = e.Value.(Input).Key
			} else {
				key = callKey[e.Id]
			}
			buckets[key] = append(buckets[key], e)
		}
		result := make([][]porcupine.Event, 0, len(buckets))
		for _, evs := range buckets {
			result = append(result, evs)
		}
		return result
	},

	DescribeOperation: func(inp, outp interface{}) string {
		in := inp.(Input)
		out := outp.(Output)
		if out.Err {
			switch {
			case out.Deferred:
				return fmt.Sprintf("%s(%q) -> UNKNOWN (pending: may be linearized anywhere)", in.Op, in.Key)
			case out.Applied:
				return fmt.Sprintf("%s(%q, %q) -> ERR but applied", in.Op, in.Key, in.Value)
			}
			return fmt.Sprintf("%s(%q) -> ERR", in.Op, in.Key)
		}
		switch in.Op {
		case "put":
			return fmt.Sprintf("put(%q, %q) -> ok", in.Key, in.Value)
		case "get":
			if out.Value == "" {
				return fmt.Sprintf("get(%q) -> <absent>", in.Key)
			}
			return fmt.Sprintf("get(%q) -> %q", in.Key, out.Value)
		case "delete":
			return fmt.Sprintf("delete(%q) -> ok", in.Key)
		}
		return fmt.Sprintf("op(%q)", in.Key)
	},
}

// Recorder is a thread-safe Porcupine event collector.
type Recorder struct {
	mu     sync.Mutex
	events []porcupine.Event
	// deferredReturns holds the returns of pending operations. They are appended
	// after every recorded event when the history is checked; see EndUnknown.
	deferredReturns []porcupine.Event
	// meta is indexed by call ID and carries what the model deliberately does
	// not: which client issued the operation and when it was called and
	// returned in wall-clock time. Porcupine's event API uses each event's
	// index as its timestamp, so a checked history has no real times in it —
	// but a counterexample is useless without them, because correlating an
	// anomaly with a fault window is the whole point of printing one. See
	// Counterexample.
	meta   []opMeta
	nextID int64 // atomic for Begin; protected by mu for append
}

// opMeta is the reporting-only record of one operation. It is never shown to
// the model.
type opMeta struct {
	worker   int // -1 when the caller did not attribute the operation
	input    Input
	output   Output
	returned bool
	call     time.Time
	ret      time.Time
}

// Begin records the invocation of an operation and returns a unique call ID.
// The caller must call End(id, out) or EndUnknown(id) when the operation
// returns.
//
// The operation is not attributed to a client; use BeginWorker to record which
// one issued it.
func (r *Recorder) Begin(inp Input) int {
	return r.begin(-1, inp)
}

// BeginWorker is Begin, attributing the operation to a client/worker id so a
// counterexample can name which concurrent client issued it.
func (r *Recorder) BeginWorker(worker int, inp Input) int {
	return r.begin(worker, inp)
}

func (r *Recorder) begin(worker int, inp Input) int {
	id := int(atomic.AddInt64(&r.nextID, 1)) - 1
	now := time.Now()
	clientID := worker
	if clientID < 0 {
		clientID = 0
	}
	r.mu.Lock()
	r.events = append(r.events, porcupine.Event{
		Kind:     porcupine.CallEvent,
		Value:    inp,
		Id:       id,
		ClientId: clientID,
	})
	m := r.metaFor(id)
	m.worker = worker
	m.input = inp
	m.call = now
	r.mu.Unlock()
	return id
}

// metaFor returns the metadata slot for id, growing the slice as needed. The
// caller must hold r.mu.
func (r *Recorder) metaFor(id int) *opMeta {
	for id >= len(r.meta) {
		r.meta = append(r.meta, opMeta{worker: -1})
	}
	return &r.meta[id]
}

// End records the return of an operation previously started with Begin.
func (r *Recorder) End(id int, out Output) {
	now := time.Now()
	r.mu.Lock()
	r.events = append(r.events, porcupine.Event{
		Kind:  porcupine.ReturnEvent,
		Value: out,
		Id:    id,
	})
	m := r.metaFor(id)
	m.output = out
	m.returned = true
	m.ret = now
	r.mu.Unlock()
}

// EndUnknown records a *write* whose effect on the store the caller could not
// determine — the connection died mid-request, a deadline expired, a forwarding
// hop failed after the request may already have been applied.
//
// Neither of End's encodings is sound here. Output{Err: true} claims the write
// did not happen, so a later read that correctly returns the value is reported
// as an anomaly. Output{Err: true, Applied: true} claims it did, so a later read
// that correctly returns the old value is reported as an anomaly. Only one
// treatment asserts nothing: Porcupine's pending operation, Jepsen's :info.
//
// A pending operation is an invocation with no observed return, so the checker
// may place it anywhere after its call — including after every other operation
// in the history, which is indistinguishable from it never having happened.
// "Anywhere or nowhere" is exactly the knowledge the caller has.
//
// Porcupine's event API cannot express that by simply omitting the return: a
// call with no matching return is a dead end for the checker rather than a
// pending operation (pinned by TestKVModelRequiresAReturnForEveryCall in
// cmd/chaos). What it does support is time: the event API uses each event's
// index as its timestamp, so placing this operation's return after every other
// event in the history is the equivalent of the operation-API's Return = +∞ —
// the operation's interval extends past everything that was observed.
//
// The cost is search space. A pending operation overlaps every later operation
// on its key, so a history with many of them can push the checker into a
// timeout (UNKNOWN) instead of a verdict. That is the honest failure mode: the
// caller should keep the unknown class small by classifying precisely, not by
// guessing an outcome.
func (r *Recorder) EndUnknown(id int) {
	now := time.Now()
	out := Output{Err: true, Applied: true, Deferred: true}
	r.mu.Lock()
	r.deferredReturns = append(r.deferredReturns, porcupine.Event{
		Kind: porcupine.ReturnEvent,
		// Applied so the write is a state transition wherever it lands;
		// Deferred so a failing history says "pending" rather than implying the
		// store confirmed it.
		Value: out,
		Id:    id,
	})
	m := r.metaFor(id)
	m.output = out
	m.returned = true
	m.ret = now
	r.mu.Unlock()
}

// history returns the full event list to check: everything recorded, followed by
// the returns of pending operations.
func (r *Recorder) history() []porcupine.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := make([]porcupine.Event, 0, len(r.events)+len(r.deferredReturns))
	h = append(h, r.events...)
	h = append(h, r.deferredReturns...)
	return h
}

// Check verifies that the recorded history is linearizable.
// Returns true if linearizable. Blocks until the check completes.
func (r *Recorder) Check() bool {
	return porcupine.CheckEvents(KVModel, r.history())
}

// CheckTimeout verifies linearizability with a time limit. Returns (true, false)
// if linearizable, (false, false) if not, or (false, true) on timeout.
func (r *Recorder) CheckTimeout(d time.Duration) (ok bool, timedOut bool) {
	res := porcupine.CheckEventsTimeout(KVModel, r.history(), d)
	switch res {
	case porcupine.Ok:
		return true, false
	case porcupine.Illegal:
		return false, false
	default: // Unknown (timeout)
		return false, true
	}
}

// Len returns the number of events in the history that would be checked — two
// per operation, counting the synthesized return of a pending one.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events) + len(r.deferredReturns)
}

// Pending returns the number of operations recorded through EndUnknown.
func (r *Recorder) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deferredReturns)
}
