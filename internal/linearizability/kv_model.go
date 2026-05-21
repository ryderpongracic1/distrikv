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
	Err   bool   // true → the operation returned an error; result is unknown
}

// KVModel is a Porcupine model for a multi-key KV store that operates on
// string values. State is map[string]string; an absent key means no value.
//
// Failed operations (Output.Err == true) are treated as no-ops: the state
// is not modified. This is the correct assumption for operations that return
// before the server has a chance to process them (network timeout, etc.).
// For in-process LSM tests it is exact: a failed Put never reaches the
// memtable.
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

		switch in.Op {
		case "put":
			if out.Err {
				return true, s // failed: conservatively model as no-op
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
			if out.Err {
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
	nextID int64 // atomic for Begin; protected by mu for append
}

// Begin records the invocation of an operation and returns a unique call ID.
// The caller must call End(id, out) when the operation returns.
func (r *Recorder) Begin(inp Input) int {
	id := int(atomic.AddInt64(&r.nextID, 1)) - 1
	r.mu.Lock()
	r.events = append(r.events, porcupine.Event{
		Kind:  porcupine.CallEvent,
		Value: inp,
		Id:    id,
	})
	r.mu.Unlock()
	return id
}

// End records the return of an operation previously started with Begin.
func (r *Recorder) End(id int, out Output) {
	r.mu.Lock()
	r.events = append(r.events, porcupine.Event{
		Kind:  porcupine.ReturnEvent,
		Value: out,
		Id:    id,
	})
	r.mu.Unlock()
}

// Check verifies that the recorded history is linearizable.
// Returns true if linearizable. Blocks until the check completes.
func (r *Recorder) Check() bool {
	r.mu.Lock()
	events := make([]porcupine.Event, len(r.events))
	copy(events, r.events)
	r.mu.Unlock()
	return porcupine.CheckEvents(KVModel, events)
}

// CheckTimeout verifies linearizability with a time limit. Returns (true, nil)
// if linearizable, (false, nil) if not, or (false, err) on timeout.
func (r *Recorder) CheckTimeout(d time.Duration) (ok bool, timedOut bool) {
	r.mu.Lock()
	events := make([]porcupine.Event, len(r.events))
	copy(events, r.events)
	r.mu.Unlock()
	res := porcupine.CheckEventsTimeout(KVModel, events, d)
	switch res {
	case porcupine.Ok:
		return true, false
	case porcupine.Illegal:
		return false, false
	default: // Unknown (timeout)
		return false, true
	}
}

// Len returns the number of recorded events (each op produces 2 events).
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}
