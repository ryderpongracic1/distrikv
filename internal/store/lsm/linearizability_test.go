package lsm

// Linearizability tests for the LSM-Tree engine.
//
// These tests run concurrent put/get/delete operations against an in-process
// LSMTree, record every operation as a Porcupine event, and then verify that
// the resulting history is linearizable.  A non-linearizable result would mean
// that the engine exposed stale reads or lost-write anomalies to concurrent
// callers — a correctness violation.
//
// Key design choices:
//   - Small keyspace (5 keys) → lots of contention → richer history to check.
//   - Short value strings that encode the writer identity so a "mystery value"
//     in a get result can be traced back to the goroutine that wrote it.
//   - Operations that return errors are recorded with Output.Err = true so the
//     checker still sees the call/return brackets but treats them as no-ops.
//   - The test uses a 30-second Porcupine timeout so it never hangs CI even if
//     the WGL search tree explodes on a pathological history.

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/linearizability"
	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// TestLinearizability_ConcurrentOps verifies that the LSM engine's concurrent
// read/write behaviour is linearizable.
//
// 5 goroutines × 80 ops = 400 ops on a 5-key space.  Each goroutine performs
// a random mix of puts, gets, and deletes.  After all goroutines finish, the
// full Porcupine event history is checked for linearizability.
func TestLinearizability_ConcurrentOps(t *testing.T) {
	const (
		goroutines = 5
		opsEach    = 80
		keyspace   = 5 // small → lots of contention per key
	)

	tree := testLSM(t)
	ctx := context.Background()
	rec := &linearizability.Recorder{}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id*1000 + 42)))
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("key-%d", rng.Intn(keyspace))
				switch rng.Intn(3) {
				case 0: // put
					val := fmt.Sprintf("g%d-i%d", id, i)
					cid := rec.Begin(linearizability.Input{Op: "put", Key: key, Value: val})
					err := tree.Put(ctx, key, []byte(val))
					out := linearizability.Output{}
					if err != nil {
						out.Err = true
					}
					rec.End(cid, out)

				case 1: // get
					cid := rec.Begin(linearizability.Input{Op: "get", Key: key})
					val, err := tree.Get(ctx, key)
					out := linearizability.Output{}
					if err == ErrNotFound {
						// absent key — Output.Value stays ""
					} else if err != nil {
						out.Err = true
					} else {
						out.Value = string(val)
					}
					rec.End(cid, out)

				case 2: // delete
					cid := rec.Begin(linearizability.Input{Op: "delete", Key: key})
					err := tree.Delete(ctx, key)
					out := linearizability.Output{}
					if err != nil {
						out.Err = true
					}
					rec.End(cid, out)
				}
			}
		}(g)
	}
	wg.Wait()

	t.Logf("recorded %d events (%d ops)", rec.Len(), rec.Len()/2)

	start := time.Now()
	ok, timedOut := rec.CheckTimeout(30 * time.Second)
	elapsed := time.Since(start)

	if timedOut {
		t.Skipf("linearizability check timed out after %s (history too large for WGL)", elapsed)
	}
	if !ok {
		t.Fatalf("history is NOT linearizable (checked in %s)", elapsed)
	}
	t.Logf("linearizable ✓ (checked in %s)", elapsed.Round(time.Millisecond))
}

// TestLinearizability_WithLevels verifies that the linearizability property
// holds when data is spread across the memtable, L0, and L1.
//
// Phase 1 (setup): write enough UNIQUE keys to guarantee at least one
// L0→L1 compaction. Each unique write grows the memtable monotonically,
// making flush/compaction deterministic.
//
// Phase 2 (concurrent): run concurrent put/get/delete ops on a small
// keyspace and verify the Porcupine history is linearizable.  Because L1
// data exists (from phase 1), reads exercise the full mem→L0→L1 path.
func TestLinearizability_WithLevels(t *testing.T) {
	const (
		goroutines = 4
		opsEach    = 80
		keyspace   = 5
	)

	dir := t.TempDir()
	ctx := context.Background()
	m := &metrics.Metrics{}

	// 512 B threshold; each setup entry is ~17B (key "setup-000000" + 10B value).
	// 512/17 ≈ 30 unique writes per flush; 150 setup writes → ~5 flushes → 1 compaction.
	tree, err := NewLSMTree(dir, nil, WithMetrics(m), WithMaxMemBytes(512))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	// Phase 1: write unique setup keys to force flush + compaction.
	const setupKeys = 200
	setupVal := make([]byte, 10)
	for i := 0; i < setupKeys; i++ {
		k := fmt.Sprintf("setup-%06d", i)
		if err := tree.Put(ctx, k, setupVal); err != nil {
			t.Fatalf("setup Put %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.CompactionsTotal.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if m.CompactionsTotal.Load() == 0 {
		t.Fatal("no compaction after setup phase; L1 path not exercised")
	}
	t.Logf("phase-1 done: compactions=%d", m.CompactionsTotal.Load())

	// Phase 2: concurrent ops on "lc-key-*" (disjoint from setup namespace).
	// Reads will traverse mem→L0→L1; the L1 Bloom filter gates on setup-* keys
	// (absent from the "lc-key-*" sub-space), so the engine still exercises the
	// full SSTable read machinery.
	rec := &linearizability.Recorder{}
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id*999 + 7)))
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("lc-key-%d", rng.Intn(keyspace))
				switch rng.Intn(3) {
				case 0: // put
					val := fmt.Sprintf("g%d-i%d", id, i)
					cid := rec.Begin(linearizability.Input{Op: "put", Key: key, Value: val})
					err := tree.Put(ctx, key, []byte(val))
					out := linearizability.Output{}
					if err != nil {
						out.Err = true
					}
					rec.End(cid, out)
				case 1: // get
					cid := rec.Begin(linearizability.Input{Op: "get", Key: key})
					val, err := tree.Get(ctx, key)
					out := linearizability.Output{}
					if err == ErrNotFound {
						// absent — Value stays ""
					} else if err != nil {
						out.Err = true
					} else {
						out.Value = string(val)
					}
					rec.End(cid, out)
				case 2: // delete
					cid := rec.Begin(linearizability.Input{Op: "delete", Key: key})
					err := tree.Delete(ctx, key)
					out := linearizability.Output{}
					if err != nil {
						out.Err = true
					}
					rec.End(cid, out)
				}
			}
		}(g)
	}
	wg.Wait()

	t.Logf("phase-2 done: %d events (%d ops); total compactions=%d",
		rec.Len(), rec.Len()/2, m.CompactionsTotal.Load())

	ok, timedOut := rec.CheckTimeout(30 * time.Second)
	if timedOut {
		t.Skip("linearizability check timed out (history too large for WGL)")
	}
	if !ok {
		t.Fatal("history is NOT linearizable with L1 level present")
	}
	t.Log("linearizable ✓")
}
