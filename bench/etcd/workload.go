package main

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// opKind tags one of the three operations dispatched by the bench. Values and
// ordering mirror cmd/bench so the two harnesses report the same op names.
type opKind int

const (
	opPut opKind = iota
	opGet
	opDelete

	numOpKinds = 3
)

func (k opKind) String() string {
	switch k {
	case opPut:
		return "put"
	case opGet:
		return "get"
	case opDelete:
		return "delete"
	}
	return "?"
}

// workload owns key/value generation and op selection.
//
// This is a deliberate line-for-line port of cmd/bench/workload.go: the same
// Zipf parameters, the same key format, the same value bytes, and the same
// cumulative-threshold mix selection. Any divergence here would silently make
// the etcd numbers incomparable to distrikv's, which is the entire point of the
// harness — so changes to one file must be mirrored in the other.
//
// One instance is shared across all workers; all methods are safe for
// concurrent use.
type workload struct {
	keyspace int
	keyDist  string
	value    []byte

	// Cumulative thresholds in [0, 100] for op selection.
	putThresh    int
	getThresh    int
	deleteThresh int

	// rngPool gives each worker a non-contended *rand.Rand. We can't share one
	// because math/rand's top-level functions take a global lock.
	rngPool sync.Pool

	// zipf state — only used if keyDist == "zipf".
	zipfMu  sync.Mutex
	zipf    *mathrand.Zipf
	zipfRng *mathrand.Rand
}

// parseMix parses "p:g:d" (e.g. "20:80:0") into cumulative thresholds.
func parseMix(s string) (put, get, del int, err error) {
	var p, g, d int
	if _, scanErr := fmt.Sscanf(s, "%d:%d:%d", &p, &g, &d); scanErr != nil {
		return 0, 0, 0, fmt.Errorf("invalid mix %q (want put:get:delete): %w", s, scanErr)
	}
	if p < 0 || g < 0 || d < 0 {
		return 0, 0, 0, errors.New("mix components must be non-negative")
	}
	if p+g+d == 0 {
		return 0, 0, 0, errors.New("mix components sum to zero")
	}
	return p, p + g, p + g + d, nil
}

func newWorkload(keyspace int, keyDist string, mix string, valueSize int) (*workload, error) {
	p, pg, pgd, err := parseMix(mix)
	if err != nil {
		return nil, err
	}
	if keyspace <= 0 {
		return nil, errors.New("keyspace must be positive")
	}
	if valueSize <= 0 {
		return nil, errors.New("valuesize must be positive")
	}

	// Printable ASCII, byte-identical to cmd/bench's value generator. etcd
	// carries the value as opaque bytes over gRPC while distrikv sends it inside
	// a JSON body, so the payload is the same size but distrikv additionally
	// pays the JSON framing — noted in docs/benchmarks.md rather than compensated
	// for.
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'a' + byte(i%26)
	}

	w := &workload{
		keyspace:     keyspace,
		keyDist:      keyDist,
		value:        value,
		putThresh:    p,
		getThresh:    pg,
		deleteThresh: pgd,
	}
	w.rngPool = sync.Pool{
		New: func() any {
			return mathrand.New(mathrand.NewSource(mathrand.Int63()))
		},
	}

	switch keyDist {
	case "uniform", "sequential":
		// no extra state
	case "zipf":
		w.zipfRng = mathrand.New(mathrand.NewSource(1))
		// s=1.1, v=1, imax=keyspace-1 — classic skew, ~80/20 hot-key access.
		w.zipf = mathrand.NewZipf(w.zipfRng, 1.1, 1.0, uint64(keyspace-1))
		if w.zipf == nil {
			return nil, fmt.Errorf("zipf init failed for keyspace=%d", keyspace)
		}
	default:
		return nil, fmt.Errorf("unknown keydist %q (uniform|zipf|sequential)", keyDist)
	}
	return w, nil
}

// nextKey returns the next key as a 16-char zero-padded decimal, matching
// cmd/bench byte for byte so both stores see the same key strings.
func (w *workload) nextKey(seq uint64) string {
	var n uint64
	switch w.keyDist {
	case "sequential":
		n = seq % uint64(w.keyspace)
	case "zipf":
		w.zipfMu.Lock()
		n = w.zipf.Uint64()
		w.zipfMu.Unlock()
	default: // uniform
		r := w.rngPool.Get().(*mathrand.Rand)
		n = uint64(r.Intn(w.keyspace))
		w.rngPool.Put(r)
	}
	return fmt.Sprintf("k%015d", n)
}

// nextOp picks an op based on the configured mix.
func (w *workload) nextOp() opKind {
	r := w.rngPool.Get().(*mathrand.Rand)
	pick := r.Intn(w.deleteThresh)
	w.rngPool.Put(r)
	switch {
	case pick < w.putThresh:
		return opPut
	case pick < w.getThresh:
		return opGet
	default:
		return opDelete
	}
}

// dispatch issues one op against etcd. It returns nil if the op completed
// successfully OR failed in a benign, workload-expected way.
//
// "Benign" needs a different shape here than in cmd/bench. distrikv's HTTP
// client surfaces a missing key as client.ErrNotFound, which cmd/bench
// swallows; etcd reports the same condition without an error — an empty Kvs
// slice on Get, Deleted==0 on Delete — so the equivalent tolerance is implicit.
// The observable outcome matches: a read of an unwritten key counts as a
// successful op in both harnesses.
func (w *workload) dispatch(ctx context.Context, cli *clientv3.Client, op opKind, key string) error {
	switch op {
	case opPut:
		_, err := cli.Put(ctx, key, string(w.value))
		return err
	case opGet:
		_, err := cli.Get(ctx, key)
		return err
	case opDelete:
		_, err := cli.Delete(ctx, key)
		return err
	}
	return fmt.Errorf("unknown op %v", op)
}
