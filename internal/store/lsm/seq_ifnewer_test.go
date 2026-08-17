package lsm

// Apply-if-newer, covered from the engine's public surface: the comparison, the
// tombstone ordering, the compatibility path for an unsequenced write, and the
// sequence Put/Delete now return.
//
// seq_apply_if_newer_test.go covers the same mechanism from the other side — the
// SSTable tiers of the lookup, and the sequence surviving a graceful reopen and a
// crash. The two files were written independently against the same contract and
// are both kept: the overlap is on the contract, and the parts that do not
// overlap are the parts that caught bugs.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"
)

// newIfNewerTree opens a fresh engine in a temp dir for the apply-if-newer tests.
func newIfNewerTree(t *testing.T) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// getOrEmpty returns the stored value for key, or "" when the key is absent or
// tombstoned. Any other error fails the test.
func getOrEmpty(t *testing.T, l *LSMTree, key string) string {
	t.Helper()
	v, err := l.Get(context.Background(), key)
	switch {
	case errors.Is(err, ErrNotFound):
		return ""
	case err != nil:
		t.Fatalf("Get %q: %v", key, err)
	}
	return string(v)
}

// TestPutIfNewer is the core apply-if-newer contract: a write below or at the
// stored sequence is skipped, a write above it is applied, and the stored value
// afterwards is the one belonging to the highest sequence — never the one that
// happened to arrive last.
func TestPutIfNewer(t *testing.T) {
	l := newIfNewerTree(t)
	ctx := context.Background()

	// Pad first: on a fresh engine the first write is sequence 1, and seq-1
	// would then be 0 — which means "unsequenced, apply unconditionally" and
	// would exercise the compatibility path instead of the comparison.
	for i := 0; i < 4; i++ {
		if _, err := l.Put(ctx, "pad", []byte("v")); err != nil {
			t.Fatalf("pad Put: %v", err)
		}
	}

	seq, err := l.Put(ctx, "k", []byte("local"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if seq == 0 {
		t.Fatal("Put returned sequence 0; the engine assigns sequences from 1")
	}

	// Older: refused, and the stored value is untouched.
	applied, err := l.PutIfNewer(ctx, "k", []byte("older"), seq-1)
	if err != nil {
		t.Fatalf("PutIfNewer(older): %v", err)
	}
	if applied {
		t.Errorf("PutIfNewer at seq %d = applied, want skipped (stored seq is %d)", seq-1, seq)
	}
	if got := getOrEmpty(t, l, "k"); got != "local" {
		t.Errorf("after older write: value = %q, want %q", got, "local")
	}

	// Equal: also refused. Equal sequences are the same write, so re-applying it
	// is at best redundant and at worst a rollback of a same-sequence retry.
	applied, err = l.PutIfNewer(ctx, "k", []byte("same-seq"), seq)
	if err != nil {
		t.Fatalf("PutIfNewer(equal): %v", err)
	}
	if applied {
		t.Errorf("PutIfNewer at the stored seq %d = applied, want skipped", seq)
	}
	if got := getOrEmpty(t, l, "k"); got != "local" {
		t.Errorf("after equal-seq write: value = %q, want %q", got, "local")
	}

	// Newer: applied, and it wins.
	applied, err = l.PutIfNewer(ctx, "k", []byte("newer"), seq+10)
	if err != nil {
		t.Fatalf("PutIfNewer(newer): %v", err)
	}
	if !applied {
		t.Errorf("PutIfNewer at seq %d = skipped, want applied (stored seq is %d)", seq+10, seq)
	}
	if got := getOrEmpty(t, l, "k"); got != "newer" {
		t.Errorf("after newer write: value = %q, want %q", got, "newer")
	}

	// And the older write still loses after the newer one landed — this is the
	// inversion the whole mechanism exists for: arrival order does not decide.
	applied, err = l.PutIfNewer(ctx, "k", []byte("older-again"), seq+1)
	if err != nil {
		t.Fatalf("PutIfNewer(older after newer): %v", err)
	}
	if applied {
		t.Errorf("PutIfNewer at seq %d = applied, want skipped (stored seq is %d)", seq+1, seq+10)
	}
	if got := getOrEmpty(t, l, "k"); got != "newer" {
		t.Errorf("after out-of-order write: value = %q, want %q", got, "newer")
	}
}

// TestDeleteIfNewer covers the tombstone side, including the case a
// tombstone-blind comparison would get wrong: an older value must not be able to
// overwrite a newer delete.
func TestDeleteIfNewer(t *testing.T) {
	l := newIfNewerTree(t)
	ctx := context.Background()

	// Pad so that seq-1 below is a real sequence rather than 0, which means
	// "unsequenced" (see TestPutIfNewer).
	for i := 0; i < 4; i++ {
		if _, err := l.Put(ctx, "pad", []byte("v")); err != nil {
			t.Fatalf("pad Put: %v", err)
		}
	}

	seq, err := l.Put(ctx, "k", []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Older tombstone: refused, value survives.
	applied, err := l.DeleteIfNewer(ctx, "k", seq-1)
	if err != nil {
		t.Fatalf("DeleteIfNewer(older): %v", err)
	}
	if applied {
		t.Errorf("DeleteIfNewer at seq %d = applied, want skipped", seq-1)
	}
	if got := getOrEmpty(t, l, "k"); got != "v" {
		t.Errorf("after older tombstone: value = %q, want %q", got, "v")
	}

	// Newer tombstone: applied, key reads as absent.
	applied, err = l.DeleteIfNewer(ctx, "k", seq+5)
	if err != nil {
		t.Fatalf("DeleteIfNewer(newer): %v", err)
	}
	if !applied {
		t.Errorf("DeleteIfNewer at seq %d = skipped, want applied", seq+5)
	}
	if got := getOrEmpty(t, l, "k"); got != "" {
		t.Errorf("after newer tombstone: value = %q, want it gone", got)
	}

	// An older value arriving after the delete must not resurrect the key. The
	// tombstone is a version, so the comparison has to see its sequence.
	applied, err = l.PutIfNewer(ctx, "k", []byte("resurrected"), seq+1)
	if err != nil {
		t.Fatalf("PutIfNewer(older than tombstone): %v", err)
	}
	if applied {
		t.Errorf("PutIfNewer at seq %d = applied, want skipped (tombstone is at %d)", seq+1, seq+5)
	}
	if got := getOrEmpty(t, l, "k"); got != "" {
		t.Errorf("key was resurrected by an older write: value = %q", got)
	}

	// A newer value after the delete is a legitimate re-write and must land.
	applied, err = l.PutIfNewer(ctx, "k", []byte("rewritten"), seq+9)
	if err != nil {
		t.Fatalf("PutIfNewer(newer than tombstone): %v", err)
	}
	if !applied {
		t.Errorf("PutIfNewer at seq %d = skipped, want applied", seq+9)
	}
	if got := getOrEmpty(t, l, "k"); got != "rewritten" {
		t.Errorf("after re-write: value = %q, want %q", got, "rewritten")
	}
}

// TestPutIfNewerSeqZeroAppliesUnconditionally pins the compatibility escape
// hatch: a sender that assigns no sequence still replicates, by falling back to
// the pre-sequencing behaviour of applying in arrival order.
func TestPutIfNewerSeqZeroAppliesUnconditionally(t *testing.T) {
	l := newIfNewerTree(t)
	ctx := context.Background()

	if _, err := l.Put(ctx, "k", []byte("high-seq")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	applied, err := l.PutIfNewer(ctx, "k", []byte("unsequenced"), 0)
	if err != nil {
		t.Fatalf("PutIfNewer(seq=0): %v", err)
	}
	if !applied {
		t.Error("PutIfNewer(seq=0) = skipped, want applied unconditionally")
	}
	if got := getOrEmpty(t, l, "k"); got != "unsequenced" {
		t.Errorf("value = %q, want %q", got, "unsequenced")
	}

	applied, err = l.DeleteIfNewer(ctx, "k", 0)
	if err != nil {
		t.Fatalf("DeleteIfNewer(seq=0): %v", err)
	}
	if !applied {
		t.Error("DeleteIfNewer(seq=0) = skipped, want applied unconditionally")
	}
	if got := getOrEmpty(t, l, "k"); got != "" {
		t.Errorf("after unconditional delete: value = %q, want it gone", got)
	}
}

// TestPutIfNewerAdvancesSeqGenerator pins the invariant that keeps compaction
// honest: after a foreign sequence is stored, every sequence the engine mints is
// above it. Without the bump a later local write would carry a lower sequence
// than a value already on disk, and compaction's keep-the-higher merge would
// resolve "newer" backwards.
func TestPutIfNewerAdvancesSeqGenerator(t *testing.T) {
	l := newIfNewerTree(t)
	ctx := context.Background()

	const foreign = 5000
	if _, err := l.PutIfNewer(ctx, "replicated", []byte("v"), foreign); err != nil {
		t.Fatalf("PutIfNewer: %v", err)
	}

	seq, err := l.Put(ctx, "local", []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if seq <= foreign {
		t.Errorf("local write got seq %d, want > %d (the foreign sequence already stored)", seq, foreign)
	}

	// A skipped write must not consume or advance anything either.
	before := l.seqNum.Load()
	applied, err := l.PutIfNewer(ctx, "replicated", []byte("older"), foreign-1)
	if err != nil {
		t.Fatalf("PutIfNewer(older): %v", err)
	}
	if applied {
		t.Error("PutIfNewer below the stored sequence was applied")
	}
	if after := l.seqNum.Load(); after != before {
		t.Errorf("skipped write moved the generator: %d → %d", before, after)
	}
}

// TestPutReturnsMonotonicSeq pins the sequence Put and Delete now surface: it is
// the version a ring-primary puts on the replication wire, so it has to increase
// on every write, deletes included.
func TestPutReturnsMonotonicSeq(t *testing.T) {
	l := newIfNewerTree(t)
	ctx := context.Background()

	var last uint64
	for i := 0; i < 8; i++ {
		seq, err := l.Put(ctx, "k", []byte("v"))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if seq <= last {
			t.Fatalf("Put %d: seq = %d, want > %d", i, seq, last)
		}
		last = seq
	}

	seq, err := l.Delete(ctx, "k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if seq <= last {
		t.Fatalf("Delete: seq = %d, want > %d", seq, last)
	}
}

// TestGetSeqNum covers the memtable lookup the apply-if-newer check is built on:
// it reports the sequence of a resident entry, reports tombstones like any other
// version, and reports absence for a key it does not hold.
func TestGetSeqNum(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal-0001.log")
	w, err := storewal.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	var gen atomic.Uint64
	mem := NewMemtable(w, walPath, &gen, 4<<20)

	if _, ok := mem.GetSeqNum("absent"); ok {
		t.Error("GetSeqNum on an absent key reported a sequence")
	}

	putSeq, _, err := mem.Put("k", []byte("v"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := mem.GetSeqNum("k")
	if !ok {
		t.Fatal("GetSeqNum after Put reported absence")
	}
	if got != putSeq {
		t.Errorf("GetSeqNum = %d, want %d (the sequence Put assigned)", got, putSeq)
	}

	// An overwrite replaces the version, so the lookup must follow it.
	overwriteSeq, _, err := mem.Put("k", []byte("v2"))
	if err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	if got, _ := mem.GetSeqNum("k"); got != overwriteSeq {
		t.Errorf("GetSeqNum after overwrite = %d, want %d", got, overwriteSeq)
	}

	// A tombstone is a version too: reporting absence here is what would let an
	// older value overwrite a newer delete.
	delSeq, _, err := mem.Delete("k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, ok = mem.GetSeqNum("k")
	if !ok {
		t.Fatal("GetSeqNum on a tombstoned key reported absence; a delete is a version")
	}
	if got != delSeq {
		t.Errorf("GetSeqNum after Delete = %d, want %d", got, delSeq)
	}

	// An externally sequenced write stores exactly the sequence it was given,
	// and carries the shared generator up to it so that this node's own later
	// writes still sort above everything it holds. (Thread A's version of this
	// test asserted the opposite — that PutWithSeq leaves the generator alone —
	// because it raised the generator one layer up, in LSMTree. The merged
	// engine raises it here, at the point the sequence is stamped, so that every
	// caller of PutWithSeq gets the invariant, WAL replay included.)
	if _, err := mem.PutWithSeq("ext", []byte("v"), 9999); err != nil {
		t.Fatalf("PutWithSeq: %v", err)
	}
	if got, _ := mem.GetSeqNum("ext"); got != 9999 {
		t.Errorf("GetSeqNum after PutWithSeq = %d, want 9999", got)
	}
	if genAfter := gen.Load(); genAfter != 9999 {
		t.Errorf("generator after PutWithSeq = %d, want it carried up to 9999", genAfter)
	}
}
