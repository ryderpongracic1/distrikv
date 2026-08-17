package lsm

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
)

// apply-if-newer is the engine half of per-key ordering: a replica stores the
// sequence its ring-primary assigned, and refuses a write that is not newer than
// what it already holds. These tests pin the comparison itself — the arrival
// order it defends against is covered at the node level.

func seqTree(t *testing.T, dir string) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(dir, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return l
}

func mustGet(t *testing.T, l *LSMTree, key string) string {
	t.Helper()
	v, err := l.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return string(v)
}

func mustBeAbsent(t *testing.T, l *LSMTree, key string) {
	t.Helper()
	if _, err := l.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get %q = %v, want ErrNotFound", key, err)
	}
}

// TestPutIfNewerComparesSequences covers the three orderings against a stored
// version: strictly newer applies, equal and older do not.
//
// Equality has to lose, not win. Two writes never share a sequence on one
// primary, so an equal sequence means the engine already holds this very write —
// re-applying it is at best wasted work and at worst reinstates a value a newer
// write has since replaced.
func TestPutIfNewerComparesSequences(t *testing.T) {
	l := seqTree(t, t.TempDir())
	defer l.Close()
	ctx := context.Background()
	const key = "k"

	applied, err := l.PutIfNewer(ctx, key, []byte("at-10"), 10)
	if err != nil || !applied {
		t.Fatalf("first write: applied=%v err=%v", applied, err)
	}

	if applied, err := l.PutIfNewer(ctx, key, []byte("at-9"), 9); err != nil || applied {
		t.Errorf("an older write was applied: applied=%v err=%v", applied, err)
	}
	if applied, err := l.PutIfNewer(ctx, key, []byte("at-10-again"), 10); err != nil || applied {
		t.Errorf("an equal-sequence write was applied: applied=%v err=%v", applied, err)
	}
	if mustGet(t, l, key) != "at-10" {
		t.Errorf("value = %q, want at-10", mustGet(t, l, key))
	}

	if applied, err := l.PutIfNewer(ctx, key, []byte("at-11"), 11); err != nil || !applied {
		t.Fatalf("a newer write was refused: applied=%v err=%v", applied, err)
	}
	if mustGet(t, l, key) != "at-11" {
		t.Errorf("value = %q, want at-11", mustGet(t, l, key))
	}
}

// TestPutIfNewerStampsTheSuppliedSequence pins that the write is stored under the
// sequence the caller gave it, not one drawn from this engine's counter.
//
// That is the whole point: the stored number has to be comparable with the next
// number the same primary sends. A locally-drawn sequence would be a number from
// a different counter, and the comparison above would be meaningless.
func TestPutIfNewerStampsTheSuppliedSequence(t *testing.T) {
	l := seqTree(t, t.TempDir())
	defer l.Close()
	ctx := context.Background()
	const key = "stamped"

	// A sequence far above anything this engine would have assigned on its own.
	const primarySeq = 1 << 40
	if applied, err := l.PutIfNewer(ctx, key, []byte("v"), primarySeq); err != nil || !applied {
		t.Fatalf("write: applied=%v err=%v", applied, err)
	}

	l.mu.Lock()
	stored, found, err := l.storedSeqLocked(key)
	l.mu.Unlock()
	if err != nil || !found {
		t.Fatalf("stored sequence lookup: found=%v err=%v", found, err)
	}
	if stored != primarySeq {
		t.Errorf("stored sequence = %d, want %d (the primary's, not a local one)", stored, primarySeq)
	}

	// The local counter must have been carried above it, or this engine's own next
	// write would sort below what it already holds — the invariant compaction uses
	// to resolve duplicate keys.
	if got := l.seqNum.Load(); got < primarySeq {
		t.Errorf("local counter = %d, want >= %d", got, primarySeq)
	}
}

// TestDeleteIfNewerOrdersTombstones pins that a tombstone is ordered like a
// value, in both directions.
func TestDeleteIfNewerOrdersTombstones(t *testing.T) {
	ctx := context.Background()

	t.Run("newer delete wins over the stored value", func(t *testing.T) {
		l := seqTree(t, t.TempDir())
		defer l.Close()
		if _, err := l.PutIfNewer(ctx, "k", []byte("v"), 5); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if applied, err := l.DeleteIfNewer(ctx, "k", 6); err != nil || !applied {
			t.Fatalf("newer delete: applied=%v err=%v", applied, err)
		}
		mustBeAbsent(t, l, "k")
	})

	t.Run("older delete loses to the stored value", func(t *testing.T) {
		l := seqTree(t, t.TempDir())
		defer l.Close()
		if _, err := l.PutIfNewer(ctx, "k", []byte("v"), 5); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if applied, err := l.DeleteIfNewer(ctx, "k", 4); err != nil || applied {
			t.Errorf("an older delete was applied: applied=%v err=%v", applied, err)
		}
		if mustGet(t, l, "k") != "v" {
			t.Error("an older delete removed a newer value")
		}
	})

	t.Run("a newer put resurrects over an older tombstone", func(t *testing.T) {
		l := seqTree(t, t.TempDir())
		defer l.Close()
		if _, err := l.DeleteIfNewer(ctx, "k", 5); err != nil {
			t.Fatalf("seed tombstone: %v", err)
		}
		if applied, err := l.PutIfNewer(ctx, "k", []byte("back"), 6); err != nil || !applied {
			t.Fatalf("newer put: applied=%v err=%v", applied, err)
		}
		if mustGet(t, l, "k") != "back" {
			t.Error("a newer put did not win over an older tombstone")
		}
	})
}

// TestPutIfNewerComparesAgainstFlushedVersions is the case a memtable-only
// comparison gets wrong.
//
// Once a key has been flushed it is no longer resident in memory, and an engine
// that only consulted its memtables would find nothing to compare against and
// apply the write unconditionally — reinstating the inversion for every key that
// has aged out of the active memtable, which over a run is most of them.
func TestPutIfNewerComparesAgainstFlushedVersions(t *testing.T) {
	l := seqTree(t, t.TempDir())
	defer l.Close()
	ctx := context.Background()
	const key = "flushed"

	if applied, err := l.PutIfNewer(ctx, key, []byte("at-100"), 100); err != nil || !applied {
		t.Fatalf("seed: applied=%v err=%v", applied, err)
	}
	// Force the key out of the active memtable and onto disk.
	l.mu.Lock()
	err := l.rotateMemtable()
	l.mu.Unlock()
	if err != nil {
		t.Fatalf("rotate memtable: %v", err)
	}
	waitForIdleCompaction(t, l)

	if _, ok := l.mem.Get(key); ok {
		t.Fatal("precondition: the key is still in the active memtable, so this test would not exercise the SSTable path")
	}

	if applied, err := l.PutIfNewer(ctx, key, []byte("at-99"), 99); err != nil || applied {
		t.Errorf("an older write was applied over a flushed version: applied=%v err=%v", applied, err)
	}
	if mustGet(t, l, key) != "at-100" {
		t.Errorf("value = %q, want at-100", mustGet(t, l, key))
	}
	if applied, err := l.PutIfNewer(ctx, key, []byte("at-101"), 101); err != nil || !applied {
		t.Errorf("a newer write was refused over a flushed version: applied=%v err=%v", applied, err)
	}
}

// TestIfNewerWithoutASequenceAppliesUnconditionally pins the compatibility path:
// 0 means the sender did not supply an ordering, which must behave as it did
// before sequences existed rather than as an ordering below every real write.
func TestIfNewerWithoutASequenceAppliesUnconditionally(t *testing.T) {
	l := seqTree(t, t.TempDir())
	defer l.Close()
	ctx := context.Background()

	if _, err := l.PutIfNewer(ctx, "k", []byte("sequenced"), 900); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if applied, err := l.PutIfNewer(ctx, "k", []byte("unsequenced"), 0); err != nil || !applied {
		t.Fatalf("an unsequenced write was refused: applied=%v err=%v", applied, err)
	}
	if mustGet(t, l, "k") != "unsequenced" {
		t.Error("an unsequenced write did not apply")
	}
	if applied, err := l.DeleteIfNewer(ctx, "k", 0); err != nil || !applied {
		t.Fatalf("an unsequenced delete was refused: applied=%v err=%v", applied, err)
	}
	mustBeAbsent(t, l, "k")
}

// TestStoredSequenceSurvivesReopen is the durability half of the comparison
// across a *graceful* close, where the sequence comes back from the SSTable the
// memtable was flushed into.
func TestStoredSequenceSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const key = "durable"
	const primarySeq = 4242

	l := seqTree(t, dir)
	if applied, err := l.PutIfNewer(ctx, key, []byte("v"), primarySeq); err != nil || !applied {
		t.Fatalf("write: applied=%v err=%v", applied, err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	l2 := seqTree(t, dir)
	defer l2.Close()

	assertStoredSeq(t, l2, key, primarySeq)

	if applied, err := l2.PutIfNewer(ctx, key, []byte("older"), primarySeq-1); err != nil || applied {
		t.Errorf("an older write was applied after reopen: applied=%v err=%v", applied, err)
	}
	if applied, err := l2.PutIfNewer(ctx, key, []byte("newer"), primarySeq+1); err != nil || !applied {
		t.Errorf("a newer write was refused after reopen: applied=%v err=%v", applied, err)
	}
	if mustGet(t, l2, key) != "newer" {
		t.Errorf("value = %q, want newer", mustGet(t, l2, key))
	}
}

// TestStoredSequenceSurvivesCrashRecovery is the test that makes the sequence
// worth persisting, and the one a graceful reopen cannot stand in for.
//
// A graceful close flushes the memtable, so the sequence comes back from an
// SSTable, which has recorded it since long before this change. Recovery from the
// WAL is the path that used to lose it: replay assigned every recovered entry a
// fresh sequence from this node's counter, and because the counter is seeded above
// every sequence the node has ever stored, the primary's next write would look
// OLDER than what the replica holds and be discarded — permanently, and silently.
//
// A replica crashes (or is SIGKILLed by the nemesis) with unflushed writes on
// every chaos run, so this is the ordinary case.
func TestStoredSequenceSurvivesCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const key = "crash-durable"
	const primarySeq = 4242

	// A memtable large enough that nothing flushes: the write exists only in the
	// WAL when the crash happens, so recovery has to read the sequence back.
	l, err := NewLSMTree(dir, nil, WithMaxMemBytes(64<<20))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	if applied, err := l.PutIfNewer(ctx, key, []byte("v"), primarySeq); err != nil || !applied {
		t.Fatalf("write: applied=%v err=%v", applied, err)
	}
	crashEngine(t, l)

	l2 := seqTree(t, dir)
	defer l2.Close()

	if got := mustGet(t, l2, key); got != "v" {
		t.Fatalf("precondition: the write did not survive recovery at all (got %q)", got)
	}
	assertStoredSeq(t, l2, key, primarySeq)

	if applied, err := l2.PutIfNewer(ctx, key, []byte("older"), primarySeq-1); err != nil || applied {
		t.Errorf("an older write was applied after recovery: applied=%v err=%v", applied, err)
	}
	if applied, err := l2.PutIfNewer(ctx, key, []byte("newer"), primarySeq+1); err != nil || !applied {
		t.Errorf("a newer write was refused after recovery: applied=%v err=%v", applied, err)
	}
	if mustGet(t, l2, key) != "newer" {
		t.Errorf("value = %q, want newer", mustGet(t, l2, key))
	}
}

// assertStoredSeq checks the sequence the engine holds for key.
func assertStoredSeq(t *testing.T, l *LSMTree, key string, want uint64) {
	t.Helper()
	l.mu.Lock()
	stored, found, err := l.storedSeqLocked(key)
	l.mu.Unlock()
	if err != nil || !found {
		t.Fatalf("stored sequence lookup for %q: found=%v err=%v", key, found, err)
	}
	if stored != want {
		t.Fatalf("stored sequence for %q = %d, want %d — the ordering was not preserved, "+
			"so the comparison is now against a local counter", key, stored, want)
	}
}

// crashEngine abandons the engine the way a power loss or SIGKILL does: it stops
// the background goroutines and closes the file handles WITHOUT the flush that
// Close performs, so the active WAL survives on disk with unflushed writes in it.
// That is the state recovery has to read the sequences back out of.
func crashEngine(t *testing.T, l *LSMTree) {
	t.Helper()
	close(l.stopCh)
	l.wg.Wait()
	if l.mem != nil && l.mem.w != nil {
		_ = l.mem.w.Close()
	}
	l.mu.RLock()
	for _, r := range l.l0 {
		_ = r.Close()
	}
	for _, r := range l.l1 {
		_ = r.Close()
	}
	l.mu.RUnlock()
}
