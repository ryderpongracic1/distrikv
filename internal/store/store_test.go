package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// newTestStore opens a fresh LSM-backed store in a temp dir.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// ---------------------------------------------------------------------------
// Basic operations
// ---------------------------------------------------------------------------

func TestStorePutGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("alpha"))))

	v, err := s.Get(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, []byte("alpha"), v)
}

func TestStoreGetMissing(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Get(context.Background(), "no-such-key")
	assert.ErrorIs(t, err, ErrNotFound)
}

// TestStoreDeleteMissing pins the blind-delete semantics: deleting a key that
// does not exist succeeds. See store.Delete for why the previous
// Get-then-Delete existence check was removed.
func TestStoreDeleteMissing(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, discardSeq(s.Delete(context.Background(), "no-such-key")))

	// Idempotent: repeating the delete is still not an error.
	require.NoError(t, discardSeq(s.Delete(context.Background(), "no-such-key")))

	// The key is still absent afterwards.
	_, err := s.Get(context.Background(), "no-such-key")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStoreOverwrite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "k", []byte("v1"))))
	require.NoError(t, discardSeq(s.Put(ctx, "k", []byte("v2"))))

	v, err := s.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)
}

func TestStoreDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "del", []byte("gone"))))
	require.NoError(t, discardSeq(s.Delete(ctx, "del")))

	_, err := s.Get(ctx, "del")
	assert.ErrorIs(t, err, ErrNotFound)
}

// ---------------------------------------------------------------------------
// Persistence across open/close
// ---------------------------------------------------------------------------

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	func() {
		s, err := New(dir, nil)
		require.NoError(t, err)
		require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("alpha"))))
		require.NoError(t, discardSeq(s.Put(ctx, "b", []byte("bravo"))))
		require.NoError(t, discardSeq(s.Put(ctx, "c", []byte("charlie"))))
		require.NoError(t, discardSeq(s.Delete(ctx, "b")))
		require.NoError(t, s.Close())
	}()

	s2, err := New(dir, nil)
	require.NoError(t, err)
	defer s2.Close()

	v, err := s2.Get(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, []byte("alpha"), v)

	_, err = s2.Get(ctx, "b")
	assert.ErrorIs(t, err, ErrNotFound, "deleted key must be absent after reopen")

	v, err = s2.Get(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, []byte("charlie"), v)
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestStoreConcurrentPuts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			val := fmt.Sprintf("val-%d", i)
			require.NoError(t, discardSeq(s.Put(ctx, key, []byte(val))))
		}()
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("val-%d", i)
		v, err := s.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte(expected), v)
	}
}

// ---------------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------------

func TestStoreSnapshotRestore(t *testing.T) {
	src := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%d", i)
		require.NoError(t, discardSeq(src.Put(ctx, k, []byte(fmt.Sprintf("v%d", i)))))
	}

	snap, err := src.Snapshot(ctx)
	require.NoError(t, err)
	assert.Len(t, snap, 20)

	dst, err := New(t.TempDir(), nil)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.RestoreFromSnapshot(ctx, snap))

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%d", i)
		v, err := dst.Get(ctx, k)
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}
}

// ---------------------------------------------------------------------------
// Counts
// ---------------------------------------------------------------------------

func TestStoreCounts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("1"))))
	require.NoError(t, discardSeq(s.Put(ctx, "b", []byte("2"))))
	_, _ = s.Get(ctx, "a")
	_, _ = s.Get(ctx, "missing")
	_, _ = s.Delete(ctx, "b")

	puts, gets, dels, _ := s.Counts()
	assert.Equal(t, uint64(2), puts)
	assert.Equal(t, uint64(2), gets)
	assert.Equal(t, uint64(1), dels)
}

// ---------------------------------------------------------------------------
// Observability fields served by /status and /metrics
// ---------------------------------------------------------------------------

// TestStoreWALWrites pins that Counts reports the engine's real WAL append
// counter rather than a hardcoded zero: it moves on Put and on Delete, and
// reads do not touch it.
func TestStoreWALWrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, _, _, walWrites := s.Counts()
	require.Zero(t, walWrites, "no writes yet")

	require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("1"))))
	_, _, _, walWrites = s.Counts()
	assert.Equal(t, uint64(1), walWrites, "Put appends to the WAL")

	require.NoError(t, discardSeq(s.Delete(ctx, "a")))
	_, _, _, walWrites = s.Counts()
	assert.Equal(t, uint64(2), walWrites, "Delete appends a tombstone to the WAL")

	// Deletes are blind, so even a delete of an absent key is a real append.
	require.NoError(t, discardSeq(s.Delete(ctx, "never-existed")))
	_, _, _, walWrites = s.Counts()
	assert.Equal(t, uint64(3), walWrites)

	// Reads never append.
	_, _ = s.Get(ctx, "a")
	_, _ = s.Get(ctx, "missing")
	_, _, _, walWrites = s.Counts()
	assert.Equal(t, uint64(3), walWrites, "Get must not append to the WAL")
}

// TestStoreWALWritesMirroredToMetrics pins that a metrics-wired store also
// populates metrics.WALWrites, so /metrics reports the same number whether it
// reads the snapshot or the store counters.
func TestStoreWALWritesMirroredToMetrics(t *testing.T) {
	m := &metrics.Metrics{}
	s, err := NewWithMetrics(t.TempDir(), nil, m)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("1"))))
	require.NoError(t, discardSeq(s.Delete(ctx, "a")))

	_, _, _, walWrites := s.Counts()
	assert.Equal(t, uint64(2), walWrites)
	assert.Equal(t, walWrites, m.Snapshot()["wal_writes"],
		"metrics snapshot must agree with the store counter")
}

// TestStoreKeyCount pins the approximate live-key count served as
// /status key_count. See store.KeyCount and lsm.LSMTree.LiveKeys for the
// accuracy contract this locks in.
func TestStoreKeyCount(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	assert.Equal(t, 0, s.KeyCount(), "empty store")

	// Put of new keys: +1 each.
	require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("1"))))
	require.NoError(t, discardSeq(s.Put(ctx, "b", []byte("2"))))
	require.NoError(t, discardSeq(s.Put(ctx, "c", []byte("3"))))
	assert.Equal(t, 3, s.KeyCount())

	// Overwrite of a resident key: neutral.
	require.NoError(t, discardSeq(s.Put(ctx, "b", []byte("2b"))))
	assert.Equal(t, 3, s.KeyCount(), "overwrite must not inflate the count")

	// Delete of a live key: -1.
	require.NoError(t, discardSeq(s.Delete(ctx, "c")))
	assert.Equal(t, 2, s.KeyCount())

	// Re-deleting an already-tombstoned resident key: neutral.
	require.NoError(t, discardSeq(s.Delete(ctx, "c")))
	assert.Equal(t, 2, s.KeyCount(), "repeat delete must not double-decrement")

	// Re-creating a tombstoned key: +1 again.
	require.NoError(t, discardSeq(s.Put(ctx, "c", []byte("3b"))))
	assert.Equal(t, 3, s.KeyCount())
}

// TestStoreKeyCountDeleteNonexistentDrifts documents the accepted inaccuracy:
// a blind delete of a key that never existed cannot be distinguished from one
// that removed a live key without a read, so the count drifts down. The count
// is clamped at zero rather than being allowed to go negative.
func TestStoreKeyCountDeleteNonexistentDrifts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("1"))))
	require.NoError(t, discardSeq(s.Put(ctx, "b", []byte("2"))))
	require.Equal(t, 2, s.KeyCount())

	require.NoError(t, discardSeq(s.Delete(ctx, "never-existed")))
	assert.Equal(t, 1, s.KeyCount(),
		"documented drift: delete of an absent key decrements the approximation")

	// Drive the counter well past zero; the exposed value must clamp, not wrap.
	for i := 0; i < 10; i++ {
		require.NoError(t, discardSeq(s.Delete(ctx, fmt.Sprintf("ghost-%d", i))))
	}
	assert.Equal(t, 0, s.KeyCount(), "count is clamped at zero")
}

// TestStoreKeyCountSurvivesReopen pins that the count is rebuilt on open from
// the manifest record written at flush time plus WAL replay of writes that
// never reached an SSTable — no startup scan of the SSTables required.
func TestStoreKeyCountSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Round 1: writes small enough to stay in the memtable, so the reopened
	// count comes entirely from WAL replay.
	func() {
		s, err := New(dir, nil)
		require.NoError(t, err)
		require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("alpha"))))
		require.NoError(t, discardSeq(s.Put(ctx, "b", []byte("bravo"))))
		require.NoError(t, discardSeq(s.Put(ctx, "c", []byte("charlie"))))
		require.NoError(t, discardSeq(s.Put(ctx, "a", []byte("alpha2")))) // overwrite: neutral
		require.NoError(t, discardSeq(s.Delete(ctx, "b")))                // -1
		require.Equal(t, 2, s.KeyCount())
		require.NoError(t, s.Close()) // Close flushes and records the count
	}()

	s2, err := New(dir, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, s2.KeyCount(), "count rebuilt after reopen")

	// Round 2: keep writing on top of the recovered count, then reopen again.
	require.NoError(t, discardSeq(s2.Put(ctx, "d", []byte("delta"))))
	require.NoError(t, discardSeq(s2.Delete(ctx, "a")))
	require.Equal(t, 2, s2.KeyCount())
	require.NoError(t, s2.Close())

	s3, err := New(dir, nil)
	require.NoError(t, err)
	defer s3.Close()
	assert.Equal(t, 2, s3.KeyCount(), "count survives a second reopen")

	// Cross-check against the authoritative (expensive) live key set.
	snap, err := s3.Snapshot(ctx)
	require.NoError(t, err)
	assert.Len(t, snap, 2, "approximation agrees with the real live key set here")
}

// TestStoreKeyCountAfterRestore pins that a Raft InstallSnapshot restore resets
// the count to the restored key set rather than accumulating on top of it.
func TestStoreKeyCountAfterRestore(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, discardSeq(s.Put(ctx, "stale-1", []byte("x"))))
	require.NoError(t, discardSeq(s.Put(ctx, "stale-2", []byte("y"))))
	require.Equal(t, 2, s.KeyCount())

	require.NoError(t, s.RestoreFromSnapshot(ctx, map[string][]byte{
		"r1": []byte("1"), "r2": []byte("2"), "r3": []byte("3"),
	}))

	assert.Equal(t, 3, s.KeyCount(), "count reflects the restored snapshot only")
}
