package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, s.Put(ctx, "a", []byte("alpha")))

	v, err := s.Get(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, []byte("alpha"), v)
}

func TestStoreGetMissing(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Get(context.Background(), "no-such-key")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStoreDeleteMissing(t *testing.T) {
	s := newTestStore(t)

	err := s.Delete(context.Background(), "no-such-key")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStoreOverwrite(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Put(ctx, "k", []byte("v1")))
	require.NoError(t, s.Put(ctx, "k", []byte("v2")))

	v, err := s.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)
}

func TestStoreDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Put(ctx, "del", []byte("gone")))
	require.NoError(t, s.Delete(ctx, "del"))

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
		require.NoError(t, s.Put(ctx, "a", []byte("alpha")))
		require.NoError(t, s.Put(ctx, "b", []byte("bravo")))
		require.NoError(t, s.Put(ctx, "c", []byte("charlie")))
		require.NoError(t, s.Delete(ctx, "b"))
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
			require.NoError(t, s.Put(ctx, key, []byte(val)))
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
		require.NoError(t, src.Put(ctx, k, []byte(fmt.Sprintf("v%d", i))))
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

	require.NoError(t, s.Put(ctx, "a", []byte("1")))
	require.NoError(t, s.Put(ctx, "b", []byte("2")))
	_, _ = s.Get(ctx, "a")
	_, _ = s.Get(ctx, "missing")
	_ = s.Delete(ctx, "b")

	puts, gets, dels, _ := s.Counts()
	assert.Equal(t, uint64(2), puts)
	assert.Equal(t, uint64(2), gets)
	assert.Equal(t, uint64(1), dels)
}
