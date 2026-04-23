package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tempWAL(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.log")
}

// ---------------------------------------------------------------------------
// WAL replay correctness
// ---------------------------------------------------------------------------

// TestWALReplay verifies that a store created from a pre-existing WAL file
// faithfully reconstructs the in-memory state: puts are visible and deleted
// keys are absent.
func TestWALReplay(t *testing.T) {
	path := tempWAL(t)
	ctx := context.Background()

	// Phase 1: write some data and close.
	s1, err := New(path)
	require.NoError(t, err)

	require.NoError(t, s1.Put(ctx, "a", []byte("alpha")))
	require.NoError(t, s1.Put(ctx, "b", []byte("bravo")))
	require.NoError(t, s1.Put(ctx, "c", []byte("charlie")))
	require.NoError(t, s1.Delete(ctx, "b"))
	require.NoError(t, s1.Close())

	// Phase 2: reopen from the same WAL and verify state.
	s2, err := New(path)
	require.NoError(t, err)
	defer s2.Close()

	v, err := s2.Get(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, []byte("alpha"), v)

	_, err = s2.Get(ctx, "b")
	assert.ErrorIs(t, err, ErrNotFound, "deleted key must be absent after replay")

	v, err = s2.Get(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, []byte("charlie"), v)

	assert.Equal(t, 2, s2.KeyCount())
}

// TestWALReplayTruncated verifies that a truncated (torn) WAL entry does not
// cause replay to fail — it stops cleanly at the corruption point and the
// entries before it are still applied.
func TestWALReplayTruncated(t *testing.T) {
	path := tempWAL(t)
	ctx := context.Background()

	// Write one valid entry.
	s1, err := New(path)
	require.NoError(t, err)
	require.NoError(t, s1.Put(ctx, "x", []byte("xray")))
	require.NoError(t, s1.Close())

	// Append a partial, corrupt entry directly to the file.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{byte(opPut), 0, 0, 0, 3, 'b', 'a', 'd'}) // missing value + CRC
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Replay should succeed and expose only the valid first entry.
	s2, err := New(path)
	require.NoError(t, err)
	defer s2.Close()

	v, err := s2.Get(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, []byte("xray"), v)
	assert.Equal(t, 1, s2.KeyCount())
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// TestStoreConcurrentPuts exercises the RWMutex under concurrent Put/Get load.
// The -race detector will catch data races if the locking is wrong.
func TestStoreConcurrentPuts(t *testing.T) {
	s, err := New(tempWAL(t))
	require.NoError(t, err)
	defer s.Close()

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

	// Verify all keys are readable.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("val-%d", i)
		v, err := s.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte(expected), v)
	}
}

// ---------------------------------------------------------------------------
// Basic operations
// ---------------------------------------------------------------------------

func TestStoreGetMissing(t *testing.T) {
	s, err := New(tempWAL(t))
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Get(context.Background(), "no-such-key")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStoreDeleteMissing(t *testing.T) {
	s, err := New(tempWAL(t))
	require.NoError(t, err)
	defer s.Close()

	err = s.Delete(context.Background(), "no-such-key")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestStoreOverwrite(t *testing.T) {
	s, err := New(tempWAL(t))
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()
	require.NoError(t, s.Put(ctx, "k", []byte("v1")))
	require.NoError(t, s.Put(ctx, "k", []byte("v2")))

	v, err := s.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)
}
