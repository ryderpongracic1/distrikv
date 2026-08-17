package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStorePutReturnsSeq pins what the ring-primary write path depends on: every
// Put and Delete reports the sequence the storage engine assigned, and those
// sequences strictly increase. That number is the version of the key on the
// replication wire, so a repeated or non-monotonic one would make two distinct
// writes indistinguishable to a replica's apply-if-newer check.
func TestStorePutReturnsSeq(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var last uint64
	for i := 0; i < 5; i++ {
		seq, err := s.Put(ctx, "k", []byte("v"))
		require.NoError(t, err)
		require.Greater(t, seq, last, "Put must return a sequence above every earlier one")
		last = seq
	}

	// A different key draws from the same counter — the sequence is per node, not
	// per key, and only compared between versions of one key.
	seq, err := s.Put(ctx, "other", []byte("v"))
	require.NoError(t, err)
	require.Greater(t, seq, last)
	last = seq

	// A delete is a version too, so it is sequenced like a write.
	seq, err = s.Delete(ctx, "k")
	require.NoError(t, err)
	require.Greater(t, seq, last)
}

// TestStorePutIfNewer covers the replica apply path at the store boundary: an
// older sequence is refused and leaves the stored value alone, a newer one is
// applied, and the value that survives belongs to the highest sequence rather
// than to whichever write arrived last.
func TestStorePutIfNewer(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Pad so the sequence under test is above 1: seq 0 means "unsequenced" and
	// applies unconditionally.
	for i := 0; i < 4; i++ {
		require.NoError(t, discardSeq(s.Put(ctx, "pad", []byte("v"))))
	}

	seq, err := s.Put(ctx, "k", []byte("current"))
	require.NoError(t, err)

	applied, err := s.PutIfNewer(ctx, "k", []byte("older"), seq-1)
	require.NoError(t, err)
	require.False(t, applied, "a write below the stored sequence must be refused")
	val, err := s.Get(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, "current", string(val))

	applied, err = s.PutIfNewer(ctx, "k", []byte("newer"), seq+1)
	require.NoError(t, err)
	require.True(t, applied, "a write above the stored sequence must be applied")
	val, err = s.Get(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, "newer", string(val))
}

// TestStoreDeleteIfNewer covers the tombstone side, including the resurrection
// an ordering-blind replica would allow: a value older than the delete must not
// bring the key back.
func TestStoreDeleteIfNewer(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		require.NoError(t, discardSeq(s.Put(ctx, "pad", []byte("v"))))
	}

	seq, err := s.Put(ctx, "k", []byte("v"))
	require.NoError(t, err)

	applied, err := s.DeleteIfNewer(ctx, "k", seq-1)
	require.NoError(t, err)
	require.False(t, applied, "a tombstone below the stored sequence must be refused")
	val, err := s.Get(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, "v", string(val))

	applied, err = s.DeleteIfNewer(ctx, "k", seq+5)
	require.NoError(t, err)
	require.True(t, applied)
	_, err = s.Get(ctx, "k")
	require.ErrorIs(t, err, ErrNotFound)

	applied, err = s.PutIfNewer(ctx, "k", []byte("resurrected"), seq+1)
	require.NoError(t, err)
	require.False(t, applied, "a value older than the tombstone must not resurrect the key")
	_, err = s.Get(ctx, "k")
	require.ErrorIs(t, err, ErrNotFound)
}

// TestStorePutIfNewerSeqZeroAppliesUnconditionally pins the compatibility path:
// a sender that assigns no sequence still replicates, applying in arrival order
// exactly as it did before sequences existed.
func TestStorePutIfNewerSeqZeroAppliesUnconditionally(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.Put(ctx, "k", []byte("current"))
	require.NoError(t, err)

	applied, err := s.PutIfNewer(ctx, "k", []byte("unsequenced"), 0)
	require.NoError(t, err)
	require.True(t, applied)
	val, err := s.Get(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, "unsequenced", string(val))

	applied, err = s.DeleteIfNewer(ctx, "k", 0)
	require.NoError(t, err)
	require.True(t, applied)
	_, err = s.Get(ctx, "k")
	require.ErrorIs(t, err, ErrNotFound)
}
