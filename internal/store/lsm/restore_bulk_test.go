package lsm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Restore bulk path.
//
// Restore used to replay the snapshot through putInternal, which fsyncs the WAL
// once per key. At Raft-snapshot scale that is one fsync per key — 200k of them
// for the state a bench run leaves behind — during which the engine serves no
// writes at all, to make durable data that is already durable in the snapshot
// file the caller restored from.
//
// The bulk path writes one L0 SSTable instead: no WAL traffic, one file sync,
// one manifest rewrite, regardless of key count. These tests pin the properties
// that made the old path safe, so the cheaper path cannot quietly lose them.
// ---------------------------------------------------------------------------

// TestRestore_BulkLoadPerformsNoWALAppends is the revert-check for the bulk
// path: with the per-key write loop back in place, WALAppends grows by one per
// restored key and this fails.
func TestRestore_BulkLoadPerformsNoWALAppends(t *testing.T) {
	const keys = 500

	ctx := context.Background()
	src, err := NewLSMTree(t.TempDir(), nil, WithMaxMemBytes(1<<12))
	if err != nil {
		t.Fatalf("source NewLSMTree: %v", err)
	}
	defer src.Close()

	for i := 0; i < keys; i++ {
		if err := src.Put(ctx, fmt.Sprintf("snap-%05d", i), []byte(fmt.Sprintf("value-%05d", i))); err != nil {
			t.Fatalf("source Put %d: %v", i, err)
		}
	}
	snap, err := src.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != keys {
		t.Fatalf("Snapshot returned %d keys; want %d", len(snap), keys)
	}

	dstDir := t.TempDir()
	dst, err := NewLSMTree(dstDir, nil)
	if err != nil {
		t.Fatalf("dest NewLSMTree: %v", err)
	}
	defer dst.Close()

	// A write of our own first, so the counter is provably live rather than
	// stuck at zero for an unrelated reason.
	if err := dst.Put(ctx, "pre-restore", []byte("x")); err != nil {
		t.Fatalf("dest Put: %v", err)
	}
	before := dst.WALAppends()
	if before == 0 {
		t.Fatal("WALAppends is 0 after a write; the counter is not recording")
	}

	if err := dst.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got := dst.WALAppends() - before; got != 0 {
		t.Errorf("Restore performed %d WAL appends for %d keys; want 0 — the bulk path "+
			"must not go through the per-key write path", got, keys)
	}

	// The payload is the store, and only the payload: the pre-restore write is
	// gone, every restored key reads back, and it all sits in one L0 file.
	if _, err := dst.Get(ctx, "pre-restore"); err != ErrNotFound {
		t.Errorf("Get(pre-restore) after restore = %v; want ErrNotFound", err)
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("snap-%05d", i)
		got, err := dst.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get(%s) after restore: %v", k, err)
		}
		if want := snap[k]; !bytes.Equal(got, want) {
			t.Fatalf("Get(%s) = %q; want %q", k, got, want)
		}
	}
	if got := dst.l0Count.Load(); got != 1 {
		t.Errorf("l0Count after restore = %d; want 1 (a single bulk-loaded SSTable)", got)
	}
	if got := dst.LiveKeys(); got != int64(keys) {
		t.Errorf("LiveKeys after restore = %d; want %d", got, keys)
	}
}

// TestRestore_SurvivesReopen checks the commit point actually committed: the
// restored state must come back after a close/open cycle, live-key count
// included, with no WAL left over from the restore to replay.
func TestRestore_SurvivesReopen(t *testing.T) {
	const keys = 200

	ctx := context.Background()
	dir := t.TempDir()

	payload := make(map[string][]byte, keys)
	for i := 0; i < keys; i++ {
		payload[fmt.Sprintf("restored-%05d", i)] = []byte(fmt.Sprintf("v-%05d", i))
	}

	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	if err := tree.Put(ctx, "doomed", []byte("should not survive")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tree.Restore(ctx, payload); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The sentinel must be gone, or the next open would wipe the restore.
	if _, err := os.Stat(filepath.Join(dir, "restore-in-progress")); !os.IsNotExist(err) {
		t.Fatalf("restore sentinel still present after a successful restore (stat err = %v)", err)
	}

	reopened, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("reopen NewLSMTree: %v", err)
	}
	defer reopened.Close()

	for k, want := range payload {
		got, err := reopened.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get(%s) after reopen: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%s) after reopen = %q; want %q", k, got, want)
		}
	}
	if _, err := reopened.Get(ctx, "doomed"); err != ErrNotFound {
		t.Errorf("Get(doomed) after reopen = %v; want ErrNotFound — the pre-restore store "+
			"must not partially survive", err)
	}
	if got := reopened.LiveKeys(); got != int64(keys) {
		t.Errorf("LiveKeys after reopen = %d; want %d — the count must be recorded in the "+
			"manifest by the bulk load", got, keys)
	}
}

// TestRestore_EmptySnapshotEmptiesStore covers the degenerate payload: a
// snapshot of an empty state machine must leave an empty, writable store rather
// than an SSTable with no entries.
func TestRestore_EmptySnapshotEmptiesStore(t *testing.T) {
	ctx := context.Background()
	tree, err := NewLSMTree(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	defer tree.Close()

	if err := tree.Put(ctx, "before", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tree.Restore(ctx, map[string][]byte{}); err != nil {
		t.Fatalf("Restore(empty): %v", err)
	}

	if _, err := tree.Get(ctx, "before"); err != ErrNotFound {
		t.Errorf("Get(before) = %v; want ErrNotFound", err)
	}
	if got := tree.l0Count.Load(); got != 0 {
		t.Errorf("l0Count = %d after restoring an empty snapshot; want 0", got)
	}
	if got := tree.LiveKeys(); got != 0 {
		t.Errorf("LiveKeys = %d after restoring an empty snapshot; want 0", got)
	}

	// The store must still be usable afterwards — the background goroutines
	// Restore stops have to come back.
	if err := tree.Put(ctx, "after", []byte("v")); err != nil {
		t.Fatalf("Put after empty restore: %v", err)
	}
	if _, err := tree.Get(ctx, "after"); err != nil {
		t.Fatalf("Get(after): %v", err)
	}
}
