package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Rename durability
// ---------------------------------------------------------------------------

// TestSyncDir_MakesARenameDurableWithoutBreakingWriteAll guards the directory
// fsync that Manifest.writeAll and SSTableWriter.Close both depend on.
//
// The crash it defends against — the new manifest durable under its temporary
// name while the directory entry naming it is still in the page cache, so
// OpenManifest reads the previous manifest after writeAll returned nil and the
// flushed WAL has already been deleted — cannot be produced in a unit test.
// What can, and is the real regression risk, is the opposite failure: fsync on
// a directory handle is not portable, and a platform that rejects it would turn
// every manifest write, every memtable flush and every compaction into an
// error, wedging the engine completely.
//
// So the subtests below pin that syncDir succeeds on a real directory, that
// each save path it was added to still works end to end, and that a genuine
// failure is still reported rather than swallowed. They overlap with
// TestSSTable_WriteRead and the manifest tests by design: those cover the
// formats, these cover the same paths surviving the added sync.
func TestSyncDir_MakesARenameDurableWithoutBreakingWriteAll(t *testing.T) {
	t.Run("fsync on a directory handle is supported here", func(t *testing.T) {
		if err := syncDir(t.TempDir()); err != nil {
			t.Fatalf("syncDir on a real directory: %v", err)
		}
	})

	t.Run("the manifest save paths still complete and stay readable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "manifest.log")
		m, err := OpenManifest(path)
		if err != nil {
			t.Fatalf("OpenManifest: %v", err)
		}
		if err := m.Add("sst-00000001.sst", 1, 0, 10); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := m.Add("sst-00000002.sst", 2, 0, 20); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := m.RemoveAll([]string{"sst-00000001.sst"}); err != nil {
			t.Fatalf("RemoveAll: %v", err)
		}

		reopened, err := OpenManifest(path)
		if err != nil {
			t.Fatalf("OpenManifest after writes: %v", err)
		}
		if live := reopened.LiveFiles(); len(live) != 1 || live[0].Path != "sst-00000002.sst" {
			t.Fatalf("live files after reopen = %+v, want only sst-00000002.sst", live)
		}

		// The rename must have consumed the temporary file, not left it behind.
		if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
			t.Fatalf("stat manifest tmp: got %v, want not-exist", err)
		}

		// Reset is the third writeAll caller and must survive the sync too.
		if err := m.Reset(); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		if emptied, err := OpenManifest(path); err != nil {
			t.Fatalf("OpenManifest after Reset: %v", err)
		} else if got := emptied.LiveFiles(); len(got) != 0 {
			t.Fatalf("live files after Reset = %+v, want none", got)
		}
	})

	t.Run("the SSTable writer still produces a readable file", func(t *testing.T) {
		sstPath := filepath.Join(t.TempDir(), "sst-00000009.sst")
		w, err := NewSSTableWriter(sstPath, 4)
		if err != nil {
			t.Fatalf("NewSSTableWriter: %v", err)
		}
		if err := w.Write(Entry{Key: "k", Value: []byte("v"), SeqNum: 1}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("SSTableWriter.Close: %v", err)
		}

		r, err := OpenSSTableReader(sstPath, 9)
		if err != nil {
			t.Fatalf("OpenSSTableReader: %v", err)
		}
		defer r.Close()
		if e, found, err := r.Get("k"); err != nil || !found || string(e.Value) != "v" {
			t.Fatalf("Get after sync-and-close = (%+v, %v, %v), want the written entry", e, found, err)
		}
	})

	t.Run("a genuine failure is an error, not a silent success", func(t *testing.T) {
		if err := syncDir(filepath.Join(t.TempDir(), "definitely-not-here")); err == nil {
			t.Fatal("syncDir on a missing directory returned nil, want an error")
		}
	})
}
