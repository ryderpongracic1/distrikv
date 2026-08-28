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
// error, wedging the engine completely. So this pins that syncDir succeeds on a
// real directory, that the save paths still work end to end, and that a genuine
// failure is still reported rather than swallowed.
func TestSyncDir_MakesARenameDurableWithoutBreakingWriteAll(t *testing.T) {
	dir := t.TempDir()

	// 1. fsync on a directory handle works on this platform.
	if err := syncDir(dir); err != nil {
		t.Fatalf("syncDir on a real directory: %v", err)
	}

	// 2. The manifest save path still completes and is readable afterwards.
	path := filepath.Join(dir, "manifest.log")
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
	if err := m.Remove("sst-00000001.sst"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	reopened, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("OpenManifest after writes: %v", err)
	}
	live := reopened.LiveFiles()
	if len(live) != 1 || live[0].Path != "sst-00000002.sst" {
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

	// 3. The SSTable writer's directory sync does not break a normal write.
	sstPath := filepath.Join(dir, "sst-00000009.sst")
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

	// 4. A genuine failure is still an error, not a silent success.
	if err := syncDir(filepath.Join(dir, "definitely-not-here")); err == nil {
		t.Fatal("syncDir on a missing directory returned nil, want an error")
	}
}
