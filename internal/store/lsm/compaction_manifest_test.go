package lsm

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Regression: a compaction whose manifest update fails must not unlink its
// input SSTables.
//
// The manifest is the only thing that reconstructs the live file set at
// startup — there is no rebuild-by-scanning-the-data-directory path — so a
// manifest naming an SSTable that is not on disk is fatal, not degraded:
// OpenSSTableReader fails, NewLSMTree returns an error, and the node does not
// start. Before the fix, Compact logged the failed manifest removal and then
// unlinked the file anyway, producing exactly that state. Leaving the inputs on
// disk instead costs their space until a later compaction retires them, and the
// store still opens.
//
// To revert-check: in Compact, unlink the input files unconditionally rather
// than only when RemoveAll succeeded. Both tests below must then fail.
// ---------------------------------------------------------------------------

// TestCompact_FailedManifestRemoveKeepsInputFiles drives Compact against a
// manifest that cannot be written and asserts that every input SSTable is still
// on disk and still named by the manifest afterwards.
//
// The inputs hold nothing but tombstones so that the merged output is empty.
// That is deliberate: a non-empty output would fail at the manifest Add first
// and return early, so the removal step this test is about would never run.
func TestCompact_FailedManifestRemoveKeepsInputFiles(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.log")

	manifest, err := OpenManifest(manifestPath)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}

	// Three input SSTables of tombstones only, each recorded in the manifest as
	// a compaction would find them.
	const inputs = 3
	readers := make([]*SSTableReader, inputs)
	paths := make([]string, inputs)
	for i := 0; i < inputs; i++ {
		name := fmt.Sprintf("sst-%08d.sst", i+1)
		paths[i] = filepath.Join(dir, name)

		w, err := NewSSTableWriter(paths[i], 4)
		if err != nil {
			t.Fatalf("NewSSTableWriter %s: %v", name, err)
		}
		seq := uint64(i + 1)
		if err := w.Write(Entry{Key: fmt.Sprintf("k-%d", i), Tombstone: true, SeqNum: seq}); err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("SSTableWriter.Close %s: %v", name, err)
		}
		if err := manifest.Add(name, seq, 0, seq); err != nil {
			t.Fatalf("manifest.Add %s: %v", name, err)
		}
		r, err := OpenSSTableReader(paths[i], seq)
		if err != nil {
			t.Fatalf("OpenSSTableReader %s: %v", name, err)
		}
		defer r.Close()
		readers[i] = r
	}

	// Wedge every subsequent manifest write: writeAll starts by creating
	// <path>.tmp, and os.Create on an existing directory fails.
	if err := os.Mkdir(manifestPath+".tmp", 0o755); err != nil {
		t.Fatalf("mkdir manifest tmp: %v", err)
	}

	c := NewCompactor(dir, manifest, discardLogger(), inputs, func() uint64 { return 99 })
	out, err := c.Compact(context.Background(), readers)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if out != nil {
		out.Close()
		t.Fatal("Compact returned an SSTable for a tombstone-only merge, want nil")
	}

	// The point of the test: the manifest write failed, so nothing may be gone.
	for i, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("input %d (%s) was unlinked after a failed manifest remove: %v; "+
				"the manifest still names it, so the store will not open", i, filepath.Base(p), err)
		}
	}

	// And the on-disk manifest must still name them — i.e. the store that reads
	// it at startup finds every file it expects.
	if err := os.Remove(manifestPath + ".tmp"); err != nil {
		t.Fatalf("remove manifest tmp: %v", err)
	}
	reopened, err := OpenManifest(manifestPath)
	if err != nil {
		t.Fatalf("OpenManifest after failed compaction: %v", err)
	}
	live := reopened.LiveFiles()
	if len(live) != inputs {
		t.Fatalf("live files after failed compaction = %+v, want all %d inputs", live, inputs)
	}
	for _, ev := range live {
		if _, err := os.Stat(filepath.Join(dir, ev.Path)); err != nil {
			t.Errorf("manifest names %s but it is not on disk: %v", ev.Path, err)
		}
	}
}

// TestCompact_SuccessfulManifestRemoveUnlinksInputs is the other half of the
// pair: with a writable manifest the inputs must actually be retired, so the
// guard above cannot be satisfied by never deleting anything.
func TestCompact_SuccessfulManifestRemoveUnlinksInputs(t *testing.T) {
	dir := t.TempDir()

	manifest, err := OpenManifest(filepath.Join(dir, "manifest.log"))
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}

	const inputs = 3
	readers := make([]*SSTableReader, inputs)
	paths := make([]string, inputs)
	for i := 0; i < inputs; i++ {
		name := fmt.Sprintf("sst-%08d.sst", i+1)
		paths[i] = filepath.Join(dir, name)

		w, err := NewSSTableWriter(paths[i], 4)
		if err != nil {
			t.Fatalf("NewSSTableWriter %s: %v", name, err)
		}
		seq := uint64(i + 1)
		if err := w.Write(Entry{Key: fmt.Sprintf("k-%d", i), Value: []byte("v"), SeqNum: seq}); err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("SSTableWriter.Close %s: %v", name, err)
		}
		if err := manifest.Add(name, seq, 0, seq); err != nil {
			t.Fatalf("manifest.Add %s: %v", name, err)
		}
		r, err := OpenSSTableReader(paths[i], seq)
		if err != nil {
			t.Fatalf("OpenSSTableReader %s: %v", name, err)
		}
		defer r.Close()
		readers[i] = r
	}

	c := NewCompactor(dir, manifest, discardLogger(), inputs, func() uint64 { return 99 })
	out, err := c.Compact(context.Background(), readers)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if out == nil {
		t.Fatal("Compact returned no output SSTable for a merge of live keys")
	}
	defer out.Close()

	for i, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("input %d (%s) still on disk after a successful compaction: stat err = %v",
				i, filepath.Base(p), err)
		}
	}

	live := manifest.LiveFiles()
	if len(live) != 1 || live[0].Path != "sst-00000099.sst" {
		t.Fatalf("live files after compaction = %+v, want only the merged output", live)
	}

	// Every merged key must be readable from the output.
	for i := 0; i < inputs; i++ {
		if e, found, err := out.Get(fmt.Sprintf("k-%d", i)); err != nil || !found || string(e.Value) != "v" {
			t.Errorf("output.Get(k-%d) = (%+v, %v, %v), want the merged entry", i, e, found, err)
		}
	}
}

// TestManifestRemoveAll_BatchesAndSkipsEmpty pins the two properties Compact
// relies on: every name in one call is retired together, and a call with
// nothing to retire performs no write at all (asserted by wedging writeAll and
// requiring success anyway).
func TestManifestRemoveAll_BatchesAndSkipsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.log")

	m, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}

	var names []string
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("sst-%08d.sst", i)
		if err := m.Add(name, uint64(i), 0, uint64(i)); err != nil {
			t.Fatalf("Add %s: %v", name, err)
		}
		names = append(names, name)
	}
	// Keep one file live so the assertion distinguishes "removed the batch"
	// from "emptied the manifest".
	kept := fmt.Sprintf("sst-%08d.sst", 5)
	if err := m.Add(kept, 5, 0, 5); err != nil {
		t.Fatalf("Add %s: %v", kept, err)
	}

	if err := m.RemoveAll(names); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	reopened, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("OpenManifest after RemoveAll: %v", err)
	}
	live := reopened.LiveFiles()
	if len(live) != 1 || live[0].Path != kept {
		t.Fatalf("live files after RemoveAll = %+v, want only %s", live, kept)
	}

	// An empty batch must not reach writeAll: with <path>.tmp held by a
	// directory any real write fails, so a nil error proves nothing was written.
	if err := os.Mkdir(path+".tmp", 0o755); err != nil {
		t.Fatalf("mkdir manifest tmp: %v", err)
	}
	if err := m.RemoveAll(nil); err != nil {
		t.Fatalf("RemoveAll(nil) = %v, want nil without writing the manifest", err)
	}
	if err := m.RemoveAll([]string{}); err != nil {
		t.Fatalf("RemoveAll(empty) = %v, want nil without writing the manifest", err)
	}
	if err := m.RemoveAll([]string{kept}); err == nil {
		t.Fatal("RemoveAll with a name returned nil while the manifest was unwritable, want an error")
	}
}

// discardLogger returns a logger that writes nowhere, for tests that exercise
// error paths Compact deliberately only logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
