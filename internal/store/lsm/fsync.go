package lsm

import (
	"fmt"
	"os"
)

// syncDir fsyncs a directory, making a rename, create, or unlink within it
// durable.
//
// Syncing a file guarantees only its contents. The directory entry that names
// the file is separate metadata: without this call a crash can leave a fully
// synced file that nothing on disk points at (a lost create) or that is still
// reachable only under its temporary name (a lost rename).
//
// The helper is local to this package rather than shared with the identical
// code in internal/raft: the dependency runs internal/store → internal/store/lsm,
// so lsm cannot reach up into store for a common implementation, and raft is
// deliberately free of any dependency on the data path. A dozen duplicated
// lines is the cheaper of the two.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open %q: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync %q: %w", dir, err)
	}
	return nil
}
