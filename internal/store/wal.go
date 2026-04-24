package store

// WAL types re-exported locally from store/wal so internal store code
// compiles unchanged. All new code should import internal/store/wal directly.

import storewal "github.com/ryderpongracic1/distrikv/internal/store/wal"

type opType = storewal.OpType
type WAL = storewal.WAL

const (
	opPut    opType = storewal.OpPut
	opDelete opType = storewal.OpDelete
)

func openWAL(path string) (*WAL, error) {
	return storewal.Open(path)
}
