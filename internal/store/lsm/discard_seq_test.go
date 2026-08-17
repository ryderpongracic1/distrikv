package lsm

// discardSeq drops the sequence number a seq-returning write reports, leaving
// just the error.
//
// Put and Delete return the sequence assigned to the write so a ring-primary can
// replicate it (see LSMTree.PutIfNewer). Tests that only care whether the write
// succeeded pass the call straight through this, which keeps them a single
// expression instead of splitting every write into an assignment and an
// assertion.
func discardSeq(_ uint64, err error) error { return err }
