package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
)

// These tests previously round-tripped a map[string][]byte, because the
// snapshot payload was a dump of the key/value store. The payload is now
// whatever the state machine produced and Raft treats it as opaque bytes, so
// the fixtures changed while the properties under test did not: metadata and
// payload survive a save/load cycle byte-for-byte, a missing snapshot is
// reported as absent rather than as an error, and Meta reads the bounds without
// interpreting the payload.

// testPayload returns a byte payload with enough length and byte diversity to
// catch a truncating or re-encoding bug.
func testPayload() []byte {
	p := make([]byte, 512)
	for i := range p {
		p[i] = byte(i * 7 % 251)
	}
	return p
}

func TestSnapshot_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	ss := NewSnapshotStore(dir, nil)

	data := testPayload()
	snap := Snapshot{
		LastIncludedIndex: 42,
		LastIncludedTerm:  3,
		Data:              data,
	}

	if err := ss.Save(snap); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, ok, err := ss.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("Load: expected snapshot to exist")
	}
	if loaded.LastIncludedIndex != 42 || loaded.LastIncludedTerm != 3 {
		t.Fatalf("metadata mismatch: got index=%d term=%d", loaded.LastIncludedIndex, loaded.LastIncludedTerm)
	}
	if !bytes.Equal(loaded.Data, data) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(loaded.Data), len(data))
	}

	lastIdx, lastTerm, exists := ss.Meta()
	if !exists || lastIdx != 42 || lastTerm != 3 {
		t.Fatalf("Meta: got idx=%d term=%d exists=%v", lastIdx, lastTerm, exists)
	}
}

func TestSnapshot_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	ss := NewSnapshotStore(dir, nil)

	_, ok, err := ss.Load()
	if err != nil {
		t.Fatalf("Load on empty store: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing snapshot")
	}

	_, _, exists := ss.Meta()
	if exists {
		t.Fatal("Meta: expected exists=false")
	}
}

func TestSnapshot_GobRoundTrip(t *testing.T) {
	data := []byte{0x00, 0xFF, 0x01, 'v', 'a', 'l'}
	snap := Snapshot{LastIncludedIndex: 99, LastIncludedTerm: 7, Data: data}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snap); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded Snapshot
	if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.LastIncludedIndex != 99 || decoded.LastIncludedTerm != 7 {
		t.Fatalf("metadata mismatch")
	}
	if !bytes.Equal(decoded.Data, data) {
		t.Fatalf("payload mismatch: got %v want %v", decoded.Data, data)
	}
}

// TestSnapshot_EmptyPayloadRoundTrips pins the placeholder state machine's case:
// a state machine with nothing to save returns no bytes, and that must survive
// the store rather than being mistaken for a missing snapshot.
func TestSnapshot_EmptyPayloadRoundTrips(t *testing.T) {
	ss := NewSnapshotStore(t.TempDir(), nil)

	if err := ss.Save(Snapshot{LastIncludedIndex: 7, LastIncludedTerm: 2}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, ok, err := ss.Load()
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if loaded.LastIncludedIndex != 7 || loaded.LastIncludedTerm != 2 {
		t.Fatalf("metadata mismatch: got index=%d term=%d", loaded.LastIncludedIndex, loaded.LastIncludedTerm)
	}
	if len(loaded.Data) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(loaded.Data))
	}
}
