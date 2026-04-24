package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func TestSnapshot_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	ss := NewSnapshotStore(dir, nil)

	data := make(map[string][]byte, 100)
	for i := 0; i < 100; i++ {
		data[string(rune('a'+i%26))+string(rune('0'+i%10))] = []byte{byte(i)}
	}

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
	if len(loaded.Data) != len(data) {
		t.Fatalf("data len: got %d want %d", len(loaded.Data), len(data))
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
	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": {0x00, 0xFF, 0x01},
	}
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
	for k, want := range data {
		got, ok := decoded.Data[k]
		if !ok {
			t.Fatalf("missing key %s", k)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %s: got %v want %v", k, got, want)
		}
	}
}
