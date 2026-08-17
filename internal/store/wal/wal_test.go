package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func newWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

// replayAll replays the WAL and returns all (op, key, value) tuples.
func replayAll(t *testing.T, w *WAL) []struct {
	op    OpType
	key   string
	value []byte
} {
	t.Helper()
	var out []struct {
		op    OpType
		key   string
		value []byte
	}
	err := w.Replay(func(op OpType, key string, value []byte, _ uint64) {
		out = append(out, struct {
			op    OpType
			key   string
			value []byte
		}{op, key, value})
	})
	require.NoError(t, err, "Replay must not return an error for valid or torn-write WALs")
	return out
}

// encodeEntry returns the raw wire bytes for a single WAL entry.
// Used to construct torn-write test fixtures.
func encodeEntry(op OpType, key string, value []byte) []byte {
	var buf []byte
	var lenBuf [4]byte

	buf = append(buf, byte(op))
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, []byte(key)...)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, value...)

	var crc uint32
	crc = crc32.Update(crc, crc32.IEEETable, buf)
	binary.BigEndian.PutUint32(lenBuf[:], crc)
	buf = append(buf, lenBuf[:]...)
	return buf
}

// writeRaw writes arbitrary bytes to a new WAL file, bypassing the WAL API.
func writeRaw(t *testing.T, data []byte) *WAL {
	t.Helper()
	path := filepath.Join(t.TempDir(), "raw.wal")
	require.NoError(t, os.WriteFile(path, data, 0o644))
	w, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// ── basic correctness ─────────────────────────────────────────────────────────

func TestWAL_AppendAndReplay(t *testing.T) {
	w, _ := newWAL(t)

	require.NoError(t, w.Append(OpPut, "hello", []byte("world")))
	require.NoError(t, w.Append(OpPut, "foo", []byte("bar")))
	require.NoError(t, w.Append(OpDelete, "hello", nil))

	entries := replayAll(t, w)
	require.Len(t, entries, 3)

	assert.Equal(t, OpPut, entries[0].op)
	assert.Equal(t, "hello", entries[0].key)
	assert.Equal(t, []byte("world"), entries[0].value)

	assert.Equal(t, OpPut, entries[1].op)
	assert.Equal(t, "foo", entries[1].key)
	assert.Equal(t, []byte("bar"), entries[1].value)

	assert.Equal(t, OpDelete, entries[2].op)
	assert.Equal(t, "hello", entries[2].key)
	assert.Nil(t, entries[2].value)
}

func TestWAL_EmptyFile(t *testing.T) {
	w, _ := newWAL(t)
	entries := replayAll(t, w)
	assert.Empty(t, entries, "empty WAL must replay zero entries")
}

func TestWAL_EmptyKey(t *testing.T) {
	w, _ := newWAL(t)
	require.NoError(t, w.Append(OpPut, "", []byte("val")))
	entries := replayAll(t, w)
	require.Len(t, entries, 1)
	assert.Equal(t, "", entries[0].key)
	assert.Equal(t, []byte("val"), entries[0].value)
}

func TestWAL_EmptyValue(t *testing.T) {
	// []byte{} and nil both encode as valLen=0 on the wire; on replay both
	// are returned as nil (the wire format cannot encode the distinction).
	w, _ := newWAL(t)
	require.NoError(t, w.Append(OpPut, "k", []byte{}))
	entries := replayAll(t, w)
	require.Len(t, entries, 1)
	assert.Empty(t, entries[0].value, "empty value must replay as nil or []byte{}")
}

func TestWAL_NilValue(t *testing.T) {
	w, _ := newWAL(t)
	require.NoError(t, w.Append(OpDelete, "k", nil))
	entries := replayAll(t, w)
	require.Len(t, entries, 1)
	assert.Empty(t, entries[0].value)
}

func TestWAL_LargeValueRoundTrip(t *testing.T) {
	w, _ := newWAL(t)
	// 64KB value — exercises pool buffer growth
	large := make([]byte, 64*1024)
	for i := range large {
		large[i] = byte(i & 0xFF)
	}
	require.NoError(t, w.Append(OpPut, "bigkey", large))
	entries := replayAll(t, w)
	require.Len(t, entries, 1)
	assert.Equal(t, large, entries[0].value)
}

func TestWAL_ReplaySeesFreshCopies(t *testing.T) {
	// Verify that the value slice delivered to fn is an independent copy —
	// not a pooled buffer that gets overwritten by the next Replay iteration.
	w, _ := newWAL(t)
	require.NoError(t, w.Append(OpPut, "a", []byte("AAAA")))
	require.NoError(t, w.Append(OpPut, "b", []byte("BBBB")))

	var vals [][]byte
	err := w.Replay(func(_ OpType, _ string, value []byte, _ uint64) {
		vals = append(vals, value) // retain the slice
	})
	require.NoError(t, err)
	require.Len(t, vals, 2)

	// Both slices must be independently valid after Replay returns.
	assert.Equal(t, []byte("AAAA"), vals[0])
	assert.Equal(t, []byte("BBBB"), vals[1])
}

// ── torn-write scenarios ─────────────────────────────────────────────────────
//
// A "torn write" is a partial write caused by a sudden power loss or kill-9
// mid-fsync. Replay must stop cleanly (return nil) at any such boundary and
// not return any partial entry to the caller.

// tornCases enumerates every byte offset within an entry where the file could
// be truncated by a crash.
func tornCases(t *testing.T) []struct {
	name     string
	truncate func(full []byte) []byte
} {
	full := encodeEntry(OpPut, "mykey", []byte("myvalue"))
	// full layout: [1:op][4:keylen][5:key][4:vallen][7:value][4:crc] = 25 bytes

	positions := []struct {
		name     string
		truncate func([]byte) []byte
	}{
		{"truncated_after_0_bytes", func(b []byte) []byte { return b[:0] }},
		{"truncated_after_op_byte", func(b []byte) []byte { return b[:1] }},
		{"truncated_mid_key_len", func(b []byte) []byte { return b[:3] }},
		{"truncated_after_key_len", func(b []byte) []byte { return b[:5] }},
		{"truncated_mid_key", func(b []byte) []byte { return b[:7] }},
		{"truncated_after_key", func(b []byte) []byte { return b[:10] }},
		{"truncated_mid_val_len", func(b []byte) []byte { return b[:12] }},
		{"truncated_after_val_len", func(b []byte) []byte { return b[:14] }},
		{"truncated_mid_value", func(b []byte) []byte { return b[:17] }},
		{"truncated_after_value", func(b []byte) []byte { return b[:21] }},
		{"truncated_mid_crc", func(b []byte) []byte { return b[:23] }},
	}
	_ = full
	_ = t
	return positions
}

func TestWAL_TornWrite_FirstEntry(t *testing.T) {
	for _, tc := range tornCases(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			full := encodeEntry(OpPut, "mykey", []byte("myvalue"))
			data := tc.truncate(full)
			w := writeRaw(t, data)

			entries := replayAll(t, w)
			assert.Empty(t, entries,
				"torn first entry must replay zero entries (truncation at %d/%d bytes)",
				len(data), len(full))
		})
	}
}

func TestWAL_TornWrite_SecondEntry(t *testing.T) {
	// A complete first entry followed by a torn second entry.
	// Replay must return exactly the first entry and stop cleanly.
	first := encodeEntry(OpPut, "first", []byte("value1"))

	for _, tc := range tornCases(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			second := encodeEntry(OpPut, "second", []byte("value2"))
			partial := tc.truncate(second)

			data := append(append([]byte(nil), first...), partial...)
			w := writeRaw(t, data)

			entries := replayAll(t, w)
			require.Len(t, entries, 1,
				"only the complete first entry must be replayed (torn second at %d bytes)",
				len(partial))
			assert.Equal(t, "first", entries[0].key)
			assert.Equal(t, []byte("value1"), entries[0].value)
		})
	}
}

func TestWAL_CRCMismatch_StopsReplay(t *testing.T) {
	// Flip a bit in the value region — CRC mismatch must stop replay cleanly.
	full := encodeEntry(OpPut, "key", []byte("value"))
	full[len(full)-5] ^= 0xFF // corrupt last byte of value (before CRC trailer)

	w := writeRaw(t, full)
	entries := replayAll(t, w)
	assert.Empty(t, entries, "CRC mismatch must stop replay with zero entries")
}

func TestWAL_CRCMismatch_AfterGoodEntry(t *testing.T) {
	// Good entry + corrupted second entry → only first entry surfaced.
	good := encodeEntry(OpPut, "good", []byte("ok"))
	bad := encodeEntry(OpPut, "bad", []byte("corrupt"))
	bad[len(bad)-5] ^= 0xFF // corrupt value byte before CRC trailer

	data := append(append([]byte(nil), good...), bad...)
	w := writeRaw(t, data)

	entries := replayAll(t, w)
	require.Len(t, entries, 1)
	assert.Equal(t, "good", entries[0].key)
}

func TestWAL_MultipleEntries_AllValid(t *testing.T) {
	// Round-trip 1000 entries; verify order and content.
	w, _ := newWAL(t)
	const n = 1000
	for i := 0; i < n; i++ {
		key := string(rune('a'+i%26)) + string([]byte{byte(i >> 8), byte(i)})
		val := []byte{byte(i), byte(i >> 8)}
		op := OpPut
		if i%7 == 0 {
			op = OpDelete
			val = nil
		}
		require.NoError(t, w.Append(op, key, val))
	}

	var count int
	err := w.Replay(func(_ OpType, _ string, _ []byte, _ uint64) { count++ })
	require.NoError(t, err)
	assert.Equal(t, n, count)
}

func TestWAL_PoolBufferGrowth(t *testing.T) {
	// Write entries with progressively larger values to exercise pool buffer
	// growth.  Then replay — if pool buffers are reused correctly all entries
	// should come back intact.
	w, _ := newWAL(t)
	sizes := []int{128, 512, 4096, 16384, 512, 128} // grow then shrink

	var expected [][]byte
	for i, sz := range sizes {
		val := make([]byte, sz)
		for j := range val {
			val[j] = byte((i*sz + j) & 0xFF)
		}
		expected = append(expected, val)
		require.NoError(t, w.Append(OpPut, "key", val))
	}

	var got [][]byte
	err := w.Replay(func(_ OpType, _ string, value []byte, _ uint64) {
		got = append(got, value)
	})
	require.NoError(t, err)
	require.Len(t, got, len(expected))
	for i := range expected {
		assert.Equal(t, expected[i], got[i], "entry %d mismatch", i)
	}
}

// ── persistence across open/close ────────────────────────────────────────────

func TestWAL_PersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.wal")

	// Write 5 entries and close.
	func() {
		w, err := Open(path)
		require.NoError(t, err)
		defer w.Close()
		for i := 0; i < 5; i++ {
			require.NoError(t, w.Append(OpPut, string(rune('a'+i)), []byte{byte(i)}))
		}
	}()

	// Reopen and replay — all 5 entries must be intact.
	w2, err := Open(path)
	require.NoError(t, err)
	defer w2.Close()

	entries := replayAll(t, w2)
	require.Len(t, entries, 5)
	for i, e := range entries {
		assert.Equal(t, string(rune('a'+i)), e.key)
		assert.Equal(t, []byte{byte(i)}, e.value)
	}
}

// ── allocation benchmarks ─────────────────────────────────────────────────────

// BenchmarkWAL_Append measures per-call allocations for the hot Append path.
// Target: 0 allocs/op on the warm path (pool buffers pre-warmed by prior calls).
func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	key := "benchmark-key"
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.Append(OpPut, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWAL_Replay measures per-entry allocations during WAL replay.
// Pre-populates N entries then replays — pool buffers warm up after the first
// few entries so steady-state allocations should be limited to the unavoidable
// string(key) + valCopy makes.
func BenchmarkWAL_Replay(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench-replay.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}

	const entries = 10000
	key := "bench-key"
	value := make([]byte, 256)
	for i := 0; i < entries; i++ {
		if err := w.Append(OpPut, key, value); err != nil {
			b.Fatal(err)
		}
	}
	w.Close()

	// Reopen for replay (seeks to 0 each Replay call).
	rw, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer rw.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := rw.Replay(func(_ OpType, _ string, _ []byte, _ uint64) {}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWAL_Append_LargeValue exercises the pool buffer growth path for
// large values (> default pool bucket size of 4KB).
func BenchmarkWAL_Append_LargeValue(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench-large.wal")
	w, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	key := "bigkey"
	value := make([]byte, 64*1024) // 64 KB

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.Append(OpPut, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// ── torn write at specific simulated crash points ────────────────────────────

// TestWAL_SimulatedPowerLoss writes a complete entry, then appends exactly
// N-1 bytes of a second entry (simulating power loss mid-write), and verifies:
//  1. Replay returns exactly one entry (the complete one).
//  2. Replay returns nil, not an error.
//  3. A subsequent Append to the same WAL succeeds and is readable.
func TestWAL_SimulatedPowerLoss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.wal")

	// Write a complete entry.
	w, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, w.Append(OpPut, "survivor", []byte("intact")))
	require.NoError(t, w.Close())

	// Corrupt: append N-1 bytes of a second entry (torn write simulation).
	torn := encodeEntry(OpPut, "lost", []byte("gone"))
	partial := torn[:len(torn)-1] // all bytes except the last CRC byte

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Write(partial)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Replay must surface only the first entry.
	w2, err := Open(path)
	require.NoError(t, err)
	entries := replayAll(t, w2)
	require.Len(t, entries, 1)
	assert.Equal(t, "survivor", entries[0].key)

	// Recovery: in production the LSM truncates the WAL file at the last known
	// good byte before re-opening for writes, so that subsequent Replays don't
	// hit the torn tail and stop early.  Simulate that here:
	//   1. Stat the good entry size to know the truncation point.
	//   2. Truncate the file to that position.
	//   3. Append a new entry.
	//   4. Replay must now see both the survivor and the new entry.
	goodLen := int64(len(encodeEntry(OpPut, "survivor", []byte("intact"))))
	require.NoError(t, w2.f.Truncate(goodLen), "truncate to last good position")

	// Reset the persistent bufio.Writer so it doesn't think there is buffered
	// data beyond the new file end.
	w2.bw.Reset(w2.f)

	// Seek the underlying file to the end before appending.
	_, err = w2.f.Seek(0, io.SeekEnd)
	require.NoError(t, err)

	require.NoError(t, w2.Append(OpPut, "recovered", []byte("newdata")))

	all := replayAll(t, w2)
	// Survivor + recovered; torn entry must not appear.
	require.Len(t, all, 2, "expected survivor + recovered after truncation")
	assert.Equal(t, "survivor", all[0].key)
	assert.Equal(t, "recovered", all[1].key)
	require.NoError(t, w2.Close())
}

// TestWAL_TruncatedCRCByte covers the edge case where the file is truncated
// exactly mid-CRC (3 of 4 CRC bytes written). io.ReadFull returns
// io.ErrUnexpectedEOF, which must be treated as a torn write, not an error.
func TestWAL_TruncatedCRCByte(t *testing.T) {
	full := encodeEntry(OpPut, "k", []byte("v"))
	// Truncate to remove the last byte of the CRC trailer.
	truncated := full[:len(full)-1]
	w := writeRaw(t, truncated)
	entries := replayAll(t, w)
	assert.Empty(t, entries,
		"entry with truncated CRC must not be replayed")
}

// TestWAL_ReplayAfterAppend verifies that a WAL can be replayed after new
// entries are appended (i.e., Seek(0) correctly resets the read position).
func TestWAL_ReplayAfterAppend(t *testing.T) {
	w, _ := newWAL(t)

	require.NoError(t, w.Append(OpPut, "a", []byte("1")))
	e1 := replayAll(t, w)
	require.Len(t, e1, 1)

	require.NoError(t, w.Append(OpPut, "b", []byte("2")))
	e2 := replayAll(t, w)
	require.Len(t, e2, 2)
	assert.Equal(t, "a", e2[0].key)
	assert.Equal(t, "b", e2[1].key)
}

// TestWAL_ReplayDoesNotReturnErrorForTornWrite explicitly asserts the error
// contract: torn writes return nil, not wrapped errors.
func TestWAL_ReplayDoesNotReturnErrorForTornWrite(t *testing.T) {
	// Truncate after just the op byte of a second entry.
	good := encodeEntry(OpPut, "g", []byte("v"))
	torn := []byte{byte(OpPut)} // only op byte of second entry

	data := append(append([]byte(nil), good...), torn...)
	w := writeRaw(t, data)

	var called int
	err := w.Replay(func(_ OpType, _ string, _ []byte, _ uint64) { called++ })
	assert.NoError(t, err, "torn write must not produce a non-nil error")
	assert.Equal(t, 1, called, "only the complete entry must be delivered")
}

// TestWAL_IOError_NotTreatedAsTornWrite uses a closed file handle to simulate
// a genuine I/O error during Replay. The WAL must return a non-nil error
// instead of silently treating it as a clean truncation.
func TestWAL_IOError_NotTreatedAsTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ioerr.wal")
	w, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, w.Append(OpPut, "k", []byte("v")))

	// Close the underlying file — the next Replay will hit a real I/O error
	// (not EOF) when it tries to Seek.
	require.NoError(t, w.f.Close())

	err = w.Replay(func(_ OpType, _ string, _ []byte, _ uint64) {})
	assert.Error(t, err, "genuine I/O error (closed fd) must be returned, not silently swallowed")
}

// ── string/value ownership ────────────────────────────────────────────────────

// TestWAL_UnsafeStringBytes verifies that unsafeStringBytes produces bytes
// identical to []byte(s) for all tested inputs.
func TestWAL_UnsafeStringBytes(t *testing.T) {
	// Non-empty strings: content must match []byte(s) exactly.
	cases := []string{"a", "hello world", string(make([]byte, 256))}
	for _, s := range cases {
		got := unsafeStringBytes(s)
		want := []byte(s)
		assert.Equal(t, want, got, "unsafeStringBytes(%q)", s)
	}

	// Empty string: unsafeStringBytes returns nil (unsafe.StringData is
	// unspecified for ""); []byte("") returns a non-nil zero-length slice.
	// Both are valid zero-length byte sequences and crc32.Update treats
	// them identically — so we only check length here.
	assert.Empty(t, unsafeStringBytes(""), "unsafeStringBytes(\"\") must be empty")
}

// TestWAL_GrowBuf verifies that growBuf expands the slice correctly.
func TestWAL_GrowBuf(t *testing.T) {
	b := make([]byte, 4)
	bp := &b

	// No growth needed.
	growBuf(bp, 4)
	assert.Equal(t, 4, cap(*bp))

	// Growth required.
	growBuf(bp, 1024)
	assert.GreaterOrEqual(t, cap(*bp), 1024)

	// Shrink request — no change to capacity.
	old := cap(*bp)
	growBuf(bp, 1)
	assert.Equal(t, old, cap(*bp))
}

// Ensure io.ReadFull returns io.ErrUnexpectedEOF when reading a non-empty
// buffer that hits EOF before filling — this is the torn-write signal we rely on.
func TestWAL_ReadFullEOFSemantics(t *testing.T) {
	// 2-byte reader containing only 1 byte.
	r := &limitedReader{data: []byte{0x42}}
	buf := make([]byte, 2)
	_, err := io.ReadFull(r, buf)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"io.ReadFull on short read must return io.ErrUnexpectedEOF")
}

// limitedReader is a minimal io.Reader that returns its data then EOF.
type limitedReader struct {
	data []byte
	pos  int
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
