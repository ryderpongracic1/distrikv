package lsm

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
)

// blockTargetSize is the soft limit for a single data block before the writer
// starts a new block. 4 KB matches a typical OS page.
const blockTargetSize = 4096

// On-disk entry encoding within a data block:
//   [4B key_len][key bytes][4B val_len][val bytes][1B tombstone][8B seq_num]
//
// Block trailer (appended by finalize):
//   [4B CRC32-IEEE over all preceding bytes in the block]

type blockEncoder struct {
	buf []byte
	crc hash.Hash32
}

func newBlockEncoder() *blockEncoder {
	return &blockEncoder{crc: crc32.NewIEEE()}
}

// appendEntry encodes one entry into the encoder's buffer.
func (e *blockEncoder) appendEntry(entry Entry) {
	keyBytes := []byte(entry.Key)
	var tmp [8]byte

	binary.BigEndian.PutUint32(tmp[:4], uint32(len(keyBytes)))
	e.buf = append(e.buf, tmp[:4]...)
	e.buf = append(e.buf, keyBytes...)

	binary.BigEndian.PutUint32(tmp[:4], uint32(len(entry.Value)))
	e.buf = append(e.buf, tmp[:4]...)
	e.buf = append(e.buf, entry.Value...)

	if entry.Tombstone {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}

	binary.BigEndian.PutUint64(tmp[:], entry.SeqNum)
	e.buf = append(e.buf, tmp[:]...)
}

// size returns the current encoded byte count (without trailer).
func (e *blockEncoder) size() int { return len(e.buf) }

// finalize appends the CRC32 trailer and returns the complete block bytes.
// The encoder must not be used after calling finalize.
func (e *blockEncoder) finalize() []byte {
	e.crc.Reset()
	e.crc.Write(e.buf)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], e.crc.Sum32())
	return append(e.buf, crcBuf[:]...)
}

// reset clears the encoder for reuse.
func (e *blockEncoder) reset() {
	e.buf = e.buf[:0]
	e.crc.Reset()
}

// blockDecoder reads entries from a single data block (including CRC trailer).
type blockDecoder struct {
	data []byte // block payload (excluding CRC trailer)
	pos  int
}

// newBlockDecoder validates the CRC trailer and returns a decoder for the payload.
func newBlockDecoder(raw []byte) (*blockDecoder, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("block: too short to contain CRC (%d bytes)", len(raw))
	}
	payload := raw[:len(raw)-4]
	storedCRC := binary.BigEndian.Uint32(raw[len(raw)-4:])

	h := crc32.NewIEEE()
	h.Write(payload)
	if h.Sum32() != storedCRC {
		return nil, fmt.Errorf("block: CRC mismatch — data corrupted")
	}
	return &blockDecoder{data: payload}, nil
}

// next decodes the next entry. Returns (entry, true, nil) or (_, false, nil) at EOF.
func (d *blockDecoder) next() (Entry, bool, error) {
	if d.pos >= len(d.data) {
		return Entry{}, false, nil
	}
	if d.pos+4 > len(d.data) {
		return Entry{}, false, fmt.Errorf("block: truncated key-len at pos %d", d.pos)
	}
	keyLen := binary.BigEndian.Uint32(d.data[d.pos : d.pos+4])
	d.pos += 4

	if d.pos+int(keyLen) > len(d.data) {
		return Entry{}, false, fmt.Errorf("block: truncated key at pos %d", d.pos)
	}
	key := string(d.data[d.pos : d.pos+int(keyLen)])
	d.pos += int(keyLen)

	if d.pos+4 > len(d.data) {
		return Entry{}, false, fmt.Errorf("block: truncated val-len at pos %d", d.pos)
	}
	valLen := binary.BigEndian.Uint32(d.data[d.pos : d.pos+4])
	d.pos += 4

	if d.pos+int(valLen) > len(d.data) {
		return Entry{}, false, fmt.Errorf("block: truncated val at pos %d", d.pos)
	}
	value := make([]byte, valLen)
	copy(value, d.data[d.pos:d.pos+int(valLen)])
	d.pos += int(valLen)

	if d.pos+1 > len(d.data) {
		return Entry{}, false, fmt.Errorf("block: truncated tombstone at pos %d", d.pos)
	}
	tombstone := d.data[d.pos] != 0
	d.pos++

	if d.pos+8 > len(d.data) {
		return Entry{}, false, fmt.Errorf("block: truncated seqnum at pos %d", d.pos)
	}
	seqNum := binary.BigEndian.Uint64(d.data[d.pos : d.pos+8])
	d.pos += 8

	return Entry{Key: key, Value: value, Tombstone: tombstone, SeqNum: seqNum}, true, nil
}
