package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

// BloomFilter is a probabilistic set membership structure.
// MightContain returning true means "possibly present" — false positives are
// possible (~1% at 10 bits/key). False negatives are impossible.
// A false positive causes one extra SSTable read; not a correctness issue.
type BloomFilter struct {
	bits    []byte
	numHash int
	size    uint64 // number of bits = len(bits)*8
}

// NewBloomFilter creates a Bloom filter sized for expectedKeys entries.
// Uses 10 bits/key and 7 hash functions (optimal for ~1% FPR).
func NewBloomFilter(expectedKeys int) *BloomFilter {
	if expectedKeys < 1 {
		expectedKeys = 1
	}
	numBits := expectedKeys * 10
	numBytes := (numBits + 7) / 8
	return &BloomFilter{
		bits:    make([]byte, numBytes),
		numHash: 7,
		size:    uint64(numBytes * 8),
	}
}

// Add inserts key into the filter.
func (b *BloomFilter) Add(key string) {
	h1, h2 := twoHashes(key)
	for i := uint64(0); i < uint64(b.numHash); i++ {
		bit := (h1 + i*h2) % b.size
		b.bits[bit/8] |= 1 << (bit % 8)
	}
}

// MightContain returns false if key is definitely absent, true if possibly present.
func (b *BloomFilter) MightContain(key string) bool {
	h1, h2 := twoHashes(key)
	for i := uint64(0); i < uint64(b.numHash); i++ {
		bit := (h1 + i*h2) % b.size
		if b.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Marshal serialises the filter to bytes for writing into an SSTable.
// Format: [8B size][1B numHash][bits...]
func (b *BloomFilter) Marshal() []byte {
	out := make([]byte, 9+len(b.bits))
	binary.BigEndian.PutUint64(out[:8], b.size)
	out[8] = byte(b.numHash)
	copy(out[9:], b.bits)
	return out
}

// UnmarshalBloom deserialises a BloomFilter from the given bytes.
func UnmarshalBloom(data []byte) (*BloomFilter, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("bloom: data too short (%d bytes)", len(data))
	}
	size := binary.BigEndian.Uint64(data[:8])
	numHash := int(data[8])
	bits := make([]byte, len(data)-9)
	copy(bits, data[9:])
	if uint64(len(bits)*8) < size {
		return nil, fmt.Errorf("bloom: bits length mismatch (have %d bits, want %d)", len(bits)*8, size)
	}
	return &BloomFilter{bits: bits, numHash: numHash, size: size}, nil
}

// twoHashes computes h1 (FNV-1a 64) and h2 (FNV-1 64) for key.
// Combined as h(i,k) = h1 + i*h2 (Kirsch-Mitzenmacher technique).
func twoHashes(key string) (h1, h2 uint64) {
	a := fnv.New64a()
	a.Write([]byte(key))
	h1 = a.Sum64()

	b := fnv.New64()
	b.Write([]byte(key))
	h2 = b.Sum64()

	if h2 == 0 {
		h2 = 1 // prevent degenerate all-same-bucket case
	}
	return h1, h2
}
