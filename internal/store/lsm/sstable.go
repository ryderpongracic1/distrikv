package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// IndexEntry points to one data block within an SSTable file.
type IndexEntry struct {
	FirstKey string
	Offset   uint32 // byte offset of data block start
	Length   uint32 // byte length of data block (including CRC trailer)
}

// SSTable file layout:
//
//	[Data Blocks...]
//	[Index Block]   -- serialized IndexEntry slice
//	[Filter Block]  -- serialized BloomFilter
//	[Footer]        -- 16 bytes: index_offset(4) index_len(4) filter_offset(4) filter_len(4)
const footerSize = 16

// ---- Writer ----------------------------------------------------------------

// SSTableWriter writes an SSTable file. Entries must be supplied in strictly
// ascending key order. Call Close to finalize and sync the file.
type SSTableWriter struct {
	f          *os.File
	bw         *bufio.Writer
	blockEnc   *blockEncoder
	index      []IndexEntry
	filter     *BloomFilter
	offset     uint32 // current logical write position
	firstEntry bool   // true until the first entry is written
}

// NewSSTableWriter creates a new SSTable file at path.
// expectedKeys is used to size the Bloom filter.
func NewSSTableWriter(path string, expectedKeys int) (*SSTableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %q: %w", path, err)
	}
	return &SSTableWriter{
		f:          f,
		bw:         bufio.NewWriterSize(f, 256*1024),
		blockEnc:   newBlockEncoder(),
		filter:     NewBloomFilter(expectedKeys),
		firstEntry: true,
	}, nil
}

// Write appends one entry. Entries must arrive in strictly ascending key order.
func (w *SSTableWriter) Write(entry Entry) error {
	if w.firstEntry {
		w.index = append(w.index, IndexEntry{FirstKey: entry.Key, Offset: w.offset})
		w.firstEntry = false
	} else if w.blockEnc.size() >= blockTargetSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
		w.index = append(w.index, IndexEntry{FirstKey: entry.Key, Offset: w.offset})
	}
	w.blockEnc.appendEntry(entry)
	w.filter.Add(entry.Key)
	return nil
}

func (w *SSTableWriter) flushBlock() error {
	if w.blockEnc.size() == 0 {
		return nil
	}
	blockBytes := w.blockEnc.finalize()
	if _, err := w.bw.Write(blockBytes); err != nil {
		return fmt.Errorf("sstable: write block: %w", err)
	}
	w.index[len(w.index)-1].Length = uint32(len(blockBytes))
	w.offset += uint32(len(blockBytes))
	w.blockEnc.reset()
	return nil
}

// Close flushes remaining data, writes the index/filter/footer, then syncs.
func (w *SSTableWriter) Close() error {
	if err := w.flushBlock(); err != nil {
		w.f.Close()
		return err
	}

	// Index block: [4B count][for each: 4B keyLen, key bytes, 4B offset, 4B length]
	indexOffset := w.offset
	var tmp [8]byte
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(w.index)))
	if err := w.writeBuf(tmp[:4]); err != nil {
		w.f.Close()
		return fmt.Errorf("sstable: write index count: %w", err)
	}
	indexLen := uint32(4)
	for _, ie := range w.index {
		kb := []byte(ie.FirstKey)
		binary.BigEndian.PutUint32(tmp[:4], uint32(len(kb)))
		if err := w.writeBuf(tmp[:4]); err != nil {
			w.f.Close()
			return err
		}
		if err := w.writeBuf(kb); err != nil {
			w.f.Close()
			return err
		}
		binary.BigEndian.PutUint32(tmp[:4], ie.Offset)
		binary.BigEndian.PutUint32(tmp[4:], ie.Length)
		if err := w.writeBuf(tmp[:]); err != nil {
			w.f.Close()
			return err
		}
		indexLen += uint32(4 + len(kb) + 8)
	}

	// Filter block
	filterOffset := indexOffset + indexLen
	filterBytes := w.filter.Marshal()
	if err := w.writeBuf(filterBytes); err != nil {
		w.f.Close()
		return fmt.Errorf("sstable: write filter: %w", err)
	}
	filterLen := uint32(len(filterBytes))

	// Footer: [4B index_offset][4B index_len][4B filter_offset][4B filter_len]
	var footer [footerSize]byte
	binary.BigEndian.PutUint32(footer[:4], indexOffset)
	binary.BigEndian.PutUint32(footer[4:8], indexLen)
	binary.BigEndian.PutUint32(footer[8:12], filterOffset)
	binary.BigEndian.PutUint32(footer[12:16], filterLen)
	if err := w.writeBuf(footer[:]); err != nil {
		w.f.Close()
		return fmt.Errorf("sstable: write footer: %w", err)
	}

	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return fmt.Errorf("sstable: flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return fmt.Errorf("sstable: sync: %w", err)
	}
	return w.f.Close()
}

func (w *SSTableWriter) writeBuf(data []byte) error {
	_, err := w.bw.Write(data)
	return err
}

// ---- Reader ----------------------------------------------------------------

// SSTableReader reads entries from a single SSTable file.
// SSTSeq is used by the LSMTree to order readers: higher = newer.
type SSTableReader struct {
	f      *os.File
	index  []IndexEntry
	filter *BloomFilter
	size   int64
	SSTSeq uint64
	path   string // full path, used for deletion during compaction
}

// OpenSSTableReader opens an SSTable file and loads its index and filter into memory.
func OpenSSTableReader(path string, sstSeq uint64) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %q: %w", path, err)
	}
	size := info.Size()
	if size < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable: file %q too small (%d bytes)", path, size)
	}

	var footerBuf [footerSize]byte
	if _, err := f.ReadAt(footerBuf[:], size-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	indexOffset := binary.BigEndian.Uint32(footerBuf[:4])
	indexLen := binary.BigEndian.Uint32(footerBuf[4:8])
	filterOffset := binary.BigEndian.Uint32(footerBuf[8:12])
	filterLen := binary.BigEndian.Uint32(footerBuf[12:16])

	indexData := make([]byte, indexLen)
	if _, err := f.ReadAt(indexData, int64(indexOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}
	index, err := decodeIndex(indexData)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: decode index: %w", err)
	}

	filterData := make([]byte, filterLen)
	if _, err := f.ReadAt(filterData, int64(filterOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read filter: %w", err)
	}
	filter, err := UnmarshalBloom(filterData)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: decode filter: %w", err)
	}

	return &SSTableReader{
		f: f, index: index, filter: filter,
		size: size, SSTSeq: sstSeq, path: path,
	}, nil
}

// Get looks up key. Returns (entry, true, nil) if found (tombstones included — caller checks).
func (r *SSTableReader) Get(key string) (Entry, bool, error) {
	if !r.filter.MightContain(key) {
		return Entry{}, false, nil
	}
	blockIdx := r.blockIndexFor(key)
	if blockIdx < 0 {
		return Entry{}, false, nil
	}
	entries, err := r.readBlock(blockIdx)
	if err != nil {
		return Entry{}, false, err
	}
	for _, e := range entries {
		if e.Key == key {
			return e, true, nil
		}
		if e.Key > key {
			break
		}
	}
	return Entry{}, false, nil
}

// blockIndexFor returns the index of the block that may contain key, or -1.
func (r *SSTableReader) blockIndexFor(key string) int {
	n := len(r.index)
	if n == 0 {
		return -1
	}
	// Find rightmost block where FirstKey <= key.
	i := sort.Search(n, func(i int) bool { return r.index[i].FirstKey > key })
	i--
	if i < 0 {
		return -1
	}
	return i
}

// readBlock reads and decodes all entries from the block at index blockIdx.
func (r *SSTableReader) readBlock(blockIdx int) ([]Entry, error) {
	ie := r.index[blockIdx]
	raw := make([]byte, ie.Length)
	if _, err := r.f.ReadAt(raw, int64(ie.Offset)); err != nil {
		return nil, fmt.Errorf("sstable: read block %d: %w", blockIdx, err)
	}
	dec, err := newBlockDecoder(raw)
	if err != nil {
		return nil, fmt.Errorf("sstable: decode block %d: %w", blockIdx, err)
	}
	var entries []Entry
	for {
		e, ok, err := dec.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Iterator returns a forward iterator over all entries in ascending key order.
func (r *SSTableReader) Iterator() *SSTableIterator {
	return &SSTableIterator{r: r, blockIdx: -1}
}

// Close releases the file handle.
func (r *SSTableReader) Close() error { return r.f.Close() }

// ---- Iterator --------------------------------------------------------------

// SSTableIterator iterates over entries in an SSTable in ascending key order.
type SSTableIterator struct {
	r        *SSTableReader
	blockIdx int
	entries  []Entry
	entryIdx int
	err      error
}

// Next advances and returns the next entry. Returns (_, false) at EOF or on error.
func (it *SSTableIterator) Next() (Entry, bool) {
	for {
		if it.entryIdx < len(it.entries) {
			e := it.entries[it.entryIdx]
			it.entryIdx++
			return e, true
		}
		it.blockIdx++
		if it.blockIdx >= len(it.r.index) {
			return Entry{}, false
		}
		entries, err := it.r.readBlock(it.blockIdx)
		if err != nil {
			it.err = err
			return Entry{}, false
		}
		it.entries = entries
		it.entryIdx = 0
	}
}

// Err returns any error encountered during iteration.
func (it *SSTableIterator) Err() error { return it.err }

// ---- Helpers ---------------------------------------------------------------

func decodeIndex(data []byte) ([]IndexEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("index: data too short")
	}
	count := binary.BigEndian.Uint32(data[:4])
	pos := 4
	index := make([]IndexEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("index: truncated at entry %d keyLen", i)
		}
		keyLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
		if pos+int(keyLen) > len(data) {
			return nil, fmt.Errorf("index: truncated at entry %d key", i)
		}
		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)
		if pos+8 > len(data) {
			return nil, fmt.Errorf("index: truncated at entry %d offset/length", i)
		}
		offset := binary.BigEndian.Uint32(data[pos : pos+4])
		length := binary.BigEndian.Uint32(data[pos+4 : pos+8])
		pos += 8
		index = append(index, IndexEntry{FirstKey: key, Offset: offset, Length: length})
	}
	return index, nil
}
