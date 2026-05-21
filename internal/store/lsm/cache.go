// Package lsm — BlockCache: sharded byte-bounded LRU cache for SSTable data blocks.
//
// # Design
//
// Each cached entry maps (sstable-path, block-offset) → raw block bytes.
// Storing raw bytes rather than decoded []Entry keeps the implementation simple:
// block decoding is CPU-cheap (a few hundred bytes of binary parsing) so the
// primary win is eliminating the f.ReadAt syscall on every hot-key read.
//
// # Sharding
//
// numCacheShards independent shards each hold an equal fraction of the total
// capacity.  Shard selection is based on FNV-1a(path + offset) so entries from
// the same SSTable are spread across shards, reducing lock contention when
// many goroutines read the same file.
//
// # Eviction
//
// Each shard is an LRU using container/list.  On Put, if the shard exceeds its
// byte budget, the least-recently-used entry is removed.
//
// # Thread safety
//
// Each shard is independently locked.  A nil *BlockCache is safe — all methods
// are no-ops.
package lsm

import (
	"container/list"
	"hash/fnv"
	"sync"
)

const numCacheShards = 32

// BlockCache is a sharded byte-bounded LRU cache for SSTable data blocks.
// A nil *BlockCache is always safe: Get returns (nil, false) and Put is a no-op.
type BlockCache struct {
	shards   [numCacheShards]cacheShard
	totalCap int64
}

type cacheKey struct {
	path   string
	offset uint32
}

type cacheEntry struct {
	key  cacheKey
	data []byte
}

type cacheShard struct {
	mu    sync.Mutex
	ll    *list.List
	index map[cacheKey]*list.Element
	bytes int64
	cap   int64
}

// NewBlockCache creates a BlockCache with the given total capacity in bytes.
// The capacity is divided evenly across numCacheShards shards.
// Returns nil when maxBytes <= 0, effectively disabling caching.
func NewBlockCache(maxBytes int64) *BlockCache {
	if maxBytes <= 0 {
		return nil
	}
	bc := &BlockCache{totalCap: maxBytes}
	perShard := maxBytes / numCacheShards
	if perShard < 1 {
		perShard = 1
	}
	for i := range bc.shards {
		bc.shards[i].ll = list.New()
		bc.shards[i].index = make(map[cacheKey]*list.Element)
		bc.shards[i].cap = perShard
	}
	return bc
}

// Get returns a copy of the cached block data for (path, offset).
// Returns (nil, false) on a cache miss.
func (c *BlockCache) Get(path string, offset uint32) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	s := c.shardFor(path, offset)
	s.mu.Lock()
	el, ok := s.index[cacheKey{path, offset}]
	if !ok {
		s.mu.Unlock()
		return nil, false
	}
	s.ll.MoveToFront(el)
	src := el.Value.(*cacheEntry).data
	out := make([]byte, len(src))
	copy(out, src)
	s.mu.Unlock()
	return out, true
}

// Put inserts or updates the block data for (path, offset).
// If the shard is over capacity, LRU entries are evicted until it fits.
func (c *BlockCache) Put(path string, offset uint32, data []byte) {
	if c == nil || len(data) == 0 {
		return
	}
	s := c.shardFor(path, offset)
	k := cacheKey{path, offset}

	cp := make([]byte, len(data))
	copy(cp, data)

	s.mu.Lock()
	if el, ok := s.index[k]; ok {
		old := el.Value.(*cacheEntry)
		s.bytes += int64(len(cp)) - int64(len(old.data))
		old.data = cp
		s.ll.MoveToFront(el)
		s.mu.Unlock()
		return
	}
	el := s.ll.PushFront(&cacheEntry{key: k, data: cp})
	s.index[k] = el
	s.bytes += int64(len(cp))
	for s.bytes > s.cap && s.ll.Len() > 1 {
		s.evictLRULocked()
	}
	s.mu.Unlock()
}

// Evict removes all cached blocks for the given SSTable path.
// Called when an SSTable is deleted during compaction so its memory is
// reclaimed immediately rather than waiting for LRU pressure.
func (c *BlockCache) Evict(path string) {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		var stale []*list.Element
		for el := s.ll.Front(); el != nil; el = el.Next() {
			if el.Value.(*cacheEntry).key.path == path {
				stale = append(stale, el)
			}
		}
		for _, el := range stale {
			s.removeLocked(el)
		}
		s.mu.Unlock()
	}
}

func (s *cacheShard) evictLRULocked() {
	el := s.ll.Back()
	if el != nil {
		s.removeLocked(el)
	}
}

func (s *cacheShard) removeLocked(el *list.Element) {
	s.ll.Remove(el)
	e := el.Value.(*cacheEntry)
	delete(s.index, e.key)
	s.bytes -= int64(len(e.data))
}

func (c *BlockCache) shardFor(path string, offset uint32) *cacheShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	var b [4]byte
	b[0] = byte(offset)
	b[1] = byte(offset >> 8)
	b[2] = byte(offset >> 16)
	b[3] = byte(offset >> 24)
	_, _ = h.Write(b[:])
	return &c.shards[h.Sum32()%numCacheShards]
}
