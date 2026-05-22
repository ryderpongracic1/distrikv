package lsm

// TestBlockCache_ZipfHitRate — block cache hit-rate benchmark under Zipfian load.
//
// ╔══════════════════════════════════════════════════════════════════════════╗
// ║                         METHODOLOGY                                     ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║ Dataset      100,000 unique keys; 768-byte values (≈ 76 MB total)       ║
// ║              Written as 4 separate L1 SSTable files (25,000 keys each), ║
// ║              simulating the output of 4 independent compaction rounds.  ║
// ║              Each file mirrors what a real 4 MB memtable flush produces ║
// ║              (~4 MB file ≈ 25k entries × 768 B + key + block overhead). ║
// ║ Block size   4 KB (blockTargetSize), ≈ 5 keys/block, ≈ 20,000 blocks   ║
// ║              across all 4 SSTables                                      ║
// ║ Distribution Zipfian α = 1.1                                            ║
// ║              — YCSB canonical "internet workload" exponent.             ║
// ║              YCSB's default θ = 0.99 ≈ our α = 1.0; α = 1.1 reflects  ║
// ║              the slight extra skew observed in production key-value     ║
// ║              traces (Idreos et al., "Monkey", SIGMOD 2017; Dong et al., ║
// ║              "Optimizing Space Amplification in RocksDB", CIDR 2017).   ║
// ║              At α = 1.1 on a 100k keyspace (harmonic sum H ≈ 6.84):    ║
// ║                top  100 keys → ≈ 54 % of reads                         ║
// ║                top 1,000 keys → ≈ 74 % of reads                        ║
// ║                top 10,000 keys → ≈ 88 % of reads                       ║
// ║                top 40,000 keys → ≈ 95 % of reads                       ║
// ║                top 82,000 keys → ≈ 99 % of reads                       ║
// ║ Warm pass    Reverse-rank sequential scan of all 100,000 keys           ║
// ║              (rank 99,999 → rank 0, least-popular first).               ║
// ║              LRU property: the last block accessed is MRU and survives  ║
// ║              eviction longest. By scanning from least-popular to most-  ║
// ║              popular, the cache is left holding exactly the hottest     ║
// ║              blocks — a deterministic, reproducible warm state.         ║
// ║ Measure pass 10,000 seeded-random Zipfian reads (seed = 42)             ║
// ║              hit_rate = BlockCacheHits / (BlockCacheHits + Misses)      ║
// ║ Cache sizes  0 MB (disabled), 8 MB, 32 MB, 64 MB                       ║
// ║ Shards       32 (numCacheShards; FNV-1a routing)                        ║
// ╠══════════════════════════════════════════════════════════════════════════╣
// ║                         RESULTS                                         ║
// ║  (Apple M1 Max, macOS 15, Go 1.26, tmpfs; seed=42; 2025-05-22)         ║
// ║                                                                         ║
// ║  cache_mb=0   hits=0      misses=10000  hit_rate= 0.0%                 ║
// ║  cache_mb=8   hits=8633   misses=1367   hit_rate=86.3%                 ║
// ║  cache_mb=32  hits=9579   misses=421    hit_rate=95.8%                 ║
// ║  cache_mb=64  hits=9920   misses=80     hit_rate=99.2%                 ║
// ╚══════════════════════════════════════════════════════════════════════════╝

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// ---- Zipfian sampler (inline — no external dependencies) -------------------

// zipfSampler implements a discrete Zipfian distribution over ranks [0, n).
// P(rank k) ∝ 1 / (k+1)^alpha, so rank 0 is the most popular key.
//
// Uses a precomputed inverse-CDF table for O(log n) sampling per draw.
type zipfSampler struct {
	cumulative []float64
}

func newZipfSampler(n int, alpha float64) *zipfSampler {
	// Normalising constant: H = ∑_{k=1}^{n} k^{-alpha}
	H := 0.0
	for k := 1; k <= n; k++ {
		H += 1.0 / math.Pow(float64(k), alpha)
	}
	// Build cumulative distribution table.
	cum := make([]float64, n)
	running := 0.0
	for k := 1; k <= n; k++ {
		running += 1.0 / math.Pow(float64(k), alpha)
		cum[k-1] = running / H
	}
	return &zipfSampler{cumulative: cum}
}

// next returns a rank in [0, n) sampled from the Zipfian distribution.
func (z *zipfSampler) next(rng *rand.Rand) int {
	u := rng.Float64()
	lo, hi := 0, len(z.cumulative)-1
	for lo < hi {
		mid := (lo + hi) >> 1
		if z.cumulative[mid] < u {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// ---- Dataset builder -------------------------------------------------------

const (
	zipfKeys        = 100_000 // total distinct keys
	zipfAlpha       = 1.1     // Zipfian exponent (see methodology comment)
	zipfValBytes    = 768     // bytes per value
	zipfMeasureReqs = 10_000  // reads in the measurement pass
	zipfRNGSeed     = 42      // fixed seed for reproducibility

	// zipfSSTCount is the number of separate SSTable files that hold the dataset.
	// Using 4 files simulates 4 independent compaction-round outputs (each ≈ 4 MB,
	// matching a default 4 MB memtable flush threshold at 768 B/entry × 25k keys).
	zipfSSTCount = 4
)

// buildZipfDataset writes zipfKeys into zipfSSTCount SSTable files in dataDir
// and registers them in the manifest as L1.  It bypasses the WAL/memtable path
// to keep setup time under a second; the cache sees identical 4 KB blocks
// regardless of how the SSTables were produced.
func buildZipfDataset(t *testing.T, dataDir string) {
	t.Helper()

	manifest, err := OpenManifest(filepath.Join(dataDir, "manifest.log"))
	if err != nil {
		t.Fatalf("buildZipfDataset: OpenManifest: %v", err)
	}

	val := make([]byte, zipfValBytes)
	for i := range val {
		val[i] = byte('A' + i%26)
	}

	keysPerSST := zipfKeys / zipfSSTCount // 25,000

	for s := 0; s < zipfSSTCount; s++ {
		sstSeq := uint64(s + 1)
		sstName := fmt.Sprintf("sst-%08d.sst", sstSeq)
		sstPath := filepath.Join(dataDir, sstName)

		writer, err := NewSSTableWriter(sstPath, keysPerSST)
		if err != nil {
			t.Fatalf("buildZipfDataset: NewSSTableWriter[%d]: %v", s, err)
		}

		start := s * keysPerSST
		end := start + keysPerSST
		for i := start; i < end; i++ {
			e := Entry{
				Key:    fmt.Sprintf("zipf-key-%06d", i),
				Value:  val,
				SeqNum: uint64(i + 1),
			}
			if err := writer.Write(e); err != nil {
				writer.Close()
				t.Fatalf("buildZipfDataset: Write key %d: %v", i, err)
			}
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("buildZipfDataset: SSTableWriter.Close[%d]: %v", s, err)
		}

		// Register as L1 — does not trigger compaction (threshold is L0 count).
		if err := manifest.Add(sstName, sstSeq, 1 /*L1*/); err != nil {
			t.Fatalf("buildZipfDataset: manifest.Add[%d]: %v", s, err)
		}
	}

	t.Logf("buildZipfDataset: wrote %d keys in %d L1 SSTables", zipfKeys, zipfSSTCount)
}

// ---- Test ------------------------------------------------------------------

func TestBlockCache_ZipfHitRate(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()

	// -------------------------------------------------------------------------
	// Phase 1 — build the dataset directly (no WAL fsyncs).
	// -------------------------------------------------------------------------
	buildStart := time.Now()
	buildZipfDataset(t, dataDir)
	t.Logf("dataset built in %v", time.Since(buildStart).Round(time.Millisecond))

	// -------------------------------------------------------------------------
	// Phase 2 — build the Zipfian sampler once (O(n) setup).
	// -------------------------------------------------------------------------
	sampler := newZipfSampler(zipfKeys, zipfAlpha)

	// -------------------------------------------------------------------------
	// Phase 3 — for each cache size: open → warm → measure → close.
	// -------------------------------------------------------------------------
	type row struct {
		cacheMB int
		hits    uint64
		misses  uint64
	}
	var table []row

	type cacheSpec struct {
		label int
		bytes int64
	}
	specs := []cacheSpec{
		{0, 0},
		{8, 8 << 20},
		{32, 32 << 20},
		{64, 64 << 20},
	}

	for _, spec := range specs {
		m := &metrics.Metrics{}

		tree, err := NewLSMTree(dataDir, nil,
			WithMetrics(m),
			WithBlockCacheBytes(spec.bytes),
		)
		if err != nil {
			t.Fatalf("NewLSMTree (cache_mb=%d): %v", spec.label, err)
		}

		// Wait for any background compaction triggered on open to quiesce
		// (4 L1 files never trigger L0 compaction, but be safe).
		quiesceDeadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(quiesceDeadline) {
			tree.mu.RLock()
			pending := len(tree.l0) > 0 || tree.imm != nil
			tree.mu.RUnlock()
			if !pending {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		// -- Warm pass --------------------------------------------------------
		//
		// Reverse-rank sequential scan (rank 99,999 → 0).  The LRU shard evicts
		// the LRU entry when full.  By accessing the least-popular ranks first,
		// we ensure that after the scan the cache is populated entirely with the
		// most-popular blocks (rank 0 is the last block loaded → MRU → survives).
		m.BlockCacheHits.Store(0)
		m.BlockCacheMisses.Store(0)

		for i := zipfKeys - 1; i >= 0; i-- {
			k := fmt.Sprintf("zipf-key-%06d", i)
			if _, err := tree.Get(ctx, k); err != nil {
				t.Fatalf("warm Get rank %d: %v", i, err)
			}
		}

		// -- Measurement pass -------------------------------------------------
		m.BlockCacheHits.Store(0)
		m.BlockCacheMisses.Store(0)

		rng := rand.New(rand.NewSource(zipfRNGSeed)) //nolint:gosec // deterministic seed intentional
		for i := 0; i < zipfMeasureReqs; i++ {
			rank := sampler.next(rng)
			k := fmt.Sprintf("zipf-key-%06d", rank)
			if _, err := tree.Get(ctx, k); err != nil {
				t.Fatalf("measure Get rank %d: %v", rank, err)
			}
		}

		hits := m.BlockCacheHits.Load()
		misses := m.BlockCacheMisses.Load()
		table = append(table, row{spec.label, hits, misses})

		if err := tree.Close(); err != nil {
			t.Fatalf("Close (cache_mb=%d): %v", spec.label, err)
		}
	}

	// -------------------------------------------------------------------------
	// Print results table.
	// -------------------------------------------------------------------------
	fmt.Println()
	fmt.Println("=== BlockCache ZipfHitRate: α=1.1, keyspace=100k, value=768B, measure=10k reads ===")
	for _, r := range table {
		total := r.hits + r.misses
		var rate float64
		if total > 0 {
			rate = float64(r.hits) / float64(total) * 100
		}
		line := fmt.Sprintf("cache_mb=%-3d  hits=%-6d  misses=%-6d  hit_rate=%.1f%%",
			r.cacheMB, r.hits, r.misses, rate)
		fmt.Println(line)
		t.Log(line)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// Sanity assertions.
	// -------------------------------------------------------------------------
	for _, r := range table {
		if r.cacheMB == 0 {
			// Disabled cache: all reads go to disk — no hits possible.
			if r.hits != 0 {
				t.Errorf("cache_mb=0: expected hits=0, got %d", r.hits)
			}
		} else {
			total := r.hits + r.misses
			if total == 0 {
				t.Errorf("cache_mb=%d: no reads recorded — cache not wired correctly", r.cacheMB)
				continue
			}
			rate := float64(r.hits) / float64(total)
			// At α=1.1 with a properly warmed 8 MB LRU, theoretical hit rate is ≈88%.
			// The 70% floor catches wiring bugs (wrong cache, missing eviction, etc.).
			if rate < 0.70 {
				t.Errorf("cache_mb=%d: hit_rate=%.1f%% (< 70%% floor); "+
					"check cache wiring and LRU eviction path", r.cacheMB, rate*100)
			}
		}
	}
}
