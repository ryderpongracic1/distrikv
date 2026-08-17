package lsm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// ---------------------------------------------------------------------------
// Recovery availability: a store that reopens on accumulated state must accept
// writes promptly.
//
// Field symptom (3-node docker cluster, M4 Pro/colima): a cluster whose volumes
// held ~200k bench writes plus chaos churn served 287 ops / 30s with 164 errors
// for 80+ minutes after container recreation, with no faults injected. A
// clean-slate cluster on the identical binary served 115,170 ops / 30s with 0
// errors. The difference was the data volume, not the restart.
//
// Mechanism the tests below pin:
//
//   - Write-stall backpressure keys off the live L0 file count, which is
//     restored from the manifest at open. A store that was closed with an L0
//     backlog therefore reopens already stalled.
//   - Compaction is only ever armed by a memtable flush (flushMemtable) or by a
//     finished compaction that left work behind (installCompactionResult).
//     Neither happens at open.
//   - So the only actor that can clear the stall is blocked behind the stall:
//     writes stall → no memtable fills → no flush → no compaction → L0 never
//     drains. The hard-stop branch waits on a condition variable nothing will
//     broadcast, and (before the fix) ignored ctx entirely, so the write never
//     returns at all.
//
// The dangerous zone is L0 ≥ l0SlowThreshold at open. Below it the store serves
// normally and self-heals on the first flush.
// ---------------------------------------------------------------------------

// buildL0Backlog creates a store in dir holding at least wantFiles live L0
// SSTables and closes it, leaving the directory in the state a node inherits
// from a data volume whose compaction fell behind.
//
// The build itself runs with compaction disabled and stall backpressure lifted
// (both thresholds far above wantFiles), so the backlog is produced
// deterministically instead of by racing flushes against merges — and so the
// build cannot stall on the very defect under test. The reopen the caller
// performs afterwards is what uses production settings.
func buildL0Backlog(t *testing.T, dir string, wantFiles int) {
	t.Helper()

	headroom := 10*wantFiles + 1
	tree, err := NewLSMTree(dir, nil,
		WithMaxMemBytes(1<<10),                  // ~1 KB: flush every few writes
		WithCompactThreshold(headroom),          // never compact during the build
		WithL0StallConfig(headroom, headroom+1), // never stall during the build
	)
	if err != nil {
		t.Fatalf("build: NewLSMTree: %v", err)
	}

	ctx := context.Background()
	val := make([]byte, 256)
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; int(tree.l0Count.Load()) < wantFiles; i++ {
		if time.Now().After(deadline) {
			tree.Close()
			t.Fatalf("build: only %d L0 files after 30s; want %d", tree.l0Count.Load(), wantFiles)
		}
		if _, err := tree.Put(ctx, fmt.Sprintf("backlog-%06d", i), val); err != nil {
			tree.Close()
			t.Fatalf("build: Put %d: %v", i, err)
		}
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("build: Close: %v", err)
	}

	// Verify from the manifest, without opening a tree (which would stall).
	got := liveL0Files(t, dir)
	if got < wantFiles {
		t.Fatalf("build: manifest reports %d live L0 files; want ≥ %d", got, wantFiles)
	}
	t.Logf("build: closed with %d live L0 SSTables in the manifest", got)
}

// liveL0Files counts the level-0 SSTables the manifest reports as live.
func liveL0Files(t *testing.T, dir string) int {
	t.Helper()
	mf, err := OpenManifest(dir + "/manifest.log")
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	n := 0
	for _, ev := range mf.LiveFiles() {
		if ev.Level == 0 {
			n++
		}
	}
	return n
}

// TestRecovery_ReopenWithL0Backlog_AcceptsWrites is the deterministic repro of
// the field symptom: a store closed with an L0 backlog at or above the hard-stop
// threshold must still accept a write after reopening.
//
// Before the fix this hangs forever rather than failing — the write parks on
// l0Drained, which only a completed compaction broadcasts, and no compaction is
// ever armed. The write is therefore run on a goroutine so the test can report
// the wedge instead of timing out the whole package.
func TestRecovery_ReopenWithL0Backlog_AcceptsWrites(t *testing.T) {
	dir := t.TempDir()

	// One file past the hard-stop threshold, with production defaults on reopen.
	buildL0Backlog(t, dir, defaultL0StopThreshold)

	m := &metrics.Metrics{}
	tree, err := NewLSMTree(dir, nil, WithMetrics(m))
	if err != nil {
		t.Fatalf("reopen: NewLSMTree: %v", err)
	}
	t.Cleanup(func() { _ = tree.Close() })

	if got := tree.l0Count.Load(); got < int32(defaultL0StopThreshold) {
		t.Fatalf("reopen: l0Count=%d; want ≥ %d (the backlog must survive the reopen)",
			got, defaultL0StopThreshold)
	}

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- discardSeq(tree.Put(context.Background(), "post-recovery-key", []byte("v")))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reopen: first write failed: %v", err)
		}
		t.Logf("reopen: first write accepted after %v (stalls=%d, %dµs)",
			time.Since(start), m.WriteStallCount.Load(), m.WriteStallMicros.Load())
	case <-time.After(30 * time.Second):
		t.Fatalf("reopen: no write accepted within 30s — the store reopened write-dead "+
			"(l0Count=%d, compactions=%d): nothing arms compaction at open, so the stall "+
			"can never clear", tree.l0Count.Load(), m.CompactionsTotal.Load())
	}

	// The backlog must actually drain, not merely admit one write.
	drained := false
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if tree.l0Count.Load() < int32(defaultL0SlowThreshold) {
			drained = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !drained {
		t.Fatalf("reopen: L0 still at %d after 30s; backlog never drained below the soft "+
			"threshold (%d)", tree.l0Count.Load(), defaultL0SlowThreshold)
	}
	t.Logf("reopen: L0 drained to %d after %v total", tree.l0Count.Load(), time.Since(start))
}

// TestRecovery_HardStopRespectsContext pins the second half of the defect: a
// write that is hard-stopped must answer its caller instead of blocking
// indefinitely, and must say *why* it failed.
//
// This is what a replica owes its primary. The primary bounds the replication
// RPC; a replica that sits in the stall until that deadline fires reports as
// DeadlineExceeded, which is indistinguishable from a dead node. ErrWriteStalled
// arriving before the deadline says "alive, overloaded" — a different fault with
// a different operator response.
func TestRecovery_HardStopRespectsContext(t *testing.T) {
	dir := t.TempDir()
	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	t.Cleanup(func() { _ = tree.Close() })

	// Park the engine in hard stop without writing any data, so no background
	// flush can move the count back down under us (same device as
	// TestLCS_WriteStallMetrics).
	tree.l0Count.Store(int32(tree.l0StopThreshold))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- tree.maybeStallWrite(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("maybeStallWrite returned nil while hard-stopped")
		}
		t.Logf("hard stop returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		t.Fatal("maybeStallWrite never returned while hard-stopped: the hard-stop branch " +
			"ignores context cancellation, so a stalled write is unbounded")
	}
}

// TestRecovery_HardStopReturnsStallError is the companion assertion: once the
// stall budget is exhausted with no cancellation of our own, the caller gets
// ErrWriteStalled rather than a generic failure or an unbounded wait.
func TestRecovery_HardStopReturnsStallError(t *testing.T) {
	dir := t.TempDir()
	tree, err := NewLSMTree(dir, nil)
	if err != nil {
		t.Fatalf("NewLSMTree: %v", err)
	}
	t.Cleanup(func() { _ = tree.Close() })

	// Shrink the budget so the test does not pay the production value.
	tree.maxStallWait = 150 * time.Millisecond
	tree.l0Count.Store(int32(tree.l0StopThreshold))

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- discardSeq(tree.Put(context.Background(), "stalled-key", []byte("v"))) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrWriteStalled) {
			t.Fatalf("Put while hard-stopped returned %v; want ErrWriteStalled", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("Put took %v to report ErrWriteStalled; budget was 150ms", elapsed)
		}
		t.Logf("Put reported %v after %v", err, time.Since(start))
	case <-time.After(10 * time.Second):
		t.Fatal("Put never returned while hard-stopped; want ErrWriteStalled within the stall budget")
	}
}

// ---------------------------------------------------------------------------
// Bench-scale harness (deliverable 1)
//
// Builds the state the field cluster actually held — ~200k keys × 256B — then
// reopens and measures (a) time until the store accepts a write and (b) write
// latency during the recovery window. Off by default because it writes ~50 MB
// and takes minutes; run it with:
//
//	DISTRIKV_RECOVERY_REPRO=1 go test ./internal/store/lsm \
//	    -run TestRecovery_BenchScaleReopen -v -timeout 30m
//
// Knobs: DISTRIKV_RECOVERY_KEYS (default 200000),
//        DISTRIKV_RECOVERY_COMPACT_THRESHOLD (default 4 = production).
//
// Raising the compaction threshold emulates the field's compaction backlog: on
// colima/virtiofs a compaction pays six manifest fsyncs and falls behind the
// flush rate, which is how the volume ends up closed with a deep L0. On a local
// ext4 disk compaction usually keeps up, so the default run may close with a
// shallow L0 and show no stall — the harness reports the L0 depth it actually
// achieved so the measurement is never read as more than it is.
// ---------------------------------------------------------------------------

func TestRecovery_BenchScaleReopen(t *testing.T) {
	if os.Getenv("DISTRIKV_RECOVERY_REPRO") != "1" {
		t.Skip("set DISTRIKV_RECOVERY_REPRO=1 to run the bench-scale recovery harness")
	}

	keys := envInt(t, "DISTRIKV_RECOVERY_KEYS", 200_000)
	compactThreshold := envInt(t, "DISTRIKV_RECOVERY_COMPACT_THRESHOLD", defaultCompactN)
	const valueSize = 256

	dir := t.TempDir()
	ctx := context.Background()
	val := make([]byte, valueSize)

	// ---- Build phase: bench-scale state, production memtable size ----------
	//
	// The build is instrumented to produce a target on-disk state, not to model
	// production write behaviour: when the compaction trigger is raised to
	// emulate a backlog, the stall thresholds are lifted with it so the build
	// cannot wedge on the very defect under test. Only the reopen below runs
	// with production settings, and that is what the measurement is about.
	buildSlow, buildStop := defaultL0SlowThreshold, defaultL0StopThreshold
	if compactThreshold >= buildSlow {
		buildSlow, buildStop = 2*compactThreshold, 3*compactThreshold
	}

	buildMetrics := &metrics.Metrics{}
	tree, err := NewLSMTree(dir, nil,
		WithMetrics(buildMetrics),
		WithCompactThreshold(compactThreshold),
		WithL0StallConfig(buildSlow, buildStop),
	)
	if err != nil {
		t.Fatalf("build: NewLSMTree: %v", err)
	}

	buildStart := time.Now()
	for i := 0; i < keys; i++ {
		if _, err := tree.Put(ctx, fmt.Sprintf("bench-%08d", i), val); err != nil {
			tree.Close()
			t.Fatalf("build: Put %d: %v", i, err)
		}
	}
	buildDur := time.Since(buildStart)
	l0BeforeClose := tree.l0Count.Load()
	if err := tree.Close(); err != nil {
		t.Fatalf("build: Close: %v", err)
	}

	l0AtOpen := liveL0Files(t, dir)
	t.Logf("build: %d keys × %dB in %v (%.0f writes/s); L0 depth %d before close, "+
		"%d live L0 files in manifest; compactions=%d, stalls=%d "+
		"(build config: compact≥%d, stall %d/%d)",
		keys, valueSize, buildDur, float64(keys)/buildDur.Seconds(),
		l0BeforeClose, l0AtOpen,
		buildMetrics.CompactionsTotal.Load(), buildMetrics.WriteStallCount.Load(),
		compactThreshold, buildSlow, buildStop)
	t.Logf("build: hard-stop threshold is %d, soft-stall threshold is %d — "+
		"a reopen is expected to stall iff live L0 ≥ %d",
		defaultL0StopThreshold, defaultL0SlowThreshold, defaultL0SlowThreshold)

	// ---- Reopen phase: production defaults --------------------------------
	openMetrics := &metrics.Metrics{}
	openStart := time.Now()
	tree2, err := NewLSMTree(dir, nil, WithMetrics(openMetrics))
	if err != nil {
		t.Fatalf("reopen: NewLSMTree: %v", err)
	}
	openDur := time.Since(openStart)
	t.Cleanup(func() { _ = tree2.Close() })
	t.Logf("reopen: NewLSMTree returned in %v (WAL replay + manifest load); l0Count=%d",
		openDur, tree2.l0Count.Load())

	// (a) Time until the store accepts a write.
	firstStart := time.Now()
	firstDone := make(chan error, 1)
	go func() { firstDone <- discardSeq(tree2.Put(ctx, "recovery-probe", val)) }()

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("reopen: first write failed after %v: %v", time.Since(firstStart), err)
		}
		t.Logf("MEASUREMENT time-to-first-accepted-write = %v", time.Since(firstStart))
	case <-time.After(2 * time.Minute):
		t.Fatalf("MEASUREMENT time-to-first-accepted-write > 2m — store reopened write-dead "+
			"(l0Count=%d, compactions=%d, stalls=%d)",
			tree2.l0Count.Load(), openMetrics.CompactionsTotal.Load(),
			openMetrics.WriteStallCount.Load())
	}

	// (b) Write latency through the rest of the recovery window.
	const probeWrites = 2000
	lat := make([]time.Duration, 0, probeWrites)
	probeStart := time.Now()
	for i := 0; i < probeWrites; i++ {
		w := time.Now()
		if _, err := tree2.Put(ctx, fmt.Sprintf("recovery-%06d", i), val); err != nil {
			t.Fatalf("reopen: probe write %d failed: %v", i, err)
		}
		lat = append(lat, time.Since(w))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("MEASUREMENT recovery-window writes: n=%d over %v — p50=%v p99=%v max=%v",
		len(lat), time.Since(probeStart), lat[len(lat)/2], lat[len(lat)*99/100], lat[len(lat)-1])
	t.Logf("reopen: l0Count=%d compactions=%d stalls=%d (%dµs)",
		tree2.l0Count.Load(), openMetrics.CompactionsTotal.Load(),
		openMetrics.WriteStallCount.Load(), openMetrics.WriteStallMicros.Load())
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q: want a positive integer", name, v)
	}
	return n
}
