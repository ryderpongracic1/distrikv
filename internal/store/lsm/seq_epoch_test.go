package lsm

// The incarnation epoch: the high field of every sequence number this engine
// issues (see seq.go).
//
// What these tests are protecting is not the arithmetic — it is the one property
// the pre-epoch counter could not have. A write counter is seeded from local
// state, so a primary that comes back on storage it no longer has restarts near
// zero while its replicas still hold the sequences its previous incarnation
// assigned. Its writes then lose apply-if-newer and are discarded *and ACKed*.
// TestWipedPrimaryWritesSurviveTheReplicaComparison is that scenario end to end,
// and the rest pin the pieces it depends on.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/metrics"
)

// openEpochTree opens an engine in dir, closing it when the test ends. Unlike
// newIfNewerTree it takes the directory, because the epoch tests reopen the same
// one — that is the whole subject.
func openEpochTree(t *testing.T, dir string, opts ...Option) *LSMTree {
	t.Helper()
	l, err := NewLSMTree(dir, slog.New(slog.NewTextHandler(io.Discard, nil)), opts...)
	if err != nil {
		t.Fatalf("NewLSMTree(%q): %v", dir, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// TestSeqSplitsIntoEpochAndCounter pins the encoding and, more importantly, the
// consequence of the layout: comparing the composite compares (epoch, counter)
// lexicographically, so every existing `seq > stored` comparison orders across
// epoch boundaries for free and nothing on the wire or on disk has to change.
func TestSeqSplitsIntoEpochAndCounter(t *testing.T) {
	for _, tc := range []struct{ epoch, counter uint64 }{
		{0, 0}, {0, 1}, {1, 0}, {1, 7}, {maxSeqEpoch, maxSeqCounter},
	} {
		seq := makeSeq(tc.epoch, tc.counter)
		if got := seqEpoch(seq); got != tc.epoch {
			t.Errorf("seqEpoch(makeSeq(%d,%d)) = %d, want %d", tc.epoch, tc.counter, got, tc.epoch)
		}
	}

	// The property the wipe fix rests on: the lowest sequence of a higher epoch
	// still outranks the highest sequence of a lower one. A wiped primary comes
	// back with a fresh counter, so this is exactly the comparison its first
	// write has to win.
	if got, want := makeSeq(2, 0), makeSeq(1, maxSeqCounter); !(got > want) {
		t.Errorf("makeSeq(2,0)=%d must outrank makeSeq(1,max)=%d", got, want)
	}
	if got, want := epochFloor(3), makeSeq(3, 0); got != want {
		t.Errorf("epochFloor(3) = %d, want %d", got, want)
	}

	// Pre-upgrade sequences are bare counters, which read as epoch 0 — below
	// every epoch this code assigns. That is the true ordering: they were written
	// first.
	if seqEpoch(12345) != 0 {
		t.Errorf("a pre-upgrade sequence must read as epoch 0, got %d", seqEpoch(12345))
	}
}

// TestNextEpochTakesTheHigherOfClockAndRecord covers the two sources an epoch can
// come from and why neither alone is enough: the clock is the only one a wiped
// directory still has, and the record is the only one that advances when the
// clock does not.
func TestNextEpochTakesTheHigherOfClockAndRecord(t *testing.T) {
	// at returns the instant whose epoch is ms — so the tests below can talk in
	// epochs rather than in dates.
	at := func(ms int64) time.Time { return time.UnixMilli(epochOriginUnixMilli + ms) }

	// Fresh or wiped directory: nothing recorded, so the clock is the epoch.
	if got := nextEpoch(0, false, at(1000)); got != 1000 {
		t.Errorf("no record: epoch = %d, want the clock (1000)", got)
	}

	// Normal restart: the clock has moved on, so it wins.
	if got := nextEpoch(900, true, at(1000)); got != 1000 {
		t.Errorf("clock ahead: epoch = %d, want 1000", got)
	}

	// Two opens inside one millisecond, or a clock stepped backwards: the record
	// is what keeps the epoch from repeating. Reusing the predecessor's epoch would
	// put this incarnation's writes in the same epoch as the ones it must outrank,
	// leaving the comparison to the counter alone — the pre-epoch behaviour.
	if got := nextEpoch(1000, true, at(1000)); got != 1001 {
		t.Errorf("same millisecond: epoch = %d, want 1001", got)
	}
	if got := nextEpoch(5000, true, at(1000)); got != 5001 {
		t.Errorf("clock stepped back: epoch = %d, want 5001", got)
	}

	// A clock before the origin cannot produce an epoch, so the record is all
	// there is. Without one the answer is 0 — the epoch every pre-upgrade
	// sequence carries, and the most conservative value available.
	if got := nextEpoch(0, false, time.UnixMilli(0)); got != 0 {
		t.Errorf("clock before the origin: epoch = %d, want 0", got)
	}
	if got := nextEpoch(77, true, time.UnixMilli(0)); got != 78 {
		t.Errorf("clock before the origin with a record: epoch = %d, want 78", got)
	}

	// Saturation (the 2160s, for a 42-bit millisecond field): the epoch stops
	// rather than wrapping, because a wrap would order this incarnation *below*
	// its predecessor — the one thing the field exists to prevent. The scheme
	// degrades to the pre-epoch behaviour instead.
	if got := nextEpoch(maxSeqEpoch, true, at(1000)); got != maxSeqEpoch {
		t.Errorf("saturated: epoch = %d, want %d", got, maxSeqEpoch)
	}
}

// TestOpenAdvancesTheIncarnationEpoch pins the durable half: an open records its
// epoch, and the next open comes back above it — so a restart's writes outrank
// the previous incarnation's even when the clock has not visibly moved.
func TestOpenAdvancesTheIncarnationEpoch(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	first := openEpochTree(t, dir)
	firstEpoch := first.epoch.Load()
	if firstEpoch == 0 {
		t.Fatal("a fresh open must stamp a non-zero incarnation epoch")
	}
	seq1, err := first.Put(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if seqEpoch(seq1) != firstEpoch {
		t.Errorf("write issued in epoch %d, want %d", seqEpoch(seq1), firstEpoch)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := openEpochTree(t, dir)
	if second.epoch.Load() <= firstEpoch {
		t.Fatalf("reopened epoch %d must be above the recorded %d",
			second.epoch.Load(), firstEpoch)
	}
	seq2, err := second.Put(ctx, "k", []byte("v2"))
	if err != nil {
		t.Fatalf("Put after reopen: %v", err)
	}
	if seq2 <= seq1 {
		t.Errorf("post-restart write %d must outrank pre-restart write %d", seq2, seq1)
	}
}

// TestPreUpgradeSequencesOrderBelowEpochStampedWrites is the compatibility case:
// a data directory written before the epoch existed holds bare counters, and an
// upgraded cluster must not misorder them.
//
// It holds by construction rather than by special-casing — a bare counter reads
// as epoch 0, which is below every epoch this code assigns — but "by
// construction" is exactly the kind of claim that stops being true when someone
// changes the encoding, so it is pinned here.
//
// It also pins the rolling-upgrade reading of the regression counter: an
// un-upgraded peer's bare counter arriving at an epoch-stamped replica *is*
// classified as an epoch regression, because it is one. That is expected traffic
// for the length of an upgrade, which is why the counter is documented with three
// causes rather than one.
func TestPreUpgradeSequencesOrderBelowEpochStampedWrites(t *testing.T) {
	ctx := context.Background()
	m := &metrics.Metrics{}
	l := openEpochTree(t, t.TempDir(), WithMetrics(m))

	// An in-flight write from a peer that predates the epoch: a small bare
	// counter, which is what every pre-upgrade sequence looks like.
	applied, err := l.PutIfNewer(ctx, "k", []byte("pre-upgrade"), 42)
	if err != nil {
		t.Fatalf("PutIfNewer(pre-upgrade): %v", err)
	}
	if !applied {
		t.Fatal("a pre-upgrade sequence must apply to a key with no stored version")
	}

	// This node's own next write is epoch-stamped and must outrank it.
	seq, err := l.Put(ctx, "k", []byte("post-upgrade"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if seq <= 42 {
		t.Fatalf("epoch-stamped write %d must outrank pre-upgrade sequence 42", seq)
	}
	if got := getOrEmpty(t, l, "k"); got != "post-upgrade" {
		t.Errorf("stored value = %q, want post-upgrade", got)
	}

	// And a second pre-upgrade arrival now loses — correctly, it is older — but it
	// must lose as a *stale* write rather than as an epoch regression. The two are
	// counted separately because only one of them means something is wrong, and a
	// mixed-version cluster would otherwise light up the alarming counter for
	// entirely ordinary traffic.
	applied, err = l.PutIfNewer(ctx, "k", []byte("pre-upgrade-2"), 43)
	if err != nil {
		t.Fatalf("PutIfNewer(pre-upgrade-2): %v", err)
	}
	if applied {
		t.Error("a pre-upgrade sequence below the stored version must be discarded")
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 1 {
		t.Errorf("replica_writes_discarded = %d, want 1", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 1 {
		t.Errorf("a discard against an epoch-stamped version is an epoch regression: "+
			"replica_writes_epoch_regressed = %d, want 1", got)
	}
}

// TestManifestEpochSurvivesRewritesAndStaysSingle pins the durable half of the
// epoch at the manifest boundary: it survives the rewrites a flush or compaction
// performs, it reads back as the newest value, and repeated opens leave one record
// rather than one per open — a manifest that is rewritten whole on every flush must
// not accumulate them.
func TestManifestEpochSurvivesRewritesAndStaysSingle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.log")
	m, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}

	if _, ok := m.Epoch(); ok {
		t.Error("a fresh manifest must report no recorded epoch — that is how a wiped " +
			"directory is distinguished from one that recorded epoch 0")
	}

	for _, e := range []uint64{7, 8, 9} {
		if err := m.SetEpoch(e); err != nil {
			t.Fatalf("SetEpoch(%d): %v", e, err)
		}
		if err := m.Add("sst-0000000"+string(rune('0'+e))+".sst", e, 0, e*100); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	reopened, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("re-OpenManifest: %v", err)
	}
	got, ok := reopened.Epoch()
	if !ok || got != 9 {
		t.Errorf("recorded epoch = (%d, %v), want (9, true)", got, ok)
	}
	var epochEvents int
	for _, ev := range reopened.events {
		if ev.Type == "epoch" {
			epochEvents++
		}
	}
	if epochEvents != 1 {
		t.Errorf("manifest holds %d epoch records, want 1", epochEvents)
	}
	if n := len(reopened.LiveFiles()); n != 3 {
		t.Errorf("epoch records must not disturb the live-file set: got %d files, want 3", n)
	}
}

// TestWipedPrimaryWritesSurviveTheReplicaComparison is finding 1, end to end.
//
// A primary writes, its replica stores the sequence it assigned, and then the
// primary comes back on an empty data directory — volume recreated, node
// redeployed. Before the epoch its counter restarted near zero, so every write it
// then made lost the replica's comparison and was discarded *and ACKed*: the
// client saw 200 and the replica kept its pre-wipe value, forever, with the
// catch-up pass shipping the same low sequences and being discarded too.
//
// The two engines here are the two nodes: writes go through the primary's Put
// (which assigns the sequence) and are handed to the replica's PutIfNewer exactly
// as the replication RPC does.
//
// The reborn primary's clock is driven rather than read, because what is under
// test is the epoch advancing across the wipe — not whether a `RemoveAll` plus an
// open happens to take a millisecond. Two seconds is a redeploy's order of
// magnitude; the same-epoch case that a *faster* wipe would hit is covered by
// TestNextEpochTakesTheHigherOfClockAndRecord and stated as a limit in
// seq.go.
func TestWipedPrimaryWritesSurviveTheReplicaComparison(t *testing.T) {
	ctx := context.Background()
	primaryDir, replicaDir := t.TempDir(), t.TempDir()
	m := &metrics.Metrics{}

	born := time.Now()
	primary := openEpochTree(t, primaryDir, withClock(func() time.Time { return born }))
	replica := openEpochTree(t, replicaDir, WithMetrics(m))

	// Enough writes that the pre-wipe counter is unmistakably above where a
	// wiped node's counter would restart.
	var lastSeq uint64
	for i := 0; i < 50; i++ {
		seq, err := primary.Put(ctx, "k", []byte("before-wipe"))
		if err != nil {
			t.Fatalf("primary Put: %v", err)
		}
		if _, err := replica.PutIfNewer(ctx, "k", []byte("before-wipe"), seq); err != nil {
			t.Fatalf("replica PutIfNewer: %v", err)
		}
		lastSeq = seq
	}

	// The wipe: the data directory is gone, so nothing local records what
	// sequences this node ever issued.
	if err := primary.Close(); err != nil {
		t.Fatalf("primary Close: %v", err)
	}
	if err := os.RemoveAll(primaryDir); err != nil {
		t.Fatalf("wipe primary dir: %v", err)
	}
	if err := os.MkdirAll(primaryDir, 0o755); err != nil {
		t.Fatalf("recreate primary dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(primaryDir, "manifest.log")); !os.IsNotExist(err) {
		t.Fatalf("the wiped directory must hold no manifest, got err=%v", err)
	}

	reborn := openEpochTree(t, primaryDir,
		withClock(func() time.Time { return born.Add(2 * time.Second) }))
	seq, err := reborn.Put(ctx, "k", []byte("after-wipe"))
	if err != nil {
		t.Fatalf("reborn Put: %v", err)
	}
	if seq <= lastSeq {
		t.Fatalf("a write from the reborn primary (%d) must outrank what its replica "+
			"already holds (%d); this is what the incarnation epoch is for", seq, lastSeq)
	}

	applied, err := replica.PutIfNewer(ctx, "k", []byte("after-wipe"), seq)
	if err != nil {
		t.Fatalf("replica PutIfNewer after wipe: %v", err)
	}
	if !applied {
		t.Fatal("the replica discarded a live write from its primary — the client would " +
			"have been told 200 while the replica kept its pre-wipe value")
	}
	if got := getOrEmpty(t, replica, "k"); got != "after-wipe" {
		t.Errorf("replica holds %q, want after-wipe", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 0 {
		t.Errorf("nothing regressed here — the epoch advanced: "+
			"replica_writes_epoch_regressed = %d, want 0", got)
	}
}

// TestRestoreAdvancesTheIncarnationEpoch covers the wipe this engine performs on
// itself. RestoreFromSnapshot deletes the manifest, the SSTables and the WAL, so
// the pre-restore write history is gone — and it used to reset the sequence
// counter to zero, which put every post-restore write below the versions this
// node's replicas still held. The epoch has to advance across a restore for the
// same reason it advances across a restart.
func TestRestoreAdvancesTheIncarnationEpoch(t *testing.T) {
	ctx := context.Background()
	l := openEpochTree(t, t.TempDir())

	seqBefore, err := l.Put(ctx, "k", []byte("before-restore"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	epochBefore := l.epoch.Load()

	if err := l.Restore(ctx, map[string][]byte{"restored": []byte("v")}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if l.epoch.Load() <= epochBefore {
		t.Fatalf("post-restore epoch %d must be above the pre-restore %d",
			l.epoch.Load(), epochBefore)
	}
	seqAfter, err := l.Put(ctx, "k", []byte("after-restore"))
	if err != nil {
		t.Fatalf("Put after restore: %v", err)
	}
	if seqAfter <= seqBefore {
		t.Fatalf("post-restore write %d must outrank the pre-restore %d that this "+
			"node's replicas still hold", seqAfter, seqBefore)
	}
}

// TestEpochRegressionIsCountedAndLogged covers what is left when the epoch cannot
// carry a node forward — a clock stepped backwards, or a sequence from a sender's
// previous incarnation arriving late.
//
// The replica cannot tell those two apart from one RPC: both are an arriving
// epoch below the stored one, and discarding is right for the stale arrival and
// lossy for the wiped sender. So it discards and reports. What it must never do
// is what the pre-epoch code did — ACK with nothing recorded anywhere.
func TestEpochRegressionIsCountedAndLogged(t *testing.T) {
	ctx := context.Background()
	m := &metrics.Metrics{}
	l := openEpochTree(t, t.TempDir(), WithMetrics(m))

	stored := makeSeq(2_000, 10)
	if applied, err := l.PutIfNewer(ctx, "k", []byte("current"), stored); err != nil || !applied {
		t.Fatalf("seeding the stored version: applied=%v err=%v", applied, err)
	}

	// Same epoch, lower counter: an ordinary suppressed inversion. Counted as a
	// discard, but not as a regression — conflating the two would make the
	// alarming counter fire on the mechanism working as designed.
	applied, err := l.PutIfNewer(ctx, "k", []byte("inverted"), makeSeq(2_000, 9))
	if err != nil {
		t.Fatalf("PutIfNewer(inversion): %v", err)
	}
	if applied {
		t.Error("a lower counter in the same epoch must be discarded")
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 1 {
		t.Errorf("replica_writes_discarded = %d, want 1", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 0 {
		t.Errorf("an inversion is not an epoch regression: got %d, want 0", got)
	}
	if got := m.ReplicaEpochRegressed.Load(); got != 0 {
		t.Errorf("the gauge must stay clear for an inversion: got %d", got)
	}

	// Lower epoch: not an inversion at all, because two writes from one
	// incarnation share an epoch.
	applied, err = l.PutIfNewer(ctx, "k", []byte("regressed"), makeSeq(1_999, 999))
	if err != nil {
		t.Fatalf("PutIfNewer(regressed): %v", err)
	}
	if applied {
		t.Error("a lower epoch must be discarded, not applied — it may be a stale " +
			"write from the sender's previous incarnation")
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 2 {
		t.Errorf("replica_writes_discarded = %d, want 2", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 1 {
		t.Errorf("replica_writes_epoch_regressed = %d, want 1", got)
	}
	if got := m.ReplicaEpochRegressed.Load(); got != 1 {
		t.Errorf("replica_epoch_regressed gauge = %d, want 1 (latched)", got)
	}

	// A tombstone is ordered like a value, so the same classification applies to
	// the delete path — which is the half a counter added only to PutIfNewer would
	// silently miss.
	applied, err = l.DeleteIfNewer(ctx, "k", makeSeq(1_999, 1_000))
	if err != nil {
		t.Fatalf("DeleteIfNewer(regressed): %v", err)
	}
	if applied {
		t.Error("a tombstone from a regressed epoch must be discarded")
	}
	if got := m.ReplicaWritesDiscarded.Load(); got != 3 {
		t.Errorf("replica_writes_discarded after a discarded delete = %d, want 3", got)
	}
	if got := m.ReplicaWritesEpochRegressed.Load(); got != 2 {
		t.Errorf("replica_writes_epoch_regressed after a discarded delete = %d, want 2", got)
	}

	// The gauge latches: nothing in v1 reconciles a sender that lost history, so
	// a later ordinary write must not clear the signal before anyone reads it.
	if applied, err := l.PutIfNewer(ctx, "k", []byte("newer"), makeSeq(2_001, 1)); err != nil || !applied {
		t.Fatalf("a newer epoch must apply: applied=%v err=%v", applied, err)
	}
	if got := m.ReplicaEpochRegressed.Load(); got != 1 {
		t.Errorf("replica_epoch_regressed must stay latched, got %d", got)
	}
}

// TestUnsequencedReplayOutranksTheStoredVersion pins finding 3's sharper reading
// rather than the doc's earlier one.
//
// A WAL record written before the log carried sequences replays with seq 0, which
// PutIfNewer routes through Put so the entry is not stored at 0 (where compaction
// would drop it). The consequence is stronger than "applies unconditionally": Put
// draws from this node's counter, which has been carried above every foreign
// sequence it stores, so the replayed value does not merely apply — it *wins*
// over a newer stored version, and keeps winning until the next real write for
// the key.
//
// That is why the idempotence claim for a catch-up pass holds only for v2
// records. It is behaviour, not a bug to fix here, so it is pinned: if it ever
// changes, the doc that describes it has to change with it.
func TestUnsequencedReplayOutranksTheStoredVersion(t *testing.T) {
	ctx := context.Background()
	l := openEpochTree(t, t.TempDir())

	newer := makeSeq(3_000, 500)
	if applied, err := l.PutIfNewer(ctx, "k", []byte("newer-from-primary"), newer); err != nil || !applied {
		t.Fatalf("seeding the newer version: applied=%v err=%v", applied, err)
	}

	applied, err := l.PutIfNewer(ctx, "k", []byte("v1-replay"), 0)
	if err != nil {
		t.Fatalf("PutIfNewer(seq=0): %v", err)
	}
	if !applied {
		t.Fatal("an unsequenced write applies unconditionally")
	}
	if got := getOrEmpty(t, l, "k"); got != "v1-replay" {
		t.Errorf("stored value = %q, want v1-replay: an unsequenced replay reverts a "+
			"newer value rather than losing to it", got)
	}

	// And it outranks it durably: the fresh local sequence is above the foreign
	// one, so compaction keeps the replayed value rather than the newer one.
	storedSeq, found, err := func() (uint64, bool, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.storedSeqLocked("k")
	}()
	if err != nil || !found {
		t.Fatalf("storedSeqLocked: found=%v err=%v", found, err)
	}
	if storedSeq <= newer {
		t.Errorf("the replayed entry is stored at %d, which does not outrank the "+
			"newer sequence %d it replaced", storedSeq, newer)
	}
}
