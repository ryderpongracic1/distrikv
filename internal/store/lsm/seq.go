package lsm

import "time"

// Write sequence numbers are split into two fields inside the one uint64 that
// already flows through Entry.SeqNum, the WAL v2 records, the manifest's
// MaxSeqNum and ReplicateRequest.seq:
//
//	 63                              22 21                  0
//	+----------------------------------+---------------------+
//	|        incarnation epoch         |    write counter    |
//	+----------------------------------+---------------------+
//	          42 bits (milliseconds)          22 bits
//
// The split costs nothing on the wire or on disk: a sequence is still one
// uint64, still compared with >, and comparing the composite is exactly
// comparing (epoch, counter) lexicographically. Nothing in the replication
// protocol, the WAL format or the SSTable format changes.
//
// # Why the high field exists
//
// The counter alone is a *local* quantity: it is seeded at open from local state
// (manifest MaxSeqNum, else a scan of the live SSTables — see seedSeqNum). A
// primary that comes back on empty storage — volume recreated, data directory
// lost, or the wipe a snapshot restore performs on itself — therefore restarts
// its counter near zero while its replicas still hold the high sequences its
// previous incarnation assigned. Its writes then lose the replicas'
// apply-if-newer comparison and are discarded *and ACKed*: the client sees 200,
// the replica keeps its pre-wipe value, and a catch-up pass ships the same low
// sequences and is discarded too.
//
// The epoch closes that by being derived from something a wiped node still has:
// the clock. Every open stamps the incarnation with the current millisecond (or
// one above the epoch of the previous incarnation, whichever is higher — see
// nextEpoch), so a node that loses its data directory still comes back above
// every sequence it issued before, and its writes outrank the stale versions its
// replicas hold instead of being silently dropped.
//
// Only one node's own clock monotonicity matters, never agreement between
// nodes': two sequences are compared only when they describe the same key, and a
// key has exactly one ring-primary, so both sides of every comparison were
// stamped by the same machine. Cross-node clock skew is irrelevant here — which
// is what makes the clock safe to use for this and not for ordering writes in
// general.
//
// # Why milliseconds, and what is still not covered
//
// Granularity is what bounds the fix, so it is set as fine as the field allows.
// Two incarnations that stamp the *same* epoch fall back to comparing counters,
// which is the pre-epoch behaviour — and a wipe is precisely the case where the
// second incarnation's counter is the lower one. At second granularity a
// redeploy fast enough to land in the same second would not be covered; at
// millisecond granularity a process restart cannot be. What remains uncovered is
// a clock stepped backwards across the wipe, which is reported rather than fixed
// (see LSMTree.noteDiscard).
//
// # Overflow
//
// Counter: 2^22 writes (~4.2 million) inside one incarnation carry into the
// epoch field. That is a non-event rather than a bug — the composite is a single
// atomic counter, so it stays strictly monotonic, and a carry can only make the
// epoch appear to advance, never regress, so it can neither misorder a write nor
// fabricate an epoch regression. Its only effect is to borrow ~1 ms of future
// epoch per 4.2 million writes, and the next open picks the epoch back up from
// the sequences on disk (see nextEpoch's recorded argument) rather than from the
// manifest record alone, so the borrowing does not accumulate into a regression.
//
// Epoch: 2^42 milliseconds from epochOriginUnixMilli is ~139 years, so the field
// saturates in the 2160s, at which point nextEpoch stops advancing and the scheme
// degrades to the pre-epoch behaviour — monotonic per data directory, with wipes
// detectable rather than survivable.
const (
	// seqCounterBits is the width of the low (write counter) field.
	seqCounterBits = 22

	// maxSeqEpoch is the largest representable incarnation epoch.
	maxSeqEpoch = (uint64(1) << (64 - seqCounterBits)) - 1

	// maxSeqCounter is the largest write counter representable within one
	// incarnation before it carries into the epoch field.
	maxSeqCounter = (uint64(1) << seqCounterBits) - 1

	// epochOriginUnixMilli is the zero point of the epoch clock: 2026-01-01
	// 00:00:00 UTC. Counting from a recent origin rather than from the Unix
	// epoch is what makes millisecond granularity fit in 42 bits — Unix
	// milliseconds alone are already past 2^40.
	epochOriginUnixMilli = 1767225600000
)

// makeSeq composes a sequence number from an incarnation epoch and a write
// counter.
func makeSeq(epoch, counter uint64) uint64 {
	return epoch<<seqCounterBits | (counter & maxSeqCounter)
}

// seqEpoch returns the incarnation epoch encoded in seq.
//
// A sequence written before the epoch existed reports 0, which is below every
// epoch this code assigns — so pre-upgrade data is uniformly older than
// post-upgrade data, which is exactly the true ordering.
func seqEpoch(seq uint64) uint64 { return seq >> seqCounterBits }

// epochFloor returns the lowest sequence number in epoch — the value the write
// counter starts from when an incarnation opens.
func epochFloor(epoch uint64) uint64 { return makeSeq(epoch, 0) }

// clockEpoch converts a wall-clock instant into an incarnation epoch.
//
// A clock before the origin (a container with an unset clock, say) yields 0,
// which is the epoch every pre-upgrade sequence carries — the most conservative
// answer available, and one nextEpoch's recorded argument can still improve on.
func clockEpoch(now time.Time) uint64 {
	ms := now.UnixMilli() - epochOriginUnixMilli
	if ms <= 0 {
		return 0
	}
	return uint64(ms)
}

// nextEpoch picks the incarnation epoch for an opening store.
//
// recorded is the highest epoch known to belong to the previous incarnation, and
// hasRecorded is false when none is — a fresh or wiped data directory. It takes
// the higher of the clock and recorded+1: the clock is what lets a wiped
// directory outrank the sequences its replicas still hold, since a wiped node has
// no record to read, and recorded+1 is what keeps the epoch advancing when the
// clock does not — a clock stepped backwards, or a counter that carried into the
// epoch field — so an incarnation never reuses its predecessor's epoch while
// local state survives.
func nextEpoch(recorded uint64, hasRecorded bool, now time.Time) uint64 {
	next := clockEpoch(now)
	if hasRecorded && recorded+1 > next {
		next = recorded + 1
	}
	if next > maxSeqEpoch {
		next = maxSeqEpoch
	}
	return next
}
