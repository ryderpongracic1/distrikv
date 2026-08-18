package main

// Failure classification for the chaos workload.
//
// This file answers one question about every write that did not return 200:
// what did the store do with it? The answer decides how the operation enters
// the recorded history, so it decides what the linearizability checker is
// allowed to conclude — which makes it the most safety-critical code in the
// runner.
//
// # The soundness constraint
//
// KVModel records a failed write as a no-op. So classifying a write that
// actually reached the server as "failed" tells the model the value was never
// stored, and a later *correct* read of that value then looks illegal: the
// checker reports an anomaly that the cluster did not commit. The failure is
// silent, it looks exactly like a real defect, and it destroys the only thing
// this harness is for.
//
// Therefore: a write may be recorded as a no-op only when the runner can prove
// it never left the client. Everything else — sent and unanswered, reset
// mid-response, timed out, an error shape never seen before — is recorded as a
// pending operation the checker may place anywhere or nowhere. That is always
// safe and never wrong; it only costs search time.
//
// The whole no-op decision funnels through provablyNeverSent and
// classifyForwardOutcome. Those two functions are the audit surface.
//
// # Why the phase decides, not the cause
//
// The taxonomy below is organised by *how far the request got*, because that is
// what determines what the store could possibly have seen:
//
//	kindStatus*  a server answered. The status code says what it did.
//	kindDial     no connection ever existed, so no request bytes were written.
//	kindSent     a connection existed. Bytes may be on the wire. Unknowable.
//	kindCanceled the runner's own context ended the call. Unknowable.
//
// Only kindDial is a no-op, and only because net/http writes nothing until a
// connection is established. Earlier versions of this code reasoned from the
// error's *message* instead, which is how a refused-but-applied 503 — whose body
// quotes "connection refused" from the fan-out to a dead replica — was once
// classified "provably never sent".

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"

	clientpkg "github.com/ryderpongracic1/distrikv/internal/client"
)

// writeEffect is what the runner concluded a failed write did to the store. It
// decides how the operation enters the recorded history — see finishWrite.
type writeEffect int

const (
	// effectApplied: the mutation took effect. Either the call succeeded, or it
	// failed with positive evidence that the write landed anyway.
	effectApplied writeEffect = iota
	// effectNotApplied: the mutation provably did not take effect, so modelling
	// it as a no-op is exact rather than merely convenient.
	effectNotApplied
	// effectUnknown: the mutation may or may not have taken effect. Neither
	// no-op nor applied can be asserted; the operation is left pending.
	effectUnknown
)

func (e writeEffect) String() string {
	switch e {
	case effectApplied:
		return "applied"
	case effectNotApplied:
		return "not applied"
	case effectUnknown:
		return "unknown"
	}
	return fmt.Sprintf("writeEffect(%d)", int(e))
}

// failureKind is how far a failed request got. It is the reported taxonomy and
// the input to the effect decision — one classifier, so the number printed in
// the report and the number the checker acts on can never disagree.
type failureKind uint8

const (
	// kindNone: nothing to classify. The call succeeded, or it returned a 404
	// the caller has already ruled benign (see classifyDeleteErr).
	kindNone failureKind = iota

	// kindRefusedApplied: HTTP 503. The ring-primary applied the mutation to its
	// own store and then failed to replicate it, and does not roll back.
	kindRefusedApplied
	// kindForwardNeverSent: HTTP 502 whose body says the forward never left.
	kindForwardNeverSent
	// kindForwardUnknown: HTTP 502 that does not prove the forward never left.
	kindForwardUnknown
	// kindStatusOther: any other 5xx. 500 covers both a ring-lookup failure
	// (never applied) and a local store error (possibly durable already), so it
	// cannot be resolved from the code alone.
	kindStatusOther

	// kindDial: the request failed before a connection existed. The only kind
	// that becomes a no-op.
	kindDial
	// kindSent: a connection existed and the request may have been written.
	kindSent
	// kindCanceled: the runner's own context ended the call. Says nothing about
	// what the server did with a request already on the wire, so it is unknown
	// rather than a no-op — even at shutdown, when it is tempting to assume the
	// op never happened.
	kindCanceled

	// kindCount bounds the per-kind counter array. Keep it last.
	kindCount
)

// String is the label used in the report's failure breakdown.
func (k failureKind) String() string {
	switch k {
	case kindNone:
		return "ok"
	case kindRefusedApplied:
		return "503 refused-but-applied"
	case kindForwardNeverSent:
		return "502 forward never-sent"
	case kindForwardUnknown:
		return "502 forward unknown"
	case kindStatusOther:
		return "other 5xx"
	case kindDial:
		return "dial failed (never sent)"
	case kindSent:
		return "sent, no answer"
	case kindCanceled:
		return "cancelled by shutdown"
	}
	return fmt.Sprintf("failureKind(%d)", uint8(k))
}

// effect maps a kind to what may be asserted about the store.
//
// This mapping is the soundness boundary in one place: exactly two kinds are
// no-ops, and each has a proof behind it (a dial that never connected; a server
// that stated its forward never left). Every other kind is unknown.
func (k failureKind) effect() writeEffect {
	switch k {
	case kindNone, kindRefusedApplied:
		return effectApplied
	case kindDial, kindForwardNeverSent:
		return effectNotApplied
	default:
		return effectUnknown
	}
}

// writeFailureKinds is every kind a failed write can carry, in report order.
// kindNone is excluded: it is not a failure.
var writeFailureKinds = []failureKind{
	kindRefusedApplied,
	kindForwardNeverSent,
	kindForwardUnknown,
	kindStatusOther,
	kindDial,
	kindSent,
	kindCanceled,
}

// classifyWriteEffect decides which of the three outcomes a write error carries.
func classifyWriteEffect(err error) writeEffect {
	return classifyFailure(err).effect()
}

// classifyFailure buckets a write error by the phase it failed in.
//
// The status code decides first, and it decides from the chain: internal/client
// returns *StatusError for a 5xx, so the code is matched with errors.As rather
// than read out of the message.
//
//	503  the ring-primary applied the mutation to its own store and then failed
//	     to replicate it. distrikv does not roll that back (see ErrReplication
//	     in internal/server and "CAP Position" in docs/architecture.md), and reads are
//	     served by the primary, so the write is present and readable. Applied.
//	     Both client entry points report this identically: a forwarded write
//	     returns the primary's status verbatim through ForwardKey.
//	502  the write failed on its way to the primary, in forwardRequest. Two
//	     causes hide behind one code: a request that provably never reached the
//	     primary (never applied), and a ForwardKey RPC that failed in a way that
//	     may have been applied before the response was lost. The server separates
//	     them and says which in the body's forward_outcome field — see
//	     classifyForwardOutcome.
//	5xx  anything else: 500 covers both a ring-lookup failure (never applied)
//	     and a local store error (possibly durable already). Unknown.
//
// Only when there is no status code at all — the request never got a response —
// does the transport classification run.
//
// Ordering matters, and not only for tidiness. A 503's body carries the
// replication error underneath it, which during an outage reads "…connect:
// connection refused" from the fan-out to the dead replica. Classifying on the
// message text first therefore declared refused-but-applied writes "provably
// never sent" — the reason a stop-restart run reported 0 indeterminate writes
// while failing.
//
// The transport branch checks the dial phase before context cancellation,
// because both can be true of one error and the phase is the stronger fact: a
// dial that timed out is a dial that never connected, whatever the deadline that
// cut it short belonged to.
func classifyFailure(err error) failureKind {
	if err == nil || isNotFound(err) {
		return kindNone
	}

	var se *clientpkg.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusServiceUnavailable:
			return kindRefusedApplied
		case http.StatusBadGateway:
			if classifyForwardOutcome(se.Body) == effectNotApplied {
				return kindForwardNeverSent
			}
			return kindForwardUnknown
		default:
			return kindStatusOther
		}
	}

	if provablyNeverSent(err) {
		return kindDial
	}
	// ErrUnreachable means internal/client saw a transport failure that was not
	// a dial: a connection existed. Checked before the context sentinels because
	// net/http's own deadline errors — ResponseHeaderTimeout in particular —
	// satisfy errors.Is(err, context.DeadlineExceeded), and a request that timed
	// out waiting for its response headers was measurably delivered. Both are
	// unknown either way, so this is a labelling fix, not a soundness one.
	if errors.Is(err, clientpkg.ErrUnreachable) {
		return kindSent
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return kindCanceled
	}
	// Everything left is unknown, deliberately: an error shape this runner has
	// never seen. Guessing "no-op" here is what produced false anomalies;
	// leaving the operation pending asserts nothing.
	return kindSent
}

// forwardOutcome values the server emits in a 502 body. They mirror the
// constants in internal/server; this is the wire contract between the two.
const (
	forwardOutcomeNeverSent = "never-sent"
	forwardOutcomeUnknown   = "unknown"
)

// forwardErrorBody is the 502 body shape written by internal/server's
// writeForwardError. ForwardOutcome is a pointer so an *absent* field — an older
// server that does not emit it — is distinguishable from one present with a value
// this runner does not recognise. The two are treated differently: see
// classifyForwardOutcome.
type forwardErrorBody struct {
	Error          string  `json:"error"`
	ForwardOutcome *string `json:"forward_outcome"`
}

// classifyForwardOutcome decides what a 502 says about the store, reading the
// server's typed verdict in preference to its prose.
//
// The server is the only party that can answer this. A gRPC RPC error is a
// *status.Error carrying a code and a string and nothing else — it does not wrap
// the transport failure underneath, so there is no chain to inspect even on the
// server's side, let alone after the message has crossed two process boundaries
// as text. What the server does have is the code that framed the message, which
// is what separates a connection that was never established from a stream that
// broke after the request went out. It makes that call and sends the answer;
// this function just reads it. See classifyForwardError in internal/server.
//
// Three inputs, three answers, and the difference between the last two matters:
//
//   - field present and recognised → its verdict, trusted. The server made it
//     with strictly more evidence than exists here.
//   - field present but unrecognised → unknown, and the text is *not* consulted.
//     A server that speaks this field is authoritative; falling back to a weaker
//     signal that might contradict it would be worse than declining to answer.
//   - field absent → the text scan, for a server predating the field. See
//     neverSentText.
//
// Every path that is not a recognised never-sent ends in effectUnknown, so the
// bounded-safe property holds: an unparseable, truncated, or unexpected body
// leaves the operation pending rather than asserting a no-op.
func classifyForwardOutcome(body string) writeEffect {
	var parsed forwardErrorBody
	if err := json.Unmarshal([]byte(body), &parsed); err == nil && parsed.ForwardOutcome != nil {
		switch *parsed.ForwardOutcome {
		case forwardOutcomeNeverSent:
			return effectNotApplied
		default:
			// Includes forwardOutcomeUnknown and anything unrecognised.
			return effectUnknown
		}
	}

	if neverSentText(body) {
		return effectNotApplied
	}
	return effectUnknown
}

// neverSentMarkers are transport failures that mean nothing was delivered: a
// refused connection got no SYN-ACK, an unresolvable or unroutable host got no
// packets at all. A connection that died *after* the request went out reads
// differently ("EOF", "connection reset by peer", "transport is closing") and is
// deliberately absent from this list.
var neverSentMarkers = []string{
	"connection refused",
	"no such host",
	"no route to host",
	"network is unreachable",
}

// neverSentText reports whether an error message describes a transport failure
// that delivered nothing.
//
// This is the text path. It is now a **compatibility fallback**, reached only for
// a 502 body with no forward_outcome field — a server older than that field — and
// for an error that arrives with no chain left to inspect.
//
// It was load-bearing until the server learned to classify, and its limits are
// worth recording, because they are what motivated moving the decision upstream.
// Two of the four markers above could never fire on this path: gRPC reports an
// unresolvable target as `name resolver error: produced zero addresses`, not "no
// such host", and an unroutable address as a plain DeadlineExceeded rather than
// anything naming a route. Reading a failure this far downstream means matching
// wording chosen by a library two hops away for an audience of humans.
//
// It stays bounded in the safe direction either way: an unrecognised body is
// unknown, so a rewording costs checker time, never a wrong verdict.
func neverSentText(msg string) bool {
	for _, s := range neverSentMarkers {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// provablyNeverSent reports whether err means the request never reached a
// server, so treating it as a no-op is exact rather than merely conservative.
//
// It is only consulted for an error with no HTTP status in it — a request that
// never got a response at all. classifyFailure handles status-carrying errors
// before this runs, which is what keeps a 503 (applied) from being read as
// "never sent" because its body quotes a refused replica connection.
//
// # The dial-phase rule
//
// The load-bearing check is the phase, not the cause: a *net.OpError whose Op is
// "dial" is a failure to *establish* the connection, and net/http writes no
// request bytes until a connection exists. So the request cannot have reached a
// server, whatever the reason the dial failed — refused, timed out, reset during
// the handshake, or local resource exhaustion (EADDRNOTAVAIL under port
// pressure). "dial" is the only Op net produces for connection setup; "read" and
// "write" belong to an established connection and are deliberately not matched.
//
// This is what a graceless outage mostly produces, and reading only the cause
// left the timed-out and reset-handshake dials — every one of them provably
// harmless — as pending operations that the checker then had to search around.
//
// The transport this runner installs sets no Proxy, so a dial here is always the
// dial of the target connection. (Even with one, an HTTP proxy is dialled before
// the request is written, so the conclusion would hold.)
//
// # The checks behind it
//
// The syscall and DNS checks predate the phase rule and are now mostly
// subsumed by it. They are kept because they cost nothing and they still catch a
// cause that arrives without its OpError wrapper — and because they document, in
// executable form, which causes were understood to be safe.
//
// The substring check last is a final resort for an error that arrives as text
// with no identity left to match — one that crossed a process boundary, or came
// from a source outside this repo. It is deliberately kept because
// misclassifying a never-sent write as unknown only costs checker time, whereas
// the reverse would model a genuinely unknown outcome as a no-op.
func provablyNeverSent(err error) bool {
	// Phase: exact, and independent of why the dial failed.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}

	// Cause: for an errno or DNS failure that reaches us without its OpError.
	for _, errno := range []syscall.Errno{
		syscall.ECONNREFUSED,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}

	// Fallback: the message text, for errors that reach us with no chain.
	return neverSentText(err.Error())
}

// isNotFound reports whether err represents a 404 / key-not-found response.
func isNotFound(err error) bool {
	return errors.Is(err, clientpkg.ErrNotFound)
}

// classifyDeleteErr maps a delete outcome to what should be recorded.
//
// The server answers 404 when the key was already absent. That is not a failure
// and not an unknown outcome: it leaves the store in exactly the state a
// successful delete would, and the 404 is positive evidence the key was absent
// at some instant inside the call, so recording the delete as applied is sound
// and uses more of the available evidence than recording it as an error.
//
// This is sound only because the server emits 404 on DELETE exclusively for an
// absent key (see handleDelete in internal/server). If a 404 ever comes to mean
// something else — a route miss, "not the owner of this key" — this assertion
// stops being valid and must change with it.
func classifyDeleteErr(err error) error {
	if isNotFound(err) {
		return nil
	}
	return err
}
