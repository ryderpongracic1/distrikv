package server

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// forwardOutcome is what the forwarding node concluded about a forward RPC that
// failed: whether the request provably never reached the ring-primary, or
// whether its fate is unknown.
//
// It is emitted as the "forward_outcome" field of the 502 body so a consumer
// does not have to re-derive it from prose. The consumer that needs it is the
// chaos runner's linearizability model: a write that provably never left is a
// no-op it can model exactly, whereas an unknown one has to become a pending
// operation that overlaps every later operation on its key. Getting thousands of
// never-sent forwards classed as unknown is what pushed the checker from a
// verdict into a timeout.
//
// The two values are deliberately asymmetric in cost. A wrong "never-sent"
// tells the model a write did not happen when it may have, which can invent a
// linearizability anomaly out of correct behaviour. A wrong "unknown" only
// widens the checker's search. So every case that is not provable is unknown.
type forwardOutcome string

const (
	// forwardNeverSent: no byte of this RPC left the process. The store cannot
	// have been mutated, so modelling the write as a no-op is exact.
	forwardNeverSent forwardOutcome = "never-sent"

	// forwardUnknown: the RPC may have been received and applied before the
	// response was lost. Nothing about the store's state can be asserted.
	forwardUnknown forwardOutcome = "unknown"
)

// dialFramings are the fragments grpc-go wraps around an error produced while
// *establishing* a connection, as opposed to one produced on a stream that had
// already been created.
//
// This distinction is the whole proof. A gRPC stream is only created once the
// HTTP/2 transport is READY, so an error raised inside transport creation cannot
// have carried any part of the RPC. grpc-go surfaces that as the subconn's last
// connection error, which a fail-fast RPC then returns:
//
//	rpc error: code = Unavailable desc = connection error: desc =
//	  "transport: Error while dialing: dial tcp 127.0.0.1:45983: connect: connection refused"
//
// A connection that broke *after* the request went out reads differently —
// "transport is closing", "error reading from server: EOF", "connection reset by
// peer", "the connection is draining" — and carries no dial framing, so it falls
// through to unknown, which is correct: those may have been applied.
//
// The framing also defends against a status the *remote* generated. codes.
// Unavailable is a legal application code, and a primary that failed to reach a
// replica could produce a message quoting "connection refused" from its own
// fan-out. Such a status has no dial framing, so it cannot be read as never-sent
// — which is the same trap that previously made the chaos runner classify
// refused-but-applied writes as never sent. (distrikv's own ForwardKey never
// returns a status error for a write failure — it answers (response, nil) with
// an HTTP code in the response — so this is defence in depth rather than a live
// hazard.)
var dialFramings = []string{
	"transport: Error while dialing",
	"last connection error:",
}

// deliveryImpossibleMarkers are the connection-establishment failures that prove
// the peer never received anything: a refused connection got no SYN-ACK, an
// unresolvable name was never dialed, an unroutable host got no packets.
//
// They are only consulted inside a dialFramings match. A marker alone is not
// enough — see dialFramings for why.
var deliveryImpossibleMarkers = []string{
	"connection refused",
	"no such host",
	"no route to host",
	"network is unreachable",
}

// resolverFraming is grpc-go's framing for a channel that has no address to dial
// at all, e.g. `name resolver error: produced zero addresses`. Nothing was
// dialed, so nothing was sent.
const resolverFraming = "name resolver error"

// classifyForwardError decides whether a failed forward RPC provably never
// reached the primary.
//
// It runs on the server because this is the last place the error still has its
// gRPC identity. It is also, empirically, the last place there is any identity
// at all: a grpc-go RPC error is a *status.Error carrying a code and a string,
// and it does **not** wrap the transport failure underneath it. errors.Unwrap
// returns nil, and errors.Is against syscall.ECONNREFUSED or errors.As against
// *net.OpError and *net.DNSError all fail — asserted by
// TestForwardErrorsCarryNoTypedCause. So there is no chain to dig into anywhere,
// on either side of the HTTP boundary; the code plus the status message is the
// entire evidence that exists.
//
// What this changes is therefore not the *kind* of evidence but where it is read
// and what crosses the wire. The message is inspected once, here, at its
// freshest, together with the code that frames it — and what the client receives
// is a typed verdict rather than prose it must re-parse.
//
// The decision table, with the observed gRPC behaviour behind each row:
//
//	code              message signature                       outcome      why
//	----------------  --------------------------------------  -----------  -----------------------------------------
//	Unavailable       dial framing + delivery-impossible       never-sent   transport never created ⇒ no stream ⇒ no bytes
//	Unavailable       name resolver error                     never-sent   no address was ever dialed
//	Unavailable       anything else                           unknown      may be a broken stream, or a remote status
//	DeadlineExceeded  any                                     unknown      the 2s bound can fire after the primary applied
//	Canceled          any                                     unknown      the caller gave up; the server may not have
//	any other code    any                                     unknown      a remote-generated code implies delivery
//
// Two cases are left unknown on purpose even though they are probably never-sent.
// A dial that fails with "i/o timeout" rather than a refusal also could not have
// created a transport — but "probably" is not the bar for a never-sent claim, and
// the only cost of declining is checker time. And a blackholed address (a host
// that accepts TCP but never completes the HTTP/2 handshake, which is what a
// stopped container looks like from inside a docker network) surfaces as
// DeadlineExceeded, with a message about the load-balancer wait that names no
// transport failure at all; it stays unknown.
func classifyForwardError(err error) forwardOutcome {
	if err == nil {
		return forwardUnknown
	}
	if status.Code(err) != codes.Unavailable {
		return forwardUnknown
	}

	msg := status.Convert(err).Message()

	if strings.Contains(msg, resolverFraming) {
		return forwardNeverSent
	}
	if containsAny(msg, dialFramings) && containsAny(msg, deliveryImpossibleMarkers) {
		return forwardNeverSent
	}
	return forwardUnknown
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
