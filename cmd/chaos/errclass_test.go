package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	clientpkg "github.com/ryderpongracic1/distrikv/internal/client"
)

// chaosTransport mirrors the transport main() installs, so a classification
// asserted here is a classification the real runner will make. Only the dialer
// is parameterised.
func chaosTransport(dialer *net.Dialer) *http.Transport {
	return &http.Transport{
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   8,
		MaxConnsPerHost:       8,
		IdleConnTimeout:       90 * time.Second,
		DialContext:           dialer.DialContext,
		ResponseHeaderTimeout: 300 * time.Millisecond, // 5s in main(); shortened for tests
	}
}

func chaosClient(t *testing.T, host string, dialer *net.Dialer) *clientpkg.Client {
	t.Helper()
	return clientpkg.NewWithTransport(
		clientpkg.Config{Host: host, Timeout: 2 * time.Second},
		chaosTransport(dialer),
	)
}

// rawListener accepts connections and hands each one to handle. It is how the
// tests below reproduce a *half-dead* endpoint: something answers the TCP
// handshake but no HTTP server is behind it, which is exactly what a container
// port-forwarder does while the container it points at is down.
type rawListener struct {
	ln     net.Listener
	wg     sync.WaitGroup
	closed chan struct{}
}

func newRawListener(t *testing.T, handle func(net.Conn)) *rawListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	r := &rawListener{ln: ln, closed: make(chan struct{})}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				defer func() { _ = c.Close() }()
				handle(c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		r.wg.Wait()
	})
	return r
}

func (r *rawListener) addr() string { return r.ln.Addr().String() }

// closedPort returns an address nothing is listening on, so a dial to it is
// refused.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}
	return addr
}

// TestFailureTaxonomyAgainstRealEndpoints is the measurement this change was
// built on, kept as a test so the taxonomy cannot drift silently.
//
// Every case drives internal/client through the runner's own transport against a
// real socket that fails in one specific way, and asserts which bucket the
// failure lands in. Run it with -v to read the taxonomy as a table.
//
// The soundness constraint lives in the `effect` column: exactly one transport
// bucket may be effectNotApplied, and it is the one where no connection ever
// existed. The three cases below that reach an accepting socket must all stay
// unknown, however obvious it is to a human reading the setup that the request
// was thrown away — the runner cannot see the setup, only the error.
func TestFailureTaxonomyAgainstRealEndpoints(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// host and dialer describe the endpoint; setup may return either.
		endpoint func(t *testing.T) (host string, dialer *net.Dialer)
		want     failureKind
		why      string
	}{
		{
			name: "dial refused — nothing listening",
			endpoint: func(t *testing.T) (string, *net.Dialer) {
				return closedPort(t), &net.Dialer{Timeout: 2 * time.Second}
			},
			want: kindDial,
			why:  "no SYN-ACK, so no request bytes were written",
		},
		{
			name: "dial timeout — handshake never completed",
			endpoint: func(t *testing.T) (string, *net.Dialer) {
				// A listener that exists but is never reached: the dial is cut
				// off by its own deadline before the connection is usable.
				// Reproducing a timeout by *deadline* rather than by finding a
				// blackhole address keeps the test hermetic — no reliance on an
				// unroutable IP behaving the same way on every host.
				r := newRawListener(t, func(net.Conn) {})
				return r.addr(), &net.Dialer{Timeout: time.Nanosecond}
			},
			want: kindDial,
			why:  "a dial that timed out never connected, so nothing was written",
		},
		{
			name: "accepted then closed — a port-forwarder with nothing behind it",
			endpoint: func(t *testing.T) (string, *net.Dialer) {
				r := newRawListener(t, func(c net.Conn) {
					// Close without reading or answering.
				})
				return r.addr(), &net.Dialer{Timeout: 2 * time.Second}
			},
			want: kindSent,
			why: "a connection existed, so the request may have been written and read; " +
				"that it demonstrably was not is invisible in the error",
		},
		{
			name: "accepted, request read, no response",
			endpoint: func(t *testing.T) (string, *net.Dialer) {
				r := newRawListener(t, func(c net.Conn) {
					buf := make([]byte, 4096)
					_, _ = c.Read(buf)
					time.Sleep(2 * time.Second) // outlast ResponseHeaderTimeout
				})
				return r.addr(), &net.Dialer{Timeout: 2 * time.Second}
			},
			want: kindSent,
			why:  "the request was provably delivered; the answer was not",
		},
		{
			name: "reset after the request was read",
			endpoint: func(t *testing.T) (string, *net.Dialer) {
				r := newRawListener(t, func(c net.Conn) {
					buf := make([]byte, 4096)
					_, _ = c.Read(buf)
					if tc, ok := c.(*net.TCPConn); ok {
						_ = tc.SetLinger(0) // close(2) sends RST
					}
				})
				return r.addr(), &net.Dialer{Timeout: 2 * time.Second}
			},
			want: kindSent,
			why:  "an RST after the request went out says nothing about whether it was applied",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			host, dialer := tc.endpoint(t)
			c := chaosClient(t, host, dialer)

			err := c.Put(context.Background(), "k", "v")
			if err == nil {
				t.Fatalf("the write must fail against this endpoint")
			}
			got := classifyFailure(err)
			t.Logf("%-28s → %-26s effect=%-12s err=%v",
				tc.name, got, got.effect(), err)
			if got != tc.want {
				t.Errorf("classifyFailure = %v, want %v — %s\n  err: %v", got, tc.want, tc.why, err)
			}
		})
	}
}

// TestDialPhaseIsTheNoOpBoundary states the soundness rule as an assertion over
// the whole taxonomy rather than over one case: of every kind a transport
// failure can produce, exactly one is modelled as a no-op, and it is the one
// that names the dial.
//
// Revert-check: delete the Op == "dial" branch in provablyNeverSent and the
// "dial timeout" case in TestFailureTaxonomyAgainstRealEndpoints fails. Widen
// the branch to any *net.OpError — dropping the Op check — and
// TestSentFailuresAreNeverNoOps fails, because a read-phase reset then becomes a
// no-op.
func TestDialPhaseIsTheNoOpBoundary(t *testing.T) {
	noOps := map[failureKind]bool{}
	for _, k := range writeFailureKinds {
		if k.effect() == effectNotApplied {
			noOps[k] = true
		}
	}
	want := map[failureKind]bool{kindDial: true, kindForwardNeverSent: true}
	if len(noOps) != len(want) {
		t.Fatalf("no-op kinds = %v, want %v — a new kind became a no-op without a proof behind it",
			noOps, want)
	}
	for k := range want {
		if !noOps[k] {
			t.Errorf("%v is no longer modelled as a no-op", k)
		}
	}
}

// TestSentFailuresAreNeverNoOps is the adversarial half: every error shape that
// could have left bytes on the wire must stay unknown. A regression here is the
// failure mode that matters — it makes the checker invent anomalies out of
// correct behaviour — and it is silent, so it is pinned exhaustively.
func TestSentFailuresAreNeverNoOps(t *testing.T) {
	sent := []struct {
		name string
		err  error
	}{
		{"EOF after the request went out", wrapUnreachable(errors.New("EOF"))},
		{"reset mid-response", wrapUnreachable(&net.OpError{
			Op: "read", Net: "tcp", Err: syscall.ECONNRESET,
		})},
		{"write failed mid-request", wrapUnreachable(&net.OpError{
			Op: "write", Net: "tcp", Err: syscall.EPIPE,
		})},
		{"response header timeout", wrapUnreachable(
			errors.New("net/http: timeout awaiting response headers"))},
		{"server closed a pooled idle connection", wrapUnreachable(
			errors.New("http: server closed idle connection"))},
		{"a shape this runner has never seen", wrapUnreachable(
			errors.New("something nobody has written down yet"))},
		{"the runner's own shutdown", context.Canceled},
		{"the client's overall deadline", context.DeadlineExceeded},
	}

	for _, tc := range sent {
		t.Run(tc.name, func(t *testing.T) {
			kind := classifyFailure(tc.err)
			if got := kind.effect(); got == effectNotApplied {
				t.Fatalf("classifyFailure(%v) = %v → %v; a possibly-sent write must never "+
					"be modelled as a no-op", tc.err, kind, got)
			}
			if provablyNeverSent(tc.err) {
				t.Errorf("provablyNeverSent(%v) = true; this failure may have been delivered", tc.err)
			}
		})
	}
}

// wrapUnreachable reproduces internal/client's error wrapping so a hand-built
// cause travels through the same chain the real one does.
func wrapUnreachable(cause error) error {
	return fmt.Errorf("%w: %w", clientpkg.ErrUnreachable, cause)
}

// TestDialPhaseCoversCausesTheErrnoListDoesNot names what the phase rule bought.
// Each of these is a real dial failure whose cause is not in neverSentMarkers and
// not in the errno list, so before the phase rule every one of them was a
// pending operation for the checker to search around.
func TestDialPhaseCoversCausesTheErrnoListDoesNot(t *testing.T) {
	dialFailures := []struct {
		name  string
		cause error
	}{
		{"timed out", &os.SyscallError{Syscall: "connect", Err: syscall.ETIMEDOUT}},
		{"deadline exceeded before connecting", os.ErrDeadlineExceeded},
		{"reset during the handshake", &os.SyscallError{Syscall: "connect", Err: syscall.ECONNRESET}},
		{"local ports exhausted", &os.SyscallError{Syscall: "connect", Err: syscall.EADDRNOTAVAIL}},
		{"resolution failed inside the dial", &net.DNSError{Err: "i/o timeout", Name: "node1", IsTimeout: true}},
	}

	for _, tc := range dialFailures {
		t.Run(tc.name, func(t *testing.T) {
			err := wrapUnreachable(&net.OpError{
				Op: "dial", Net: "tcp",
				Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8001},
				Err:  tc.cause,
			})
			if got := classifyFailure(err); got != kindDial {
				t.Errorf("classifyFailure = %v, want kindDial — a dial that never connected "+
					"wrote no request bytes, whatever stopped it", got)
			}
			// And the message must not be doing the work.
			hidden := opaqueError{err: err}
			if strings.Contains(hidden.Error(), "dial") {
				t.Fatal("test setup error: the opaque message still names the phase")
			}
			if !provablyNeverSent(hidden) {
				t.Error("classification came from the text, not the chain")
			}
		})
	}
}

// TestStatusFailuresOutrankTransportPhase re-pins the ordering that a previous
// defect turned on: a 503 carries a replication error underneath it whose text
// names a refused connection, and it must still be read as *applied*. The
// dial-phase rule made this sharper rather than safer on its own — an OpError
// could in principle appear under a status error too — so the order is asserted
// directly.
func TestStatusFailuresOutrankTransportPhase(t *testing.T) {
	// A 503 whose chain contains a genuine refused *dial* to the replica.
	replicaRefused := &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(172, 18, 0, 3), Port: 9003},
		Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
	err := fmt.Errorf("%w: %w",
		&clientpkg.StatusError{
			StatusCode: http.StatusServiceUnavailable,
			Body:       `{"error":"replication failed: connect: connection refused"}`,
		},
		replicaRefused)

	if got := classifyFailure(err); got != kindRefusedApplied {
		t.Fatalf("classifyFailure = %v, want kindRefusedApplied — the primary answered, "+
			"so the refused dial in the chain belongs to its fan-out, not to this request", got)
	}
	if got := classifyWriteEffect(err); got != effectApplied {
		t.Errorf("classifyWriteEffect = %v, want effectApplied", got)
	}
}

// TestPooledConnectionDeathIsSoundEitherWay covers the shape a graceful shutdown
// produces, and it is the measurement that justified building the circuit breaker
// instead of only sharpening the classifier.
//
// The intuition going in was that net/http would always retry: it can often
// prove nothing was written to a pooled connection the server closed, and it
// redials in that case. Measured, the outcome is a race, and both branches are
// real:
//
//   - the transport's read loop notices the close first, marks the connection
//     broken, and the next request dials fresh → refused → kindDial, a no-op,
//     exact.
//   - the request is handed the connection first, writes into a socket that is
//     already closing, and reads EOF → kindSent, unknown. net/http cannot retry
//     it, because Request.isReplayable() requires an idempotent *method* and PUT
//     is not on that list however idempotent this particular PUT is.
//
// The second branch is irreducible by classification: an EOF genuinely does not
// say whether the request was read. Its cost is bounded by the number of
// connections in flight when the server stops, so a handful per outage. What is
// *not* bounded — and what the breaker exists for — is the same error arriving at
// the full offered rate for as long as something keeps accepting connections
// with nothing behind them.
//
// The invariant asserted here is the one that matters: whichever branch a run
// takes, a no-op is only ever claimed for the refused dial.
func TestPooledConnectionDeathIsSoundEitherWay(t *testing.T) {
	const attempts = 20
	seen := map[failureKind]int{}

	for i := 0; i < attempts; i++ {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		host := strings.TrimPrefix(srv.URL, "http://")
		c := chaosClient(t, host, &net.Dialer{Timeout: 2 * time.Second})

		// Pool a connection, then stop gracefully: idle connections closed,
		// nothing listening afterwards.
		if err := c.Put(context.Background(), "k", "v"); err != nil {
			t.Fatalf("first write must succeed: %v", err)
		}
		srv.Close()

		err := c.Put(context.Background(), "k", "v2")
		if err == nil {
			t.Fatal("a write after the server stopped must fail")
		}
		kind := classifyFailure(err)
		seen[kind]++

		switch kind {
		case kindDial:
			// Exact: the redial was refused, so this request reached nothing.
		case kindSent:
			if kind.effect() != effectUnknown {
				t.Fatalf("a pooled-connection EOF became %v; it cannot be proven undelivered",
					kind.effect())
			}
		default:
			t.Fatalf("unexpected kind %v for a pooled-connection death: %v", kind, err)
		}
	}

	t.Logf("pooled-connection death over %d attempts: %s=%d, %s=%d",
		attempts, kindDial, seen[kindDial], kindSent, seen[kindSent])
}
