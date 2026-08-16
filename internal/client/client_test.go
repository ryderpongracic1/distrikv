package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T, handler http.Handler) *client.Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return client.New(client.Config{
		Host:    strings.TrimPrefix(srv.URL, "http://"),
		Timeout: 5 * time.Second,
	})
}

func TestGet_Success(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/keys/mykey", r.URL.Path)
		json.NewEncoder(w).Encode(map[string]string{"value": "myval"})
	}))
	val, err := c.Get(context.Background(), "mykey")
	require.NoError(t, err)
	assert.Equal(t, "myval", val)
}

func TestGet_NotFound(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}))
	_, err := c.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, client.ErrNotFound)
}

func TestGet_ServerError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"oops"}`))
	}))
	_, err := c.Get(context.Background(), "key")
	assert.ErrorIs(t, err, client.ErrServerError)
	assert.Contains(t, err.Error(), "oops")
}

func TestGet_Unreachable(t *testing.T) {
	c := client.New(client.Config{Host: "localhost:19999", Timeout: 1 * time.Second})
	_, err := c.Get(context.Background(), "key")
	assert.ErrorIs(t, err, client.ErrUnreachable)
}

// TestStatusErrorCarriesTheCode covers the contract cmd/chaos classifies writes
// with. distrikv's 5xx codes describe different effects on the store — a 503 is
// a mutation the primary applied and could not replicate, a 502 is a mutation
// that may never have reached the primary — so a consumer that can only see
// ErrServerError has to read the message text to tell them apart. The code has
// to survive in the chain.
func TestStatusErrorCarriesTheCode(t *testing.T) {
	for _, code := range []int{
		http.StatusServiceUnavailable,
		http.StatusBadGateway,
		http.StatusInternalServerError,
	} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			body := `{"error":"replication to replicas failed"}`
			c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				w.Write([]byte(body))
			}))

			for name, call := range map[string]func() error{
				"put":    func() error { return c.Put(context.Background(), "k", "v") },
				"delete": func() error { return c.Delete(context.Background(), "k") },
				"get": func() error {
					_, err := c.Get(context.Background(), "k")
					return err
				},
			} {
				t.Run(name, func(t *testing.T) {
					err := call()
					require.Error(t, err)

					// The sentinel classification is unchanged...
					assert.ErrorIs(t, err, client.ErrServerError)
					// ...and the message is byte-identical to the fmt.Errorf
					// wrapping this type replaced, so existing operator output
					// does not shift.
					assert.Equal(t, fmt.Sprintf("server error: %s", body), err.Error())

					var se *client.StatusError
					require.ErrorAs(t, err, &se, "the status code must survive in the chain")
					assert.Equal(t, code, se.StatusCode)
					assert.Equal(t, body, se.Body)
				})
			}
		})
	}
}

// closedPortHost returns a host:port that is guaranteed to refuse connections:
// a listener is opened to reserve the port and closed again before returning.
func closedPortHost(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// TestUnreachable_PreservesErrorChain is the regression test for the wrap that
// used to read fmt.Errorf("%w: %v", ErrUnreachable, urlErr.Err). The %v rendered
// the transport error as text, so ErrUnreachable was the deepest thing in the
// chain and callers could not identify *why* the node was unreachable without
// matching on message substrings — which is what cmd/chaos was forced to do to
// tell "provably never sent" from "outcome unknown".
//
// Every entry point must satisfy the same contract, since they all funnel
// through do().
func TestUnreachable_PreservesErrorChain(t *testing.T) {
	host := closedPortHost(t)
	c := client.New(client.Config{Host: host, Timeout: 2 * time.Second})
	ctx := context.Background()

	calls := map[string]func() error{
		"Get":    func() error { _, err := c.Get(ctx, "key"); return err },
		"Put":    func() error { return c.Put(ctx, "key", "value") },
		"Delete": func() error { return c.Delete(ctx, "key") },
		"Status": func() error { _, err := c.Status(ctx); return err },
		"Metrics": func() error {
			_, err := c.Metrics(ctx)
			return err
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.Error(t, err)

			// The sentinel still classifies the failure.
			assert.ErrorIs(t, err, client.ErrUnreachable)

			// ...and the cause is still reachable through it, both by value
			// and by type.
			assert.ErrorIs(t, err, syscall.ECONNREFUSED,
				"the refusing syscall must survive the ErrUnreachable wrap")
			var opErr *net.OpError
			require.ErrorAs(t, err, &opErr,
				"the *net.OpError must survive the ErrUnreachable wrap")
			assert.Equal(t, "dial", opErr.Op)

			// The message keeps naming the cause — the fix changed the chain,
			// not the human-readable text.
			assert.Contains(t, err.Error(), client.ErrUnreachable.Error())
			assert.Contains(t, err.Error(), "connection refused")
		})
	}
}

// TestUnreachable_ChainStopsAtTheTransportError guards the other direction: the
// wrap must not smuggle in identities the transport never reported, or the
// chaos classifier's "provably never sent" branch would fire on writes whose
// outcome is genuinely unknown.
func TestUnreachable_ChainStopsAtTheTransportError(t *testing.T) {
	c := client.New(client.Config{Host: closedPortHost(t), Timeout: 2 * time.Second})
	_, err := c.Get(context.Background(), "key")
	require.Error(t, err)

	assert.NotErrorIs(t, err, syscall.ECONNRESET, "a refused dial is not a reset")
	assert.NotErrorIs(t, err, client.ErrNotFound)
	assert.NotErrorIs(t, err, client.ErrServerError)
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
}

func TestGet_ContextCancelled(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// slow handler — context will cancel before response
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.Get(ctx, "key")
	assert.Error(t, err)
	assert.NotErrorIs(t, err, client.ErrUnreachable, "cancelled context should not map to ErrUnreachable")
}

func TestPut_Success(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/keys/foo", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		assert.Equal(t, "bar", body["value"])
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	err := c.Put(context.Background(), "foo", "bar")
	require.NoError(t, err)
}

func TestPut_ServerError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"write failed"}`))
	}))
	err := c.Put(context.Background(), "foo", "bar")
	assert.ErrorIs(t, err, client.ErrServerError)
}

func TestDelete_Success(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/keys/foo", r.URL.Path)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	err := c.Delete(context.Background(), "foo")
	require.NoError(t, err)
}

func TestDelete_NotFound(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	err := c.Delete(context.Background(), "foo")
	assert.ErrorIs(t, err, client.ErrNotFound)
}

func TestStatus_AllFields(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/status", r.URL.Path)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id":   "node1",
			"leader":    "node1",
			"term":      3,
			"role":      "leader",
			"key_count": 42,
		})
	}))
	s, err := c.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "node1", s.NodeID)
	assert.Equal(t, "node1", s.Leader)
	assert.Equal(t, uint64(3), s.Term)
	assert.Equal(t, "leader", s.Role)
	assert.Equal(t, 42, s.KeyCount)
}

func TestMetrics_Success(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/metrics", r.URL.Path)
		json.NewEncoder(w).Encode(map[string]uint64{
			"put_total": 5,
			"get_total": 10,
		})
	}))
	m, err := c.Metrics(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(5), m["put_total"])
	assert.Equal(t, uint64(10), m["get_total"])
}

// TestConnectionReuseAcrossCalls pins the drain-before-close contract: if any
// verb leaves response bytes unread, Go's transport discards the connection
// and the server sees a new one per request. One connection serving many
// sequential calls is the observable proof the pool works — the regression
// this guards caused ephemeral-port exhaustion at benchmark rates.
func TestConnectionReuseAcrossCalls(t *testing.T) {
	var mu sync.Mutex
	remotes := make(map[string]struct{})

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = struct{}{}
		mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			fmt.Fprintln(w, `{"value":"v"}`)
		default:
			fmt.Fprintln(w, `{"status":"ok"}`)
		}
	}))
	srv.Start()
	defer srv.Close()

	c := client.New(client.Config{
		Host:    strings.TrimPrefix(srv.URL, "http://"),
		Timeout: 5 * time.Second,
	})
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := c.Put(ctx, "k", "v"); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if _, err := c.Get(ctx, "k"); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if err := c.Delete(ctx, "k"); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 1 {
		t.Fatalf("60 sequential calls used %d connections, want 1 (bodies not drained → transport discards conns)", len(remotes))
	}
}
