package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
