package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Error contract.
//
// Every error this package returns is matchable with errors.Is against one of
// the sentinels below, except two deliberate pass-throughs: a cancelled or
// expired request context is returned unchanged (so callers can tell SIGINT
// from a network failure), and a malformed response body is returned as
// "unexpected response: %w" over the json error.
//
// Wrapping preserves the cause. ErrUnreachable in particular is joined to the
// transport error rather than replacing it, so both of these hold on the same
// value:
//
//	errors.Is(err, ErrUnreachable)         // this package's classification
//	errors.Is(err, syscall.ECONNREFUSED)   // the cause underneath it
//	errors.As(err, &opErr)                 // *net.OpError, *net.DNSError, ...
//
// Callers must classify with errors.Is/errors.As. The message text is not part
// of the contract — do not match on substrings.
var (
	ErrNotFound    = errors.New("key not found")
	ErrUnreachable = errors.New("node unreachable")
	ErrServerError = errors.New("server error")
)

type Config struct {
	Host    string
	Timeout time.Duration
}

type Client struct {
	base string
	http *http.Client
}

type GetResponse struct {
	Value string `json:"value"`
}

type PutRequest struct {
	Value string `json:"value"`
}

// StatusResponse matches the server's /status JSON exactly, including key_count.
//
// KeyCount is an approximation maintained by the storage engine, which is why the
// server also sends key_count_approximate; see store.Store.KeyCount for the
// accuracy contract.
type StatusResponse struct {
	NodeID              string `json:"node_id"`
	Leader              string `json:"leader"`
	Term                uint64 `json:"term"`
	Role                string `json:"role"`
	KeyCount            int    `json:"key_count"`
	KeyCountApproximate bool   `json:"key_count_approximate"`
}

// MetricsResponse is a flat map of counter name to uint64 value.
// The CLI sorts keys before display; the client returns the raw server map.
type MetricsResponse map[string]uint64

type errorBody struct {
	Error string `json:"error"`
}

// New constructs a Client. No network I/O happens; connections are lazy.
// If cfg.Timeout is zero, 5s is used.
func New(cfg Config) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		base: "http://" + cfg.Host,
		http: &http.Client{Timeout: timeout},
	}
}

// NewWithTransport constructs a Client backed by the supplied RoundTripper.
// Use this when you need to control connection pooling — for example, a
// high-concurrency benchmark must size MaxIdleConnsPerHost above the default
// 2 to avoid TCP TIME_WAIT exhaustion.
func NewWithTransport(cfg Config, rt http.RoundTripper) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Client{
		base: "http://" + cfg.Host,
		http: &http.Client{Timeout: timeout, Transport: rt},
	}
}

func (c *Client) url(path string) string {
	return c.base + path
}

// do executes the request. Context cancellation from the caller passes through
// unchanged; other network errors are wrapped as ErrUnreachable with the
// transport error kept in the chain.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		// If the caller's context was cancelled, return that error unchanged so
		// the watch/metrics loops can distinguish SIGINT from a network failure.
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			// Two %w verbs, so the result carries both the sentinel and the
			// cause: errors.Is(err, ErrUnreachable) classifies it, and
			// errors.Is(err, syscall.ECONNREFUSED) / errors.As(err, &opErr)
			// still reach the network error underneath. A single %v here would
			// render the cause as text and truncate the chain at the sentinel,
			// forcing consumers into substring matching.
			//
			// %w and %v format identically, so the message is unchanged.
			// urlErr.Err rather than urlErr keeps url.Error's method-and-URL
			// prefix ("Get \"http://host/keys/k\": ") out of that message.
			return nil, fmt.Errorf("%w: %w", ErrUnreachable, urlErr.Err)
		}
		return nil, err
	}
	return resp, nil
}

// drainAndClose consumes any unread response bytes before closing the body.
// An HTTP/1.1 connection whose body is closed with bytes still unread is
// discarded by Go's transport instead of returned to the idle pool, so every
// call would otherwise pay a fresh TCP dial — under load this exhausts
// ephemeral ports (the "cannot assign requested address" failure mode).
// The limit bounds the drain so a pathologically large error body cannot
// stall the caller; past it, dropping the connection is the right outcome.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4<<10))
	_ = body.Close()
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/keys/"+key), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode >= 500 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("%w: %s", ErrServerError, string(body))
	}
	var gr GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		return "", fmt.Errorf("unexpected response: %w", err)
	}
	return gr.Value, nil
}

func (c *Client) Put(ctx context.Context, key string, value string) error {
	body, err := json.Marshal(PutRequest{Value: value})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url("/keys/"+key), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode >= 500 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s", ErrServerError, string(b))
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.url("/keys/"+key), nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 500 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s", ErrServerError, string(b))
	}
	return nil
}

func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/status"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode >= 500 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: %s", ErrServerError, string(b))
	}
	var sr StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, fmt.Errorf("unexpected response: %w", err)
	}
	return &sr, nil
}

func (c *Client) Metrics(ctx context.Context) (MetricsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/metrics"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode >= 500 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: %s", ErrServerError, string(b))
	}
	var mr MetricsResponse
	if err := json.NewDecoder(resp.Body).Decode(&mr); err != nil {
		return nil, fmt.Errorf("unexpected response: %w", err)
	}
	return mr, nil
}
