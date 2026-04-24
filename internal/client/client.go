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
type StatusResponse struct {
	NodeID   string `json:"node_id"`
	Leader   string `json:"leader"`
	Term     uint64 `json:"term"`
	Role     string `json:"role"`
	KeyCount int    `json:"key_count"`
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

func (c *Client) url(path string) string {
	return c.base + path
}

// do executes the request. Context cancellation from the caller passes through
// unchanged; other network errors are wrapped as ErrUnreachable.
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
			return nil, fmt.Errorf("%w: %v", ErrUnreachable, urlErr.Err)
		}
		return nil, err
	}
	return resp, nil
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
