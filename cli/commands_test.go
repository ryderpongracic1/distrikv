package cli_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ryderpongracic1/distrikv/cli"
	"github.com/ryderpongracic1/distrikv/internal/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient implements cli.ClientIface for unit tests.
type mockClient struct {
	getFn     func(ctx context.Context, key string) (string, error)
	putFn     func(ctx context.Context, key, value string) error
	deleteFn  func(ctx context.Context, key string) error
	statusFn  func(ctx context.Context) (*client.StatusResponse, error)
	metricsFn func(ctx context.Context) (client.MetricsResponse, error)
}

func (m *mockClient) Get(ctx context.Context, key string) (string, error) {
	if m.getFn != nil {
		return m.getFn(ctx, key)
	}
	return "", nil
}
func (m *mockClient) Put(ctx context.Context, key, value string) error {
	if m.putFn != nil {
		return m.putFn(ctx, key, value)
	}
	return nil
}
func (m *mockClient) Delete(ctx context.Context, key string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, key)
	}
	return nil
}
func (m *mockClient) Status(ctx context.Context) (*client.StatusResponse, error) {
	if m.statusFn != nil {
		return m.statusFn(ctx)
	}
	return &client.StatusResponse{}, nil
}
func (m *mockClient) Metrics(ctx context.Context) (client.MetricsResponse, error) {
	if m.metricsFn != nil {
		return m.metricsFn(ctx)
	}
	return client.MetricsResponse{}, nil
}

// newTestCLI returns a CLI with a mock client and buffer-backed formatters.
// out and errBuf capture stdout and stderr respectively.
func newTestCLI(mock *mockClient) (*cli.CLI, *bytes.Buffer, *bytes.Buffer) {
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	appCtx := &cli.AppContext{
		Client:    mock,
		Formatter: cli.NewTableFormatter(out, errBuf),
		Config: &cli.ResolvedConfig{
			Host:    "localhost:8001",
			Timeout: 5 * time.Second,
			Output:  "table",
		},
	}
	return cli.NewWithContext(appCtx, "test"), out, errBuf
}

// extractCLIError asserts that err is a *cli.CLIError with the given exit code.
func extractCLIError(t *testing.T, err error, wantCode int) {
	t.Helper()
	var cliErr *cli.CLIError
	require.True(t, errors.As(err, &cliErr), "expected *cli.CLIError, got %T: %v", err, err)
	assert.Equal(t, wantCode, cliErr.Code)
}

// ── get ───────────────────────────────────────────────────────────────────────

func TestGetCmd_TableOutput(t *testing.T) {
	c, out, _ := newTestCLI(&mockClient{
		getFn: func(_ context.Context, key string) (string, error) {
			assert.Equal(t, "abc", key)
			return "bar", nil
		},
	})
	c.Root().SetArgs([]string{"get", "abc"})
	err := c.Execute()
	require.NoError(t, err)
	assert.Contains(t, out.String(), "KEY")
	assert.Contains(t, out.String(), "abc")
	assert.Contains(t, out.String(), "bar")
}

func TestGetCmd_JSONOutput(t *testing.T) {
	out := &bytes.Buffer{}
	appCtx := &cli.AppContext{
		Client: &mockClient{
			getFn: func(_ context.Context, key string) (string, error) {
				return "bar", nil
			},
		},
		Formatter: cli.NewJSONFormatter(out, &bytes.Buffer{}),
		Config:    &cli.ResolvedConfig{Host: "localhost:8001", Timeout: 5 * time.Second, Output: "json"},
	}
	c := cli.NewWithContext(appCtx, "test")
	c.Root().SetArgs([]string{"get", "abc"})
	err := c.Execute()
	require.NoError(t, err)
	assert.Contains(t, out.String(), `"key"`)
	assert.Contains(t, out.String(), `"abc"`)
	assert.Contains(t, out.String(), `"bar"`)
}

func TestGetCmd_NotFound(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{
		getFn: func(_ context.Context, key string) (string, error) {
			return "", client.ErrNotFound
		},
	})
	c.Root().SetArgs([]string{"get", "missing"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitKeyNotFound)
	assert.Contains(t, err.Error(), "missing")
}

func TestGetCmd_Unreachable(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{
		getFn: func(_ context.Context, key string) (string, error) {
			return "", client.ErrUnreachable
		},
	})
	c.Root().SetArgs([]string{"get", "key"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitUnreachable)
}

func TestGetCmd_BadArgs_TooMany(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"get", "a", "b"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
}

func TestGetCmd_BadArgs_None(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"get"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
}

// ── put ───────────────────────────────────────────────────────────────────────

// withStdinPipe replaces os.Stdin with a pipe that delivers content,
// restoring the original on cleanup. Test processes run without a terminal
// so the put command always reads from stdin; positional args cannot be used.
func withStdinPipe(t *testing.T, content string) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	old := os.Stdin
	os.Stdin = r
	t.Cleanup(func() {
		os.Stdin = old
		r.Close()
	})
	w.WriteString(content)
	w.Close()
}

func TestPutCmd_StdinValue(t *testing.T) {
	withStdinPipe(t, "bar\n")
	var gotKey, gotVal string
	c, out, _ := newTestCLI(&mockClient{
		putFn: func(_ context.Context, key, value string) error {
			gotKey, gotVal = key, value
			return nil
		},
	})
	c.Root().SetArgs([]string{"put", "foo"})
	err := c.Execute()
	require.NoError(t, err)
	assert.Equal(t, "foo", gotKey)
	assert.Equal(t, "bar", gotVal)
	assert.Contains(t, out.String(), "foo")
	assert.Contains(t, out.String(), "bar")
}

func TestPutCmd_ServerError(t *testing.T) {
	withStdinPipe(t, "bar\n")
	c, _, _ := newTestCLI(&mockClient{
		putFn: func(_ context.Context, key, value string) error {
			return client.ErrServerError
		},
	})
	c.Root().SetArgs([]string{"put", "foo"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitServerError)
}

func TestPutCmd_NoKey(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"put"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
}

// ── delete ────────────────────────────────────────────────────────────────────

func TestDeleteCmd_WithConfirmFlag(t *testing.T) {
	var deletedKey string
	c, out, _ := newTestCLI(&mockClient{
		deleteFn: func(_ context.Context, key string) error {
			deletedKey = key
			return nil
		},
	})
	c.Root().SetArgs([]string{"delete", "-y", "mykey"})
	err := c.Execute()
	require.NoError(t, err)
	assert.Equal(t, "mykey", deletedKey)
	assert.Contains(t, out.String(), "mykey")
}

func TestDeleteCmd_NotFound(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{
		deleteFn: func(_ context.Context, key string) error {
			return client.ErrNotFound
		},
	})
	c.Root().SetArgs([]string{"delete", "-y", "ghost"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitKeyNotFound)
	assert.Contains(t, err.Error(), "ghost")
}

func TestDeleteCmd_BadArgs(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"delete", "-y", "a", "b"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
}

// ── status ────────────────────────────────────────────────────────────────────

func TestStatusCmd_TableOutput(t *testing.T) {
	c, out, _ := newTestCLI(&mockClient{
		statusFn: func(_ context.Context) (*client.StatusResponse, error) {
			return &client.StatusResponse{
				NodeID:   "node1",
				Role:     "leader",
				Leader:   "node1",
				Term:     3,
				KeyCount: 42,
			}, nil
		},
	})
	c.Root().SetArgs([]string{"status"})
	err := c.Execute()
	require.NoError(t, err)
	assert.Contains(t, out.String(), "node1")
	assert.Contains(t, out.String(), "leader")
	assert.Contains(t, out.String(), "3")
}

func TestStatusCmd_Unreachable(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{
		statusFn: func(_ context.Context) (*client.StatusResponse, error) {
			return nil, client.ErrUnreachable
		},
	})
	c.Root().SetArgs([]string{"status"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitUnreachable)
}

// ── metrics ───────────────────────────────────────────────────────────────────

func TestMetricsCmd_TableOutput(t *testing.T) {
	c, out, _ := newTestCLI(&mockClient{
		metricsFn: func(_ context.Context) (client.MetricsResponse, error) {
			return client.MetricsResponse{
				"put_total": 5,
				"get_total": 10,
			}, nil
		},
	})
	c.Root().SetArgs([]string{"metrics"})
	err := c.Execute()
	require.NoError(t, err)
	assert.Contains(t, out.String(), "METRIC")
	assert.Contains(t, out.String(), "PutTotal")
	assert.Contains(t, out.String(), "GetTotal")
}

func TestMetricsCmd_Unreachable(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{
		metricsFn: func(_ context.Context) (client.MetricsResponse, error) {
			return nil, client.ErrUnreachable
		},
	})
	c.Root().SetArgs([]string{"metrics"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitUnreachable)
}

// ── watch state machine ───────────────────────────────────────────────────────

func TestWatchCmd_BadArgs(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"watch"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
}

// ── formatter ─────────────────────────────────────────────────────────────────

func TestTableFormatter_KeyValue(t *testing.T) {
	out := &bytes.Buffer{}
	f := cli.NewTableFormatter(out, &bytes.Buffer{})
	f.KeyValue("mykey", "myval")
	s := out.String()
	assert.Contains(t, s, "KEY")
	assert.Contains(t, s, "VALUE")
	assert.Contains(t, s, "mykey")
	assert.Contains(t, s, "myval")
}

func TestTableFormatter_PutResult(t *testing.T) {
	out := &bytes.Buffer{}
	f := cli.NewTableFormatter(out, &bytes.Buffer{})
	f.PutResult("k", "v")
	assert.Contains(t, out.String(), "OK")
	assert.Contains(t, out.String(), "k")
	assert.Contains(t, out.String(), "v")
}

func TestTableFormatter_DeleteResult(t *testing.T) {
	out := &bytes.Buffer{}
	f := cli.NewTableFormatter(out, &bytes.Buffer{})
	f.DeleteResult("abc")
	assert.Contains(t, out.String(), "Deleted")
	assert.Contains(t, out.String(), "abc")
}

func TestTableFormatter_MetricsSorted(t *testing.T) {
	out := &bytes.Buffer{}
	f := cli.NewTableFormatter(out, &bytes.Buffer{})
	f.Metrics(client.MetricsResponse{
		"put_total":    3,
		"delete_total": 1,
		"get_total":    7,
	})
	s := out.String()
	// Sorted order: DeleteTotal, GetTotal, PutTotal
	dIdx := indexOfSubstr(s, "DeleteTotal")
	gIdx := indexOfSubstr(s, "GetTotal")
	pIdx := indexOfSubstr(s, "PutTotal")
	assert.Less(t, dIdx, gIdx, "DeleteTotal should appear before GetTotal")
	assert.Less(t, gIdx, pIdx, "GetTotal should appear before PutTotal")
}

func TestJSONFormatter_KeyValue(t *testing.T) {
	out := &bytes.Buffer{}
	f := cli.NewJSONFormatter(out, &bytes.Buffer{})
	f.KeyValue("k", "v")
	s := out.String()
	assert.Contains(t, s, `"key"`)
	assert.Contains(t, s, `"k"`)
	assert.Contains(t, s, `"value"`)
	assert.Contains(t, s, `"v"`)
}

func TestFormatter_ErrorGoesToErrWriter(t *testing.T) {
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	f := cli.NewTableFormatter(out, errBuf)
	f.Error("something went wrong")
	assert.Empty(t, out.String(), "error must not appear on stdout")
	assert.Contains(t, errBuf.String(), "something went wrong")
}

// ── config set validation ─────────────────────────────────────────────────────

func TestConfigSet_InvalidKey(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"config", "set", "badkey", "val"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
	assert.Contains(t, err.Error(), "badkey")
}

func TestConfigSet_WrongArgCount(t *testing.T) {
	c, _, _ := newTestCLI(&mockClient{})
	c.Root().SetArgs([]string{"config", "set", "host"})
	err := c.Execute()
	extractCLIError(t, err, cli.ExitBadArgs)
}

// ── version ───────────────────────────────────────────────────────────────────

func TestVersionCmd(t *testing.T) {
	out := &bytes.Buffer{}
	appCtx := &cli.AppContext{
		Client:    &mockClient{},
		Formatter: cli.NewTableFormatter(out, &bytes.Buffer{}),
		Config:    &cli.ResolvedConfig{Host: "localhost:8001", Timeout: 5 * time.Second, Output: "table"},
	}
	c := cli.NewWithContext(appCtx, "v1.2.3")
	// version writes to os.Stdout directly, not through Formatter — just verify no error
	c.Root().SetArgs([]string{"version"})
	err := c.Execute()
	require.NoError(t, err)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func indexOfSubstr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
