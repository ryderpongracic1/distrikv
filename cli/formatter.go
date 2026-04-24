package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

// NodeStatusResult holds the outcome of querying one node for status --all.
// Err != nil means the node was unreachable; Status is nil in that case.
type NodeStatusResult struct {
	Host   string
	Status *client.StatusResponse
	Err    error
}

// Formatter is the output abstraction consumed by every command.
// Methods that emit data write to an io.Writer (normally os.Stdout).
// Error() always writes to a separate io.Writer (normally os.Stderr).
// This separation keeps stdout machine-parseable even when errors occur.
type Formatter interface {
	KeyValue(key, value string)               // get
	PutResult(key, value string)              // put
	DeleteResult(key string)                  // delete
	Status(s *client.StatusResponse)          // status single-node
	AllStatus(nodes []NodeStatusResult)       // status --all
	Metrics(m client.MetricsResponse)         // metrics
	WatchLine(ts time.Time, key, val string)  // watch (val="[deleted]" for removal)
	Error(msg string)                         // always stderr, always plain text
}

// tabwriter settings — uniform across all tables.
const (
	twMinWidth = 0
	twTabWidth = 0
	twPadding  = 3
	twPadChar  = ' '
	twFlags    = 0
)

// newTW creates a tabwriter per call so Flush() scope is always clean.
func newTW(out io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(out, twMinWidth, twTabWidth, twPadding, twPadChar, twFlags)
}

// metricDisplayNames converts snake_case server keys to PascalCase for table display.
// Raw keys are preserved in JSON mode.
var metricDisplayNames = map[string]string{
	"put_total":          "PutTotal",
	"get_total":          "GetTotal",
	"delete_total":       "DeleteTotal",
	"get_miss":           "GetMiss",
	"wal_writes":         "WALWrites",
	"raft_terms":         "RaftTerms",
	"leader_elections":   "LeaderElections",
	"forwarded_requests": "ForwardedRequests",
	"replication_errors": "ReplicationErrors",
	"key_count":          "KeyCount",
	"raft_term":          "RaftTerm",
}

func displayMetricKey(k string) string {
	if d, ok := metricDisplayNames[k]; ok {
		return d
	}
	return k
}

// ── TableFormatter ────────────────────────────────────────────────────────────

type tableFormatter struct {
	out io.Writer
	err io.Writer
}

// NewTableFormatter returns a Formatter that uses text/tabwriter for aligned output.
// out and err are injectable for tests (normally os.Stdout and os.Stderr).
func NewTableFormatter(out, err io.Writer) Formatter {
	return &tableFormatter{out: out, err: err}
}

func (f *tableFormatter) KeyValue(key, value string) {
	w := newTW(f.out)
	fmt.Fprintln(w, "KEY\tVALUE")
	fmt.Fprintf(w, "%s\t%s\n", key, value)
	w.Flush()
}

func (f *tableFormatter) PutResult(key, value string) {
	fmt.Fprintf(f.out, "OK  %s → %s\n", key, value)
}

func (f *tableFormatter) DeleteResult(key string) {
	fmt.Fprintf(f.out, "Deleted %q\n", key)
}

func (f *tableFormatter) Status(s *client.StatusResponse) {
	w := newTW(f.out)
	fmt.Fprintln(w, "NODE\tROLE\tLEADER\tTERM\tKEYS")
	fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\n", s.NodeID, s.Role, s.Leader, s.Term, s.KeyCount)
	w.Flush()
}

func (f *tableFormatter) AllStatus(nodes []NodeStatusResult) {
	w := newTW(f.out)
	fmt.Fprintln(w, "HOST\tNODE\tROLE\tLEADER\tTERM\tKEYS")
	for _, n := range nodes {
		if n.Err != nil {
			fmt.Fprintf(w, "%s\t-\t[unreachable]\t-\t-\t-\n", n.Host)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\n",
				n.Host, n.Status.NodeID, n.Status.Role, n.Status.Leader,
				n.Status.Term, n.Status.KeyCount)
		}
	}
	w.Flush()
}

func (f *tableFormatter) Metrics(m client.MetricsResponse) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	w := newTW(f.out)
	fmt.Fprintln(w, "METRIC\tVALUE")
	for _, k := range keys {
		fmt.Fprintf(w, "%s\t%d\n", displayMetricKey(k), m[k])
	}
	w.Flush()
}

func (f *tableFormatter) WatchLine(ts time.Time, key, val string) {
	if val == "[deleted]" {
		fmt.Fprintf(f.out, "%s  [deleted]\n", ts.Format("2006-01-02 15:04:05"))
	} else {
		fmt.Fprintf(f.out, "%s  %s = %s\n", ts.Format("2006-01-02 15:04:05"), key, val)
	}
}

func (f *tableFormatter) Error(msg string) {
	fmt.Fprintln(f.err, msg)
}

// ── JSONFormatter ─────────────────────────────────────────────────────────────

type jsonFormatter struct {
	enc *json.Encoder
	err io.Writer
}

// NewJSONFormatter returns a Formatter that emits one newline-delimited JSON
// object per call, making output pipeable to jq.
func NewJSONFormatter(out, err io.Writer) Formatter {
	return &jsonFormatter{enc: json.NewEncoder(out), err: err}
}

func (f *jsonFormatter) KeyValue(key, value string) {
	f.enc.Encode(map[string]string{"key": key, "value": value})
}

func (f *jsonFormatter) PutResult(key, value string) {
	f.enc.Encode(map[string]string{"key": key, "value": value, "status": "ok"})
}

func (f *jsonFormatter) DeleteResult(key string) {
	f.enc.Encode(map[string]string{"key": key, "status": "deleted"})
}

func (f *jsonFormatter) Status(s *client.StatusResponse) {
	f.enc.Encode(s)
}

func (f *jsonFormatter) AllStatus(nodes []NodeStatusResult) {
	for _, n := range nodes {
		if n.Err != nil {
			f.enc.Encode(map[string]string{
				"host":   n.Host,
				"status": "unreachable",
				"error":  n.Err.Error(),
			})
		} else {
			f.enc.Encode(map[string]interface{}{
				"host":      n.Host,
				"node_id":   n.Status.NodeID,
				"role":      n.Status.Role,
				"leader":    n.Status.Leader,
				"term":      n.Status.Term,
				"key_count": n.Status.KeyCount,
			})
		}
	}
}

func (f *jsonFormatter) Metrics(m client.MetricsResponse) {
	f.enc.Encode(m)
}

func (f *jsonFormatter) WatchLine(ts time.Time, key, val string) {
	if val == "[deleted]" {
		f.enc.Encode(map[string]interface{}{
			"ts":     ts.UTC().Format(time.RFC3339),
			"key":    key,
			"status": "deleted",
		})
	} else {
		f.enc.Encode(map[string]interface{}{
			"ts":    ts.UTC().Format(time.RFC3339),
			"key":   key,
			"value": val,
		})
	}
}

func (f *jsonFormatter) Error(msg string) {
	fmt.Fprintln(f.err, msg)
}
