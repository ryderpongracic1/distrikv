package cli

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

func (c *CLI) newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster node status",
		Long: `Show the status of the configured node (role, leader, term, key count).

With --all, queries the configured node plus all configured peers concurrently.
Unreachable nodes are shown as "[unreachable]" in the table rather than failing
the entire command.`,
		RunE: c.runStatus,
	}
	cmd.Flags().Bool("all", false, "query all nodes (configured node + peers)")
	cmd.Flags().String("peers", "", "comma-separated peer addresses, e.g. localhost:8002,localhost:8003")
	cmd.Flags().StringP("output", "o", "", `output format: "table" or "json"`)
	return cmd
}

func (c *CLI) runStatus(cmd *cobra.Command, args []string) error {
	all, _ := cmd.Flags().GetBool("all")
	fmtr := c.resolveFormatter(cmd)
	cfg := c.appCtx.Config

	if !all {
		s, err := c.appCtx.Client.Status(cmd.Context())
		if err != nil {
			return translateErr(cfg.Host, "", err)
		}
		fmtr.Status(s)
		return nil
	}

	peers := cfg.Peers
	if cmd.Flags().Changed("peers") {
		raw, _ := cmd.Flags().GetString("peers")
		peers = strings.Split(raw, ",")
	}
	allHosts := append([]string{cfg.Host}, peers...)
	results := queryAllNodes(cmd.Context(), allHosts, cfg.Timeout)
	fmtr.AllStatus(results)
	return nil
}

// queryAllNodes fans out Status calls to all hosts concurrently.
// sync.WaitGroup is used instead of errgroup so all nodes are queried even if
// some fail — per-node errors are recorded in NodeStatusResult.Err.
func queryAllNodes(ctx context.Context, hosts []string, timeout time.Duration) []NodeStatusResult {
	results := make([]NodeStatusResult, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		i, host := i, host
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := client.New(client.Config{Host: host, Timeout: timeout})
			s, err := cl.Status(ctx)
			results[i] = NodeStatusResult{Host: host, Status: s, Err: err}
		}()
	}
	wg.Wait()
	return results
}
