package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newMetricsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Display metrics from the configured node",
		Long: `Display metrics from the configured node.

With --watch, re-polls every 2 seconds (or --interval) and reprints the table
in-place using ANSI escape codes. Press Ctrl+C to stop (exits 0).`,
		RunE: c.runMetrics,
	}
	cmd.Flags().BoolP("watch", "w", false, "continuously poll and reprint metrics")
	cmd.Flags().Duration("interval", 2*time.Second, "poll interval for --watch mode")
	cmd.Flags().StringP("output", "o", "", `output format: "table" or "json"`)
	return cmd
}

func (c *CLI) runMetrics(cmd *cobra.Command, args []string) error {
	watchMode, _ := cmd.Flags().GetBool("watch")
	interval, _ := cmd.Flags().GetDuration("interval")
	fmtr := c.resolveFormatter(cmd)
	cfg := c.appCtx.Config

	if !watchMode {
		m, err := c.appCtx.Client.Metrics(cmd.Context())
		if err != nil {
			return translateErr(cfg.Host, "", err)
		}
		fmtr.Metrics(m)
		return nil
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Poll once immediately before the first tick.
	if m, err := c.appCtx.Client.Metrics(ctx); err == nil {
		clearTerminal(os.Stdout)
		fmtr.Metrics(m)
	}

	for {
		select {
		case <-ctx.Done():
			return nil // Ctrl-C is the expected stop mechanism; exit 0
		case <-ticker.C:
			m, err := c.appCtx.Client.Metrics(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				fmtr.Error(fmt.Sprintf("metrics fetch error: %v", err))
				continue
			}
			clearTerminal(os.Stdout)
			fmtr.Metrics(m)
		}
	}
}
