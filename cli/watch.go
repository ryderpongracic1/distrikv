package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

// watchState tracks what has been observed between polls.
type watchState struct {
	lastValue string
	everSeen  bool // prevents printing [deleted] before key ever existed
	deleted   bool // prevents repeated [deleted] lines for the same deletion
}

func (c *CLI) newWatchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch <key>",
		Short: "Poll a key and print each change with a timestamp",
		Long: `Poll a key at a configurable interval and print each observed change
with a timestamp. Unchanged values are not reprinted.

Prints "[deleted]" when a previously-observed key disappears, then keeps
polling in case the key reappears. Press Ctrl+C to stop (exits 0).`,
		RunE: c.runWatch,
	}
	cmd.Flags().DurationP("interval", "i", 1*time.Second, "poll interval")
	cmd.Flags().StringP("output", "o", "", `output format: "table" or "json"`)
	return cmd
}

func (c *CLI) runWatch(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return badArgs("watch requires exactly 1 argument (key), got %d", len(args))
	}
	key := args[0]
	interval, _ := cmd.Flags().GetDuration("interval")
	fmtr := c.resolveFormatter(cmd)
	cfg := c.appCtx.Config

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()

	fmt.Fprintf(os.Stderr, "Watching %q (interval: %s) — Ctrl+C to stop\n", key, interval)

	state := watchState{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil // Ctrl-C is the expected stop mechanism; exit 0
		case t := <-ticker.C:
			c.pollWatch(ctx, key, t, &state, fmtr, cfg.Host)
		}
	}
}

// pollWatch performs one GET poll and updates state.
//
// State transitions:
//
//	ErrNotFound + !everSeen          → silent (key never existed; keep polling)
//	ErrNotFound + everSeen + !deleted → print [deleted], set deleted=true
//	ErrNotFound + everSeen + deleted  → silent (already printed [deleted])
//	value V + (V != lastValue OR !everSeen) → print V, update state
//	value V + V == lastValue          → silent (unchanged)
//	value V after deleted             → print V (re-appeared), set deleted=false
func (c *CLI) pollWatch(
	ctx context.Context,
	key string,
	ts time.Time,
	state *watchState,
	fmtr Formatter,
	host string,
) {
	value, err := c.appCtx.Client.Get(ctx, key)
	switch {
	case errors.Is(err, client.ErrNotFound):
		if state.everSeen && !state.deleted {
			state.deleted = true
			fmtr.WatchLine(ts, key, "[deleted]")
		}
	case err != nil:
		if !errors.Is(err, context.Canceled) {
			fmtr.Error(fmt.Sprintf("poll error: %v (retrying)", err))
		}
	default:
		state.deleted = false
		if !state.everSeen || value != state.lastValue {
			state.everSeen = true
			state.lastValue = value
			fmtr.WatchLine(ts, key, value)
		}
	}
}
