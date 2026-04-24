package cli

import (
	"github.com/spf13/cobra"
)

func (c *CLI) newGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Retrieve the value for a key",
		Long: `Retrieve the value for a key from the cluster.

Exit codes:
  0  success
  1  key not found
  2  node unreachable`,
		RunE: c.runGet,
	}
	cmd.Flags().StringP("output", "o", "", `output format: "table" or "json"`)
	return cmd
}

func (c *CLI) runGet(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return badArgs("get requires exactly 1 argument (key), got %d", len(args))
	}
	key := args[0]
	fmtr := c.resolveFormatter(cmd)

	value, err := c.appCtx.Client.Get(cmd.Context(), key)
	if err != nil {
		return translateErr(c.appCtx.Config.Host, key, err)
	}
	fmtr.KeyValue(key, value)
	return nil
}
