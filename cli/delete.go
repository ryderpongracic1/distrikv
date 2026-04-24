package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a key from the cluster",
		Long: `Delete a key from the cluster.

Without --confirm, prompts for confirmation in interactive mode.
In non-interactive (piped) mode, --confirm (-y) is required.`,
		RunE: c.runDelete,
	}
	cmd.Flags().BoolP("confirm", "y", false, "skip deletion confirmation prompt")
	cmd.Flags().StringP("output", "o", "", `output format: "table" or "json"`)
	return cmd
}

func (c *CLI) runDelete(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return badArgs("delete requires exactly 1 argument (key), got %d", len(args))
	}
	key := args[0]
	confirm, _ := cmd.Flags().GetBool("confirm")

	if !confirm {
		if !isTerminal(int(os.Stdin.Fd())) {
			return badArgs("--confirm (-y) is required in non-interactive mode")
		}
		fmt.Fprintf(os.Stderr, "Delete key %q? [y/N]: ", key)
		var response string
		fmt.Fscanln(os.Stdin, &response)
		if strings.ToLower(strings.TrimSpace(response)) != "y" {
			return cancelled(fmt.Sprintf("delete of %q cancelled", key))
		}
	}

	if err := c.appCtx.Client.Delete(cmd.Context(), key); err != nil {
		return translateErr(c.appCtx.Config.Host, key, err)
	}
	fmtr := c.resolveFormatter(cmd)
	fmtr.DeleteResult(key)
	return nil
}
