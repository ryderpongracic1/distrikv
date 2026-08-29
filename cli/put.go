package cli

import (
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func (c *CLI) newPutCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put <key> [value]",
		Short: "Set a key to a value",
		Long: `Set a key to a value in the cluster.

Value can be provided as a positional argument or piped via stdin:

  distrikv-cli put mykey myvalue
  echo "myvalue" | distrikv-cli put mykey
  cat data.json | distrikv-cli put mykey`,
		RunE: c.runPut,
	}
	cmd.Flags().StringP("output", "o", "", `output format: "table" or "json"`)
	return cmd
}

func (c *CLI) runPut(cmd *cobra.Command, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return badArgs("put requires <key> <value>, or <key> with a value piped via stdin")
	}
	key := args[0]

	var value string
	if len(args) == 2 {
		// Prefer an explicit positional value even when stdin is not a terminal.
		// CI runners and shell scripts commonly attach /dev/null as stdin; treating
		// that as piped data made the documented form unusable outside a TTY.
		value = args[1]
	} else if !isTerminal(int(os.Stdin.Fd())) {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return &CLIError{Msg: "failed to read stdin: " + err.Error(), Code: ExitServerError}
		}
		value = strings.TrimRight(string(data), "\r\n")
	} else {
		return badArgs("put requires a value argument (or pipe via stdin)")
	}

	fmtr := c.resolveFormatter(cmd)
	if err := c.appCtx.Client.Put(cmd.Context(), key, value); err != nil {
		return translateErr(c.appCtx.Config.Host, "", err)
	}
	fmtr.PutResult(key, value)
	return nil
}
