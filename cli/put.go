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
	if len(args) < 1 {
		return badArgs("put requires at least 1 argument (key)")
	}
	key := args[0]

	stdinIsPipe := !isTerminal(int(os.Stdin.Fd()))

	if stdinIsPipe && len(args) > 1 {
		return badArgs("value must come from stdin or positional argument, not both")
	}

	var value string
	if stdinIsPipe {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return &CLIError{Msg: "failed to read stdin: " + err.Error(), Code: ExitServerError}
		}
		value = strings.TrimRight(string(data), "\r\n")
	} else if len(args) >= 2 {
		value = args[1]
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
