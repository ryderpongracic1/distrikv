package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func (c *CLI) newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("distrikv-cli %s\n", c.ver)
		},
	}
	// Override PersistentPreRunE: version doesn't need a client, config file,
	// or reachable node. A no-op here prevents Viper setup and client construction.
	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return nil
	}
	return cmd
}
