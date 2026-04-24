package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var validConfigKeys = map[string]bool{
	"host": true, "timeout": true, "output": true, "peers": true,
}

func (c *CLI) newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "View and modify CLI configuration",
		Long: `View and modify the distrikv-cli configuration.

Configuration is read from (in priority order):
  1. --host / --output / --timeout flags
  2. DISTRIKV_HOST / DISTRIKV_OUTPUT / DISTRIKV_TIMEOUT environment variables
  3. .distrikv.yaml in the current directory
  4. .distrikv.yaml in $HOME
  5. Built-in defaults`,
	}
	cmd.AddCommand(c.newConfigShowCmd(), c.newConfigSetCmd())
	return cmd
}

func (c *CLI) newConfigShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Print the fully resolved configuration as YAML",
		RunE:  c.runConfigShow,
	}
}

func (c *CLI) runConfigShow(cmd *cobra.Command, args []string) error {
	settings := map[string]interface{}{
		"host":    viper.GetString("host"),
		"timeout": viper.GetString("timeout"),
		"output":  viper.GetString("output"),
		"peers":   viper.GetStringSlice("peers"),
	}
	if f := viper.ConfigFileUsed(); f != "" {
		fmt.Fprintf(os.Stdout, "# config file: %s\n", f)
	}
	data, err := yaml.Marshal(settings)
	if err != nil {
		return &CLIError{Msg: "failed to marshal config: " + err.Error(), Code: ExitServerError}
	}
	fmt.Fprint(os.Stdout, string(data))
	return nil
}

func (c *CLI) newConfigSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a config value in ~/.distrikv.yaml",
		Long:  "Set a config value in ~/.distrikv.yaml (creates the file if it does not exist).\n\nValid keys: host, timeout, output, peers",
		RunE:  c.runConfigSet,
	}
}

func (c *CLI) runConfigSet(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return badArgs("config set requires exactly 2 arguments: <key> <value>")
	}
	key, value := args[0], args[1]
	if !validConfigKeys[key] {
		return badArgs("unknown config key %q; valid keys: host, timeout, output, peers", key)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return &CLIError{Msg: "cannot find home directory: " + err.Error(), Code: ExitServerError}
	}
	cfgPath := filepath.Join(home, ".distrikv.yaml")

	// Use a fresh Viper instance so CWD config values and env vars are not
	// accidentally written into the user's home config file.
	v := viper.New()
	v.SetConfigFile(cfgPath)
	_ = v.ReadInConfig() // ignore not-found; WriteConfigAs will create the file

	v.Set(key, value)
	if err := v.WriteConfigAs(cfgPath); err != nil {
		return &CLIError{Msg: "failed to write config: " + err.Error(), Code: ExitServerError}
	}
	fmt.Fprintf(os.Stdout, "Set %s = %s in %s\n", key, value, cfgPath)
	return nil
}
