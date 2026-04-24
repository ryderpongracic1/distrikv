package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ryderpongracic1/distrikv/internal/client"
)

// ResolvedConfig is the fully merged configuration after priority resolution.
type ResolvedConfig struct {
	Host    string
	Timeout time.Duration
	Output  string   // "table" | "json"
	Peers   []string // peer addresses for status --all
}

// ClientIface is defined here so CLI tests can inject a mock without a real HTTP client.
// *client.Client satisfies this interface via duck typing.
type ClientIface interface {
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	Status(ctx context.Context) (*client.StatusResponse, error)
	Metrics(ctx context.Context) (client.MetricsResponse, error)
}

// AppContext holds the resolved dependencies injected into every command.
// Commands read from it instead of reaching into Cobra globals or Viper directly.
type AppContext struct {
	Client    ClientIface
	Formatter Formatter
	Config    *ResolvedConfig
}

// CLI is the root of the command tree.
type CLI struct {
	root   *cobra.Command
	appCtx *AppContext
	ver    string
}

// New builds a CLI for normal execution. AppContext is built lazily in PersistentPreRunE.
func New(version string) *CLI {
	c := &CLI{ver: version}
	c.root = c.buildRoot()
	return c
}

// NewWithContext builds a CLI with a pre-built AppContext for tests.
// PersistentPreRunE is replaced with a no-op so no real client or Viper setup occurs.
func NewWithContext(ctx *AppContext, version string) *CLI {
	c := &CLI{appCtx: ctx, ver: version}
	c.root = c.buildRoot()
	c.root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return nil
	}
	return c
}

// Execute runs the root command and returns any error for HandleErr.
func (c *CLI) Execute() error {
	return c.root.Execute()
}

// Root returns the root cobra.Command. Used in tests to set args via SetArgs.
func (c *CLI) Root() *cobra.Command {
	return c.root
}

func (c *CLI) buildRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "distrikv-cli",
		Short:         "CLI for the distrikv distributed key-value store",
		Long:          "A command-line interface for interacting with a distrikv cluster over HTTP.",
		SilenceUsage:  true,  // don't print usage on RunE errors
		SilenceErrors: true,  // don't prepend "Error: " — HandleErr does that
		PersistentPreRunE: c.initAppContext,
	}

	root.PersistentFlags().String("host", "", "target node host:port (overrides config and DISTRIKV_HOST)")
	root.PersistentFlags().StringP("output", "o", "", `output format: "table" or "json"`)
	root.PersistentFlags().Duration("timeout", 0, "HTTP request timeout (e.g. 5s, 500ms)")

	root.AddCommand(
		c.newGetCmd(),
		c.newPutCmd(),
		c.newDeleteCmd(),
		c.newStatusCmd(),
		c.newMetricsCmd(),
		c.newWatchCmd(),
		c.newConfigCmd(),
		c.newVersionCmd(),
	)
	return root
}

// initAppContext is the PersistentPreRunE for all commands except version.
func (c *CLI) initAppContext(cmd *cobra.Command, args []string) error {
	cfg, err := resolveConfig(cmd)
	if err != nil {
		return err
	}
	cl := client.New(client.Config{
		Host:    cfg.Host,
		Timeout: cfg.Timeout,
	})
	var f Formatter
	if cfg.Output == "json" {
		f = NewJSONFormatter(os.Stdout, os.Stderr)
	} else {
		f = NewTableFormatter(os.Stdout, os.Stderr)
	}
	c.appCtx = &AppContext{Client: cl, Formatter: f, Config: cfg}
	return nil
}

// resolveConfig builds a ResolvedConfig from Viper (env + config file + defaults)
// with explicit flag overrides applied only when the flag was Changed.
//
// Priority order (highest first):
//  1. Per-command persistent flag (--host, --output, --timeout) if Changed
//  2. DISTRIKV_HOST / DISTRIKV_OUTPUT / DISTRIKV_TIMEOUT environment variables
//  3. .distrikv.yaml in CWD
//  4. .distrikv.yaml in $HOME
//  5. Built-in defaults
func resolveConfig(cmd *cobra.Command) (*ResolvedConfig, error) {
	home, _ := os.UserHomeDir()

	viper.SetConfigName(".distrikv")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")    // CWD — higher priority
	viper.AddConfigPath(home)   // HOME — fallback

	viper.SetEnvPrefix("DISTRIKV")
	viper.AutomaticEnv() // DISTRIKV_HOST → "host", etc.

	viper.SetDefault("host", "localhost:8001")
	viper.SetDefault("timeout", "5s")
	viper.SetDefault("output", "table")
	viper.SetDefault("peers", []string{})

	if err := viper.ReadInConfig(); err != nil {
		if _, notFound := err.(viper.ConfigFileNotFoundError); !notFound {
			return nil, fmt.Errorf("config file error: %w", err)
		}
		// silently ignore missing config file — it is optional
	}

	// Manual flag override: do NOT use BindPFlag because it reads the flag's
	// zero-value ("") even when the user hasn't set it, overriding env/config.
	host := viper.GetString("host")
	if cmd.Root().PersistentFlags().Changed("host") {
		host, _ = cmd.Root().PersistentFlags().GetString("host")
	}

	outputStr := viper.GetString("output")
	if cmd.Root().PersistentFlags().Changed("output") {
		outputStr, _ = cmd.Root().PersistentFlags().GetString("output")
	}

	timeoutStr := viper.GetString("timeout")
	if cmd.Root().PersistentFlags().Changed("timeout") {
		td, _ := cmd.Root().PersistentFlags().GetDuration("timeout")
		timeoutStr = td.String()
	}

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, badArgs("invalid timeout %q: must be a Go duration (e.g. 5s, 500ms)", timeoutStr)
	}
	if outputStr != "table" && outputStr != "json" {
		return nil, badArgs("invalid output %q: must be \"table\" or \"json\"", outputStr)
	}

	return &ResolvedConfig{
		Host:    host,
		Timeout: timeout,
		Output:  outputStr,
		Peers:   viper.GetStringSlice("peers"),
	}, nil
}

// resolveFormatter returns the formatter from appCtx, or a new one if the
// per-command --output flag was explicitly changed on this subcommand.
func (c *CLI) resolveFormatter(cmd *cobra.Command) Formatter {
	if cmd.Flags().Changed("output") {
		out, _ := cmd.Flags().GetString("output")
		if out == "json" {
			return NewJSONFormatter(os.Stdout, os.Stderr)
		}
		return NewTableFormatter(os.Stdout, os.Stderr)
	}
	return c.appCtx.Formatter
}
