// Command node is the distrikv node binary. One instance runs per cluster
// member. Configuration is read entirely from environment variables so that
// Docker Compose can inject node-specific values without rebuilding the image.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryderpongracic1/distrikv/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Top-level context — cancelled on SIGINT or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	node, err := NewNode(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to initialise node", "error", err)
		os.Exit(1)
	}

	if err := node.Run(ctx); err != nil {
		logger.Error("node exited with error", "error", err)
		os.Exit(1)
	}
}
