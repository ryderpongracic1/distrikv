// Command node is the distrikv node binary. One instance runs per cluster
// member. Configuration is read entirely from environment variables so that
// Docker Compose can inject node-specific values without rebuilding the image.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ryderpongracic1/distrikv/internal/config"
)

// version is injected by release builds with -X main.version=<tag>.
var version = "dev"

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		fmt.Printf("distrikv-node %s\n", version)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logger.Info("configuration loaded",
		"version", version,
		"node_id", cfg.NodeID,
		"http_addr", cfg.HTTPAddr,
		"grpc_addr", cfg.GRPCAddr,
		"data_dir", cfg.DataDir,
		"peer_count", len(cfg.Peers),
		"replica_count", cfg.ReplicaCount,
		"election_timeout_min", cfg.ElectionTimeoutMin,
		"election_timeout_max", cfg.ElectionTimeoutMax,
		"heartbeat_interval", cfg.HeartbeatInterval,
	)

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
