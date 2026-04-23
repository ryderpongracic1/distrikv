// Package config provides runtime configuration for a distrikv node.
// All values are read from environment variables so that Docker Compose can
// inject node-specific settings without rebuilding the binary.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for a single node.
type Config struct {
	// NodeID is a unique, human-readable name for this node (e.g. "node1").
	NodeID string

	// HTTPAddr is the address the HTTP REST API listens on (e.g. ":8001").
	HTTPAddr string

	// GRPCAddr is the address the gRPC peer server listens on (e.g. ":9001").
	GRPCAddr string

	// Peers is the list of all OTHER nodes in the cluster.
	Peers []PeerConfig

	// WALPath is the file path for the Write-Ahead Log (e.g. "/data/wal.log").
	WALPath string

	// DataDir is the base directory for all persistent state files.
	DataDir string

	// ReplicaCount is the number of nodes each key is replicated to (R).
	// Default: 2.
	ReplicaCount int

	// ElectionTimeoutMin is the lower bound of the randomized Raft election
	// timeout. Default: 150ms.
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax is the upper bound of the randomized Raft election
	// timeout. Default: 300ms.
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often the Raft leader sends AppendEntries
	// heartbeats to followers. Default: 75ms.
	HeartbeatInterval time.Duration
}

// PeerConfig describes a single remote node.
type PeerConfig struct {
	// ID is the unique name of the peer node (e.g. "node2").
	ID string

	// GRPCAddr is the host:port the peer's gRPC server listens on (e.g.
	// "node2:9002").
	GRPCAddr string
}

// LoadFromEnv reads all configuration from environment variables.
//
// Required variables:
//
//	NODE_ID   — unique node name, e.g. "node1"
//	HTTP_ADDR — HTTP listen address, e.g. ":8001"
//	GRPC_ADDR — gRPC listen address, e.g. ":9001"
//	WAL_PATH  — path to the WAL file, e.g. "/data/wal.log"
//	DATA_DIR  — base directory for persistent state
//	PEERS     — comma-separated "id=grpcaddr" pairs,
//	            e.g. "node2=node2:9002,node3=node3:9003"
//
// Optional variables:
//
//	REPLICA_COUNT          — default 2
//	ELECTION_TIMEOUT_MIN   — default 150ms  (Go duration string)
//	ELECTION_TIMEOUT_MAX   — default 300ms
//	HEARTBEAT_INTERVAL     — default 75ms
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		ReplicaCount:       2,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
	}

	var missing []string
	required := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}

	cfg.NodeID = required("NODE_ID")
	cfg.HTTPAddr = required("HTTP_ADDR")
	cfg.GRPCAddr = required("GRPC_ADDR")
	cfg.WALPath = required("WAL_PATH")
	cfg.DataDir = required("DATA_DIR")

	if len(missing) > 0 {
		return nil, fmt.Errorf("config: missing required env vars: %s", strings.Join(missing, ", "))
	}

	// Parse PEERS: "node2=node2:9002,node3=node3:9003"
	if raw := os.Getenv("PEERS"); raw != "" {
		for _, pair := range strings.Split(raw, ",") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return nil, fmt.Errorf("config: malformed PEERS entry %q (expected id=grpcaddr)", pair)
			}
			cfg.Peers = append(cfg.Peers, PeerConfig{ID: parts[0], GRPCAddr: parts[1]})
		}
	}

	// Optional overrides.
	if v := os.Getenv("REPLICA_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("config: invalid REPLICA_COUNT %q: %w", v, err)
		}
		cfg.ReplicaCount = n
	}

	if v := os.Getenv("ELECTION_TIMEOUT_MIN"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid ELECTION_TIMEOUT_MIN %q: %w", v, err)
		}
		cfg.ElectionTimeoutMin = d
	}

	if v := os.Getenv("ELECTION_TIMEOUT_MAX"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid ELECTION_TIMEOUT_MAX %q: %w", v, err)
		}
		cfg.ElectionTimeoutMax = d
	}

	if v := os.Getenv("HEARTBEAT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("config: invalid HEARTBEAT_INTERVAL %q: %w", v, err)
		}
		cfg.HeartbeatInterval = d
	}

	if cfg.ElectionTimeoutMin >= cfg.ElectionTimeoutMax {
		return nil, fmt.Errorf("config: ELECTION_TIMEOUT_MIN (%v) must be less than ELECTION_TIMEOUT_MAX (%v)",
			cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax)
	}

	return cfg, nil
}
