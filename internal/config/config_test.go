package config

import (
	"strings"
	"testing"
)

func validEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NODE_ID", "node1")
	t.Setenv("HTTP_ADDR", ":8001")
	t.Setenv("GRPC_ADDR", ":9001")
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("PEERS", "node2=node2:9002,node3=node3:9003")
	t.Setenv("REPLICA_COUNT", "2")
	t.Setenv("ELECTION_TIMEOUT_MIN", "500ms")
	t.Setenv("ELECTION_TIMEOUT_MAX", "1s")
	t.Setenv("HEARTBEAT_INTERVAL", "150ms")
}

func TestLoadFromEnvValidatesTopologyAndTiming(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{"replicas exceed nodes", "REPLICA_COUNT", "4", "exceeds cluster size"},
		{"heartbeat reaches election timeout", "HEARTBEAT_INTERVAL", "500ms", "must be less"},
		{"duplicate peer id", "PEERS", "node2=node2:9002,node2=node3:9003", "duplicate or self"},
		{"self peer id", "PEERS", "node1=node2:9002,node3=node3:9003", "duplicate or self"},
		{"duplicate peer address", "PEERS", "node2=node2:9002,node3=node2:9002", "duplicate PEERS address"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			validEnv(t)
			t.Setenv(tc.key, tc.value)
			_, err := LoadFromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadFromEnv error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestLoadFromEnvAcceptsSingleNodeWithOneReplica(t *testing.T) {
	validEnv(t)
	t.Setenv("PEERS", "")
	t.Setenv("REPLICA_COUNT", "1")
	if _, err := LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv single node: %v", err)
	}
}
