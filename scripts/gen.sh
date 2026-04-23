#!/usr/bin/env bash
# gen.sh — regenerate Go code from proto/kv.proto.
# Run from the repo root: ./scripts/gen.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p proto/kvpb

protoc \
  --go_out=proto/kvpb \
  --go_opt=paths=source_relative \
  --go-grpc_out=proto/kvpb \
  --go-grpc_opt=paths=source_relative \
  --proto_path=proto \
  proto/kv.proto

echo "proto generation complete → proto/kvpb/"
