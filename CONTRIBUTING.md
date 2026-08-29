# Contributing

## Development setup

Requirements: Go 1.25+, Docker with Compose v2, `make`, and `curl`.

```bash
go mod download
make build
make test-race
make demo
```

## Pull requests

- Keep changes focused and include tests for behavior changes.
- Run `make lint` and `make test-race` before opening a pull request.
- Preserve the documented consistency and durability boundaries. New claims
  need a test, benchmark, or fault-injection result that supports them.
- Regenerate protobuf files with `scripts/gen.sh` when the schema changes.

Benchmark changes must include the exact command, hardware/VM allocation,
dataset state, and saturation result. Follow
[`docs/benchmarking-macos.md`](docs/benchmarking-macos.md).
