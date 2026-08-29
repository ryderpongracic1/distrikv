# Changelog

All notable changes to distrikv will be recorded here. The project follows
[Semantic Versioning](https://semver.org/) from its first tagged release.

## [Unreleased]

### Added

- Reproducible release builds for `distrikv-cli` and `distrikv-node`.
- Docker readiness checks, Prometheus metrics, and request IDs.
- A macOS/Apple Silicon benchmark runbook and one-command cluster smoke test.

## [0.1.0] - TBD

Initial public release of the three-node distributed key-value store, including
the LSM-tree engine, synchronous replication, Raft-backed node health,
anti-entropy, chaos verification, benchmark harness, and CLI.

[Unreleased]: https://github.com/ryderpongracic1/distrikv/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ryderpongracic1/distrikv/releases/tag/v0.1.0
