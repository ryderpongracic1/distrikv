.PHONY: build-cli install-cli build-node build test test-client test-cli lint

GIT_VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS     := -ldflags="-X main.version=$(GIT_VERSION)"

build-cli:
	go build $(LDFLAGS) -o bin/distrikv-cli ./cmd/cli

install-cli:
	go install $(LDFLAGS) ./cmd/cli

build-node:
	go build -o bin/distrikv-node ./cmd/node

build: build-cli build-node

test:
	go test ./...

test-client:
	go test -v ./internal/client/...

test-cli:
	go test -v ./cli/...
