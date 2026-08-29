.PHONY: build-cli install-cli build-node build test test-race test-client test-cli lint demo clean

GIT_VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS     := -ldflags="-X main.version=$(GIT_VERSION)"

build-cli:
	mkdir -p bin
	go build $(LDFLAGS) -o bin/distrikv-cli ./cmd/distrikv-cli

install-cli:
	go install $(LDFLAGS) ./cmd/distrikv-cli

build-node:
	mkdir -p bin
	go build $(LDFLAGS) -o bin/distrikv-node ./cmd/node

build: build-cli build-node

test:
	go test ./...

test-race:
	go test ./... -race -count=1

test-client:
	go test -v ./internal/client/...

test-cli:
	go test -v ./cli/...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

demo:
	./scripts/quickstart.sh

clean:
	rm -rf bin
