.PHONY: build install clean test setup check

BINARY_NAME=wvc
BUILD_DIR=./bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-s -w \
	-X github.com/kilupskalvis/wvc/internal/cli.Version=$(VERSION) \
	-X github.com/kilupskalvis/wvc/internal/cli.Commit=$(COMMIT) \
	-X github.com/kilupskalvis/wvc/internal/cli.BuildDate=$(BUILD_DATE)

build:
	go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) .

install: build
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/

clean:
	rm -rf $(BUILD_DIR)
	go clean

test:
	go test ./...

# Development helpers
dev: build
	$(BUILD_DIR)/$(BINARY_NAME) $(ARGS)

deps:
	go mod tidy
	go mod download

fmt:
	go fmt ./...

lint:
	golangci-lint run

# Setup development environment (install git hooks)
setup:
	go install github.com/evilmartians/lefthook@latest
	$(shell go env GOPATH)/bin/lefthook install

# Run all pre-commit checks manually
check:
	$(shell go env GOPATH)/bin/lefthook run pre-commit
