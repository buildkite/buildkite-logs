# Makefile for buildkite-logs-parquet

# Variables
BINARY_NAME=bklog
BUILD_DIR=build
DIST_DIR=dist
DIST_TARGETS=linux-amd64 linux-arm64 darwin-amd64 darwin-arm64
CMD_DIR=cmd/bklog
VERSION?=$(shell cat VERSION)
LDFLAGS=-ldflags "-X main.version=$(VERSION) -s -w"

# Go related variables
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Default target
.PHONY: all
all: clean test lint build

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR) $(DIST_DIR)
	rm -f $(BINARY_NAME)

# Run tests
.PHONY: test
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run linting
.PHONY: lint
lint:
	@echo "Running golangci-lint..."
	golangci-lint run

# Run linting with auto-fix
.PHONY: lint-fix
lint-fix:
	@echo "Running golangci-lint with auto-fix..."
	golangci-lint run --fix ./...

# Build the binary
.PHONY: build
build:
	@echo "Building $(BINARY_NAME) $(VERSION)..."
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -trimpath -o $(BUILD_DIR)/$(BINARY_NAME) ./$(CMD_DIR)

# Cross-compile gzipped release binaries and their SHA-256 checksums
.PHONY: dist
dist:
	@echo "Building $(BINARY_NAME) $(VERSION) release binaries..."
	rm -rf $(DIST_DIR)
	mkdir -p $(DIST_DIR)
	set -e; for target in $(DIST_TARGETS); do \
		name=$(BINARY_NAME)-$(VERSION)-$$target; \
		CGO_ENABLED=0 GOOS=$${target%-*} GOARCH=$${target#*-} \
			$(GOBUILD) $(LDFLAGS) -trimpath -o $(BUILD_DIR)/$$name ./$(CMD_DIR); \
		gzip -9 -n -c $(BUILD_DIR)/$$name > $(DIST_DIR)/$$name.gz; \
	done
	cd $(DIST_DIR) && shasum -a 256 *.gz > $(BINARY_NAME)-$(VERSION)-sha256sums.txt

# Print the version from VERSION
.PHONY: version
version:
	@echo $(VERSION)

# Install dependencies
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Run benchmarks
.PHONY: bench
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

# Development target - fast build without version
.PHONY: dev
dev:
	@echo "Building development version..."
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) ./$(CMD_DIR)

# Install the binary to GOPATH/bin
.PHONY: install
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOBUILD) $(LDFLAGS) -o $(GOPATH)/bin/$(BINARY_NAME) ./$(CMD_DIR)

# Run the binary with test data
.PHONY: run-test
run-test: build
	@echo "Running with test data..."
	./$(BUILD_DIR)/$(BINARY_NAME) parse -file testdata/bash-example.log -summary

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Check code formatting
.PHONY: fmt-check
fmt-check:
	@echo "Checking code formatting..."
	@if [ -n "$$($(GOCMD) fmt -l .)" ]; then \
		echo "Code is not formatted. Run 'make fmt' to fix."; \
		exit 1; \
	fi

# Continuous integration target
.PHONY: ci
ci: deps fmt-check test lint build

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all        - Run clean, test, lint, and build"
	@echo "  clean      - Clean build artifacts"
	@echo "  test       - Run tests"
	@echo "  lint       - Run golangci-lint"
	@echo "  lint-fix   - Run golangci-lint with auto-fix"
	@echo "  build      - Build the binary with version $(VERSION)"
	@echo "  dist       - Build gzipped release binaries and checksums in $(DIST_DIR)/"
	@echo "  version    - Print the version from VERSION"
	@echo "  dev        - Quick development build"
	@echo "  deps       - Install and tidy dependencies"
	@echo "  bench      - Run benchmarks"
	@echo "  install    - Install binary to GOPATH/bin"
	@echo "  run-test   - Build and run with test data"
	@echo "  fmt        - Format code"
	@echo "  fmt-check  - Check code formatting"
	@echo "  ci         - Run continuous integration checks"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION    - Version to build (default: $(VERSION))"
	@echo ""
	@echo "Examples:"
	@echo "  make build VERSION=v1.2.3"
	@echo "  make dist"
	@echo "  make all"
	@echo "  make dev"