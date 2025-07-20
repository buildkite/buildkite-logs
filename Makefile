# Makefile for buildkite-logs-parquet

# Variables
BINARY_NAME=bklog
BUILD_DIR=build
CMD_DIR=cmd/bklog
VERSION?=dev
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
	rm -rf $(BUILD_DIR)
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
	@echo "  make all"
	@echo "  make dev"