# Makefile for CacheX

.PHONY: all test test-cover test-race lint vet build clean install help fmt lint-ci

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOLANGCI_LINT=golangci-lint

# Directories
COVER_DIR=coverage
BIN_DIR=bin

# Go version requirements
MIN_GO_VERSION=1.21

all: fmt vet lint test

# Test all modules
test:
	@echo "Running tests..."
	$(GOTEST) -v -short ./...

# Test with coverage
test-cover:
	@echo "Running tests with coverage..."
	mkdir -p $(COVER_DIR)
	$(GOTEST) -coverprofile=$(COVER_DIR)/coverage.out -covermode=atomic ./...
	$(GOCMD) tool cover -html=$(COVER_DIR)/coverage.out -o $(COVER_DIR)/coverage.html

# Test with race detector
test-race:
	@echo "Running tests with race detector..."
	$(GOTEST) -race -v ./...

# Lint all modules (optional, may fail if golangci-lint not installed)
lint:
	@echo "Running linter..."
	@if command -v $(GOLANGCI_LINT) > /dev/null; then \
		$(GOLANGCI_LINT) run --timeout=5m ./...; \
	else \
		echo "Warning: golangci-lint not found, skipping lint"; \
	fi

# Lint for CI (fails if golangci-lint not installed)
lint-ci:
	@echo "Running linter for CI..."
	@if command -v $(GOLANGCI_LINT) > /dev/null; then \
		$(GOLANGCI_LINT) run --timeout=10m ./...; \
	else \
		echo "Error: golangci-lint not installed"; \
		echo "Install with: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin"; \
		exit 1; \
	fi

# Vet all modules
vet:
	@echo "Running go vet..."
	$(GOVET) ./...

# Build all modules
build:
	@echo "Building all modules..."
	$(GOBUILD) ./...

# Build examples
build-examples:
	@echo "Building examples..."
	mkdir -p $(BIN_DIR)
	$(GOBUILD) -o $(BIN_DIR)/redis-demo ./examples/redis/
	$(GOBUILD) -o $(BIN_DIR)/badger-demo ./examples/badger/
	$(GOBUILD) -o $(BIN_DIR)/bbolt-demo ./examples/bbolt/
	$(GOBUILD) -o $(BIN_DIR)/pebble-demo ./examples/pebble/

# Run specific module tests
test-backends:
	$(GOTEST) -v ./backends/...

test-observability:
	$(GOTEST) -v ./observability/...

test-extensions:
	$(GOTEST) -v ./extensions/...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf $(BIN_DIR)/
	rm -f $(COVER_DIR)/*.out $(COVER_DIR)/*.html
	find . -name "*.test" -delete
	find . -name "*.cover" -delete

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) download ./...

# Tidy modules
tidy:
	@echo "Tidying modules..."
	$(GOMOD) tidy
	$(GOMOD) tidy -e ./...

# Format code
fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .
	$(GOFMT) -s -w ./backends/
	$(GOFMT) -s -w ./observability/
	$(GOFMT) -s -w ./extensions/
	$(GOFMT) -s -w ./examples/

# Check Go version
check-go-version:
	@echo "Checking Go version..."
	@$(GOCMD) version | grep -q "go1\.\(2[1-9]\|[3-9][0-9]\)" && echo "Go version OK" || (echo "Error: Go 1.21+ required" && exit 1)

# Generate mocks (if needed)
mocks:
	@echo "Generating mocks..."
	@if command -v mockgen > /dev/null; then \
		mockgen -source=cachex.go -destination=cachex_mock.go -package=cachex; \
	else \
		echo "Warning: mockgen not found, skipping mock generation"; \
	fi

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

# Run integration tests (requires running services)
test-integration:
	@echo "Running integration tests..."
	@if command -v docker > /dev/null; then \
		docker-compose -f test/docker-compose.yml up -d; \
		sleep 5; \
		$(GOTEST) -tags=integration ./...; \
		docker-compose -f test/docker-compose.yml down; \
	else \
		echo "Warning: docker not found, skipping integration tests"; \
	fi

# Show help
help:
	@echo "CacheX Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Run fmt, vet, lint, test"
	@echo "  test         - Run all tests"
	@echo "  test-cover   - Run tests with coverage"
	@echo "  test-race    - Run tests with race detector"
	@echo "  lint         - Run linter (optional)"
	@echo "  lint-ci      - Run linter for CI"
	@echo "  vet          - Run go vet"
	@echo "  build        - Build all modules"
	@echo "  build-examples - Build example programs"
	@echo "  clean        - Clean build artifacts"
	@echo "  deps         - Download dependencies"
	@echo "  tidy         - Tidy modules"
	@echo "  fmt          - Format code"
	@echo "  bench        - Run benchmarks"
	@echo "  test-integration - Run integration tests"
	@echo "  help         - Show this help"