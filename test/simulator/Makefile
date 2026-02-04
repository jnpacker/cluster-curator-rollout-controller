.PHONY: build clean help test init-dry init-complex

BINARY_NAME=simulate
BINARY_PATH=./bin/$(BINARY_NAME)
GO=go
GOFLAGS=-v

help:
	@echo "Synthetic ClusterCuratorRollout Test Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  make build            - Build the binary to ./bin/simulate"
	@echo "  make clean            - Remove built binaries"
	@echo "  make init-dry         - Test init with dry-run (10 clusters)"
	@echo "  make init-complex     - Test init dry-run with complex config"
	@echo "  make help             - Show this help message"
	@echo ""
	@echo "Commands:"
	@echo "  ./bin/simulate init <config>         - Create test infrastructure"
	@echo "  ./bin/simulate run <config>          - Run upgrade simulator"
	@echo "  ./bin/simulate query <config>        - Query active test IDs"
	@echo "  ./bin/simulate remove <config>       - Remove test infrastructure"
	@echo "  ./bin/simulate reset <config>        - Reset ClusterCurators"

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p ./bin
	$(GO) mod tidy
	$(GO) build $(GOFLAGS) -o $(BINARY_PATH) .
	@echo "✓ Build complete: $(BINARY_PATH)"

clean:
	@echo "Cleaning..."
	rm -f $(BINARY_PATH)
	$(GO) clean
	@echo "✓ Clean complete"

init-dry: build
	@echo "Running init dry-run test with simple config (10 clusters)..."
	$(BINARY_PATH) init config/simple_clusters.yaml --dry-run

init-complex: build
	@echo "Running init dry-run test with complex config (25 clusters)..."
	$(BINARY_PATH) init config/infra_clusters.json --dry-run

test: init-dry init-complex
	@echo "✓ All tests passed"

