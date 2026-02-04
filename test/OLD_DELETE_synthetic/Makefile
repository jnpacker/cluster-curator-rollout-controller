.PHONY: build clean help test test-dry remove-test

BINARY_NAME=synthetic-test-tool
GO=go
GOFLAGS=-v

help:
	@echo "Synthetic ClusterCuratorRollout Test Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  make build          - Build the binary"
	@echo "  make clean          - Remove built binaries"
	@echo "  make test-dry       - Test with dry-run (10 clusters, simple config)"
	@echo "  make test-complex   - Test dry-run with complex config (100 clusters)"
	@echo "  make help           - Show this help message"
	@echo ""
	@echo "Usage:"
	@echo "  ./$(BINARY_NAME) create <config.yaml> [--dry-run]"
	@echo "  ./$(BINARY_NAME) remove <config.yaml>"

build:
	@echo "Building $(BINARY_NAME)..."
	$(GO) mod tidy
	$(GO) build $(GOFLAGS) -o $(BINARY_NAME) .
	@echo "✓ Build complete: ./$(BINARY_NAME)"

clean:
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	$(GO) clean
	@echo "✓ Clean complete"

test-dry: build
	@echo "Running dry-run test with simple config (10 clusters)..."
	./$(BINARY_NAME) create config/simple_clusters.yaml --dry-run

test-complex: build
	@echo "Running dry-run test with complex config (100 clusters)..."
	./$(BINARY_NAME) create config/synthetic_clusters.yaml --dry-run

test: test-dry test-complex
	@echo "✓ All tests passed"

