# RRQ root Makefile. Delegates to sub-Makefiles:
#   deploy/Makefile  — application image builds and local kind loading
#   go-services/Makefile — Go build, test, lint, format
#   api/proto/Makefile — protobuf generate, lint

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# Configuration
BIN            ?= $(HOME)/.local/bin
GOBIN          := $(shell go env GOPATH 2>/dev/null)/bin

# Pinned tool versions (bump deliberately).
BUF_VERSION                ?= v1.50.0
MIGRATE_VERSION            ?= v4.18.2
PROTOC_GEN_GO_VERSION      ?= v1.36.5
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

# Help
.PHONY: help
help: ## List available targets
	@echo "RRQ — make targets:"
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Typical flow:  make tools  →  make dev  →  make build"
	@echo "Sub-Makefiles: make -C deploy help | make -C services/go-services help | make -C api/proto help"

.PHONY: path
path: ## Print PATH additions for installed tools
	@echo 'export PATH="$(GOBIN):$(BIN):$$PATH"'

# Tool installation
.PHONY: tools
tools: $(BIN) tools-go ## Install development CLI tools
	@echo "All tools installed. Run: $$(make -s path)"

$(BIN):
	@mkdir -p $(BIN)

.PHONY: tools-go
tools-go: ## Install Go-based tools (buf, migrate, protoc plugins)
	@command -v go >/dev/null || { echo "Go is required: https://go.dev/dl/"; exit 1; }
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)



.PHONY: tools-check
tools-check: ## Report which tools are installed
	@for t in go buf migrate protoc-gen-go; do \
	  printf "  %-16s %s\n" "$$t" "$$(command -v $$t || echo MISSING)"; done

# Development
.PHONY: dev
dev: ## Run local development with hot-reloading (Skaffold, no cleanup on exit)
	skaffold fix && skaffold run

.PHONY: psql
psql: ## Open psql against a shard (SHARD=shard-a|shard-b|merchants-db)
	kubectl -n rrq exec -it $${SHARD:-shard-a}-1 -- psql -U postgres

# Delegated targets — go-services/
.PHONY: build-go
build-go: ## Build Go services
	$(MAKE) -C services/go-services build

.PHONY: test-go
test-go: ## Run Go fast tests (in-memory mode)
	$(MAKE) -C services/go-services test

.PHONY: test-containers
test-containers: ## Run Go storage container tests (persistent Docker containers)
	cd services/go-services && go test -v -tags=integration ./...

.PHONY: test-clean
test-clean: ## Kill and remove all persistent test containers
	@docker rm -f $$(docker ps -q --filter label=org.testcontainers=true) 2>/dev/null || true
	@echo "Test containers cleaned up."

.PHONY: lint-go
lint-go: ## Go lint + proto lint
	$(MAKE) -C services/go-services lint

.PHONY: fmt-go
fmt-go: ## Format Go code
	$(MAKE) -C services/go-services fmt

# Combined targets
.PHONY: build
build: build-go ## Build all services

.PHONY: test
test: test-go ## Run all tests

.PHONY: lint
lint: lint-go ## Lint all services
	$(MAKE) -C api/proto lint

.PHONY: fmt
fmt: fmt-go ## Format all services

# Delegated targets — proto/
.PHONY: proto
proto: ## Generate Go and Rust code from proto definitions
	$(MAKE) -C api/proto generate

