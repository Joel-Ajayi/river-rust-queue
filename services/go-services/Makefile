# services/Makefile — Go build, test, lint, format.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all Go services
	go build ./...

.PHONY: test
test: test-unit test-integration ## Run all tests

.PHONY: test-unit
test-unit: ## Unit tests only (fast, no containers)
	go test -race -short -count=1 ./...

.PHONY: test-integration
test-integration: ## Integration tests (requires Docker containers)
	go test -tags=integration -race -count=1 ./...

.PHONY: test-coverage
test-coverage: ## Generate HTML coverage report
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

.PHONY: test-coverage-unit
test-coverage-unit: ## Unit coverage (excludes integration-tagged files)
	go test -race -short -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: vet ## gofmt check + go vet
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w .

.PHONY: generate
generate: ## Run go generate (mockgen, etc.)
	go generate ./...
