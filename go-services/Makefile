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
test: ## Run tests with the race detector
	go test -race ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: vet ## gofmt check + go vet
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w .
