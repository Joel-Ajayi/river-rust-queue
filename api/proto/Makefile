# proto/Makefile — protobuf generation and linting.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: generate
generate: ## Generate Go and Rust code from .proto files
	buf generate

.PHONY: lint
lint: ## Lint proto definitions
	buf lint

.PHONY: breaking
breaking: ## Check for breaking changes against main
	buf breaking --against '.git#branch=main'
