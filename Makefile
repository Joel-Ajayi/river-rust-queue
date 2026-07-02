# RRQ root Makefile. Delegates to sub-Makefiles:
#   deploy/Makefile  — application image builds and local kind loading
#   go-services/Makefile — Go build, test, lint, format
#   api/proto/Makefile — protobuf generate, lint

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BIN            ?= $(HOME)/.local/bin
GOBIN          := $(shell go env GOPATH 2>/dev/null)/bin
OS             := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH           := $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')

# Pinned tool versions (bump deliberately).
KIND_VERSION               ?= v0.31.0
KUBECTL_VERSION            ?= v1.31.4
HELM_VERSION               ?= v3.16.4
KUSTOMIZE_VERSION          ?= v5.5.0
BUF_VERSION                ?= v1.47.2
MIGRATE_VERSION            ?= v4.18.1
KUBESEAL_VERSION           ?= 0.27.3
ARGOCD_VERSION             ?= v2.13.3
SKAFFOLD_VERSION           ?= v2.23.0
K6_VERSION                 ?= v0.55.0
PROTOC_GEN_GO_VERSION      ?= v1.35.2
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
YQ_VERSION                 ?= v4.44.3

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help: ## List available targets
	@echo "RRQ — make targets:"
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Typical flow:  make tools  →  make dev  →  make build"
	@echo "Sub-Makefiles: make -C deploy help | make -C services/go-services help | make -C services/rust-services help | make -C api/proto help"

.PHONY: path
path: ## Print PATH additions for installed tools
	@echo 'export PATH="$(GOBIN):$(BIN):$$PATH"'

# ===========================================================================
# Tool installation
# ===========================================================================
.PHONY: tools
tools: $(BIN) tools-go tools-kubectl tools-helm tools-kind tools-kubeseal tools-argocd tools-skaffold tools-k6 tools-jq tools-yq ## Install every CLI
	@echo "All tools installed. Run: $$(make -s path)"

$(BIN):
	@mkdir -p $(BIN)

.PHONY: tools-go
tools-go: ## Install Go-based tools (kustomize, buf, migrate, protoc plugins)
	@command -v go >/dev/null || { echo "Go is required: https://go.dev/dl/"; exit 1; }
	go install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@$(MIGRATE_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: tools-kubectl
tools-kubectl: $(BIN) ## Install kubectl
	@command -v kubectl >/dev/null && echo "kubectl present" || { \
	  curl -fsSLo $(BIN)/kubectl "https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(OS)/$(ARCH)/kubectl" && \
	  chmod +x $(BIN)/kubectl ; }

.PHONY: tools-helm
tools-helm: $(BIN) ## Install helm
	@command -v helm >/dev/null && echo "helm present" || \
	  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 \
	    | USE_SUDO=false HELM_INSTALL_DIR=$(BIN) DESIRED_VERSION=$(HELM_VERSION) bash

.PHONY: tools-kind
tools-kind: ## Install kind
	@command -v kind >/dev/null && echo "kind present" || go install sigs.k8s.io/kind@$(KIND_VERSION)

.PHONY: tools-kubeseal
tools-kubeseal: $(BIN) ## Install kubeseal
	@command -v kubeseal >/dev/null && echo "kubeseal present" || { \
	  curl -fsSL "https://github.com/bitnami-labs/sealed-secrets/releases/download/v$(KUBESEAL_VERSION)/kubeseal-$(KUBESEAL_VERSION)-$(OS)-$(ARCH).tar.gz" \
	    | tar -xz -C $(BIN) kubeseal && chmod +x $(BIN)/kubeseal ; }

.PHONY: tools-argocd
tools-argocd: $(BIN) ## Install argocd CLI
	@command -v argocd >/dev/null && echo "argocd present" || { \
	  curl -fsSLo $(BIN)/argocd "https://github.com/argoproj/argo-cd/releases/download/$(ARGOCD_VERSION)/argocd-$(OS)-$(ARCH)" && \
	  chmod +x $(BIN)/argocd ; }

.PHONY: tools-skaffold
tools-skaffold: $(BIN) ## Install skaffold
	@command -v skaffold >/dev/null && echo "skaffold present" || { \
	  curl -fsSLo $(BIN)/skaffold "https://storage.googleapis.com/skaffold/releases/$(SKAFFOLD_VERSION)/skaffold-$(OS)-$(ARCH)" && \
	  chmod +x $(BIN)/skaffold ; }

.PHONY: tools-k6
tools-k6: $(BIN) ## Install k6
	@command -v k6 >/dev/null && echo "k6 present" || { \
	  curl -fsSL "https://github.com/grafana/k6/releases/download/$(K6_VERSION)/k6-$(K6_VERSION)-$(OS)-$(ARCH).tar.gz" \
	    | tar -xz --strip-components=1 -C $(BIN) "k6-$(K6_VERSION)-$(OS)-$(ARCH)/k6" ; }

.PHONY: tools-jq
tools-jq: $(BIN) ## Install jq
	@command -v jq >/dev/null && echo "jq present" || { \
	  curl -fsSLo $(BIN)/jq "https://github.com/jqlang/jq/releases/latest/download/jq-$(OS)-$(ARCH)" && \
	  chmod +x $(BIN)/jq ; }

.PHONY: tools-yq
tools-yq: $(BIN) ## Install yq (YAML CLI)
	@command -v yq >/dev/null && echo "yq present" || { \
	  curl -fsSLo $(BIN)/yq "https://github.com/mikefarah/yq/releases/download/$(YQ_VERSION)/yq_$(OS)_$(ARCH)" && \
	  chmod +x $(BIN)/yq ; }

.PHONY: tools-check
tools-check: ## Report which tools are installed
	@for t in go kubectl helm kind kustomize buf migrate kubeseal argocd skaffold k6 jq yq protoc-gen-go; do \
	  printf "  %-16s %s\n" "$$t" "$$(command -v $$t || echo MISSING)"; done

# ===========================================================================
# Development
# ===========================================================================
.PHONY: dev
dev: ## Run local development with hot-reloading (Skaffold)
	skaffold fix && skaffold dev --port-forward

.PHONY: psql
psql: ## Open psql against a shard (SHARD=shard-a|shard-b|merchants-db)
	kubectl -n rrq exec -it $${SHARD:-shard-a}-1 -- psql -U postgres

# ===========================================================================
# Delegated targets — go-services/
# ===========================================================================
.PHONY: build-go
build-go: ## Build Go services
	$(MAKE) -C services/go-services build

.PHONY: test-go
test-go: ## Run Go tests
	$(MAKE) -C services/go-services test

.PHONY: lint-go
lint-go: ## Go lint + proto lint
	$(MAKE) -C services/go-services lint

.PHONY: fmt-go
fmt-go: ## Format Go code
	$(MAKE) -C services/go-services fmt

# ===========================================================================
# Delegated targets — rust-services/
# ===========================================================================
.PHONY: build-rust
build-rust: ## Build Rust services
	$(MAKE) -C services/rust-services build

.PHONY: test-rust
test-rust: ## Run Rust tests
	$(MAKE) -C services/rust-services test

.PHONY: lint-rust
lint-rust: ## Rust lint (clippy)
	$(MAKE) -C services/rust-services lint

.PHONY: fmt-rust
fmt-rust: ## Format Rust code
	$(MAKE) -C services/rust-services fmt

# ===========================================================================
# Combined targets
# ===========================================================================
.PHONY: build
build: build-go build-rust ## Build all services

.PHONY: test
test: test-go test-rust ## Run all tests

.PHONY: lint
lint: lint-go lint-rust ## Lint all services
	$(MAKE) -C api/proto lint

.PHONY: fmt
fmt: fmt-go fmt-rust ## Format all services

# ===========================================================================
# Delegated targets — proto/
# ===========================================================================
.PHONY: proto
proto: ## Generate Go and Rust code from proto definitions
	$(MAKE) -C api/proto generate

# ===========================================================================
# Simulation harness
# ===========================================================================
.PHONY: sim
sim: ## Run merchant-sim in steady mode
	cd tools/merchant-sim && go run ./cmd steady

