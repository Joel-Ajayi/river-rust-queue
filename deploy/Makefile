# deploy/Makefile — Image builds, local kind loading, and database operations.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

REGISTRY ?= rrq
TAG      ?= dev
KIND     ?= rrq

# ===========================================================================
# Image build & load
# ===========================================================================
.PHONY: build
build: ## Build all service images
	cd .. && docker buildx bake --set "*.tags=$(REGISTRY)/{}:$(TAG)"

.PHONY: load
load: build ## Build and load images into kind
	@kind get clusters 2>/dev/null | grep -qx "$(KIND)" || { echo "kind cluster '$(KIND)' not running"; exit 1; }
	kind load docker-image --name $(KIND) \
	  $(REGISTRY)/core-api-go:$(TAG) \
	  $(REGISTRY)/api-gateway-go:$(TAG) \
	  $(REGISTRY)/webhook-echo-go:$(TAG) \
	  $(REGISTRY)/ledger-worker-go:$(TAG) \
	  $(REGISTRY)/webhook-worker-go:$(TAG) \
	  $(REGISTRY)/fraud-worker-go:$(TAG) \
	  $(REGISTRY)/outbox-relay-go:$(TAG) \
	  $(REGISTRY)/recon-worker-go:$(TAG) \
	  $(REGISTRY)/migrate:$(TAG)

# ===========================================================================
# Database
# ===========================================================================
.PHONY: seed
seed: ## Apply seed data to all databases
	@echo "Seeding global DB..."
	@kubectl -n rrq exec -it merchants-db-1 -- psql -U postgres -d merchants -f - < deploy/db/seed/global.sql
	@echo "Seeding shard-a..."
	@kubectl -n rrq exec -it shard-a-1 -- psql -U postgres -d rrq -f - < deploy/db/seed/shard.sql
	@echo "Seeding shard-b..."
	@kubectl -n rrq exec -it shard-b-1 -- psql -U postgres -d rrq -f - < deploy/db/seed/shard.sql
	@echo "Seed complete."

.PHONY: reset
reset: ## Drop and recreate all databases (dev only)
	kubectl -n rrq exec -it merchants-db-1 -- psql -U postgres -c "DROP DATABASE IF EXISTS merchants; CREATE DATABASE merchants;"
	kubectl -n rrq exec -it shard-a-1 -- psql -U postgres -c "DROP DATABASE IF EXISTS rrq; CREATE DATABASE rrq;"
	kubectl -n rrq exec -it shard-b-1 -- psql -U postgres -c "DROP DATABASE IF EXISTS rrq; CREATE DATABASE rrq;"

.PHONY: migrate
migrate: ## Run database migrations
	kubectl -n rrq apply -f ../rrq-gitops/rrq/base/migrations

# ===========================================================================
# Help
# ===========================================================================
.PHONY: help
help:
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
