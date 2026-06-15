# RRQ Makefile
#
# This is the developer entry point. Most targets are placeholders for now:
# the Go services and merchant-sim are designed but not yet built (see STATUS.md),
# so the targets that would run them print a not-yet-wired notice instead of
# pretending to work. They are filled in as each piece lands.

.DEFAULT_GOAL := help
.PHONY: help dev migrate build test lint sim

help: ## List available targets
	@echo "RRQ targets:"
	@echo "  make dev      Start local infra (Postgres, Redis, Jaeger, Prometheus, Grafana)"
	@echo "  make migrate  Apply schema migrations to local Postgres"
	@echo "  make build    Build the Go implementation"
	@echo "  make test     Run the Go test suite with -race, including the scenario suite"
	@echo "  make lint     Vet, gofmt, buf lint"
	@echo "  make sim      Run merchant-sim in steady mode against the local stack"
	@echo
	@echo "Most targets are not wired up yet. See STATUS.md for what exists."

dev: ## Start local infrastructure (not yet wired: docker-compose.yml does not exist yet)
	@echo "make dev: not yet wired up. docker-compose.yml has not been added. See STATUS.md."

migrate: ## Apply schema migrations (not yet wired: migrations/ is empty)
	@echo "make migrate: not yet wired up. migrations/ is empty. See docs/appendices/40-DATA-MODEL.md."

build: ## Build the Go implementation (not yet wired: no service binaries yet)
	@echo "make build: not yet wired up. The Go services are not built yet. See STATUS.md."

test: ## Run the Go test suite (not yet wired: no tests yet)
	@echo "make test: not yet wired up. No test suite exists yet. See docs/02-INVARIANTS.md."

lint: ## Vet, gofmt, buf lint (not yet wired)
	@echo "make lint: not yet wired up."

sim: ## Run merchant-sim in steady mode (not yet functional: merchant-sim is designed, not built)
	@echo "make sim: not yet functional. merchant-sim is designed but not built."
	@echo "See tools/merchant-sim/README.md and docs/services/17-SIMULATION-HARNESS.md."
