variable "TAG" {
  default = "dev"
}

variable "REGISTRY" {
  default = "rrq"
}

group "default" {
  targets = [
    "core-api-go",
    "webhook-echo-go",
    "outbox-relay-go",
    "ledger-worker-go",
    "webhook-worker-go",
    "fraud-worker-go",
    "migrate",
    "seed-performance"
  ]
}

target "base-go" {
  context = "services/go-services"
  dockerfile = "Dockerfile"
}



target "core-api-go" {
  inherits = ["base-go"]
  args = { SERVICE = "core-api" }
  tags = ["${REGISTRY}/core-api-go:${TAG}"]
}

target "webhook-echo-go" {
  inherits = ["base-go"]
  args = { SERVICE = "webhook-echo" }
  tags = ["${REGISTRY}/webhook-echo-go:${TAG}"]
}

target "migrate" {
  context = "deploy/db/migrations"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/migrate:${TAG}"]
}

target "seed-performance" {
  context = "deploy/db/seed"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/seed-performance:${TAG}"]
}

// ---- Go Workers ----

target "outbox-relay-go" {
  inherits = ["base-go"]
  args = { SERVICE = "outbox-relay" }
  tags = ["${REGISTRY}/outbox-relay-go:${TAG}"]
}

target "ledger-worker-go" {
  inherits = ["base-go"]
  args = { SERVICE = "ledger-worker" }
  tags = ["${REGISTRY}/ledger-worker-go:${TAG}"]
}

target "webhook-worker-go" {
  inherits = ["base-go"]
  args = { SERVICE = "webhook-worker" }
  tags = ["${REGISTRY}/webhook-worker-go:${TAG}"]
}

target "fraud-worker-go" {
  inherits = ["base-go"]
  args = { SERVICE = "fraud-worker" }
  tags = ["${REGISTRY}/fraud-worker-go:${TAG}"]
}



