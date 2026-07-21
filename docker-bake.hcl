variable "TAG" {
  default = "dev"
}

variable "REGISTRY" {
  default = "rrq"
}

group "default" {
  targets = [
    "api-gateway-go",
    "outbox-relay-go",
    "ledger-worker-go",
    "webhook-worker-go",
    "fraud-worker-go",
    "recon-worker-go",
    "migrate"
  ]
}

target "base-go" {
  context = "services/go-services"
  dockerfile = "Dockerfile"
}

target "api-gateway-go" {
  inherits = ["base-go"]
  args = { SERVICE = "api-gateway" }
  tags = ["${REGISTRY}/api-gateway-go:${TAG}"]
}

target "migrate" {
  context = "deploy/db/migrations"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/migrate:${TAG}"]
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

target "recon-worker-go" {
  inherits = ["base-go"]
  args = { SERVICE = "recon-worker" }
  tags = ["${REGISTRY}/recon-worker-go:${TAG}"]
}


