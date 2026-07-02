variable "TAG" {
  default = "dev"
}

variable "REGISTRY" {
  default = "rrq"
}

group "default" {
  targets = [
    "api-gateway-go",
    "api-gateway-rust",
    "outbox-relay-go",
    "outbox-relay-rust",
    "ledger-worker-go",
    "webhook-worker-go",
    "fraud-worker-go",
    "recon-worker-go",
    "admin-dashboard",
    "migrate"
  ]
}

target "base-go" {
  context = "services/go-services"
  dockerfile = "Dockerfile"
}

target "base-rust" {
  context = "services/rust-services"
  dockerfile = "Dockerfile"
}

target "api-gateway-go" {
  inherits = ["base-go"]
  args = { SERVICE = "api-gateway" }
  tags = ["${REGISTRY}/api-gateway-go:${TAG}"]
}

target "admin-dashboard" {
  context = "services/admin-dashboard"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/admin-dashboard:${TAG}"]
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

// ---- Rust Workers ----

target "outbox-relay-rust" {
  inherits = ["base-rust"]
  args = { SERVICE = "outbox-relay" }
  tags = ["${REGISTRY}/outbox-relay-rust:${TAG}"]
}

target "ledger-worker-rust" {
  inherits = ["base-rust"]
  args = { SERVICE = "ledger-worker" }
  tags = ["${REGISTRY}/ledger-worker-rust:${TAG}"]
}

target "webhook-worker-rust" {
  inherits = ["base-rust"]
  args = { SERVICE = "webhook-worker" }
  tags = ["${REGISTRY}/webhook-worker-rust:${TAG}"]
}

target "fraud-worker-rust" {
  inherits = ["base-rust"]
  args = { SERVICE = "fraud-worker" }
  tags = ["${REGISTRY}/fraud-worker-rust:${TAG}"]
}

target "recon-worker-rust" {
  inherits = ["base-rust"]
  args = { SERVICE = "recon-worker" }
  tags = ["${REGISTRY}/recon-worker-rust:${TAG}"]
}

target "api-gateway-rust" {
  inherits = ["base-rust"]
  args = { SERVICE = "api-gateway" }
  tags = ["${REGISTRY}/api-gateway-rust:${TAG}"]
}

