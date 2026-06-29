variable "TAG" {
  default = "dev"
}

variable "REGISTRY" {
  default = "rrq"
}

group "default" {
  targets = [
    "api-gateway",
    "outbox-relay",
    "ledger-worker",
    "webhook-worker",
    "fraud-worker",
    "recon-worker",
    "admin-dashboard",
    "migrate"
  ]
}

target "base-go" {
  context = "go-services"
  dockerfile = "Dockerfile"
}

target "api-gateway" {
  inherits = ["base-go"]
  args = { SERVICE = "api-gateway" }
  tags = ["${REGISTRY}/api-gateway:${TAG}"]
}

target "outbox-relay" {
  inherits = ["base-go"]
  args = { SERVICE = "outbox-relay" }
  tags = ["${REGISTRY}/outbox-relay:${TAG}"]
}

target "ledger-worker" {
  inherits = ["base-go"]
  args = { SERVICE = "ledger-worker" }
  tags = ["${REGISTRY}/ledger-worker:${TAG}"]
}

target "webhook-worker" {
  inherits = ["base-go"]
  args = { SERVICE = "webhook-worker" }
  tags = ["${REGISTRY}/webhook-worker:${TAG}"]
}

target "fraud-worker" {
  inherits = ["base-go"]
  args = { SERVICE = "fraud-worker" }
  tags = ["${REGISTRY}/fraud-worker:${TAG}"]
}

target "recon-worker" {
  inherits = ["base-go"]
  args = { SERVICE = "recon-worker" }
  tags = ["${REGISTRY}/recon-worker:${TAG}"]
}

target "admin-dashboard" {
  inherits = ["base-go"]
  args = { SERVICE = "admin-dashboard" }
  tags = ["${REGISTRY}/admin-dashboard:${TAG}"]
}

target "migrate" {
  context = "deploy/db/migrations"
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY}/migrate:${TAG}"]
}
