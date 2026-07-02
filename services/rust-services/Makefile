.PHONY: build
build:
	cargo build

.PHONY: test
test:
	cargo test

.PHONY: lint
lint:
	cargo clippy -- -D warnings
	cargo fmt -- --check

.PHONY: fmt
fmt:
	cargo fmt
