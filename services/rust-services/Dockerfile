FROM rust:1.80-bullseye as builder
WORKDIR /app
COPY . .
# We require an argument for which service to build
ARG SERVICE
RUN cargo build --release --bin ${SERVICE}

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ARG SERVICE
COPY --from=builder /app/target/release/${SERVICE} /app/${SERVICE}
ENV BINARY_PATH=/app/${SERVICE}
CMD ["sh", "-c", "${BINARY_PATH}"]
