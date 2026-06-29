# Multi-stage build for RRQ Go services.
FROM golang:1.26-alpine AS builder

ARG SERVICE=api-gateway

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 disables CGO, GOOS=linux sets the target OS to Linux
# CGO means that the Go compiler will not use C code. Useful for building static binaries that can run in minimal environments like Alpine Linux.
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./${SERVICE}/cmd

FROM alpine:3.20
# Install CA certificates and create a non-root user for running the service
# -D flag creates a user without a home directory, and -u 1000 sets the user ID to 1000.
RUN apk add --no-cache ca-certificates && adduser -D -u 1000 rrq
USER rrq
COPY --from=builder /bin/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
