FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./cmd

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 1000 rrq
USER rrq
COPY --from=builder /bin/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
