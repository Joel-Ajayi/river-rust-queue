package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	redisOnce      sync.Once
	redisContainer testcontainers.Container
	redisClient    *redis.Client
	redisURI       string
	redisInitError error
)

// StartRedis spins up a persistent Redis container (reused across test runs).
// The container stays running until explicitly terminated by running `make test-clean` or `docker rm -f $(docker ps -q --filter label=org.testcontainers=true)`.
func StartRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	ctx := context.Background()

	redisOnce.Do(func() {
		req := testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(30 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			redisInitError = fmt.Errorf("failed to start persistent redis container: %w", err)
			return
		}
		redisContainer = container

		host, err := container.Host(ctx)
		if err != nil {
			redisInitError = fmt.Errorf("failed to get redis container host: %w", err)
			return
		}
		port, err := container.MappedPort(ctx, "6379")
		if err != nil {
			redisInitError = fmt.Errorf("failed to get redis container port: %w", err)
			return
		}

		redisURI = fmt.Sprintf("%s:%s", host, port.Port())
		rdb := redis.NewClient(&redis.Options{
			Addr: redisURI,
		})

		if err := rdb.Ping(ctx).Err(); err != nil {
			redisInitError = fmt.Errorf("failed to ping persistent redis container: %w", err)
			return
		}

		redisClient = rdb
	})

	if redisInitError != nil {
		t.Fatalf("failed to setup persistent test redis container: %v", redisInitError)
	}

	return redisClient, redisURI
}
