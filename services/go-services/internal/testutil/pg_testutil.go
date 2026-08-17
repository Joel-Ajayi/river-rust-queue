package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// TestDB holds the connection pool and connection string for a single test database instance.
type TestDB struct {
	URI  string
	Pool *pgxpool.Pool
}

// TestCluster wraps all three logical database instances (global merchants, shard-a, shard-b)
// and provides a compatible platform.ShardPools instance.
type TestCluster struct {
	MerchantsDB TestDB
	ShardA      TestDB
	ShardB      TestDB
	ShardPools  *platform.ShardPools
}

var (
	pgOnce        sync.Once
	pgCluster     *TestCluster
	pgContainer   *postgres.PostgresContainer
	pgHost        string
	pgPort        string
	pgInitError   error
	migrationsDir string
	seedDir       string
)

// SetupTestDB provisions a persistent PostgreSQL container (reused across test runs).
// The container stays running until explicitly terminated by running `make test-clean` or `docker rm -f $(docker ps -q --filter label=org.testcontainers=true)`.
func SetupTestDB(t *testing.T) *TestCluster {
	t.Helper()
	ctx := context.Background()

	pgOnce.Do(func() {
		// Locate project root deploy/db path using runtime.Caller
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			pgInitError = fmt.Errorf("failed to get caller path")
			return
		}
		// filename is .../services/go-services/internal/testutil/pg_testutil.go
		projectRoot := filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filename))))))
		migrationsDir = filepath.Join(projectRoot, "deploy", "db", "migrations")
		seedDir = filepath.Join(projectRoot, "deploy", "db", "seed")

		// 1. Start Persistent Postgres Container
		container, err := postgres.Run(ctx,
			"postgres:16-alpine",
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
			),
		)
		if err != nil {
			pgInitError = fmt.Errorf("failed to start persistent postgres container: %w", err)
			return
		}
		pgContainer = container

		host, err := container.Host(ctx)
		if err != nil {
			pgInitError = fmt.Errorf("failed to get container host: %w", err)
			return
		}
		port, err := container.MappedPort(ctx, "5432")
		if err != nil {
			pgInitError = fmt.Errorf("failed to get container port: %w", err)
			return
		}
		pgHost = host
		pgPort = port.Port()

		// 2. Connect to root database to create roles & logical databases
		defaultURI := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", pgHost, pgPort)
		pool, err := pgxpool.New(ctx, defaultURI)
		if err != nil {
			pgInitError = fmt.Errorf("failed to connect to root postgres: %w", err)
			return
		}

		roles := []string{"rrq_app", "rrq_relay", "rrq_admin"}
		for _, role := range roles {
			_, _ = pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s NOLOGIN", role))
		}

		dbs := []string{"merchants_db", "shard_a", "shard_b"}
		for _, dbName := range dbs {
			_, _ = pool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName))
			if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
				pgInitError = fmt.Errorf("failed to create database %s: %w", dbName, err)
				pool.Close()
				return
			}
		}
		pool.Close()

		setupInstance := func(dbName, migrationSubDir, seedFileName string) (TestDB, error) {
			uri := fmt.Sprintf("postgres://postgres:postgres@%s:%s/%s?sslmode=disable", pgHost, pgPort, dbName)

			// Run DDL Migrations
			mPath := fmt.Sprintf("file://%s/%s", migrationsDir, migrationSubDir)
			m, err := migrate.New(mPath, uri)
			if err != nil {
				return TestDB{}, fmt.Errorf("failed to create migrator for %s: %w", dbName, err)
			}
			if err := m.Up(); err != nil && err != migrate.ErrNoChange {
				return TestDB{}, fmt.Errorf("failed to run migrations for %s: %w", dbName, err)
			}

			// Create Connection Pool
			dbPool, err := pgxpool.New(ctx, uri)
			if err != nil {
				return TestDB{}, fmt.Errorf("failed to create pool for %s: %w", dbName, err)
			}

			// Apply Baseline Seed SQL from deploy/db/seed
			if seedFileName != "" {
				seedPath := filepath.Join(seedDir, seedFileName)
				seedBytes, err := os.ReadFile(seedPath)
				if err == nil && len(seedBytes) > 0 {
					if _, err := dbPool.Exec(ctx, string(seedBytes)); err != nil {
						return TestDB{}, fmt.Errorf("failed to execute seed SQL %s on %s: %w", seedFileName, dbName, err)
					}
				}
			}

			return TestDB{URI: uri, Pool: dbPool}, nil
		}

		merchantsDB, err := setupInstance("merchants_db", "global", "global.sql")
		if err != nil {
			pgInitError = err
			return
		}
		shardA, err := setupInstance("shard_a", "shard", "shard.sql")
		if err != nil {
			pgInitError = err
			return
		}
		shardB, err := setupInstance("shard_b", "shard", "shard.sql")
		if err != nil {
			pgInitError = err
			return
		}

		pgCluster = &TestCluster{
			MerchantsDB: merchantsDB,
			ShardA:      shardA,
			ShardB:      shardB,
			ShardPools:  platform.NewTestShardPools(merchantsDB.Pool, shardA.Pool, shardB.Pool),
		}
	})

	if pgInitError != nil {
		t.Fatalf("failed to setup persistent test database cluster: %v", pgInitError)
	}

	return pgCluster
}
