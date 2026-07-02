package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDB holds the connection pool and connection string for a test database.
type TestDB struct {
	URI  string
	Pool *pgxpool.Pool
}

// SetupTestDB spins up a Postgres container and returns three databases: global merchants, shard-a, and shard-b.
func SetupTestDB(t *testing.T) (merchantsDB TestDB, shardA TestDB, shardB TestDB) {
	ctx := context.Background()

	// 1. Start Postgres Container
	pgContainer, err := postgres.Run(ctx,
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
		t.Fatalf("failed to start postgres container: %s", err)
	}

	// Ensure cleanup
	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	// Get Host & Port
	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %s", err)
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %s", err)
	}

	// 2. Connect to default db to create logical databases
	defaultURI := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", host, port.Port())
	pool, err := pgxpool.New(ctx, defaultURI)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %s", err)
	}

	// Create roles needed by migrations
	roles := []string{"rrq_app", "rrq_relay", "rrq_admin"}
	for _, role := range roles {
		// Ignore errors if the role already exists, though it shouldn't in a fresh container
		pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s NOLOGIN", role))
	}

	dbs := []string{"merchants_db", "shard_a", "shard_b"}
	for _, dbName := range dbs {
		if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			t.Fatalf("failed to create database %s: %s", dbName, err)
		}
	}
	pool.Close()

	// 3. Helper to create connection pools & run migrations
	// Find the project root by walking up
	cwd, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err == nil {
			break
		}
		cwd = filepath.Dir(cwd)
		if cwd == "/" || cwd == "." {
			t.Fatalf("could not find go.mod")
		}
	}
	baseDir := filepath.Join(filepath.Dir(cwd), "deploy", "db", "migrations")

	setupDB := func(dbName, migrationSubDir string) TestDB {
		uri := fmt.Sprintf("postgres://postgres:postgres@%s:%s/%s?sslmode=disable", host, port.Port(), dbName)

		// Run Migrations
		mPath := fmt.Sprintf("file://%s/%s", baseDir, migrationSubDir)
		m, err := migrate.New(mPath, uri)
		if err != nil {
			t.Fatalf("failed to create migrator for %s: %s", dbName, err)
		}
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			t.Fatalf("failed to run migrations for %s: %s", dbName, err)
		}

		// Create Connection Pool
		dbPool, err := pgxpool.New(ctx, uri)
		if err != nil {
			t.Fatalf("failed to create pool for %s: %s", dbName, err)
		}
		t.Cleanup(func() { dbPool.Close() })

		return TestDB{URI: uri, Pool: dbPool}
	}

	merchantsDB = setupDB("merchants_db", "global")
	shardA = setupDB("shard_a", "shard")
	shardB = setupDB("shard_b", "shard")

	return merchantsDB, shardA, shardB
}
