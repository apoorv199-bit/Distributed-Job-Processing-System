package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// migrations holds all .sql files in the migrations/ subdirectory.
// The //go:embed directive works here because this file lives in the
// same package directory as the migrations/ folder.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// RunMigrations applies all embedded SQL migration files using pressly/goose/v3.
// It wraps migrations in a Postgres advisory lock to ensure concurrency safety in multi-replica environments.
func (db *DB) RunMigrations(ctx context.Context) error {
	// Convert pgxpool connection config to standard database/sql DB
	sqlDB := stdlib.OpenDB(*db.pool.Config().ConnConfig)
	defer sqlDB.Close()

	// Set up Postgres Session Locker for goose concurrency safety
	sessionLocker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create postgres session locker: %w", err)
	}

	// Extract the subdirectory "migrations" from the embedded FS so that Goose finds the files at the root
	migrationsFS, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("get migrations sub-FS: %w", err)
	}

	// Create a new Goose provider
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		sqlDB,
		migrationsFS,
		goose.WithSessionLocker(sessionLocker),
		goose.WithVerbose(false),
	)
	if err != nil {
		return fmt.Errorf("create goose provider: %w", err)
	}

	slog.Info("Running database migrations via Goose...")

	// Apply migrations
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("apply migrations via goose: %w", err)
	}

	slog.Info("Goose migrations completed successfully", "applied_count", len(results))
	for _, res := range results {
		slog.Debug("Applied migration", "version", res.Source.Version, "duration", res.Duration)
	}

	return nil
}
