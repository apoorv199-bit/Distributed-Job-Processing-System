package postgres

import (
	"context"
	"embed"
	"fmt"
)

// migrations holds all .sql files in the migrations/ subdirectory.
// The //go:embed directive works here because this file lives in the
// same package directory as the migrations/ folder.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// RunMigrations applies all embedded SQL migration files in lexical order.
// It is idempotent — all statements use IF NOT EXISTS, so re-running on
// startup is safe.
func (db *DB) RunMigrations(ctx context.Context) error {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		sql, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}

		if _, err := db.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}

	return nil
}
