package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps a pgxpool and implements all job persistence operations.
type DB struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and returns a ready DB.
func New(ctx context.Context, connStr string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}

	// Connection pool sizing: rule of thumb = 2–4x CPU cores of the DB server.
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	var pool *pgxpool.Pool
	// Retry with backoff — gives Postgres time to start
	for attempt := 1; attempt <= 10; attempt++ {
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				break
			}
		}
		wait := time.Duration(attempt) * 2 * time.Second
		slog.Warn("db.connect_retry",
			"attempt", attempt,
			"wait", wait,
			"error", err,
		)
		time.Sleep(wait)
	}
	if err != nil {
		return nil, fmt.Errorf("db connect after retries: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Ping checks connectivity to the database.
func (db *DB) Ping(ctx context.Context) error {
	return db.pool.Ping(ctx)
}

// Close releases all pool connections.
func (db *DB) Close() {
	db.pool.Close()
}
