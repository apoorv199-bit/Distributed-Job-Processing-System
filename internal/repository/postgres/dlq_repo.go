package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------
// DLQ Write Operations
// -----------------------------------------------------------------------

// MoveToDLQ atomically:
//  1. Inserts a record into dead_letter_jobs
//  2. Updates the original job's status to 'dead'
//
// Both happen in a single transaction — you never get a dead job without
// a DLQ entry, and never a DLQ entry for a non-dead job.
func (db *DB) MoveToDLQ(ctx context.Context, job *domain.Job, lastError string) (*domain.DeadLetterJob, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if tx.Commit() succeeds

	// 1. Insert DLQ record
	const insertDLQ = `
		INSERT INTO dead_letter_jobs
		    (job_id, job_type, queue, payload, last_error, attempt_count, max_attempts, job_created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, job_id, job_type, queue, payload, last_error,
		          attempt_count, max_attempts, job_created_at, died_at,
		          replayed_at, replayed_job_id`

	row := tx.QueryRow(ctx, insertDLQ,
		job.ID, job.JobType, job.Queue, job.Payload,
		lastError, job.AttemptCount, job.MaxAttempts, job.CreatedAt,
	)

	dlqJob, err := scanDLQJob(row)
	if err != nil {
		return nil, fmt.Errorf("insert dlq record: %w", err)
	}

	// 2. Mark the original job as dead
	const markDead = `
		UPDATE jobs
		SET status     = 'dead',
		    last_error = $2,
		    failed_at  = NOW(),
		    updated_at = NOW()
		WHERE id = $1`

	if _, err := tx.Exec(ctx, markDead, job.ID, lastError); err != nil {
		return nil, fmt.Errorf("mark job dead: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return dlqJob, nil
}

// ReplayDLQJob atomically:
//  1. Creates a fresh pending job from the DLQ entry's payload
//  2. Marks the DLQ entry as replayed (sets replayed_at + replayed_job_id)
//
// Returns the new job so the caller can return its ID to the API client.
// Returns ErrAlreadyReplayed if the entry has been replayed before.
func (db *DB) ReplayDLQJob(ctx context.Context, dlqID string) (*domain.Job, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Fetch and lock the DLQ entry
	const fetchDLQ = `
		SELECT id, job_id, job_type, queue, payload,
		       last_error, attempt_count, max_attempts,
		       job_created_at, died_at, replayed_at, replayed_job_id
		FROM dead_letter_jobs
		WHERE id = $1
		FOR UPDATE`

	row := tx.QueryRow(ctx, fetchDLQ, dlqID)
	dlqJob, err := scanDLQJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrDLQEntryNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("fetch dlq entry: %w", err)
	}
	if dlqJob.ReplayedAt != nil {
		return nil, domain.ErrAlreadyReplayed
	}

	// 2. Insert a new job — reset attempt_count, preserve original payload + config
	const insertJob = `
		INSERT INTO jobs (job_type, queue, payload, priority, max_attempts, run_at)
		VALUES ($1, $2, $3, 5, $4, NOW())
		RETURNING id, job_type, queue, payload, status, priority,
		          max_attempts, attempt_count, run_at,
		          started_at, completed_at, failed_at,
		          worker_id, last_error, created_at, updated_at`

	jobRow := tx.QueryRow(ctx, insertJob, dlqJob.JobType, dlqJob.Queue, dlqJob.Payload, dlqJob.MaxAttempts)
	newJob, err := scanJob(jobRow)
	if err != nil {
		return nil, fmt.Errorf("insert replayed job: %w", err)
	}

	// 3. Mark DLQ entry as replayed
	const markReplayed = `
		UPDATE dead_letter_jobs
		SET replayed_at     = NOW(),
		    replayed_job_id = $2
		WHERE id = $1`

	if _, err := tx.Exec(ctx, markReplayed, dlqID, newJob.ID); err != nil {
		return nil, fmt.Errorf("mark replayed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return newJob, nil
}

// DeleteDLQJob permanently removes a DLQ entry (and does NOT affect the original job).
func (db *DB) DeleteDLQJob(ctx context.Context, dlqID string) error {
	const q = `DELETE FROM dead_letter_jobs WHERE id = $1`
	result, err := db.pool.Exec(ctx, q, dlqID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return domain.ErrDLQEntryNotFound
	}
	return nil
}

// -----------------------------------------------------------------------
// DLQ Read Operations
// -----------------------------------------------------------------------

// GetDLQByID fetches a single DLQ entry.
func (db *DB) GetDLQByID(ctx context.Context, dlqID string) (*domain.DeadLetterJob, error) {
	const q = `
		SELECT id, job_id, job_type, queue, payload, last_error,
		       attempt_count, max_attempts, job_created_at, died_at,
		       replayed_at, replayed_job_id
		FROM dead_letter_jobs
		WHERE id = $1`

	row := db.pool.QueryRow(ctx, q, dlqID)
	dlqJob, err := scanDLQJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrDLQEntryNotFound
	}
	return dlqJob, err
}

// ListDLQ returns paginated DLQ entries with optional queue and replay filters.
func (db *DB) ListDLQ(ctx context.Context, f domain.DLQListFilter) (*domain.DLQListResult, error) {
	if f.Limit == 0 {
		f.Limit = 50
	}
	if f.Page == 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	where := []string{"1=1"}
	args := []any{}
	idx := 1

	if f.Queue != "" {
		where = append(where, fmt.Sprintf("queue = $%d", idx))
		args = append(args, f.Queue)
		idx++
	}
	if f.OnlyUnreplayed {
		where = append(where, "replayed_at IS NULL")
	}

	whereClause := strings.Join(where, " AND ")

	var total int
	countQ := fmt.Sprintf("SELECT COUNT(*) FROM dead_letter_jobs WHERE %s", whereClause)
	if err := db.pool.QueryRow(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count dlq: %w", err)
	}

	dataQ := fmt.Sprintf(`
		SELECT id, job_id, job_type, queue, payload, last_error,
		       attempt_count, max_attempts, job_created_at, died_at,
		       replayed_at, replayed_job_id
		FROM dead_letter_jobs
		WHERE %s
		ORDER BY died_at DESC
		LIMIT $%d OFFSET $%d`,
		whereClause, idx, idx+1,
	)
	args = append(args, f.Limit, offset)

	rows, err := db.pool.Query(ctx, dataQ, args...)
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.DeadLetterJob
	for rows.Next() {
		j, err := scanDLQJobFromRows(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	return &domain.DLQListResult{
		Jobs:  jobs,
		Total: total,
		Page:  f.Page,
		Limit: f.Limit,
	}, nil
}

// DLQStats returns aggregate counts for a queue's DLQ — useful for dashboards.
func (db *DB) DLQStats(ctx context.Context, queue string) (map[string]int, error) {
	const q = `
		SELECT
			COUNT(*)                                       AS total,
			COUNT(*) FILTER (WHERE replayed_at IS NULL)    AS pending_replay,
			COUNT(*) FILTER (WHERE replayed_at IS NOT NULL) AS replayed
		FROM dead_letter_jobs
		WHERE queue = $1`

	var total, pendingReplay, replayed int
	if err := db.pool.QueryRow(ctx, q, queue).Scan(&total, &pendingReplay, &replayed); err != nil {
		return nil, err
	}

	return map[string]int{
		"total":          total,
		"pending_replay": pendingReplay,
		"replayed":       replayed,
	}, nil
}

// BulkReplayDLQ replays all unreplayed DLQ entries for a queue.
// Returns the count of jobs successfully replayed.
// Each replay is its own transaction — a failure on one doesn't roll back others.
func (db *DB) BulkReplayDLQ(ctx context.Context, queue string) (int, error) {
	// Fetch all unreplayed IDs first
	const listQ = `
		SELECT id FROM dead_letter_jobs
		WHERE queue = $1 AND replayed_at IS NULL
		ORDER BY died_at ASC`

	rows, err := db.pool.Query(ctx, listQ, queue)
	if err != nil {
		return 0, fmt.Errorf("list dlq for bulk replay: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()

	replayed := 0
	for _, id := range ids {
		if _, err := db.ReplayDLQJob(ctx, id); err != nil {
			// Log but continue — don't abort the bulk operation on one failure
			continue
		}
		replayed++
	}
	return replayed, nil
}

// -----------------------------------------------------------------------
// Scan helpers
// -----------------------------------------------------------------------

func scanDLQJob(row pgx.Row) (*domain.DeadLetterJob, error) {
	j := &domain.DeadLetterJob{}
	err := row.Scan(
		&j.ID, &j.JobID, &j.JobType, &j.Queue, &j.Payload,
		&j.LastError, &j.AttemptCount, &j.MaxAttempts,
		&j.JobCreatedAt, &j.DiedAt, &j.ReplayedAt, &j.ReplayedJobID,
	)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func scanDLQJobFromRows(rows pgx.Rows) (*domain.DeadLetterJob, error) {
	j := &domain.DeadLetterJob{}
	err := rows.Scan(
		&j.ID, &j.JobID, &j.JobType, &j.Queue, &j.Payload,
		&j.LastError, &j.AttemptCount, &j.MaxAttempts,
		&j.JobCreatedAt, &j.DiedAt, &j.ReplayedAt, &j.ReplayedJobID,
	)
	if err != nil {
		return nil, err
	}
	return j, nil
}

// purgeOldDLQEntries removes DLQ entries older than the given duration.
// Useful for periodic cleanup to prevent unbounded table growth.
func (db *DB) PurgeDLQOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	const q = `DELETE FROM dead_letter_jobs WHERE died_at < $1`
	cutoff := time.Now().Add(-age)
	result, err := db.pool.Exec(ctx, q, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
