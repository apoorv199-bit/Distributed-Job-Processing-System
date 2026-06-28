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

// MoveToDLQ atomically:
//  1. Inserts a record into dead_letter_jobs
//  2. Updates the original job's status to 'dead'
//
// Both happen in a single transaction — you never get a dead job without a DLQ entry, and never a DLQ entry for a non-dead job.
func (db *DB) MoveToDLQ(ctx context.Context, job *domain.Job, lastError string) (*domain.DeadLetterJob, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if tx.Commit() succeeds

	// 1. Insert DLQ record
	const insertDLQ = `
		INSERT INTO dead_letter_jobs
		    (job_id, job_type, queue, payload, last_error, attempt_count, max_attempts, job_created_at, client_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, client_id, job_id, job_type, queue, payload, last_error,
		          attempt_count, max_attempts, job_created_at, died_at,
		          replayed_at, replayed_job_id`

	row := tx.QueryRow(ctx, insertDLQ,
		job.ID, job.JobType, job.Queue, job.Payload,
		lastError, job.AttemptCount, job.MaxAttempts, job.CreatedAt, job.ClientID,
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
func (db *DB) ReplayDLQJob(ctx context.Context, clientID, dlqID string) (*domain.Job, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Fetch and lock the DLQ entry
	var row pgx.Row
	if clientID == "admin" {
		const fetchDLQAdmin = `
			SELECT id, client_id, job_id, job_type, queue, payload,
			       last_error, attempt_count, max_attempts,
			       job_created_at, died_at, replayed_at, replayed_job_id
			FROM dead_letter_jobs
			WHERE id = $1
			FOR UPDATE`
		row = tx.QueryRow(ctx, fetchDLQAdmin, dlqID)
	} else {
		const fetchDLQ = `
			SELECT id, client_id, job_id, job_type, queue, payload,
			       last_error, attempt_count, max_attempts,
			       job_created_at, died_at, replayed_at, replayed_job_id
			FROM dead_letter_jobs
			WHERE id = $1 AND client_id = $2
			FOR UPDATE`
		row = tx.QueryRow(ctx, fetchDLQ, dlqID, clientID)
	}

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

	// 2. Insert a new job — reset attempt_count, preserve original payload + config, and retain original owner ClientID
	const insertJob = `
		INSERT INTO jobs (job_type, queue, payload, priority, max_attempts, run_at, client_id)
		VALUES ($1, $2, $3, 5, $4, NOW(), $5)
		RETURNING id, client_id, job_type, queue, payload, status, priority,
		          max_attempts, attempt_count, run_at,
		          started_at, completed_at, failed_at,
		          worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at`

	jobRow := tx.QueryRow(ctx, insertJob, dlqJob.JobType, dlqJob.Queue, dlqJob.Payload, dlqJob.MaxAttempts, dlqJob.ClientID)
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

// DeleteDLQJob permanently removes a DLQ entry (isolated by clientID unless admin).
func (db *DB) DeleteDLQJob(ctx context.Context, clientID, dlqID string) error {
	var q string
	var args []any
	if clientID == "admin" {
		q = `DELETE FROM dead_letter_jobs WHERE id = $1`
		args = []any{dlqID}
	} else {
		q = `DELETE FROM dead_letter_jobs WHERE id = $1 AND client_id = $2`
		args = []any{dlqID, clientID}
	}
	result, err := db.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return domain.ErrDLQEntryNotFound
	}
	return nil
}

// GetDLQByID fetches a single DLQ entry (isolated by clientID unless admin).
func (db *DB) GetDLQByID(ctx context.Context, clientID, dlqID string) (*domain.DeadLetterJob, error) {
	var q string
	var row pgx.Row
	if clientID == "admin" {
		q = `
			SELECT id, client_id, job_id, job_type, queue, payload, last_error,
			       attempt_count, max_attempts, job_created_at, died_at,
			       replayed_at, replayed_job_id
			FROM dead_letter_jobs
			WHERE id = $1`
		row = db.pool.QueryRow(ctx, q, dlqID)
	} else {
		q = `
			SELECT id, client_id, job_id, job_type, queue, payload, last_error,
			       attempt_count, max_attempts, job_created_at, died_at,
			       replayed_at, replayed_job_id
			FROM dead_letter_jobs
			WHERE id = $1 AND client_id = $2`
		row = db.pool.QueryRow(ctx, q, dlqID, clientID)
	}

	dlqJob, err := scanDLQJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrDLQEntryNotFound
	}
	return dlqJob, err
}

// ListDLQ returns paginated DLQ entries (filtered by clientID unless admin).
func (db *DB) ListDLQ(ctx context.Context, clientID string, f domain.DLQListFilter) (*domain.DLQListResult, error) {
	if f.Limit == 0 {
		f.Limit = 50
	}
	if f.Page == 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	var where []string
	var args []any
	var idx int

	if clientID == "admin" {
		where = []string{"1=1"}
		args = []any{}
		idx = 1
	} else {
		where = []string{"client_id = $1"}
		args = []any{clientID}
		idx = 2
	}

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
		SELECT id, client_id, job_id, job_type, queue, payload, last_error,
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

// DLQStats returns aggregate counts for a queue's DLQ (filtered by clientID unless admin).
func (db *DB) DLQStats(ctx context.Context, clientID, queue string) (map[string]int, error) {
	var q string
	var row pgx.Row
	if clientID == "admin" {
		q = `
			SELECT
				COUNT(*)                                       AS total,
				COUNT(*) FILTER (WHERE replayed_at IS NULL)    AS pending_replay,
				COUNT(*) FILTER (WHERE replayed_at IS NOT NULL) AS replayed
			FROM dead_letter_jobs
			WHERE queue = $1`
		row = db.pool.QueryRow(ctx, q, queue)
	} else {
		q = `
			SELECT
				COUNT(*)                                       AS total,
				COUNT(*) FILTER (WHERE replayed_at IS NULL)    AS pending_replay,
				COUNT(*) FILTER (WHERE replayed_at IS NOT NULL) AS replayed
			FROM dead_letter_jobs
			WHERE queue = $1 AND client_id = $2`
		row = db.pool.QueryRow(ctx, q, queue, clientID)
	}

	var total, pendingReplay, replayed int
	if err := row.Scan(&total, &pendingReplay, &replayed); err != nil {
		return nil, err
	}

	return map[string]int{
		"total":          total,
		"pending_replay": pendingReplay,
		"replayed":       replayed,
	}, nil
}

// BulkReplayDLQ replays all unreplayed DLQ entries for a queue (filtered by clientID unless admin).
func (db *DB) BulkReplayDLQ(ctx context.Context, clientID, queue string) ([]*domain.Job, error) {
	var listQ string
	var rows pgx.Rows
	var err error
	if clientID == "admin" {
		listQ = `
			SELECT id FROM dead_letter_jobs
			WHERE queue = $1 AND replayed_at IS NULL
			ORDER BY died_at ASC`
		rows, err = db.pool.Query(ctx, listQ, queue)
	} else {
		listQ = `
			SELECT id FROM dead_letter_jobs
			WHERE queue = $1 AND replayed_at IS NULL AND client_id = $2
			ORDER BY died_at ASC`
		rows, err = db.pool.Query(ctx, listQ, queue, clientID)
	}
	if err != nil {
		return nil, fmt.Errorf("list dlq for bulk replay: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()

	var replayed []*domain.Job
	for _, id := range ids {
		job, err := db.ReplayDLQJob(ctx, clientID, id)
		if err != nil {
			continue
		}
		replayed = append(replayed, job)
	}
	return replayed, nil
}

func scanDLQJob(row pgx.Row) (*domain.DeadLetterJob, error) {
	j := &domain.DeadLetterJob{}
	err := row.Scan(
		&j.ID, &j.ClientID, &j.JobID, &j.JobType, &j.Queue, &j.Payload,
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
		&j.ID, &j.ClientID, &j.JobID, &j.JobType, &j.Queue, &j.Payload,
		&j.LastError, &j.AttemptCount, &j.MaxAttempts,
		&j.JobCreatedAt, &j.DiedAt, &j.ReplayedAt, &j.ReplayedJobID,
	)
	if err != nil {
		return nil, err
	}
	return j, nil
}

// PurgeDLQOlderThan removes DLQ entries for a client older than the given duration (cleans all if admin).
func (db *DB) PurgeDLQOlderThan(ctx context.Context, clientID string, age time.Duration) (int64, error) {
	var q string
	var args []any
	cutoff := time.Now().Add(-age)
	if clientID == "admin" {
		q = `DELETE FROM dead_letter_jobs WHERE died_at < $1`
		args = []any{cutoff}
	} else {
		q = `DELETE FROM dead_letter_jobs WHERE died_at < $1 AND client_id = $2`
		args = []any{cutoff, clientID}
	}
	result, err := db.pool.Exec(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
