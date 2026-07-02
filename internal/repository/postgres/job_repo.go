package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/jackc/pgx/v5"
)

// InsertJob persists a new job and returns it with DB-assigned fields populated.
// If a job with the same idempotency key exists, it returns the existing job.
// InsertJob persists a new job and returns it with DB-assigned fields populated.
// If a job with the same idempotency key exists, it returns the existing job.
func (db *DB) InsertJob(ctx context.Context, clientID string, req *domain.SubmitRequest) (*domain.Job, error) {
	runAt := time.Now()
	if req.RunAt != nil {
		runAt = *req.RunAt
	}

	// Compute request hash for idempotency key verification
	hasher := sha256.New()
	hasher.Write([]byte(req.JobType))
	hasher.Write([]byte(req.Queue))
	hasher.Write(req.Payload)
	requestHash := hex.EncodeToString(hasher.Sum(nil))

	idempotencyKey := ""
	if req.IdempotencyKey != nil {
		idempotencyKey = *req.IdempotencyKey
	}

	const q = `
		INSERT INTO jobs (job_type, queue, payload, priority, max_attempts, run_at, idempotency_key, client_id, request_hash)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
		ON CONFLICT (client_id, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET updated_at = jobs.updated_at
		RETURNING id, client_id, job_type, queue, payload, status, priority,
		          max_attempts, attempt_count, run_at,
		          started_at, completed_at, failed_at,
		          worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at`

	row := db.pool.QueryRow(ctx, q, req.JobType, req.Queue, req.Payload, req.Priority, req.MaxAttempts, runAt, idempotencyKey, clientID, requestHash)

	job, err := scanJob(row)
	if err != nil {
		return nil, err
	}

	// If the job already exists (idempotency key conflict), check if the request hash matches
	if job.IdempotencyKey != nil && *job.IdempotencyKey != "" {
		if job.RequestHash != requestHash {
			return nil, domain.ErrIdempotencyConflict
		}
	}

	return job, nil
}

// MarkRunning atomically claims the job for a worker.
// Returns ErrJobNotFound if the job is no longer in pending state (or running but not yet timed out).
func (db *DB) MarkRunning(ctx context.Context, jobID, workerID string, visibilityTimeout time.Duration) (*domain.Job, error) {
	const q = `
		UPDATE jobs
		SET status = 'running',
			started_at = Now(),
			worker_id = $2,
			attempt_count = attempt_count + 1,
			updated_at = Now()
		WHERE id = $1 AND (status = 'pending' OR (status = 'running' AND started_at < $3))
		RETURNING id, client_id, job_type, queue, payload, status, priority,
		          max_attempts, attempt_count, run_at,
		          started_at, completed_at, failed_at,
		          worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at`

	cutoff := time.Now().Add(-visibilityTimeout)
	row := db.pool.QueryRow(ctx, q, jobID, workerID, cutoff)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrJobNotFound
	}
	return job, nil
}

// MarkCompleted sets the job status to completed and records timing.
func (db *DB) MarkCompleted(ctx context.Context, jobID string) error {
	const q = `
		UPDATE jobs
		SET status = 'completed',
			completed_at = Now(),
			updated_at = Now()
		WHERE id = $1`

	_, err := db.pool.Exec(ctx, q, jobID)
	return err
}

// MarkFailed sets the job status to failed and records the error message.
// If attempt_count < max_attempts the job stays visible for retry (caller's responsibility).
func (db *DB) MarkFailed(ctx context.Context, jobID, errMsg string) error {
	const q = `
		UPDATE jobs
		SET status     = 'failed',
		    failed_at  = NOW(),
		    last_error = $2,
		    updated_at = NOW()
		WHERE id = $1`
	_, err := db.pool.Exec(ctx, q, jobID, errMsg)
	return err
}

// RescheduleRetry resets a failed job back to pending with a future run_at
// so the scheduler will pick it up again after the backoff delay.
func (db *DB) RescheduleRetry(ctx context.Context, jobID string, runAt time.Time) error {
	const q = `
		UPDATE jobs
		SET status     = 'pending',
		    run_at     = $2,
		    worker_id  = NULL,
		    started_at = NULL,
		    updated_at = NOW()
		WHERE id = $1`
	_, err := db.pool.Exec(ctx, q, jobID, runAt)
	return err
}

// RecordAttempt writes an audit entry to job_attempts after each execution.
func (db *DB) RecordAttempt(ctx context.Context, a *domain.JobAttempt) error {
	const q = `
		INSERT INTO job_attempts (job_id, attempt_num, worker_id, started_at, finished_at, status, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := db.pool.Exec(ctx, q,
		a.JobID, a.AttemptNum, a.WorkerID,
		a.StartedAt, a.FinishedAt, a.Status, a.Error,
	)
	return err
}

// GetByID fetches a single job by its primary key. If clientID is not "admin", filters by clientID.
func (db *DB) GetByID(ctx context.Context, clientID, id string) (*domain.Job, error) {
	var q string
	var row pgx.Row
	if clientID == "admin" {
		q = `
			SELECT id, client_id, job_type, queue, payload, status, priority,
			       max_attempts, attempt_count, run_at,
			       started_at, completed_at, failed_at,
			       worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at
			FROM jobs WHERE id = $1`
		row = db.pool.QueryRow(ctx, q, id)
	} else {
		q = `
			SELECT id, client_id, job_type, queue, payload, status, priority,
			       max_attempts, attempt_count, run_at,
			       started_at, completed_at, failed_at,
			       worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at
			FROM jobs WHERE id = $1 AND client_id = $2`
		row = db.pool.QueryRow(ctx, q, id, clientID)
	}

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrJobNotFound
	}
	return job, err
}

// List returns a paginated, optionally filtered list of jobs for a specific client.
func (db *DB) List(ctx context.Context, clientID string, f domain.ListFilter) (*domain.ListResult, error) {
	if f.Limit == 0 {
		f.Limit = 50
	}

	if f.Page == 0 {
		f.Page = 1
	}

	offset := (f.Page - 1) * f.Limit

	// Build dynamic WHERE clause
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
	if f.Status != "" {
		where = append(where, fmt.Sprintf("status = $%d", idx))
		args = append(args, f.Status)
		idx++
	}

	whereClause := strings.Join(where, " AND ")

	// Count total (for pagination metadata)
	var total int
	countQ := fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE %s", whereClause)
	if err := db.pool.QueryRow(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}

	// Fetch page
	dataQ := fmt.Sprintf(`
		SELECT id, client_id, job_type, queue, payload, status, priority,
		       max_attempts, attempt_count, run_at,
		       started_at, completed_at, failed_at,
		       worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at
		FROM jobs
		WHERE %s
		ORDER BY priority ASC, created_at DESC
		LIMIT $%d OFFSET $%d`,
		whereClause, idx, idx+1,
	)
	args = append(args, f.Limit, offset)

	rows, err := db.pool.Query(ctx, dataQ, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJobFromRows(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return &domain.ListResult{
		Jobs:  jobs,
		Total: total,
		Page:  f.Page,
		Limit: f.Limit,
	}, nil
}

// GetAttempts returns all execution attempts for a job, newest first.
func (db *DB) GetAttempts(ctx context.Context, jobID string) ([]*domain.JobAttempt, error) {
	const q = `
		SELECT id, job_id, attempt_num, worker_id,
		       started_at, finished_at, status, error
		FROM job_attempts
		WHERE job_id = $1
		ORDER BY attempt_num DESC`

	rows, err := db.pool.Query(ctx, q, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []*domain.JobAttempt
	for rows.Next() {
		a := &domain.JobAttempt{}
		err := rows.Scan(
			&a.ID, &a.JobID, &a.AttemptNum, &a.WorkerID,
			&a.StartedAt, &a.FinishedAt, &a.Status, &a.Error,
		)
		if err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	return attempts, nil
}

// ReclaimStalledJobs resets jobs that have been stuck in 'running' for longer
// than visibilityTimeout. This handles worker crashes.
// Returns the number of jobs reclaimed.
func (db *DB) ReclaimStalledJobs(ctx context.Context, visibilityTimeout time.Duration) (int64, error) {
	const q = `
		UPDATE jobs
		SET status     = 'pending',
		    worker_id  = NULL,
		    started_at = NULL,
		    updated_at = NOW()
		WHERE status     = 'running'
		  AND started_at < $1`

	cutoff := time.Now().Add(-visibilityTimeout)
	result, err := db.pool.Exec(ctx, q, cutoff)
	if err != nil {
		return 0, fmt.Errorf("reclaim stalled: %w", err)
	}
	return result.RowsAffected(), nil
}

// QueueStats returns pending/running/failed counts for a given queue.
func (db *DB) QueueStats(ctx context.Context, queue string) (map[string]int, error) {
	const q = `
		SELECT status, COUNT(*)
		FROM jobs
		WHERE queue = $1
		GROUP BY status`

	rows, err := db.pool.Query(ctx, q, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := map[string]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[status] = count
	}
	return stats, nil
}

// ListPendingForSync returns pending jobs for a queue that run_at has passed.
// Used by the Postgres sync loop to find jobs that should be in Redis but aren't.
// Limit is applied so we never load unbounded rows.
func (db *DB) ListPendingForSync(ctx context.Context, queue string, limit int) ([]*domain.Job, error) {
	const q = `
		SELECT id, client_id, job_type, queue, payload, status, priority,
		       max_attempts, attempt_count, run_at,
		       started_at, completed_at, failed_at,
		       worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at
		FROM jobs
		WHERE queue  = $1
		  AND status = 'pending'
		  AND run_at <= NOW()
		ORDER BY priority ASC, run_at ASC
		LIMIT $2`

	rows, err := db.pool.Query(ctx, q, queue, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending for sync: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJobFromRows(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func scanJob(row pgx.Row) (*domain.Job, error) {
	j := &domain.Job{}
	err := row.Scan(
		&j.ID, &j.ClientID, &j.JobType, &j.Queue, &j.Payload, &j.Status, &j.Priority,
		&j.MaxAttempts, &j.AttemptCount, &j.RunAt,
		&j.StartedAt, &j.CompletedAt, &j.FailedAt,
		&j.WorkerID, &j.LastError, &j.IdempotencyKey, &j.RequestHash, &j.CreatedAt, &j.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return j, nil
}

func scanJobFromRows(rows pgx.Rows) (*domain.Job, error) {
	j := &domain.Job{}
	err := rows.Scan(
		&j.ID, &j.ClientID, &j.JobType, &j.Queue, &j.Payload, &j.Status, &j.Priority,
		&j.MaxAttempts, &j.AttemptCount, &j.RunAt,
		&j.StartedAt, &j.CompletedAt, &j.FailedAt,
		&j.WorkerID, &j.LastError, &j.IdempotencyKey, &j.RequestHash, &j.CreatedAt, &j.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	return j, nil
}
