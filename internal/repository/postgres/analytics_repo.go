package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
)

// OverviewStats aggregates job counts grouped by status.
func (db *DB) OverviewStats(ctx context.Context) (map[string]int, error) {
	const q = "SELECT status, COUNT(*) FROM jobs GROUP BY status"
	rows, err := db.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("overview stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]int)
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

// ThroughputTimeSeries pulls job events rolled up to hourly/daily intervals.
func (db *DB) ThroughputTimeSeries(ctx context.Context, interval string, since time.Time) (*domain.ThroughputData, error) {
	// 1. Completed jobs time series
	qCompleted := fmt.Sprintf(`
		SELECT date_trunc($1, completed_at) AS bucket, COUNT(*) 
		FROM jobs 
		WHERE status = 'completed' AND completed_at >= $2 
		GROUP BY bucket 
		ORDER BY bucket ASC`,
	)
	completedRows, err := db.pool.Query(ctx, qCompleted, interval, since)
	if err != nil {
		return nil, fmt.Errorf("throughput completed: %w", err)
	}
	defer completedRows.Close()

	var completed []domain.TimePoint
	for completedRows.Next() {
		var tp domain.TimePoint
		if err := completedRows.Scan(&tp.Timestamp, &tp.Count); err != nil {
			return nil, err
		}
		completed = append(completed, tp)
	}

	// 2. Failed / dead jobs time series
	qFailed := fmt.Sprintf(`
		SELECT date_trunc($1, failed_at) AS bucket, COUNT(*) 
		FROM jobs 
		WHERE status IN ('failed', 'dead') AND failed_at >= $2 
		GROUP BY bucket 
		ORDER BY bucket ASC`,
	)
	failedRows, err := db.pool.Query(ctx, qFailed, interval, since)
	if err != nil {
		return nil, fmt.Errorf("throughput failed: %w", err)
	}
	defer failedRows.Close()

	var failed []domain.TimePoint
	for failedRows.Next() {
		var tp domain.TimePoint
		if err := failedRows.Scan(&tp.Timestamp, &tp.Count); err != nil {
			return nil, err
		}
		failed = append(failed, tp)
	}

	// 3. Submitted jobs time series
	qSubmitted := fmt.Sprintf(`
		SELECT date_trunc($1, created_at) AS bucket, COUNT(*) 
		FROM jobs 
		WHERE created_at >= $2 
		GROUP BY bucket 
		ORDER BY bucket ASC`,
	)
	submittedRows, err := db.pool.Query(ctx, qSubmitted, interval, since)
	if err != nil {
		return nil, fmt.Errorf("throughput submitted: %w", err)
	}
	defer submittedRows.Close()

	var submitted []domain.TimePoint
	for submittedRows.Next() {
		var tp domain.TimePoint
		if err := submittedRows.Scan(&tp.Timestamp, &tp.Count); err != nil {
			return nil, err
		}
		submitted = append(submitted, tp)
	}

	return &domain.ThroughputData{
		Completed: completed,
		Failed:    failed,
		Submitted: submitted,
	}, nil
}

// LatencyPercentiles aggregates duration percentiles for completed jobs in milliseconds.
func (db *DB) LatencyPercentiles(ctx context.Context, queue string, since time.Time) (*domain.LatencyStats, error) {
	var q string
	var stats domain.LatencyStats
	var err error

	if queue != "" {
		q = `
			SELECT 
				COALESCE(AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS avg_lat,
				COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS p50_lat,
				COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS p95_lat,
				COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS p99_lat,
				COALESCE(MIN(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS min_lat,
				COALESCE(MAX(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS max_lat
			FROM jobs 
			WHERE status = 'completed' AND completed_at >= $1 AND queue = $2`
		err = db.pool.QueryRow(ctx, q, since, queue).Scan(
			&stats.Avg, &stats.P50, &stats.P95, &stats.P99, &stats.Min, &stats.Max,
		)
	} else {
		q = `
			SELECT 
				COALESCE(AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS avg_lat,
				COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS p50_lat,
				COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS p95_lat,
				COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS p99_lat,
				COALESCE(MIN(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS min_lat,
				COALESCE(MAX(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0) AS max_lat
			FROM jobs 
			WHERE status = 'completed' AND completed_at >= $1`
		err = db.pool.QueryRow(ctx, q, since).Scan(
			&stats.Avg, &stats.P50, &stats.P95, &stats.P99, &stats.Min, &stats.Max,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("latency percentiles: %w", err)
	}
	return &stats, nil
}

// JobTypeDistribution counts job records grouped by type and status.
func (db *DB) JobTypeDistribution(ctx context.Context, since time.Time) ([]*domain.JobTypeStat, error) {
	const q = `
		SELECT job_type, status, COUNT(*) 
		FROM jobs 
		WHERE created_at >= $1 
		GROUP BY job_type, status`
	rows, err := db.pool.Query(ctx, q, since)
	if err != nil {
		return nil, fmt.Errorf("job type distribution: %w", err)
	}
	defer rows.Close()

	var stats []*domain.JobTypeStat
	for rows.Next() {
		s := &domain.JobTypeStat{}
		if err := rows.Scan(&s.JobType, &s.Status, &s.Count); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// WorkerStats reviews throughput metrics per worker ID.
func (db *DB) WorkerStats(ctx context.Context, since time.Time) ([]*domain.WorkerStat, error) {
	const q = `
		SELECT 
			worker_id, 
			COUNT(*) FILTER (WHERE status = 'completed') AS completed, 
			COUNT(*) FILTER (WHERE status IN ('failed', 'dead')) AS failed, 
			COUNT(*) AS total 
		FROM jobs 
		WHERE started_at >= $1 AND worker_id IS NOT NULL 
		GROUP BY worker_id`
	rows, err := db.pool.Query(ctx, q, since)
	if err != nil {
		return nil, fmt.Errorf("worker stats: %w", err)
	}
	defer rows.Close()

	var stats []*domain.WorkerStat
	for rows.Next() {
		s := &domain.WorkerStat{}
		if err := rows.Scan(&s.WorkerID, &s.Completed, &s.Failed, &s.Total); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// TopErrors aggregates the most frequent failure reasons.
func (db *DB) TopErrors(ctx context.Context, since time.Time, limit int) ([]*domain.ErrorStat, error) {
	const q = `
		SELECT job_type, COALESCE(last_error, '') AS last_error, COUNT(*) AS count 
		FROM jobs 
		WHERE status IN ('failed', 'dead') AND failed_at >= $1 AND last_error IS NOT NULL 
		GROUP BY job_type, last_error 
		ORDER BY count DESC 
		LIMIT $2`
	rows, err := db.pool.Query(ctx, q, since, limit)
	if err != nil {
		return nil, fmt.Errorf("top errors: %w", err)
	}
	defer rows.Close()

	var stats []*domain.ErrorStat
	for rows.Next() {
		s := &domain.ErrorStat{}
		if err := rows.Scan(&s.JobType, &s.LastError, &s.Count); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// RetryDistribution maps total job attempt counts.
func (db *DB) RetryDistribution(ctx context.Context) ([]*domain.RetryStat, error) {
	const q = "SELECT attempt_count, COUNT(*) FROM jobs GROUP BY attempt_count ORDER BY attempt_count ASC"
	rows, err := db.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("retry distribution: %w", err)
	}
	defer rows.Close()

	var stats []*domain.RetryStat
	for rows.Next() {
		s := &domain.RetryStat{}
		if err := rows.Scan(&s.AttemptCount, &s.Count); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// QueuesSummary aggregates job status statistics per queue.
func (db *DB) QueuesSummary(ctx context.Context) (map[string]map[string]int, error) {
	const q = "SELECT queue, status, COUNT(*) FROM jobs GROUP BY queue, status"
	rows, err := db.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("queues summary: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]map[string]int)
	for rows.Next() {
		var queue string
		var status string
		var count int
		if err := rows.Scan(&queue, &status, &count); err != nil {
			return nil, err
		}
		if _, exists := stats[queue]; !exists {
			stats[queue] = make(map[string]int)
		}
		stats[queue][status] = count
	}
	return stats, nil
}

// DLQOverview counts dead-lettered entries per queue.
func (db *DB) DLQOverview(ctx context.Context) ([]*domain.DLQQueueStat, error) {
	const q = `
		SELECT queue, COUNT(*) AS total, COUNT(*) FILTER (WHERE replayed_at IS NULL) AS pending_replay 
		FROM dead_letter_jobs 
		GROUP BY queue`
	rows, err := db.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("dlq overview: %w", err)
	}
	defer rows.Close()

	var stats []*domain.DLQQueueStat
	for rows.Next() {
		s := &domain.DLQQueueStat{}
		if err := rows.Scan(&s.Queue, &s.Total, &s.PendingReplay); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

// RecentFailures lists the most recently failed jobs.
func (db *DB) RecentFailures(ctx context.Context, limit int) ([]*domain.Job, error) {
	const q = `
		SELECT id, client_id, job_type, queue, payload, status, priority, 
		       max_attempts, attempt_count, run_at, started_at, completed_at, failed_at, 
		       worker_id, last_error, idempotency_key, COALESCE(request_hash, ''), created_at, updated_at 
		FROM jobs 
		WHERE status IN ('failed', 'dead') 
		ORDER BY failed_at DESC NULLS LAST, updated_at DESC 
		LIMIT $1`
	rows, err := db.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("recent failures: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		j, err := scanJobFromRows(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
