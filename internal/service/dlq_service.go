package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/metrics"
	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
)

// DLQService handles all business logic for dead-letter queue operations.
type DLQService struct {
	db  *postgres.DB
	log *slog.Logger
}

func NewDLQService(db *postgres.DB, log *slog.Logger) *DLQService {
	return &DLQService{db: db, log: log}
}

// List returns paginated DLQ entries with optional filters.
func (s *DLQService) List(ctx context.Context, f domain.DLQListFilter) (*domain.DLQListResult, error) {
	return s.db.ListDLQ(ctx, f)
}

// GetByID fetches a single DLQ entry by its ID.
func (s *DLQService) GetByID(ctx context.Context, dlqID string) (*domain.DeadLetterJob, error) {
	return s.db.GetDLQByID(ctx, dlqID)
}

// Replay re-enqueues a dead job as a fresh pending job.
// The original DLQ entry is kept for audit purposes (marked with replayed_at).
func (s *DLQService) Replay(ctx context.Context, dlqID string) (*domain.Job, error) {
	newJob, err := s.db.ReplayDLQJob(ctx, dlqID)
	if err != nil {
		return nil, err // domain errors (ErrAlreadyReplayed, ErrDLQEntryNotFound) bubble up as-is
	}

	metrics.DLQTotal.WithLabelValues(newJob.Queue).Dec()

	s.log.InfoContext(ctx, "dlq.replayed",
		"dlq_id", dlqID,
		"new_job_id", newJob.ID,
		"job_type", newJob.JobType,
		"queue", newJob.Queue,
	)
	return newJob, nil
}

// Delete permanently removes a DLQ entry.
// Use when you've inspected a dead job and decided it should be discarded.
func (s *DLQService) Delete(ctx context.Context, dlqID string) error {
	// Fetch first so we know the queue for the label
	entry, err := s.db.GetDLQByID(ctx, dlqID)
	if err != nil {
		return err
	}
	if err := s.db.DeleteDLQJob(ctx, dlqID); err != nil {
		return err
	}
	metrics.DLQTotal.WithLabelValues(entry.Queue).Dec()
	s.log.InfoContext(ctx, "dlq.deleted", "dlq_id", dlqID)
	return nil
}

// Stats returns aggregate DLQ counts for a named queue.
func (s *DLQService) Stats(ctx context.Context, queue string) (map[string]int, error) {
	return s.db.DLQStats(ctx, queue)
}

// BulkReplay replays all unreplayed entries for a queue.
// Returns the number of jobs successfully re-enqueued.
func (s *DLQService) BulkReplay(ctx context.Context, queue string) (int, error) {
	count, err := s.db.BulkReplayDLQ(ctx, queue)
	if err != nil {
		return 0, fmt.Errorf("bulk replay: %w", err)
	}
	s.log.InfoContext(ctx, "dlq.bulk_replayed", "queue", queue, "count", count)
	return count, nil
}

// Purge deletes DLQ entries older than the given duration.
// Safe to run on a schedule (e.g., delete anything older than 30 days).
func (s *DLQService) Purge(ctx context.Context, olderThan time.Duration) (int64, error) {
	n, err := s.db.PurgeDLQOlderThan(ctx, olderThan)
	if err != nil {
		return 0, err
	}
	s.log.InfoContext(ctx, "dlq.purged", "deleted_count", n, "older_than", olderThan)
	return n, nil
}
