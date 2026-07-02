package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
)

// DLQService handles all business logic for dead-letter queue operations.
type DLQService struct {
	db    domain.DLQRepository
	redis domain.QueueBroker
	log   *slog.Logger
}

func NewDLQService(db domain.DLQRepository, redis domain.QueueBroker, log *slog.Logger) *DLQService {
	return &DLQService{db: db, redis: redis, log: log}
}

// List returns paginated DLQ entries for a specific client with optional filters.
func (s *DLQService) List(ctx context.Context, clientID string, f domain.DLQListFilter) (*domain.DLQListResult, error) {
	return s.db.ListDLQ(ctx, clientID, f)
}

// GetByID fetches a single DLQ entry by its ID and client ID.
func (s *DLQService) GetByID(ctx context.Context, clientID, dlqID string) (*domain.DeadLetterJob, error) {
	return s.db.GetDLQByID(ctx, clientID, dlqID)
}

// Replay re-enqueues a dead job as a fresh pending job for a specific client.
// The original DLQ entry is kept for audit purposes (marked with replayed_at).
func (s *DLQService) Replay(ctx context.Context, clientID, dlqID string) (*domain.Job, error) {
	newJob, err := s.db.ReplayDLQJob(ctx, clientID, dlqID)
	if err != nil {
		return nil, err // domain errors (ErrAlreadyReplayed, ErrDLQEntryNotFound) bubble up as-is
	}

	// Immediately push the replayed job to Redis
	if err := s.redis.Enqueue(ctx, newJob.Queue, newJob.ID, newJob.Priority, newJob.RunAt); err != nil {
		s.log.ErrorContext(ctx, "dlq.replay.redis_enqueue_failed",
			"job_id", newJob.ID,
			"error", err,
			"impact", "replayed job will start after postgres sync delay",
		)
	}

	s.log.InfoContext(ctx, "dlq.replayed",
		"dlq_id", dlqID,
		"new_job_id", newJob.ID,
		"job_type", newJob.JobType,
		"queue", newJob.Queue,
	)
	return newJob, nil
}

// Delete permanently removes a DLQ entry for a specific client.
// Use when you've inspected a dead job and decided it should be discarded.
func (s *DLQService) Delete(ctx context.Context, clientID, dlqID string) error {
	if err := s.db.DeleteDLQJob(ctx, clientID, dlqID); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "dlq.deleted", "dlq_id", dlqID)
	return nil
}

// Stats returns aggregate DLQ counts for a named queue and client.
func (s *DLQService) Stats(ctx context.Context, clientID, queue string) (map[string]int, error) {
	return s.db.DLQStats(ctx, clientID, queue)
}

// BulkReplay replays all unreplayed entries for a queue and client.
// Returns the number of jobs successfully re-enqueued.
func (s *DLQService) BulkReplay(ctx context.Context, clientID, queue string) (int, error) {
	replayedJobs, err := s.db.BulkReplayDLQ(ctx, clientID, queue)
	if err != nil {
		return 0, fmt.Errorf("bulk replay: %w", err)
	}

	for _, newJob := range replayedJobs {
		if err := s.redis.Enqueue(ctx, newJob.Queue, newJob.ID, newJob.Priority, newJob.RunAt); err != nil {
			s.log.ErrorContext(ctx, "dlq.bulk_replay.redis_enqueue_failed",
				"job_id", newJob.ID,
				"error", err,
			)
		}
	}

	s.log.InfoContext(ctx, "dlq.bulk_replayed", "queue", queue, "count", len(replayedJobs))
	return len(replayedJobs), nil
}

// Purge deletes DLQ entries for a client older than the given duration.
// Safe to run on a schedule (e.g., delete anything older than 30 days).
func (s *DLQService) Purge(ctx context.Context, clientID string, olderThan time.Duration) (int64, error) {
	n, err := s.db.PurgeDLQOlderThan(ctx, clientID, olderThan)
	if err != nil {
		return 0, err
	}
	s.log.InfoContext(ctx, "dlq.purged", "deleted_count", n, "older_than", olderThan)
	return n, nil
}
