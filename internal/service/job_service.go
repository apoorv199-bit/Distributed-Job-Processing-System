package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/metrics"
	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
	redisrepo "github.com/apoorv/distributed-job-processor/internal/repository/redis"
)

// JobService handles all business logic for job submission and querying.
// It sits between the HTTP handlers and the Postgres repository.
type JobService struct {
	db    *postgres.DB
	redis *redisrepo.Client
	log   *slog.Logger
}

func NewJobService(db *postgres.DB, redis *redisrepo.Client, log *slog.Logger) *JobService {
	return &JobService{
		db:    db,
		redis: redis,
		log:   log,
	}
}

// Submit validates, persists, and returns a new job.
func (s *JobService) Submit(ctx context.Context, req *domain.SubmitRequest) (*domain.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	job, err := s.db.InsertJob(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}

	// Enqueue to Redis — if this fails, the Postgres fallback in the worker
	// will still pick it up. Never block job submission over Redis failure.
	if err := s.redis.Enqueue(ctx, job.Queue, job.ID, job.Priority, job.RunAt); err != nil {
		s.log.Error("redis.enqueue_failed",
			"job_id", job.ID,
			"queue", job.Queue,
			"error", err,
			"impact", "worker will fall back to postgres polling — higher latency",
		)
		// Non-fatal: worker will fall back to Postgres SKIP LOCKED
	} else {
		s.log.Debug("redis.enqueued",
			"job_id", job.ID,
			"queue", job.Queue,
		)
	}

	metrics.JobsEnqueued.WithLabelValues(job.JobType, job.Queue).Inc()
	s.log.InfoContext(ctx, "job.submitted", "job_id", job.ID, "job_type", job.JobType, "queue", job.Queue, "priority", job.Priority)

	return job, nil
}

// GetByID fetches a job by ID, returning ErrJobNotFound if missing.
func (s *JobService) GetByID(ctx context.Context, id string) (*domain.Job, error) {
	return s.db.GetByID(ctx, id)
}

// List returns a filtered, paginated list of jobs.
func (s *JobService) List(ctx context.Context, filter domain.ListFilter) (*domain.ListResult, error) {
	return s.db.List(ctx, filter)
}

// GetAttempts returns the execution history for a job.
func (s *JobService) GetAttempts(ctx context.Context, jobID string) ([]*domain.JobAttempt, error) {
	// Verify job exists first
	if _, err := s.db.GetByID(ctx, jobID); err != nil {
		return nil, err
	}
	return s.db.GetAttempts(ctx, jobID)
}

// QueueStats returns aggregate counts by status for a named queue.
func (s *JobService) QueueStats(ctx context.Context, queue string) (map[string]int, error) {
	return s.db.QueueStats(ctx, queue)
}
