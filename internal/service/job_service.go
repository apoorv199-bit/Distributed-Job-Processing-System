package service

import (
	"context"
	"log/slog"

	"github.com/apoorv/distributed-job-processor/internal/domain"
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
func (s *JobService) Submit(ctx context.Context, clientID string, req *domain.SubmitRequest) (*domain.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	job, err := s.db.InsertJob(ctx, clientID, req)
	if err != nil {
		return nil, err // Let the caller handle ErrIdempotencyConflict directly
	}

	// Enqueue to Redis only if the job is still pending.
	// Uses EnqueueIfAbsent to avoid duplicate Redis entries on conflicts.
	if job.Status == domain.StatusPending {
		if err := s.redis.EnqueueIfAbsent(ctx, job.Queue, job.ID, job.Priority, job.RunAt); err != nil {
			s.log.Error("redis.enqueue_failed",
				"job_id", job.ID,
				"queue", job.Queue,
				"error", err,
				"impact", "worker will fall back to postgres polling — higher latency",
			)
		} else {
			s.log.Debug("redis.enqueued",
				"job_id", job.ID,
				"queue", job.Queue,
			)
		}
	}
	s.log.InfoContext(ctx, "job.submitted", "job_id", job.ID, "job_type", job.JobType, "queue", job.Queue, "priority", job.Priority)

	return job, nil
}

// GetByID fetches a job by ID and client ID, returning ErrJobNotFound if missing.
func (s *JobService) GetByID(ctx context.Context, clientID, id string) (*domain.Job, error) {
	return s.db.GetByID(ctx, clientID, id)
}

// List returns a filtered, paginated list of jobs for a specific client.
func (s *JobService) List(ctx context.Context, clientID string, filter domain.ListFilter) (*domain.ListResult, error) {
	return s.db.List(ctx, clientID, filter)
}

// GetAttempts returns the execution history for a job (isolated by clientID).
func (s *JobService) GetAttempts(ctx context.Context, clientID, jobID string) ([]*domain.JobAttempt, error) {
	// Verify job exists and belongs to client first
	if _, err := s.db.GetByID(ctx, clientID, jobID); err != nil {
		return nil, err
	}
	return s.db.GetAttempts(ctx, jobID)
}

// QueueStats returns aggregate counts by status for a named queue.
func (s *JobService) QueueStats(ctx context.Context, queue string) (map[string]int, error) {
	return s.db.QueueStats(ctx, queue)
}
