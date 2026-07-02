package worker

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/apoorv/distributed-job-processor/config"
	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/ratelimiter"
)

// Database defines the subset of repository methods required by the worker.
type Database interface {
	domain.JobRepository
	MoveToDLQ(ctx context.Context, job *domain.Job, lastError string) (*domain.DeadLetterJob, error)
}

// Worker polls one or more queues and executes jobs using a bounded goroutine pool.
type Worker struct {
	id          string
	queues      []string // polled left-to-right (put highest priority queue first)
	concurrency int
	db          Database
	registry    *Registry
	log         *slog.Logger

	// Semaphore pattern: acquire before executing, release after.
	// Capacity = max concurrent in-flight jobs.
	semaphore chan struct{}

	// WaitGroup tracks in-flight job goroutines for graceful drain.
	wg sync.WaitGroup

	// visibilityTimeout is used by the reaper goroutine.
	visibilityTimeout time.Duration

	redis       domain.QueueBroker
	rateLimiter *ratelimiter.RateLimiter
}

type Config struct {
	ID                string
	Queues            []string
	Concurrency       int
	VisibilityTimeout time.Duration
	RateLimitConfig   config.RateLimitConfig
}

func New(cfg Config, db Database, redis domain.QueueBroker, registry *Registry, log *slog.Logger) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = 10 * time.Minute
	}
	return &Worker{
		id:                cfg.ID,
		queues:            cfg.Queues,
		concurrency:       cfg.Concurrency,
		db:                db,
		redis:             redis,
		registry:          registry,
		log:               log,
		semaphore:         make(chan struct{}, cfg.Concurrency),
		visibilityTimeout: cfg.VisibilityTimeout,
		rateLimiter:       ratelimiter.New(redis.Raw(), cfg.RateLimitConfig),
	}
}

// Start launches the poll loops and the reaper. It blocks until ctx is cancelled.
// Call it in a goroutine and cancel ctx for graceful shutdown.
func (w *Worker) Start(ctx context.Context) {
	w.log.Info("worker.starting", "worker_id", w.id, "queues", w.queues, "concurrency", w.concurrency)

	// Launch one poll goroutine per concurrency slot.
	// Each goroutine independently races to dequeue the next job.
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func(slot int) {
			defer w.wg.Done()
			w.pollLoop(ctx, slot)
		}(i)
	}

	// Reaper: periodically recover jobs from crashed workers.
	go w.reaperLoop(ctx)

	// Postgres sync: periodically re-enqueues jobs that are in Postgres
	// but missing from Redis (e.g. Redis restart, missed enqueue)
	go w.postgresSyncLoop(ctx)

	// Block until context cancelled (SIGTERM / SIGINT from main).
	<-ctx.Done()
	w.log.Info("worker.draining", "worker_id", w.id)

	// Wait for all in-flight jobs to finish (or until a hard deadline).
	drainDone := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		w.log.Info("worker.shutdown_clean", "worker_id", w.id)
	case <-time.After(30 * time.Second):
		w.log.Warn("worker.shutdown_forced", "worker_id", w.id, "reason", "drain timeout exceeded 30s")
	}
}

func (w *Worker) pollLoop(ctx context.Context, slot int) {
	w.log.Debug("poll_loop.started", "slot", slot)

	backoff := 50 * time.Millisecond
	maxBackoff := 2 * time.Second

	for {
		// Exit immediately when context is cancelled.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Primary path: Redis only
		// Postgres fallback is handled separately by postgresSyncLoop
		job := w.dequeueFromRedis(ctx)
		if job == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Reset backoff immediately upon finding a job
		backoff = 50 * time.Millisecond

		// Acquire semaphore slot — blocks if we're at max concurrency.
		select {
		case w.semaphore <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// Run the job in a new goroutine so this poll loop can keep fetching.
		w.wg.Add(1)
		go func(j *domain.Job) {
			defer w.wg.Done()
			defer func() {
				<-w.semaphore
			}()
			w.executeJob(context.Background(), j) // fresh ctx — don't cancel on shutdown
		}(job)
	}
}

// dequeueFromRedis pops one job from Redis, then marks it running in
// Postgres atomically. This is the only path during normal operation.
//
// Contract: returns a job already marked status=running in Postgres,
// or nil if all queues are empty.
func (w *Worker) dequeueFromRedis(ctx context.Context) *domain.Job {
	for _, queue := range w.queues {

		// Rate limit: queue-level check before we even touch Redis
		result, err := w.rateLimiter.Allow(ctx, queue, "")
		if err != nil {
			w.log.Warn("ratelimit.error", "queue", queue, "error", err)
		}
		if !result.Allowed {
			w.log.Debug("ratelimit.queue_throttled",
				"queue", queue,
				"limit", result.QueueLimit,
			)
			continue
		}

		// Pop highest-priority job ID from Redis sorted set
		jobID, err := w.redis.Dequeue(ctx, queue)
		if err != nil {
			w.log.Error("redis.dequeue_error", "queue", queue, "error", err)
			continue
		}
		if jobID == "" {
			continue // queue empty, try next
		}

		// Track in Redis inflight set for crash recovery
		if err := w.redis.MarkInflight(ctx, queue, jobID, w.visibilityTimeout); err != nil {
			w.log.Warn("redis.inflight_mark_error",
				"job_id", jobID,
				"error", err,
			)
		}

		// Atomically claim the job in Postgres
		// MarkRunning: UPDATE jobs SET status='running' WHERE id=$1 AND (status='pending' OR status='running' AND started_at < cutoff)
		job, err := w.db.MarkRunning(ctx, jobID, w.id, w.visibilityTimeout)
		if err != nil {
			// Job may have been picked up by another worker (should not happen
			// with Redis ZPOPMIN but defensive check is worth keeping)
			w.log.Error("redis_path.mark_running_failed",
				"job_id", jobID,
				"queue", queue,
				"error", err,
			)
			_ = w.redis.RemoveInflight(ctx, queue, jobID)
			continue
		}

		w.log.Info("job.dequeued",
			"job_id", job.ID,
			"job_type", job.JobType,
			"queue", queue,
			"attempt", job.AttemptCount,
			"source", "redis",
		)
		return job
	}
	return nil
}

// postgresSyncLoop runs every 30 seconds and finds jobs that are pending
// in Postgres but absent from Redis — then re-enqueues them into Redis.
//
// This handles:
//   - Redis restart (all queues lost)
//   - Network partition during Submit (job saved to DB but Redis push failed)
//   - Any other edge case where the two stores drift
//
// It does NOT directly execute jobs — it only re-enqueues them into Redis
// so the normal pollLoop picks them up. This keeps the execution path clean.
func (w *Worker) postgresSyncLoop(ctx context.Context) {
	// Delay first sync so startup doesn't flood logs
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.syncPostgresToRedis(ctx)
		}
	}
}

func (w *Worker) syncPostgresToRedis(ctx context.Context) {
	for _, queue := range w.queues {
		// Find pending jobs in Postgres that should be in Redis
		jobs, err := w.db.ListPendingForSync(ctx, queue, 100)
		if err != nil {
			w.log.Error("postgres_sync.list_failed",
				"queue", queue,
				"error", err,
			)
			continue
		}
		if len(jobs) == 0 {
			continue
		}

		synced := 0
		for _, job := range jobs {
			// Re-enqueue into Redis — ZADD NX so we don't overwrite
			// a valid existing entry (NX = only add if not exists)
			if err := w.redis.EnqueueIfAbsent(ctx, job.Queue, job.ID, job.Priority, job.RunAt); err != nil {
				w.log.Warn("postgres_sync.enqueue_failed",
					"job_id", job.ID,
					"error", err,
				)
				continue
			}
			synced++
		}

		if synced > 0 {
			w.log.Warn("postgres_sync.requeued",
				"queue", queue,
				"count", synced,
				"reason", "jobs found in postgres missing from redis",
			)
		}
	}
}

// executeJob runs a job that is ALREADY marked running in Postgres.
// Never call MarkRunning here — dequeueFromRedis already did it.
func (w *Worker) executeJob(ctx context.Context, job *domain.Job) {
	start := time.Now()

	// Give each job a reasonable execution timeout.
	jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Job-type rate limit — checked after we know the type
	result, err := w.rateLimiter.Allow(ctx, job.Queue, job.JobType)
	if err != nil {
		w.log.Warn("ratelimit.jobtype_error", "job_id", job.ID, "error", err)
	}
	if !result.Allowed {
		w.log.Info("ratelimit.jobtype_throttled",
			"job_id", job.ID,
			"job_type", job.JobType,
			"queue", job.Queue,
			"limit", result.JobTypeLimit,
		)
		// Put the job back — reschedule 1 second from now
		if err := w.db.RescheduleRetry(ctx, job.ID, time.Now().Add(1*time.Second)); err != nil {
			w.log.Error("ratelimit.reschedule_failed", "job_id", job.ID, "error", err)
		}
		_ = w.redis.RemoveInflight(ctx, job.Queue, job.ID)
		return
	}

	handler, err := w.registry.Get(job.JobType)
	if err != nil {
		// Unknown job type — fail immediately, no retry makes sense.
		w.log.Error("job.unknown_type", "job_id", job.ID, "job_type", job.JobType, "error", err)

		w.failJob(ctx, job, err, start)
		return
	}

	// Execute the handler.
	err = handler(jobCtx, job.Payload)
	duration := time.Since(start)

	if err == nil {
		w.completeJob(ctx, job, duration)
		return
	}

	// Exhausted all attempts → DLQ
	if job.AttemptCount >= job.MaxAttempts {
		w.log.Warn("job.exhausted",
			"job_id", job.ID,
			"job_type", job.JobType,
			"attempts", job.AttemptCount,
			"max_attempts", job.MaxAttempts,
			"error", err,
		)

		dlqErr := err.Error()
		if _, moveErr := w.db.MoveToDLQ(ctx, job, dlqErr); moveErr != nil {
			w.log.Error("job.dlq_failed",
				"job_id", job.ID,
				"error", moveErr,
			)
			// Fall back to just marking failed so it's not lost
			w.failJob(ctx, job, err, start)
		} else {
			w.log.Info("job.moved_to_dlq",
				"job_id", job.ID,
				"job_type", job.JobType,
				"queue", job.Queue,
			)
			w.recordAttempt(ctx, job, start, "failed", dlqErr)
		}
		return
	}

	// Schedule retry with exponential backoff.
	backoff := calcBackoff(job.AttemptCount)
	nextRun := time.Now().Add(backoff)

	w.log.Info("job.retry_scheduled",
		"job_id", job.ID,
		"job_type", job.JobType,
		"attempt", job.AttemptCount,
		"next_run_in", backoff,
		"error", err,
	)

	if retryErr := w.db.RescheduleRetry(ctx, job.ID, nextRun); retryErr != nil {
		w.log.Error("job.reschedule_failed", "job_id", job.ID, "error", retryErr)
	}

	// Re-enqueue into Redis with the future run_at — scheduler will promote it
	if err := w.redis.Enqueue(ctx, job.Queue, job.ID, job.Priority, nextRun); err != nil {
		w.log.Warn("retry.redis_enqueue_failed",
			"job_id", job.ID,
			"error", err,
		)
		// Postgres sync loop will catch it in 30s
	}

	_ = w.redis.RemoveInflight(ctx, job.Queue, job.ID)
	w.recordAttempt(ctx, job, start, "failed", err.Error())
}

func (w *Worker) completeJob(ctx context.Context, job *domain.Job, duration time.Duration) {
	if err := w.db.MarkCompleted(ctx, job.ID); err != nil {
		w.log.Error("job.mark_completed_failed", "job_id", job.ID, "error", err)
	}

	// Clean up Redis inflight entry
	if err := w.redis.RemoveInflight(ctx, job.Queue, job.ID); err != nil {
		w.log.Warn("redis.remove_inflight_failed", "job_id", job.ID, "error", err)
	}

	w.recordAttempt(ctx, job, time.Now().Add(-duration), "completed", "")

	w.log.Info("job.completed",
		"job_id", job.ID,
		"job_type", job.JobType,
		"queue", job.Queue,
		"duration_ms", duration.Milliseconds(),
	)
}

func (w *Worker) failJob(ctx context.Context, job *domain.Job, err error, start time.Time) {
	errMsg := err.Error()
	if dbErr := w.db.MarkFailed(ctx, job.ID, errMsg); dbErr != nil {
		w.log.Error("job.mark_failed_error", "job_id", job.ID, "error", dbErr)
	}

	// Clean up Redis inflight entry
	if removeErr := w.redis.RemoveInflight(ctx, job.Queue, job.ID); removeErr != nil {
		w.log.Warn("redis.remove_inflight_failed", "job_id", job.ID, "error", removeErr)
	}

	w.recordAttempt(ctx, job, start, "failed", errMsg)
}

func (w *Worker) recordAttempt(ctx context.Context, job *domain.Job, start time.Time, status, errMsg string) {
	now := time.Now()
	a := &domain.JobAttempt{
		JobID:      job.ID,
		AttemptNum: job.AttemptCount,
		WorkerID:   &w.id,
		StartedAt:  start,
		FinishedAt: &now,
		Status:     &status,
	}

	if errMsg != "" {
		a.Error = &errMsg
	}

	if err := w.db.RecordAttempt(ctx, a); err != nil {
		w.log.Error("attempt.record_failed", "job_id", job.ID, "error", err)
	}
}

// recovers stalled jobs from crashed workers
func (w *Worker) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.db.ReclaimStalledJobs(ctx, w.visibilityTimeout)
			if err != nil {
				w.log.Error("reaper.error", "error", err)
				continue
			}
			if n > 0 {
				w.log.Warn("reaper.reclaimed", "count", n, "visibility_timeout", w.visibilityTimeout)
			}
		}
	}
}

// calcBackoff returns exponential backoff: base=10s, doubles each attempt.
// attempt=1 → 20s, attempt=2 → 40s, attempt=3 → 80s, attempt=4 → 160s ...
// Capped at 1 hour to prevent extreme delays.
func calcBackoff(attempt int) time.Duration {
	base := 10 * time.Second
	delay := time.Duration(math.Pow(2, float64(attempt))) * base
	max := 1 * time.Hour
	if delay > max {
		return max
	}
	return delay
}
