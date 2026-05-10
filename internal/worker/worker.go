package worker

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/metrics"
	"github.com/apoorv/distributed-job-processor/internal/ratelimiter"
	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
	redisrepo "github.com/apoorv/distributed-job-processor/internal/repository/redis"
)

// Worker polls one or more queues and executes jobs using a bounded goroutine pool.
//
// Architecture:
//   - N goroutines (= concurrency) each run an independent poll loop
//   - A semaphore (buffered channel) caps the number of concurrently executing jobs
//   - Graceful shutdown: stop accepting new jobs, drain in-flight jobs, then exit
type Worker struct {
	id          string
	queues      []string // polled left-to-right (put highest priority queue first)
	concurrency int
	db          *postgres.DB
	registry    *Registry
	log         *slog.Logger

	// Semaphore pattern: acquire before executing, release after.
	// Capacity = max concurrent in-flight jobs.
	semaphore chan struct{}

	// WaitGroup tracks in-flight job goroutines for graceful drain.
	wg sync.WaitGroup

	// visibilityTimeout is used by the reaper goroutine.
	visibilityTimeout time.Duration

	redis       *redisrepo.Client
	rateLimiter *ratelimiter.RateLimiter
	rateLimit   int // max jobs/sec per queue (0 = unlimited)
}

type Config struct {
	ID                string
	Queues            []string
	Concurrency       int
	VisibilityTimeout time.Duration
	RateLimit         int
}

func New(cfg Config, db *postgres.DB, redis *redisrepo.Client, registry *Registry, log *slog.Logger) *Worker {
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
		rateLimiter:       ratelimiter.New(redis.Raw()),
		rateLimit:         cfg.RateLimit,
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

// -----------------------------------------------------------------------
// Poll loop
// -----------------------------------------------------------------------

func (w *Worker) pollLoop(ctx context.Context, slot int) {
	w.log.Debug("poll_loop.started", "slot", slot)

	for {
		// Exit immediately when context is cancelled.
		select {
		case <-ctx.Done():
			return
		default:
		}

		job := w.dequeueFromAnyQueue(ctx)
		if job == nil {
			// All queues empty — back off before polling again.
			// This prevents hammering Postgres when idle.
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		// Acquire semaphore slot — blocks if we're at max concurrency.
		select {
		case w.semaphore <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// Track how many slots are in use
		metrics.WorkerConcurrencyUsed.WithLabelValues(w.id).Inc()

		// Run the job in a new goroutine so this poll loop can keep fetching.
		w.wg.Add(1)
		go func(j *domain.Job) {
			defer w.wg.Done()
			defer func() {
				<-w.semaphore
				// Release: decrement in-use gauge
				metrics.WorkerConcurrencyUsed.WithLabelValues(w.id).Dec()
			}()
			w.executeJob(context.Background(), j) // fresh ctx — don't cancel on shutdown
		}(job)
	}
}

// dequeueFromAnyQueue polls queues in priority order and returns the first job found.
// func (w *Worker) dequeueFromAnyQueue(ctx context.Context) *domain.Job {
// 	for _, queue := range w.queues {
// 		job, err := w.db.Dequeue(ctx, queue, w.id)
// 		if err != nil {
// 			w.log.Error("dequeue.error", "queue", queue, "error", err)
// 			continue
// 		}
// 		if job != nil {
// 			w.log.Info("job.dequeued", "job_id", job.ID, "job_type", job.JobType, "queue", queue, "attempt", job.AttemptCount)
// 			return job
// 		}
// 	}
// 	return nil
// }

// dequeueFromAnyQueue tries Redis first (fast path), falls back to Postgres.
// Redis gives us priority ordering and avoids table scans.
// Postgres fallback ensures no job is ever lost if Redis restarts.
func (w *Worker) dequeueFromAnyQueue(ctx context.Context) *domain.Job {
	for _, queue := range w.queues {
		// Rate limit check — skip this queue if over limit
		if w.rateLimit > 0 {
			allowed, err := w.rateLimiter.Allow(ctx, queue, w.rateLimit, time.Second)
			if err != nil {
				w.log.Warn("ratelimit.check_error", "queue", queue, "error", err)
			}
			if !allowed {
				w.log.Debug("ratelimit.throttled", "queue", queue)
				continue
			}
		}

		// ── Redis fast path ──────────────────────────────────────────────
		jobID, err := w.redis.Dequeue(ctx, queue)
		if err != nil {
			w.log.Error("redis.dequeue_error", "queue", queue, "error", err)
			// Fall through to Postgres
		}

		if jobID != "" {
			// Mark inflight in Redis for crash safety
			if err := w.redis.MarkInflight(ctx, queue, jobID, w.visibilityTimeout); err != nil {
				w.log.Warn("redis.inflight_mark_error", "job_id", jobID, "error", err)
			}

			// Fetch full job from Postgres and mark running atomically
			job, err := w.db.MarkRunning(ctx, jobID, w.id)
			if err != nil {
				w.log.Error("db.mark_running_failed",
					"job_id", jobID,
					"queue", queue,
					"error", err,
				)
				// Job was already popped from Redis — remove inflight entry
				_ = w.redis.RemoveInflight(ctx, queue, jobID)
				continue
			}

			w.log.Info("job.dequeued_redis",
				"job_id", job.ID,
				"job_type", job.JobType,
				"queue", queue,
				"attempt", job.AttemptCount,
			)
			return job
		}

		// ── Postgres fallback ────────────────────────────────────────────
		// Catches jobs that were written to Postgres but missed Redis
		// (e.g., Redis restarted, or job submitted before Redis was available)
		job, err := w.db.Dequeue(ctx, queue, w.id)
		if err != nil {
			w.log.Error("db.dequeue_error", "queue", queue, "error", err)
			continue
		}
		if job != nil {
			w.log.Info("job.dequeued_postgres_fallback",
				"job_id", job.ID,
				"job_type", job.JobType,
				"queue", queue,
			)
			return job
		}
	}
	return nil
}

// -----------------------------------------------------------------------
// Job execution
// -----------------------------------------------------------------------

func (w *Worker) executeJob(ctx context.Context, job *domain.Job) {
	start := time.Now()

	// Give each job a reasonable execution timeout.
	jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	handler, err := w.registry.Get(job.JobType)
	if err != nil {
		// Unknown job type — fail immediately, no retry makes sense.
		w.log.Error("job.unknown_type", "job_id", job.ID, "job_type", job.JobType, "error", err)

		w.failJob(ctx, job, err, start)
		// Count as a failed process so it shows up in metrics
		metrics.JobsProcessed.WithLabelValues(job.JobType, job.Queue, "failed").Inc()
		return
	}

	// Execute the handler.
	err = handler(jobCtx, job.Payload)
	duration := time.Since(start)

	if err == nil {
		w.completeJob(ctx, job, duration)
		return
	}

	// Handler returned an error — decide: retry or give up?
	if job.AttemptCount >= job.MaxAttempts {
		w.log.Warn("job.exhausted",
			"job_id", job.ID,
			"job_type", job.JobType,
			"attempts", job.AttemptCount,
			"max_attempts", job.MaxAttempts,
			"error", err,
		)

		// Phase 3 will move this to DLQ. For now, mark as dead.
		// w.failJob(ctx, job, fmt.Errorf("exhausted after %d attempts: %w", job.AttemptCount, err), start)

		dlqErr := err.Error()
		if _, moveErr := w.db.MoveToDLQ(ctx, job, dlqErr); moveErr != nil {
			w.log.Error("job.dlq_failed",
				"job_id", job.ID,
				"error", moveErr,
			)
			// Fall back to just marking failed so it's not lost
			w.failJob(ctx, job, err, start)
			metrics.JobsProcessed.WithLabelValues(job.JobType, job.Queue, "failed").Inc()
		} else {
			w.log.Info("job.moved_to_dlq",
				"job_id", job.ID,
				"job_type", job.JobType,
				"queue", job.Queue,
			)
			w.recordAttempt(ctx, job, start, "failed", dlqErr)

			// DLQ is a distinct outcome — track it separately
			metrics.JobsProcessed.WithLabelValues(job.JobType, job.Queue, "dlq").Inc()
			metrics.JobDuration.WithLabelValues(job.JobType, job.Queue).Observe(duration.Seconds())
			metrics.DLQTotal.WithLabelValues(job.Queue).Inc()
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

	w.recordAttempt(ctx, job, start, "failed", err.Error())

	// Count each retry so you can alert on retry storms
	metrics.RetryTotal.WithLabelValues(job.JobType, job.Queue).Inc()
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

	// Metrices
	metrics.JobsProcessed.WithLabelValues(job.JobType, job.Queue, "completed").Inc()
	metrics.JobDuration.WithLabelValues(job.JobType, job.Queue).Observe(duration.Seconds())

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

// -----------------------------------------------------------------------
// Reaper — recovers stalled jobs from crashed workers
// -----------------------------------------------------------------------

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

// -----------------------------------------------------------------------
// Backoff calculation
// -----------------------------------------------------------------------

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
