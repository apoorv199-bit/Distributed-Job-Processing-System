package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// SchedulerBroker defines the subset of broker operations needed by the scheduler.
type SchedulerBroker interface {
	AcquireLock(ctx context.Context, key, val string) (bool, error)
	ReleaseLock(ctx context.Context, key, val string) error
	RenewLock(ctx context.Context, key, val string) (bool, error)
	PromoteScheduled(ctx context.Context, queue string, priority int) (int, error)
	ReclaimExpiredInflight(ctx context.Context, queue string, priority int) (int, error)
}

// Scheduler is a singleton process responsible for two jobs:
//  1. Promoting scheduled jobs (run_at <= now) into the active queue
//  2. Reclaiming expired inflight jobs (worker crash recovery at Redis level)
//
// Only one scheduler should be active at a time — enforced via distributed lock.
// If the lock is held by another instance, this one waits and retries.
type Scheduler struct {
	id                string // unique ID for this instance (use hostname + PID)
	queues            []string
	redis             SchedulerBroker
	log               *slog.Logger
	pollInterval      time.Duration
	visibilityTimeout time.Duration
}

type Config struct {
	ID                string
	Queues            []string
	PollInterval      time.Duration
	VisibilityTimeout time.Duration
}

func New(cfg Config, redis SchedulerBroker, log *slog.Logger) *Scheduler {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = 10 * time.Minute
	}
	return &Scheduler{
		id:                cfg.ID,
		queues:            cfg.Queues,
		redis:             redis,
		log:               log,
		pollInterval:      cfg.PollInterval,
		visibilityTimeout: cfg.VisibilityTimeout,
	}
}

// Start runs the scheduler loop. Blocks until ctx is cancelled.
// Acquires a distributed lock before doing any work.
func (s *Scheduler) Start(ctx context.Context) {
	s.log.Info("scheduler.starting", "id", s.id, "queues", s.queues)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler.stopped")
			_ = s.redis.ReleaseLock(ctx, "scheduler", s.id)
			return
		default:
		}

		acquired, err := s.redis.AcquireLock(ctx, "scheduler", s.id)
		if err != nil {
			s.log.Error("scheduler.lock_error", "error", err)
			s.sleep(ctx, 5*time.Second)
			continue
		}

		if !acquired {
			// Another instance holds the lock — standby
			s.sleep(ctx, 5*time.Second)
			continue
		}

		s.log.Info("scheduler.lock_acquired", "id", s.id)
		s.runLoop(ctx)
	}
}

// runLoop is the main work loop, called only when the lock is held.
func (s *Scheduler) runLoop(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	renewTicker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer renewTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-renewTicker.C:
			// Keep the lock alive
			ok, err := s.redis.RenewLock(ctx, "scheduler", s.id)
			if err != nil || !ok {
				s.log.Warn("scheduler.lock_lost", "error", err)
				return // exit runLoop → outer Start() will re-acquire
			}
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick does one round of promotion + reclaim across all queues.
func (s *Scheduler) tick(ctx context.Context) {
	for _, queue := range s.queues {
		// 1. Promote scheduled jobs whose run_at has arrived
		promoted, err := s.redis.PromoteScheduled(ctx, queue, 5) // default priority=5
		if err != nil {
			s.log.Error("scheduler.promote_error", "queue", queue, "error", err)
		} else if promoted > 0 {
			s.log.Info("scheduler.promoted", "queue", queue, "count", promoted)
		}

		// 2. Reclaim inflight jobs from crashed workers
		reclaimed, err := s.redis.ReclaimExpiredInflight(ctx, queue, 5)
		if err != nil {
			s.log.Error("scheduler.reclaim_error", "queue", queue, "error", err)
		} else if reclaimed > 0 {
			s.log.Warn("scheduler.reclaimed", "queue", queue, "count", reclaimed)
		}
	}
}

func (s *Scheduler) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
