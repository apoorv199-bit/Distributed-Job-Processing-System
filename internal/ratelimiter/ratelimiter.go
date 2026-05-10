package ratelimiter

import (
	"context"
	"fmt"
	"time"

	"github.com/apoorv/distributed-job-processor/config"
	"github.com/redis/go-redis/v9"
)

// RateLimiter enforces per-queue AND per-job-type limits using a
// sliding window Lua script. Both checks must pass for a job to proceed.
//
// Key structure:
//
//	ratelimit:queue:{name}:{bucket}     — queue-level window
//	ratelimit:jobtype:{type}:{bucket}   — job-type-level window
//
// Each bucket = current unix second, so buckets auto-expire after 2s.
type RateLimiter struct {
	rdb *redis.Client
	cfg config.RateLimitConfig
}

func New(rdb *redis.Client, cfg config.RateLimitConfig) *RateLimiter {
	return &RateLimiter{rdb: rdb, cfg: cfg}
}

// luaAllow is a sliding window counter.
// Returns 1 = allowed, 0 = rate limited.
// Uses INCR + EXPIRE in a single atomic script — no race between them.
var luaAllow = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

-- limit=0 means unlimited — skip the check
	if limit == 0 then
		return 1
	end

local current = redis.call('INCR', key)
if current == 1 then
	redis.call('EXPIRE', key, window)
end
if current > limit then
	return 0
end
return 1
`)

// CheckResult carries the outcome of a rate limit check.
type CheckResult struct {
	Allowed      bool
	LimitedBy    string // "queue" | "job_type" | ""
	Queue        string
	JobType      string
	QueueLimit   int
	JobTypeLimit int
}

// Allow checks both the queue-level and job-type-level rate limits.
// Returns false if either limit is exceeded, along with which one triggered.
//
// Fail-open: if Redis is unavailable, Allow returns true so jobs
// aren't blocked by infrastructure issues.
func (rl *RateLimiter) Allow(ctx context.Context, queue, jobType string) (CheckResult, error) {
	result := CheckResult{
		Allowed: true,
		Queue:   queue,
		JobType: jobType,
	}

	queueLimit := rl.queueLimit(queue)
	jobTypeLimit := rl.jobTypeLimit(jobType)

	result.QueueLimit = queueLimit
	result.JobTypeLimit = jobTypeLimit

	// ── Check queue-level limit ───────────────────────────────────────────
	queueAllowed, err := rl.check(ctx, queueKey(queue), queueLimit)
	if err != nil {
		// Fail open — Redis error should not block job processing
		return result, fmt.Errorf("queue rate limit check: %w", err)
	}
	if !queueAllowed {
		result.Allowed = false
		result.LimitedBy = "queue"
		return result, nil
	}

	// ── Check job-type-level limit ────────────────────────────────────────
	if jobTypeLimit > 0 {
		jobTypeAllowed, err := rl.check(ctx, jobTypeKey(jobType), jobTypeLimit)
		if err != nil {
			return result, fmt.Errorf("job_type rate limit check: %w", err)
		}
		if !jobTypeAllowed {
			result.Allowed = false
			result.LimitedBy = "job_type"
			return result, nil
		}
	}

	return result, nil
}

// check runs the Lua script for a single key+limit pair.
func (rl *RateLimiter) check(ctx context.Context, key string, limit int) (bool, error) {
	if limit == 0 {
		return true, nil // 0 = unlimited
	}

	bucket := time.Now().Unix() // 1-second window bucket
	fullKey := fmt.Sprintf("%s:%d", key, bucket)

	val, err := luaAllow.Run(ctx, rl.rdb, []string{fullKey}, limit, 2).Int()
	if err != nil {
		// Fail open
		return true, err
	}
	return val == 1, nil
}

// ── Limit resolution — specific config wins over default ─────────────────────

func (rl *RateLimiter) queueLimit(queue string) int {
	if limit, ok := rl.cfg.Queues[queue]; ok {
		return limit
	}
	return rl.cfg.Default
}

func (rl *RateLimiter) jobTypeLimit(jobType string) int {
	if limit, ok := rl.cfg.JobTypes[jobType]; ok {
		return limit
	}
	return 0 // 0 = no per-type limit unless explicitly configured
}

// ── Key builders ──────────────────────────────────────────────────────────────

func queueKey(queue string) string     { return "ratelimit:queue:" + queue }
func jobTypeKey(jobType string) string { return "ratelimit:jobtype:" + jobType }
