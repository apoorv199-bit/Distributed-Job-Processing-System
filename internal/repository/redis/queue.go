package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Key conventions — all in one place so nothing drifts.
//
//	queue:{name}          → sorted set, active jobs ready to run
//	scheduled:{name}      → sorted set, future jobs (score = unix run_at)
//	inflight:{name}       → sorted set, claimed jobs (score = visibility deadline)
func queueKey(name string) string {
	return "queue:" + name
}
func scheduledKey(name string) string {
	return "scheduled:" + name
}
func inflightKey(name string) string {
	return "inflight:" + name
}

// Score encodes priority + time into a single float64 for the sorted set.
//
// Formula: priority * 1e12 + unix_nano
//
// This means:
//   - priority=1 (highest) gets score ~1e12, priority=10 gets ~10e12
//   - Within the same priority, earlier run_at wins (lower unix_nano)
//   - ZPOPMIN always returns the job that should run next
func Score(priority int, runAt time.Time) float64 {
	return float64(priority)*1e12 + float64(runAt.UnixNano())
}

// Enqueue adds a job to the active queue with priority+time scoring.
// If runAt is in the future, it goes to the scheduled set instead.
func (c *Client) Enqueue(ctx context.Context, queue, jobID string, priority int, runAt time.Time) error {
	if runAt.After(time.Now()) {
		// Future job → scheduled set, score = unix timestamp of run_at
		return c.rdb.ZAdd(ctx, scheduledKey(queue), redis.Z{
			Score:  float64(runAt.UnixNano()),
			Member: jobID,
		}).Err()
	}

	// Immediate job → active queue
	return c.rdb.ZAdd(ctx, queueKey(queue), redis.Z{
		Score:  Score(priority, runAt),
		Member: jobID,
	}).Err()
}

// Dequeue atomically pops the highest-priority job from the queue.
// Returns ("", nil) when the queue is empty — not an error.
func (c *Client) Dequeue(ctx context.Context, queue string) (jobID string, err error) {
	res, err := c.rdb.ZPopMin(ctx, queueKey(queue), 1).Result()
	if err != nil {
		return "", fmt.Errorf("zpopmin %s: %w", queue, err)
	}

	if len(res) == 0 {
		return "", nil
	}

	return res[0].Member.(string), nil
}

// MarkInflight records a claimed job with a visibility deadline.
// If the worker doesn't call RemoveInflight before the deadline,
// the reaper will return it to the queue.
func (c *Client) MarkInflight(ctx context.Context, queue, jobID string, visibilityTimeout time.Duration) error {
	deadline := time.Now().Add(visibilityTimeout).Unix()
	return c.rdb.ZAdd(ctx, inflightKey(queue), redis.Z{
		Score:  float64(deadline),
		Member: jobID,
	}).Err()
}

// RemoveInflight removes a job from the in-flight set after completion/failure.
func (c *Client) RemoveInflight(ctx context.Context, queue, jobID string) error {
	return c.rdb.ZRem(ctx, inflightKey(queue), jobID).Err()
}

// ReclaimExpiredInflight finds jobs whose visibility deadline has passed
// and moves them back to the active queue. Called by the reaper.
// Returns the number of jobs reclaimed.
func (c *Client) ReclaimExpiredInflight(ctx context.Context, queue string, priority int) (int, error) {
	now := float64(time.Now().Unix())

	// Find all jobs past their deadline
	expired, err := c.rdb.ZRangeByScore(ctx, inflightKey(queue), &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("range inflight: %w", err)
	}
	if len(expired) == 0 {
		return 0, nil
	}

	// Use a pipeline: remove from inflight, re-add to queue atomically
	pipe := c.rdb.Pipeline()
	for _, jobID := range expired {
		pipe.ZRem(ctx, inflightKey(queue), jobID)
		pipe.ZAdd(ctx, queueKey(queue), redis.Z{
			Score: Score(priority, time.Now()),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("reclaim pipeline: %w", err)
	}

	return len(expired), nil
}

// PromoteScheduled moves jobs from the scheduled set into the active queue
// when their run_at time has arrived. Called every second by the scheduler.
// Returns the number of jobs promoted.
func (c *Client) PromoteScheduled(ctx context.Context, queue string, priority int) (int, error) {
	now := float64(time.Now().Unix())

	// Find all scheduled jobs whose run_at has passed
	ready, err := c.rdb.ZRangeByScore(ctx, scheduledKey(queue), &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("range scheduled: %w", err)
	}
	if len(ready) == 0 {
		return 0, nil
	}

	pipe := c.rdb.Pipeline()
	for _, jobID := range ready {
		pipe.ZRem(ctx, scheduledKey(queue), jobID)
		pipe.ZAdd(ctx, queueKey(queue), redis.Z{
			Score:  Score(priority, time.Now()),
			Member: jobID,
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("promote pipeline: %w", err)
	}

	return len(ready), nil
}

// EnqueueIfAbsent adds a job to the Redis queue only if it isn't already there.
// Uses ZADD NX — no-op if the member already exists.
// Called by the Postgres sync loop to recover missed enqueues without
// creating duplicates.
func (c *Client) EnqueueIfAbsent(ctx context.Context, queue, jobID string, priority int, runAt time.Time) error {
	if runAt.After(time.Now()) {
		return c.rdb.ZAddNX(ctx, scheduledKey(queue), redis.Z{
			Score:  float64(runAt.Unix()),
			Member: jobID,
		}).Err()
	}
	return c.rdb.ZAddNX(ctx, queueKey(queue), redis.Z{
		Score:  Score(priority, runAt),
		Member: jobID,
	}).Err()
}

func (c *Client) QueueDepth(ctx context.Context, queue string) (int64, error) {
	return c.rdb.ZCard(ctx, queueKey(queue)).Result()
}

func (c *Client) ScheduledDepth(ctx context.Context, queue string) (int64, error) {
	return c.rdb.ZCard(ctx, scheduledKey(queue)).Result()
}

func (c *Client) InflightDepth(ctx context.Context, queue string) (int64, error) {
	return c.rdb.ZCard(ctx, inflightKey(queue)).Result()
}
