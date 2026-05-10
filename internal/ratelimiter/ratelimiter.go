package ratelimiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter implements a per-queue sliding window rate limit.
// The Lua script is atomic — INCR + EXPIRE in a single Redis round-trip.
//
// Key: ratelimit:{queue}:{window_bucket}
// Each bucket = 1 second. We keep 2 seconds of buckets for the sliding window.
type RateLimiter struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *RateLimiter {
	return &RateLimiter{rdb: rdb}
}

// luaAllow is the sliding window script.
// Returns 1 if the request is allowed, 0 if rate limited.
var luaAllow = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call('INCR', key)
if current == 1 then
	redis.call('EXPIRE', key, window)
end
if current > limit then
	return 0
end
return 1
`)

// Allow checks whether a job of the given type on the given queue
// can proceed without exceeding the rate limit.
//
// limit  = max jobs per window
// window = window size in seconds (e.g., 1 for per-second, 60 for per-minute)
func (rl *RateLimiter) Allow(ctx context.Context, queue string, limit int, window time.Duration) (bool, error) {
	// Bucket key includes the current time window so it auto-expires
	bucket := time.Now().Unix() / int64(window.Seconds())
	key := fmt.Sprintf("ratelimit:%s:%d", queue, bucket)

	result, err := luaAllow.Run(ctx, rl.rdb, []string{key}, limit, int(window.Seconds())).Int()
	if err != nil {
		// On script error, fail open — don't block jobs due to Redis issues
		return true, fmt.Errorf("ratelimit script: %w", err)
	}
	return result == 1, nil
}
