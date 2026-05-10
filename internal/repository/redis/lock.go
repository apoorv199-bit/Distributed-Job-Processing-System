package redis

import (
	"context"
	"time"
)

const lockTTL = 30 * time.Second

// AcquireLock tries to acquire a named distributed lock.
// Returns true if acquired, false if another process holds it.
// Uses SET NX EX — atomic in Redis, no Lua needed.
func (c *Client) AcquireLock(ctx context.Context, key, ownerID string) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, "lock:"+key, ownerID, lockTTL).Result()
	return ok, err
}

// RenewLock extends the TTL of a held lock.
// Call every ~10s while you hold the lock.
// Returns false if the lock was lost (e.g., network partition caused expiry).
func (c *Client) RenewLock(ctx context.Context, key, ownerID string) (bool, error) {
	// Only renew if we still own it
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("EXPIRE", KEYS[1], ARGV[2])
		end
		return 0`
	result, err := c.rdb.Eval(ctx, script,
		[]string{"lock:" + key},
		ownerID,
		int(lockTTL.Seconds()),
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// ReleaseLock releases a held lock only if we own it.
func (c *Client) ReleaseLock(ctx context.Context, key, ownerID string) error {
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0`
	return c.rdb.Eval(ctx, script, []string{"lock:" + key}, ownerID).Err()
}
