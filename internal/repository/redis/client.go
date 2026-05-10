package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func New(addr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
		MinIdleConns: 5,
	})

	var err error
	for attempt := 1; attempt <= 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = rdb.Ping(ctx).Err()
		cancel()
		if err == nil {
			break
		}
		wait := time.Duration(attempt) * 2 * time.Second
		slog.Warn("redis.connect_retry",
			"attempt", attempt,
			"wait", wait,
			"error", err,
		)
		time.Sleep(wait)
	}
	if err != nil {
		return nil, fmt.Errorf("redis connect after retries: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

// Raw exposes the underlying client for use in queue/lock packages.
func (c *Client) Raw() *redis.Client {
	return c.rdb
}
