package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port        string
	DatabaseURL string
	RedisURL    string

	WorkerID          string
	WorkerConcurrency int
	WorkerQueues      []string // ordered: workers poll left to right
	WorkerRateLimit   int

	// Visibility timeout: jobs stuck in 'running' longer than this get reset
	VisibilityTimeoutMinutes     int
	SchedulerPollIntervalSeconds int
	APIKey                       string
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:                         getEnv("PORT", "8085"),
		DatabaseURL:                  getEnv("DATABASE_URL", "postgres://app:secret@localhost:5432/jobprocessor?sslmode=disable"),
		RedisURL:                     getEnv("REDIS_URL", "localhost:6379"),
		WorkerID:                     getEnv("WORKER_ID", hostname()),
		WorkerConcurrency:            getEnvInt("WORKER_CONCURRENCY", 10),
		WorkerQueues:                 []string{"critical", "default", "batch"},
		WorkerRateLimit:              getEnvInt("WORKER_RATE_LIMIT", 0),
		VisibilityTimeoutMinutes:     getEnvInt("VISIBILITY_TIMEOUT_MINUTES", 10),
		SchedulerPollIntervalSeconds: getEnvInt("SCHEDULER_POLL_INTERVAL_SECONDS", 1),
		APIKey:                       getEnv("API_KEY", "apoorvsahu123456"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		return "worker-unknown"
	}
	return h
}
