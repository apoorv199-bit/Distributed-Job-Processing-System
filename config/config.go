package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type RateLimitConfig struct {
	// Per-queue limits: max jobs/sec for the entire queue
	// env: RATE_LIMIT_QUEUES={"critical":200,"default":100,"batch":20}
	Queues map[string]int `json:"queues"`

	// Per-job-type limits: max jobs/sec regardless of which queue
	// env: RATE_LIMIT_JOB_TYPES={"send_email":50,"generate_report":5}
	JobTypes map[string]int `json:"job_types"`

	// Global fallback if no specific config matches
	// env: RATE_LIMIT_DEFAULT=100
	Default int `json:"default"`
}

type Config struct {
	Port        string
	DatabaseURL string
	RedisURL    string
	APIKey      string

	WorkerID          string
	WorkerConcurrency int
	WorkerQueues      []string // ordered: workers poll left to right

	// Visibility timeout: jobs stuck in 'running' longer than this get reset
	VisibilityTimeoutMinutes     int
	SchedulerPollIntervalSeconds int

	RateLimit RateLimitConfig
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:                         getEnv("PORT", "8085"),
		DatabaseURL:                  getEnv("DATABASE_URL", "postgres://app:secret@localhost:5432/jobprocessor?sslmode=disable"),
		RedisURL:                     getEnv("REDIS_URL", "localhost:6379"),
		APIKey:                       getEnv("API_KEY", "apoorvsahu123456"),
		WorkerID:                     getEnv("WORKER_ID", hostname()),
		WorkerConcurrency:            getEnvInt("WORKER_CONCURRENCY", 10),
		WorkerQueues:                 []string{"critical", "default", "batch"},
		VisibilityTimeoutMinutes:     getEnvInt("VISIBILITY_TIMEOUT_MINUTES", 10),
		SchedulerPollIntervalSeconds: getEnvInt("SCHEDULER_POLL_INTERVAL_SECONDS", 1),
		RateLimit: RateLimitConfig{
			Queues:   parseJSONMap("RATE_LIMIT_QUEUES", map[string]int{"critical": 200, "default": 100, "batch": 20}),
			JobTypes: parseJSONMap("RATE_LIMIT_JOB_TYPES", map[string]int{"generate_report": 5}),
			Default:  getEnvInt("RATE_LIMIT_DEFAULT", 100),
		},
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

// parseJSONMap reads a JSON object from an env var.
// e.g. RATE_LIMIT_QUEUES={"critical":200,"default":100}
func parseJSONMap(key string, fallback map[string]int) map[string]int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	result := make(map[string]int)
	if err := json.Unmarshal([]byte(v), &result); err != nil {
		fmt.Printf("warn: failed to parse %s as JSON map, using defaults: %v\n", key, err)
		return fallback
	}
	return result
}
