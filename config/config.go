package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type RateLimitConfig struct {
	Queues   map[string]int `mapstructure:"QUEUES"`
	JobTypes map[string]int `mapstructure:"JOB_TYPES"`
	Default  int            `mapstructure:"DEFAULT"` // Global fallback if no specific config matches
}

type Config struct {
	Port              string   `mapstructure:"PORT"`
	DatabaseURL       string   `mapstructure:"DATABASE_URL"`
	RedisURL          string   `mapstructure:"REDIS_URL"`
	APIKey            string   `mapstructure:"API_KEY"`
	WorkerID          string   `mapstructure:"WORKER_ID"`
	WorkerConcurrency int      `mapstructure:"WORKER_CONCURRENCY"`
	WorkerQueues      []string `mapstructure:"WORKER_QUEUES"`

	// Visibility timeout: jobs stuck in 'running' longer than this get reset
	VisibilityTimeoutMinutes     int             `mapstructure:"VISIBILITY_TIMEOUT_MINUTES"`
	SchedulerPollIntervalSeconds int             `mapstructure:"SCHEDULER_POLL_INTERVAL_SECONDS"`
	RateLimit                    RateLimitConfig `mapstructure:"RATE_LIMIT"`
}

func Load() (*Config, error) {
	v := viper.New()

	setDefaults(v)

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.AutomaticEnv()

	v.SetConfigName("config")
	v.SetConfigType("yaml")

	v.AddConfigPath("./config")
	v.AddConfigPath("../config")
	v.AddConfigPath("../../config")

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError

		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	if cfg.WorkerID == "" {
		cfg.WorkerID = hostname()
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func setDefaults(v *viper.Viper) {

	v.SetDefault("PORT", "8085")

	v.SetDefault("WORKER_CONCURRENCY", 10)

	v.SetDefault("WORKER_QUEUES",
		[]string{"critical", "default", "batch"},
	)

	v.SetDefault("VISIBILITY_TIMEOUT_MINUTES", 10)

	v.SetDefault("SCHEDULER_POLL_INTERVAL_SECONDS", 1)

	v.SetDefault("RATE_LIMIT.DEFAULT", 100)

	v.SetDefault("RATE_LIMIT.QUEUES", map[string]int{
		"critical": 200,
		"default":  100,
		"batch":    20,
	})

	v.SetDefault("RATE_LIMIT.JOB_TYPES", map[string]int{
		"generate_report": 5,
		"send_email":      50,
	})
}

func validate(cfg *Config) error {
	if cfg.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	if cfg.RedisURL == "" {
		return errors.New("REDIS_URL is required")
	}

	if cfg.APIKey == "" {
		return errors.New("API_KEY is required")
	}

	return nil
}

func hostname() string {
	h, err := os.Hostname()

	if err != nil || h == "" {
		return "worker-unknown"
	}

	return h
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")

	result := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)

		if p != "" {
			result = append(result, p)
		}
	}

	return result
}
