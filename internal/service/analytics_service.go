package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
)

type AnalyticsService struct {
	db     domain.AnalyticsRepository
	redis  domain.QueueBroker
	queues []string
	log    *slog.Logger
}

func NewAnalyticsService(db domain.AnalyticsRepository, redis domain.QueueBroker, queues []string, log *slog.Logger) *AnalyticsService {
	return &AnalyticsService{
		db:     db,
		redis:  redis,
		queues: queues,
		log:    log,
	}
}

type OverviewStats struct {
	Jobs   map[string]int        `json:"jobs"`
	Queues map[string]QueueState `json:"queues"`
	DLQ    map[string]int        `json:"dlq"`
}

type QueueState struct {
	Active    int64 `json:"active"`
	Scheduled int64 `json:"scheduled"`
	Inflight  int64 `json:"inflight"`
}

type QueueSummary struct {
	Name  string         `json:"name"`
	Jobs  map[string]int `json:"jobs"`
	Redis QueueState     `json:"redis"`
}

// Overview aggregates high-level stats from PostgreSQL and Redis.
func (s *AnalyticsService) Overview(ctx context.Context) (*OverviewStats, error) {
	jobs, err := s.db.OverviewStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("overview stats jobs: %w", err)
	}

	queueStates := make(map[string]QueueState)
	for _, q := range s.queues {
		active, _ := s.redis.QueueDepth(ctx, q)
		scheduled, _ := s.redis.ScheduledDepth(ctx, q)
		inflight, _ := s.redis.InflightDepth(ctx, q)

		queueStates[q] = QueueState{
			Active:    active,
			Scheduled: scheduled,
			Inflight:  inflight,
		}
	}

	dlqStats, err := s.db.DLQOverview(ctx)
	dlqOverview := map[string]int{"total": 0, "pending_replay": 0}
	if err == nil {
		for _, qStat := range dlqStats {
			dlqOverview["total"] += qStat.Total
			dlqOverview["pending_replay"] += qStat.PendingReplay
		}
	}

	return &OverviewStats{
		Jobs:   jobs,
		Queues: queueStates,
		DLQ:    dlqOverview,
	}, nil
}

// Throughput fetches timeseries data for a given range.
func (s *AnalyticsService) Throughput(ctx context.Context, interval, rangeStr string) (*domain.ThroughputData, error) {
	since := parseRange(rangeStr)
	return s.db.ThroughputTimeSeries(ctx, interval, since)
}

// Latency fetches percentile latencies for a queue over a time range.
func (s *AnalyticsService) Latency(ctx context.Context, queue, rangeStr string) (*domain.LatencyStats, error) {
	since := parseRange(rangeStr)
	return s.db.LatencyPercentiles(ctx, queue, since)
}

// Queues aggregates status statistics and queue depths per queue.
func (s *AnalyticsService) Queues(ctx context.Context) ([]*QueueSummary, error) {
	summary, err := s.db.QueuesSummary(ctx)
	if err != nil {
		return nil, fmt.Errorf("queues summary: %w", err)
	}

	var results []*QueueSummary
	for _, q := range s.queues {
		active, _ := s.redis.QueueDepth(ctx, q)
		scheduled, _ := s.redis.ScheduledDepth(ctx, q)
		inflight, _ := s.redis.InflightDepth(ctx, q)

		jobsBreakdown := summary[q]
		if jobsBreakdown == nil {
			jobsBreakdown = make(map[string]int)
		}

		results = append(results, &QueueSummary{
			Name: q,
			Jobs: jobsBreakdown,
			Redis: QueueState{
				Active:    active,
				Scheduled: scheduled,
				Inflight:  inflight,
			},
		})
	}

	return results, nil
}

// JobTypes aggregates job type stats.
func (s *AnalyticsService) JobTypes(ctx context.Context, rangeStr string) ([]*domain.JobTypeStat, error) {
	since := parseRange(rangeStr)
	return s.db.JobTypeDistribution(ctx, since)
}

// Workers compiles statistics for all active workers.
func (s *AnalyticsService) Workers(ctx context.Context, rangeStr string) ([]*domain.WorkerStat, error) {
	since := parseRange(rangeStr)
	return s.db.WorkerStats(ctx, since)
}

// Errors aggregates error frequencies.
func (s *AnalyticsService) Errors(ctx context.Context, rangeStr string, limit int) ([]*domain.ErrorStat, error) {
	since := parseRange(rangeStr)
	return s.db.TopErrors(ctx, since, limit)
}

// Retries aggregates job attempt frequency distribution.
func (s *AnalyticsService) Retries(ctx context.Context) ([]*domain.RetryStat, error) {
	return s.db.RetryDistribution(ctx)
}

// DLQ compiles dead-lettered job counts per queue.
func (s *AnalyticsService) DLQ(ctx context.Context) ([]*domain.DLQQueueStat, error) {
	return s.db.DLQOverview(ctx)
}

// RecentFailures retrieves the latest failed job runs.
func (s *AnalyticsService) RecentFailures(ctx context.Context, limit int) ([]*domain.Job, error) {
	return s.db.RecentFailures(ctx, limit)
}

func parseRange(rangeStr string) time.Time {
	now := time.Now()
	switch rangeStr {
	case "1h":
		return now.Add(-1 * time.Hour)
	case "6h":
		return now.Add(-6 * time.Hour)
	case "24h":
		return now.Add(-24 * time.Hour)
	case "7d":
		return now.Add(-7 * 24 * time.Hour)
	case "30d":
		return now.Add(-30 * 24 * time.Hour)
	default:
		return now.Add(-24 * time.Hour) // default to 24 hours
	}
}
