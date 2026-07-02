package domain

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// JobRepository defines the abstract data mutations and query capabilities for Jobs.
type JobRepository interface {
	InsertJob(ctx context.Context, clientID string, req *SubmitRequest) (*Job, error)
	GetByID(ctx context.Context, clientID, id string) (*Job, error)
	List(ctx context.Context, clientID string, filter ListFilter) (*ListResult, error)
	GetAttempts(ctx context.Context, jobID string) ([]*JobAttempt, error)
	QueueStats(ctx context.Context, queue string) (map[string]int, error)
	MarkRunning(ctx context.Context, jobID string, workerID string, visibilityTimeout time.Duration) (*Job, error)
	MarkCompleted(ctx context.Context, jobID string) error
	MarkFailed(ctx context.Context, jobID, errorMsg string) error
	RescheduleRetry(ctx context.Context, jobID string, runAt time.Time) error
	ListPendingForSync(ctx context.Context, queue string, limit int) ([]*Job, error)
	ReclaimStalledJobs(ctx context.Context, visibilityTimeout time.Duration) (int64, error)
	RecordAttempt(ctx context.Context, a *JobAttempt) error
	GetAPIKeyByClientID(ctx context.Context, clientID string) (string, string, error)
	CreateClientAPIKey(ctx context.Context, clientID, prefix, hashedKey string) error
}

// DLQRepository defines the data store contracts for dead-letter job administration.
type DLQRepository interface {
	ListDLQ(ctx context.Context, clientID string, filter DLQListFilter) (*DLQListResult, error)
	GetDLQByID(ctx context.Context, clientID, dlqID string) (*DeadLetterJob, error)
	ReplayDLQJob(ctx context.Context, clientID, dlqID string) (*Job, error)
	DeleteDLQJob(ctx context.Context, clientID, dlqID string) error
	DLQStats(ctx context.Context, clientID, queue string) (map[string]int, error)
	BulkReplayDLQ(ctx context.Context, clientID, queue string) ([]*Job, error)
	PurgeDLQOlderThan(ctx context.Context, clientID string, olderThan time.Duration) (int64, error)
}

// QueueBroker defines the message broker contracts for task enqueuing and popping.
type QueueBroker interface {
	Enqueue(ctx context.Context, queue, jobID string, priority int, runAt time.Time) error
	EnqueueIfAbsent(ctx context.Context, queue, jobID string, priority int, runAt time.Time) error
	Dequeue(ctx context.Context, queue string) (string, error)
	MarkInflight(ctx context.Context, queue, jobID string, visibilityTimeout time.Duration) error
	RemoveInflight(ctx context.Context, queue, jobID string) error
	QueueDepth(ctx context.Context, queue string) (int64, error)
	ScheduledDepth(ctx context.Context, queue string) (int64, error)
	InflightDepth(ctx context.Context, queue string) (int64, error)
	Raw() *redis.Client
}

// AnalyticsRepository defines aggregation queries to drive analytics telemetry.
type AnalyticsRepository interface {
	OverviewStats(ctx context.Context) (map[string]int, error)
	ThroughputTimeSeries(ctx context.Context, interval string, since time.Time) (*ThroughputData, error)
	LatencyPercentiles(ctx context.Context, queue string, since time.Time) (*LatencyStats, error)
	JobTypeDistribution(ctx context.Context, since time.Time) ([]*JobTypeStat, error)
	WorkerStats(ctx context.Context, since time.Time) ([]*WorkerStat, error)
	TopErrors(ctx context.Context, since time.Time, limit int) ([]*ErrorStat, error)
	RetryDistribution(ctx context.Context) ([]*RetryStat, error)
	QueuesSummary(ctx context.Context) (map[string]map[string]int, error)
	DLQOverview(ctx context.Context) ([]*DLQQueueStat, error)
	RecentFailures(ctx context.Context, limit int) ([]*Job, error)
}
