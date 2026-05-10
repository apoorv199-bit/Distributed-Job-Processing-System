package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	JobsEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_enqueued_total",
			Help: "Total jobs submitted by type and queue",
		},
		[]string{"job_type", "queue"},
	)

	JobsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Total jobs completed, failed, or moved to DLQ",
		},
		[]string{"job_type", "queue", "status"}, // status: completed|failed|dlq
	)

	JobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "job_duration_seconds",
			Help:    "Job execution time in seconds",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"job_type", "queue"},
	)

	WorkerConcurrencyUsed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_concurrency_used",
			Help: "Current number of in-flight jobs per worker",
		},
		[]string{"worker_id"},
	)

	DLQTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dlq_jobs_total",
			Help: "Total jobs currently in DLQ per queue",
		},
		[]string{"queue"},
	)

	RetryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_retried_total",
			Help: "Total retry reschedules",
		},
		[]string{"job_type", "queue"},
	)

	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_queue_depth",
			Help: "Current number of jobs in each Redis queue",
		},
		[]string{"queue", "type"}, // type: active | scheduled | inflight
	)
)

func Register() {
	prometheus.MustRegister(
		JobsEnqueued,
		JobsProcessed,
		JobDuration,
		WorkerConcurrencyUsed,
		DLQTotal,
		RetryTotal,
		QueueDepth,
	)
}
