package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apoorv/distributed-job-processor/config"
	apihandler "github.com/apoorv/distributed-job-processor/internal/api/handler"
	customMiddleware "github.com/apoorv/distributed-job-processor/internal/api/middleware"
	"github.com/apoorv/distributed-job-processor/internal/metrics"
	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
	redisrepo "github.com/apoorv/distributed-job-processor/internal/repository/redis"
	"github.com/apoorv/distributed-job-processor/internal/scheduler"
	"github.com/apoorv/distributed-job-processor/internal/service"
	"github.com/apoorv/distributed-job-processor/internal/worker"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	// ── Logger ────────────────────────────────────────────────────────────
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(log)

	// ── Load ENV ────────────────────────────────────────────────────────────
	err := godotenv.Load()
	if err != nil {
		log.Warn("env.load_failed", "error", err)
	}

	// Register Prometheus metrics at startup
	metrics.Register()

	// ── Config ────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Error("config.load_failed", "error", err)
		os.Exit(1)
	}

	// ── Database ──────────────────────────────────────────────────────────
	ctx := context.Background()
	db, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("db.connect_failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Run migrations (idempotent — safe to run on every startup)
	if err := db.RunMigrations(ctx); err != nil {
		log.Error("db.migration_failed", "error", err)
		os.Exit(1)
	}
	log.Info("db.migrations_ok")

	// Redis connection
	redisClient, err := redisrepo.New(cfg.RedisURL)
	if err != nil {
		log.Error("redis.connect_failed", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()
	log.Info("redis.connected")

	// ── Services ──────────────────────────────────────────────────────────
	jobSvc := service.NewJobService(db, redisClient, log)
	dlqSvc := service.NewDLQService(db, log)

	// ── Worker ────────────────────────────────────────────────────────────
	registry := worker.NewRegistry()
	registerHandlers(registry, log) // register all job type handlers

	w := worker.New(worker.Config{
		ID:                cfg.WorkerID,
		Queues:            cfg.WorkerQueues,
		Concurrency:       cfg.WorkerConcurrency,
		VisibilityTimeout: time.Duration(cfg.VisibilityTimeoutMinutes) * time.Minute,
		RateLimit:         cfg.WorkerRateLimit,
	}, db, redisClient, registry, log)

	sched := scheduler.New(scheduler.Config{
		ID:                cfg.WorkerID + "-scheduler",
		Queues:            cfg.WorkerQueues,
		PollInterval:      time.Duration(cfg.SchedulerPollIntervalSeconds) * time.Second,
		VisibilityTimeout: time.Duration(cfg.VisibilityTimeoutMinutes) * time.Minute,
	}, redisClient, log)

	// Context for the worker — cancelled on shutdown signal
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	go sched.Start(ctx)
	go w.Start(workerCtx)

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				for _, queue := range cfg.WorkerQueues {
					if n, err := redisClient.QueueDepth(ctx, queue); err == nil {
						metrics.QueueDepth.WithLabelValues(queue, "active").Set(float64(n))
					}
					if n, err := redisClient.ScheduledDepth(ctx, queue); err == nil {
						metrics.QueueDepth.WithLabelValues(queue, "scheduled").Set(float64(n))
					}
					if n, err := redisClient.InflightDepth(ctx, queue); err == nil {
						metrics.QueueDepth.WithLabelValues(queue, "inflight").Set(float64(n))
					}
				}
			}
		}
	}()

	// ── HTTP Router ───────────────────────────────────────────────────────
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(customMiddleware.RequestLogger(log))

	jobH := apihandler.NewJobHandler(jobSvc, log)
	dlqH := apihandler.NewDLQHandler(dlqSvc, log)

	r.Route("/api/v1", func(r chi.Router) {
		// Jobs endpoints
		r.Post("/jobs", jobH.Submit)
		r.Get("/jobs", jobH.List)
		r.Get("/jobs/{id}", jobH.GetByID)
		r.Get("/jobs/{id}/attempts", jobH.GetAttempts)
		r.Get("/queues/{name}/stats", jobH.QueueStats)

		// DLQ endpoints
		r.Get("/dlq", dlqH.List)
		r.Get("/dlq/stats", dlqH.Stats)
		r.Get("/dlq/{id}", dlqH.GetByID)
		r.Post("/dlq/{id}/replay", dlqH.Replay)
		r.Post("/dlq/bulk-replay", dlqH.BulkReplay)
		r.Post("/dlq/purge", dlqH.Purge)
		r.Delete("/dlq/{id}", dlqH.Delete)
	})

	// Observability + Health
	r.Handle("/metrics", promhttp.Handler())

	// Health endpoints (used by load balancers / k8s probes)
	r.Get("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		hCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(hCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "db unavailable")
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// ── HTTP Server ───────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── Graceful Shutdown ─────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Info("server.starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server.error", "error", err)
			os.Exit(1)
		}
	}()

	sig := <-sigCh
	log.Info("shutdown.signal_received", "signal", sig)

	// Stop HTTP server — stop accepting new requests
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("server.shutdown_error", "error", err)
	}

	// Stop worker — drain in-flight jobs
	cancelWorker()

	log.Info("shutdown.complete")
}

// -----------------------------------------------------------------------
// Handler registration — add your job types here
// -----------------------------------------------------------------------

func registerHandlers(r *worker.Registry, log *slog.Logger) {
	// send_email: sends a transactional email
	r.Register("send_email", func(ctx context.Context, payload json.RawMessage) error {
		var p struct {
			To       string `json:"to"`
			Template string `json:"template"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
		// TODO: integrate real email client (SendGrid, SES, etc.)
		log.Info("send_email.executed",
			"to", p.To,
			"template", p.Template,
		)
		return nil
	})

	// generate_report: long-running report generation
	r.Register("generate_report", func(ctx context.Context, payload json.RawMessage) error {
		var p struct {
			ReportID string `json:"report_id"`
			Format   string `json:"format"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
		log.Info("generate_report.executed",
			"report_id", p.ReportID,
			"format", p.Format,
		)
		// Simulate work — replace with real report logic
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	// noop: useful for testing throughput
	r.Register("noop", func(ctx context.Context, payload json.RawMessage) error {
		return nil
	})

	// fail_always: useful for testing retry/DLQ behaviour
	r.Register("fail_always", func(ctx context.Context, payload json.RawMessage) error {
		return fmt.Errorf("this handler always fails (test job)")
	})

	log.Info("handlers.registered", "types", r.Registered())
}
