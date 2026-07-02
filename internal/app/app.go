package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/apoorv/distributed-job-processor/config"
	"github.com/apoorv/distributed-job-processor/internal/api"
	apihandler "github.com/apoorv/distributed-job-processor/internal/api/handler"
	customMiddleware "github.com/apoorv/distributed-job-processor/internal/api/middleware"
	"github.com/apoorv/distributed-job-processor/internal/repository/postgres"
	redisrepo "github.com/apoorv/distributed-job-processor/internal/repository/redis"
	"github.com/apoorv/distributed-job-processor/internal/scheduler"
	"github.com/apoorv/distributed-job-processor/internal/service"
	"github.com/apoorv/distributed-job-processor/internal/worker"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// App manages the lifecycle of the distributed job processing application.
type App struct {
	cfg         *config.Config
	log         *slog.Logger
	db          *postgres.DB
	redisClient *redisrepo.Client
	srv         *http.Server
	sched       *scheduler.Scheduler
	w           *worker.Worker
	cancelFunc  context.CancelFunc
}

// New initializes all infrastructure, drivers, services, routing, and workers.
func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	ctx := context.Background()

	// 1. Database Connection
	db, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("db connection: %w", err)
	}

	// 2. Run database migrations
	if err := db.RunMigrations(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("db migration: %w", err)
	}
	log.Info("db.migrations_ok")

	// 3. Seed API Keys
	if err := db.SeedAPIKeys(ctx, cfg.APIKeys); err != nil {
		log.Error("db.seed_api_keys_failed", "error", err)
	} else {
		log.Info("db.seed_api_keys_ok")
	}

	// 4. Redis connection
	redisClient, err := redisrepo.New(cfg.RedisURL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("redis connection: %w", err)
	}
	log.Info("redis.connected")

	// 5. Initialize Services
	jobSvc := service.NewJobService(db, redisClient, log)
	dlqSvc := service.NewDLQService(db, redisClient, log)
	analyticsSvc := service.NewAnalyticsService(db, redisClient, cfg.WorkerQueues, log)

	// 6. Worker registry & handlers registration
	registry := worker.NewRegistry()
	worker.RegisterDefaultHandlers(registry, log)

	// 7. Initialize Worker
	w := worker.New(worker.Config{
		ID:                cfg.WorkerID,
		Queues:            cfg.WorkerQueues,
		Concurrency:       cfg.WorkerConcurrency,
		VisibilityTimeout: time.Duration(cfg.VisibilityTimeoutMinutes) * time.Minute,
		RateLimitConfig:   cfg.RateLimit,
	}, db, redisClient, registry, log)

	// 8. Initialize Scheduler
	sched := scheduler.New(scheduler.Config{
		ID:                cfg.WorkerID + "-scheduler",
		Queues:            cfg.WorkerQueues,
		PollInterval:      time.Duration(cfg.SchedulerPollIntervalSeconds) * time.Second,
		VisibilityTimeout: time.Duration(cfg.VisibilityTimeoutMinutes) * time.Minute,
	}, redisClient, log)

	// 9. HTTP routing & middlewares setup
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(customMiddleware.PanicRecovery(log))
	r.Use(customMiddleware.CORS())
	r.Use(customMiddleware.APIKeyAuth(db, redisClient, cfg.MasterKey, log))
	r.Use(customMiddleware.RequestLogger(log))

	jobH := apihandler.NewJobHandler(jobSvc, log)
	dlqH := apihandler.NewDLQHandler(dlqSvc, log)
	adminH := apihandler.NewAdminHandler(db, cfg.MasterKey, log)
	analyticsH := apihandler.NewAnalyticsHandler(analyticsSvc, log)

	api.RegisterRoutes(r, jobH, dlqH, adminH, analyticsH)

	r.Get("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		hCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(hCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "database unavailable")
			return
		}
		if err := redisClient.Raw().Ping(hCtx).Err(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "redis unavailable")
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	return &App{
		cfg:         cfg,
		log:         log,
		db:          db,
		redisClient: redisClient,
		srv:         srv,
		sched:       sched,
		w:           w,
	}, nil
}

// Start launches the background scheduler, worker, and the HTTP server interface.
func (a *App) Start(ctx context.Context) error {
	workerCtx, cancel := context.WithCancel(ctx)
	a.cancelFunc = cancel

	go a.sched.Start(ctx)
	go a.w.Start(workerCtx)

	a.log.Info("server.starting", "port", a.cfg.Port)
	if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}

// Shutdown stops the HTTP server and signals background worker loops to drain gracefully.
func (a *App) Shutdown(ctx context.Context) error {
	a.log.Info("shutdown.started")

	// 1. Stop accepting new HTTP requests
	if err := a.srv.Shutdown(ctx); err != nil {
		a.log.Error("server.shutdown_error", "error", err)
	}

	// 2. Signal worker pools to terminate and drain in-flight jobs
	if a.cancelFunc != nil {
		a.cancelFunc()
	}

	// 3. Terminate backend connections
	if a.redisClient != nil {
		a.redisClient.Close()
	}
	if a.db != nil {
		a.db.Close()
	}

	a.log.Info("shutdown.complete")
	return nil
}
