package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apoorv/distributed-job-processor/config"
	"github.com/apoorv/distributed-job-processor/internal/app"
	"github.com/joho/godotenv"
)

func main() {
	// 1. Logger Setup
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(log)

	// 2. Load ENV Variables
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Warn("env.load_failed", "error", err)
	}

	// 3. Load Application Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Error("config.load_failed", "error", err)
		os.Exit(1)
	}

	// 4. Initialize Application Container
	application, err := app.New(cfg, log)
	if err != nil {
		log.Error("app.initialization_failed", "error", err)
		os.Exit(1)
	}

	// 5. Context & Interruption Signal Handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 6. Start Application
	go func() {
		if err := application.Start(ctx); err != nil {
			log.Error("app.start_failed", "error", err)
			os.Exit(1)
		}
	}()

	// Block until interruption signal is received
	<-ctx.Done()
	log.Info("shutdown.signal_received")

	// 7. Graceful Shutdown with 15s Timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := application.Shutdown(shutdownCtx); err != nil {
		log.Error("app.shutdown_failed", "error", err)
	}
}
