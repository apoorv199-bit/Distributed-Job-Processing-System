package api

import (
	apihandler "github.com/apoorv/distributed-job-processor/internal/api/handler"
	"github.com/apoorv/distributed-job-processor/internal/api/middleware"
	"github.com/go-chi/chi/v5"
)

// RegisterRoutes sets up all v1 endpoint mapping patterns for handlers.
func RegisterRoutes(r chi.Router, jobH *apihandler.JobHandler, dlqH *apihandler.DLQHandler, adminH *apihandler.AdminHandler, analyticsH *apihandler.AnalyticsHandler) {
	r.Route("/api/v1", func(r chi.Router) {
		// Admin endpoints
		r.Post("/admin/clients", adminH.CreateClient)

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

		// Analytics endpoints
		r.Route("/analytics", func(r chi.Router) {
			r.Use(middleware.RequireAdmin)

			r.Get("/overview", analyticsH.Overview)
			r.Get("/throughput", analyticsH.Throughput)
			r.Get("/latency", analyticsH.Latency)
			r.Get("/queues", analyticsH.Queues)
			r.Get("/job-types", analyticsH.JobTypes)
			r.Get("/workers", analyticsH.Workers)
			r.Get("/errors", analyticsH.Errors)
			r.Get("/retries", analyticsH.Retries)
			r.Get("/recent-failures", analyticsH.RecentFailures)
		})
	})
}
