package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/apoorv/distributed-job-processor/internal/service"
)

type AnalyticsHandler struct {
	svc *service.AnalyticsService
	log *slog.Logger
}

func NewAnalyticsHandler(svc *service.AnalyticsService, log *slog.Logger) *AnalyticsHandler {
	return &AnalyticsHandler{
		svc: svc,
		log: log,
	}
}

// GET /api/v1/analytics/overview
func (h *AnalyticsHandler) Overview(w http.ResponseWriter, r *http.Request) {
	stats, err := h.svc.Overview(r.Context())
	if err != nil {
		h.log.Error("failed to get overview stats", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load analytics overview")
		return
	}

	h.respondJSON(w, http.StatusOK, stats)
}

// GET /api/v1/analytics/throughput?interval=hour&range=24h
func (h *AnalyticsHandler) Throughput(w http.ResponseWriter, r *http.Request) {
	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "hour"
	}
	rangeStr := r.URL.Query().Get("range")
	if rangeStr == "" {
		rangeStr = "24h"
	}

	data, err := h.svc.Throughput(r.Context(), interval, rangeStr)
	if err != nil {
		h.log.Error("failed to get throughput time series", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load throughput data")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"interval": interval,
		"range":    rangeStr,
		"data":     data,
	})
}

// GET /api/v1/analytics/latency?queue=default&range=24h
func (h *AnalyticsHandler) Latency(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	rangeStr := r.URL.Query().Get("range")
	if rangeStr == "" {
		rangeStr = "24h"
	}

	stats, err := h.svc.Latency(r.Context(), queue, rangeStr)
	if err != nil {
		h.log.Error("failed to get latency percentiles", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load latency metrics")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"queue": queue,
		"range": rangeStr,
		"stats": stats,
	})
}

// GET /api/v1/analytics/queues
func (h *AnalyticsHandler) Queues(w http.ResponseWriter, r *http.Request) {
	stats, err := h.svc.Queues(r.Context())
	if err != nil {
		h.log.Error("failed to get queues overview", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load queues metrics")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"queues": stats,
	})
}

// GET /api/v1/analytics/job-types?range=24h
func (h *AnalyticsHandler) JobTypes(w http.ResponseWriter, r *http.Request) {
	rangeStr := r.URL.Query().Get("range")
	if rangeStr == "" {
		rangeStr = "24h"
	}

	stats, err := h.svc.JobTypes(r.Context(), rangeStr)
	if err != nil {
		h.log.Error("failed to get job types stats", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load job types metrics")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"range": rangeStr,
		"types": stats,
	})
}

// GET /api/v1/analytics/workers?range=24h
func (h *AnalyticsHandler) Workers(w http.ResponseWriter, r *http.Request) {
	rangeStr := r.URL.Query().Get("range")
	if rangeStr == "" {
		rangeStr = "24h"
	}

	stats, err := h.svc.Workers(r.Context(), rangeStr)
	if err != nil {
		h.log.Error("failed to get worker stats", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load worker metrics")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"range":   rangeStr,
		"workers": stats,
	})
}

// GET /api/v1/analytics/errors?range=24h&limit=20
func (h *AnalyticsHandler) Errors(w http.ResponseWriter, r *http.Request) {
	rangeStr := r.URL.Query().Get("range")
	if rangeStr == "" {
		rangeStr = "24h"
	}

	limit := h.parseIntQuery(r.URL.Query().Get("limit"), 20)

	stats, err := h.svc.Errors(r.Context(), rangeStr, limit)
	if err != nil {
		h.log.Error("failed to get top errors", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load errors data")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"range":  rangeStr,
		"errors": stats,
	})
}

// GET /api/v1/analytics/retries
func (h *AnalyticsHandler) Retries(w http.ResponseWriter, r *http.Request) {
	stats, err := h.svc.Retries(r.Context())
	if err != nil {
		h.log.Error("failed to get retry distribution", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load retry metrics")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"distribution": stats,
	})
}

// GET /api/v1/analytics/recent-failures?limit=20
func (h *AnalyticsHandler) RecentFailures(w http.ResponseWriter, r *http.Request) {
	limit := h.parseIntQuery(r.URL.Query().Get("limit"), 20)

	failures, err := h.svc.RecentFailures(r.Context(), limit)
	if err != nil {
		h.log.Error("failed to get recent failures", "err", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load recent failures")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]interface{}{
		"failures": failures,
	})
}


func (h *AnalyticsHandler) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.log.Error("failed to encode JSON response", "err", err)
	}
}

func (h *AnalyticsHandler) respondError(w http.ResponseWriter, status int, msg string) {
	h.respondJSON(w, status, map[string]string{"error": msg})
}

func (h *AnalyticsHandler) parseIntQuery(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
