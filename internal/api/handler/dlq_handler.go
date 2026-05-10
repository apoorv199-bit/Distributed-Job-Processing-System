package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/service"
	"github.com/go-chi/chi/v5"
)

// DLQHandler exposes all dead-letter queue HTTP endpoints.
type DLQHandler struct {
	svc *service.DLQService
	log *slog.Logger
}

func NewDLQHandler(svc *service.DLQService, log *slog.Logger) *DLQHandler {
	return &DLQHandler{svc: svc, log: log}
}

// -----------------------------------------------------------------------
// GET /api/v1/dlq?queue=X&unreplayed=true&page=1&limit=50
// -----------------------------------------------------------------------
// Lists dead-letter jobs. Supports filtering by queue and replay status.
//
// Example response:
//
//	{
//	  "jobs": [{ "id": "...", "job_type": "send_email", "last_error": "...", ... }],
//	  "total": 12,
//	  "page": 1,
//	  "limit": 50
//	}
func (h *DLQHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := domain.DLQListFilter{
		Queue:          q.Get("queue"),
		OnlyUnreplayed: q.Get("unreplayed") == "true",
		Page:           parseIntParam(q.Get("page"), 1),
		Limit:          parseIntParam(q.Get("limit"), 50),
	}
	if filter.Limit > 200 {
		filter.Limit = 200
	}

	result, err := h.svc.List(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to list DLQ")
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// -----------------------------------------------------------------------
// GET /api/v1/dlq/{id}
// -----------------------------------------------------------------------
// Fetch a single DLQ entry by its ID.
func (h *DLQHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	entry, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrDLQEntryNotFound) {
			respondError(w, http.StatusNotFound, "DLQ entry not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	respondJSON(w, http.StatusOK, entry)
}

// -----------------------------------------------------------------------
// POST /api/v1/dlq/{id}/replay
// -----------------------------------------------------------------------
// Re-enqueues a dead job as a fresh pending job with attempt_count reset to 0.
// The DLQ entry is kept and marked with replayed_at for audit purposes.
//
// Returns 409 if the entry has already been replayed.
//
// Example response:
//
//	{
//	  "message": "job re-enqueued successfully",
//	  "new_job_id": "550e8400-...",
//	  "dlq_id": "..."
//	}
func (h *DLQHandler) Replay(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	newJob, err := h.svc.Replay(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrDLQEntryNotFound):
			respondError(w, http.StatusNotFound, "DLQ entry not found")
		case errors.Is(err, domain.ErrAlreadyReplayed):
			respondError(w, http.StatusConflict, "job has already been replayed")
		default:
			h.log.ErrorContext(r.Context(), "dlq.replay_failed", "dlq_id", id, "error", err)
			respondError(w, http.StatusInternalServerError, "failed to replay job")
		}
		return
	}

	respondJSON(w, http.StatusCreated, map[string]any{
		"message":    "job re-enqueued successfully",
		"new_job_id": newJob.ID,
		"dlq_id":     id,
		"queue":      newJob.Queue,
		"job_type":   newJob.JobType,
	})
}

// -----------------------------------------------------------------------
// POST /api/v1/dlq/bulk-replay?queue=X
// -----------------------------------------------------------------------
// Replays ALL unreplayed entries for a given queue in one call.
// Useful for recovering from a downstream outage that caused mass failures.
//
// Example response:
//
//	{ "message": "bulk replay complete", "replayed_count": 42, "queue": "email" }
func (h *DLQHandler) BulkReplay(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		respondError(w, http.StatusBadRequest, "queue parameter is required")
		return
	}

	count, err := h.svc.BulkReplay(r.Context(), queue)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "bulk replay failed")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"message":        "bulk replay complete",
		"replayed_count": count,
		"queue":          queue,
	})
}

// -----------------------------------------------------------------------
// DELETE /api/v1/dlq/{id}
// -----------------------------------------------------------------------
// Permanently removes a DLQ entry. Use when you've decided to discard a dead job.
func (h *DLQHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.svc.Delete(r.Context(), id); err != nil {
		if errors.Is(err, domain.ErrDLQEntryNotFound) {
			respondError(w, http.StatusNotFound, "DLQ entry not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "failed to delete DLQ entry")
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"message": "deleted"})
}

// -----------------------------------------------------------------------
// GET /api/v1/dlq/stats?queue=X
// -----------------------------------------------------------------------
// Returns aggregate DLQ counts for a named queue.
//
// Example response:
//
//	{ "queue": "email", "stats": { "total": 12, "pending_replay": 9, "replayed": 3 } }
func (h *DLQHandler) Stats(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		respondError(w, http.StatusBadRequest, "queue parameter is required")
		return
	}

	stats, err := h.svc.Stats(r.Context(), queue)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"queue": queue,
		"stats": stats,
	})
}

// -----------------------------------------------------------------------
// POST /api/v1/dlq/purge?queue=X&older_than_days=30
// -----------------------------------------------------------------------
// Deletes old DLQ entries. Meant for scheduled maintenance.
func (h *DLQHandler) Purge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OlderThanDays int    `json:"older_than_days"`
		Queue         string `json:"queue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.OlderThanDays <= 0 {
		req.OlderThanDays = 30 // default: purge entries older than 30 days
	}

	olderThan := time.Duration(req.OlderThanDays) * 24 * time.Hour
	n, err := h.svc.Purge(r.Context(), olderThan)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "purge failed")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"message":       "purge complete",
		"deleted_count": n,
		"older_than":    req.OlderThanDays,
	})
}
