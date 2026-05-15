package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/apoorv/distributed-job-processor/internal/domain"
	"github.com/apoorv/distributed-job-processor/internal/service"
	"github.com/go-chi/chi/v5"
)

// JobHandler exposes all job-related HTTP endpoints.
// It is intentionally thin — no business logic here.
type JobHandler struct {
	svc *service.JobService
	log *slog.Logger
}

func NewJobHandler(svc *service.JobService, log *slog.Logger) *JobHandler {
	return &JobHandler{
		svc: svc,
		log: log,
	}
}

// POST /api/v1/jobs
func (h *JobHandler) Submit(w http.ResponseWriter, r *http.Request) {
	var req domain.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	job, err := h.svc.Submit(r.Context(), &req)
	if err != nil {
		if errors.Is(err, domain.ErrMissingJobType) || errors.Is(err, domain.ErrInvalidPriority) {
			respondError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		h.log.ErrorContext(r.Context(), "submit.internal_error", "error", err)
		respondError(w, http.StatusInternalServerError, "failed to submit job")
		return
	}

	respondJSON(w, http.StatusCreated, job)
}

// GET /api/v1/jobs/{id}
func (h *JobHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		respondError(w, http.StatusBadRequest, "missing job id")
		return
	}

	job, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "job not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}

	respondJSON(w, http.StatusOK, job)
}

// GET /api/v1/jobs?queue=X&status=Y&page=1&limit=50
func (h *JobHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := domain.ListFilter{
		Queue:  q.Get("queue"),
		Status: domain.Status(q.Get("status")),
		Page:   parseIntParam(q.Get("page"), 1),
		Limit:  parseIntParam(q.Get("limit"), 50),
	}

	if filter.Limit > 200 {
		filter.Limit = 200
	}

	result, err := h.svc.List(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}

	respondJSON(w, http.StatusOK, result)
}

// GET /api/v1/jobs/{id}/attempts
func (h *JobHandler) GetAttempts(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	attempts, err := h.svc.GetAttempts(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "job not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"job_id":   id,
		"attempts": attempts,
	})
}

// GET /api/v1/queues/{name}/stats
func (h *JobHandler) QueueStats(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	stats, err := h.svc.QueueStats(r.Context(), name)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"queue": name,
		"stats": stats,
	})
}

// Helpers
func respondJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, map[string]string{"error": msg})
}

func parseIntParam(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
