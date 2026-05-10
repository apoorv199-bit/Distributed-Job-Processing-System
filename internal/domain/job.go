package domain

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
)

// Job is the core domain entity. All fields map 1:1 to the jobs table.
type Job struct {
	ID           string          `json:"id"`
	JobType      string          `json:"job_type"`
	Queue        string          `json:"queue"`
	Payload      json.RawMessage `json:"payload"`
	Status       Status          `json:"status"`
	Priority     int             `json:"priority"`
	MaxAttempts  int             `json:"max_attempts"`
	AttemptCount int             `json:"attempt_count"`
	RunAt        time.Time       `json:"run_at"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	FailedAt     *time.Time      `json:"failed_at,omitempty"`
	WorkerID     *string         `json:"worker_id,omitempty"`
	LastError    *string         `json:"last_error,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// JobAttempt records a single execution attempt.
type JobAttempt struct {
	ID         string     `json:"id"`
	JobID      string     `json:"job_id"`
	AttemptNum int        `json:"attempt_num"`
	WorkerID   *string    `json:"worker_id,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Status     *string    `json:"status,omitempty"`
	Error      *string    `json:"error,omitempty"`
}

// SubmitRequest is the parsed input from the API layer.
type SubmitRequest struct {
	JobType     string          `json:"job_type"`
	Queue       string          `json:"queue"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       *time.Time      `json:"run_at,omitempty"`
}

// ListFilter specifies filters for listing jobs via the API.
type ListFilter struct {
	Queue  string
	Status Status
	Page   int
	Limit  int
}

// ListResult is the paginated response for job listing.
type ListResult struct {
	Jobs  []*Job `json:"jobs"`
	Total int    `json:"total"`
	Page  int    `json:"page"`
	Limit int    `json:"limit"`
}

// DeadLetterJob is a permanently failed job stored for inspection and replay.
type DeadLetterJob struct {
	ID            string          `json:"id"`
	JobID         string          `json:"job_id"`
	JobType       string          `json:"job_type"`
	Queue         string          `json:"queue"`
	Payload       json.RawMessage `json:"payload"`
	LastError     string          `json:"last_error"`
	AttemptCount  int             `json:"attempt_count"`
	MaxAttempts   int             `json:"max_attempts"`
	JobCreatedAt  time.Time       `json:"job_created_at"`
	DiedAt        time.Time       `json:"died_at"`
	ReplayedAt    *time.Time      `json:"replayed_at,omitempty"`
	ReplayedJobID *string         `json:"replayed_job_id,omitempty"`
}

// DLQListFilter for paginated DLQ queries.
type DLQListFilter struct {
	Queue          string
	OnlyUnreplayed bool
	Page           int
	Limit          int
}

// DLQListResult is the paginated DLQ response.
type DLQListResult struct {
	Jobs  []*DeadLetterJob `json:"jobs"`
	Total int              `json:"total"`
	Page  int              `json:"page"`
	Limit int              `json:"limit"`
}

// Validate enforces required fields and applies defaults.
func (r *SubmitRequest) Validate() error {
	if r.JobType == "" {
		return ErrMissingJobType
	}
	if r.Queue == "" {
		r.Queue = "default"
	}
	if r.Priority == 0 {
		r.Priority = 5
	}
	if r.Priority < 1 || r.Priority > 10 {
		return ErrInvalidPriority
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.Payload == nil {
		r.Payload = json.RawMessage("{}")
	}
	return nil
}
