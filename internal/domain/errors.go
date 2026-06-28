package domain

import "errors"

var (
	ErrJobNotFound         = errors.New("job not found")
	ErrDLQEntryNotFound    = errors.New("dead letter job not found")
	ErrAlreadyReplayed     = errors.New("job has already been replayed")
	ErrMissingJobType      = errors.New("job_type is required")
	ErrInvalidPriority     = errors.New("priority must be between 1 and 10")
	ErrNoHandler           = errors.New("no handler registered for job type")
	ErrQueueFull           = errors.New("queue is full")
	ErrDuplicate           = errors.New("duplicate idempotency key") // job already submitted
	ErrIdempotencyConflict = errors.New("idempotency key conflict: request parameters do not match")
)
