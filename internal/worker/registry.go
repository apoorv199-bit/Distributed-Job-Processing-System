package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apoorv/distributed-job-processor/internal/domain"
)

// HandlerFunc is the contract every job handler must implement.
// Returning a non-nil error marks the job as failed and triggers retry logic.
// Handlers MUST be idempotent — they may be called more than once for the
// same job if a worker crashes between execution and status update.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) error

// Registry maps job_type strings to their handler functions.
// Register all handlers at application startup before workers begin polling.
type Registry struct {
	handlers map[string]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
	}
}

// Register associates a job type with a handler. Panics on duplicate
// registration to catch wiring mistakes at startup, not at runtime.
func (r *Registry) Register(jobType string, handler HandlerFunc) {
	if _, exists := r.handlers[jobType]; exists {
		panic(fmt.Sprintf("handler already registered for job type: %s", jobType))
	}
	r.handlers[jobType] = handler
}

// Get returns the handler for a job type, or an error if unregistered.
func (r *Registry) Get(jobType string) (HandlerFunc, error) {
	h, ok := r.handlers[jobType]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrNoHandler, jobType)
	}
	return h, nil
}

// Registered returns all registered job type names (useful for logging/debug).
func (r *Registry) Registered() []string {
	names := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		names = append(names, k)
	}
	return names
}
