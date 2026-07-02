package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// RegisterDefaultHandlers registers default built-in job handlers to the registry.
func RegisterDefaultHandlers(r *Registry, log *slog.Logger) {
	// send_email: sends a transactional email
	r.Register("send_email", func(ctx context.Context, payload json.RawMessage) error {
		var p struct {
			To       string `json:"to"`
			Template string `json:"template"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("invalid payload: %w", err)
		}
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
