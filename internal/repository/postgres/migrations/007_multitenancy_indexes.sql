-- +goose Up
-- +goose StatementBegin
-- Index for multitenant job queries: filtering jobs by tenant and sorting by priority/creation time
CREATE INDEX IF NOT EXISTS idx_jobs_client_id_status
    ON jobs (client_id, status);

-- Index for multitenant DLQ queries: filtering dead letter jobs by tenant and queue, ordered by died_at
CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_tenant_queue
    ON dead_letter_jobs (client_id, queue, died_at DESC);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_jobs_client_id_status;
DROP INDEX IF EXISTS idx_dead_letter_jobs_tenant_queue;
-- +goose StatementEnd
