-- +goose Up
-- +goose StatementBegin
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS client_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS request_hash TEXT;

ALTER TABLE dead_letter_jobs ADD COLUMN IF NOT EXISTS client_id TEXT NOT NULL DEFAULT 'default';

DROP INDEX IF EXISTS idx_jobs_idempotency;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_tenant_idempotency
    ON jobs (client_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_jobs_tenant_idempotency;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

ALTER TABLE dead_letter_jobs DROP COLUMN IF EXISTS client_id;

ALTER TABLE jobs DROP COLUMN IF EXISTS request_hash;
ALTER TABLE jobs DROP COLUMN IF EXISTS client_id;
-- +goose StatementEnd
