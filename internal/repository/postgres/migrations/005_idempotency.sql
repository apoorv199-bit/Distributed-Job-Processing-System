-- +goose Up
-- +goose StatementBegin
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs(idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_jobs_idempotency;
ALTER TABLE jobs DROP COLUMN IF EXISTS idempotency_key;
-- +goose StatementEnd
