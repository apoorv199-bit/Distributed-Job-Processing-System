-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS job_attempts (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_num INT         NOT NULL,
    worker_id   TEXT,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status      TEXT        CHECK (status IN ('completed', 'failed')),
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_attempts_job_id ON job_attempts (job_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS job_attempts;
-- +goose StatementEnd