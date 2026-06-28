-- +goose Up
-- +goose StatementBegin
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type        TEXT        NOT NULL,
    queue           TEXT        NOT NULL DEFAULT 'default',
    payload         JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','running','completed','failed','dead')),
    priority        INT         NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    max_attempts    INT         NOT NULL DEFAULT 3,
    attempt_count   INT         NOT NULL DEFAULT 0,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    failed_at       TIMESTAMPTZ,
    worker_id       TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for the dequeue query: filter by queue+status, order by priority+run_at
CREATE INDEX IF NOT EXISTS idx_jobs_dequeue
    ON jobs (queue, status, priority ASC, run_at ASC)
    WHERE status = 'pending';

-- Index for the reaper (finding stalled running jobs)
CREATE INDEX IF NOT EXISTS idx_jobs_stalled
    ON jobs (started_at)
    WHERE status = 'running';

-- Index for API lookups by status
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_queue  ON jobs (queue);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS jobs;
-- +goose StatementEnd