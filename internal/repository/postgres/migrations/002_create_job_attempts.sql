-- Audit log of every execution attempt for a job
 
CREATE TABLE IF NOT EXISTS job_attempts (
    id          TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    job_id      TEXT        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_num INT         NOT NULL,
    worker_id   TEXT,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status      TEXT        CHECK (status IN ('completed', 'failed')),
    error       TEXT
);
 
CREATE INDEX IF NOT EXISTS idx_attempts_job_id ON job_attempts (job_id);