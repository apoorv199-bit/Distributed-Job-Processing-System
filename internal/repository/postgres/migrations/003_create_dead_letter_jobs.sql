-- Stores jobs that have permanently failed after exhausting all retry attempts.
-- These are never automatically retried — they require manual inspection and replay.
 
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id            TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    job_id        TEXT        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    job_type      TEXT        NOT NULL,
    queue         TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    last_error    TEXT        NOT NULL,
    attempt_count INT         NOT NULL,
    max_attempts  INT         NOT NULL,
    -- original creation time of the job (for SLA tracking)
    job_created_at TIMESTAMPTZ NOT NULL,
    -- when it landed in the DLQ
    died_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- set when someone replays this job
    replayed_at   TIMESTAMPTZ,
    replayed_job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL
);
 
-- Most common query: list DLQ entries for a specific queue, newest first
CREATE INDEX IF NOT EXISTS idx_dlq_queue_died
    ON dead_letter_jobs (queue, died_at DESC);
 
-- Look up DLQ entry by original job_id
CREATE INDEX IF NOT EXISTS idx_dlq_job_id
    ON dead_letter_jobs (job_id);
 
-- Filter: only entries that haven't been replayed yet
CREATE INDEX IF NOT EXISTS idx_dlq_unreplayed
    ON dead_letter_jobs (queue, died_at DESC)
    WHERE replayed_at IS NULL;