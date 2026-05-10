ALTER TABLE jobs ADD COLUMN IF NOT EXISTS
    idempotency_key TEXT UNIQUE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs(idempotency_key)
    WHERE idempotency_key IS NOT NULL;