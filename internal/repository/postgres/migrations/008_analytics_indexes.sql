-- +goose Up
-- +goose StatementBegin
-- Completed at index for throughput time-series aggregation
CREATE INDEX IF NOT EXISTS idx_jobs_completed_at ON jobs (completed_at) WHERE completed_at IS NOT NULL;

-- Created at index for submission rate aggregation
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at);

-- Failed at index for failure rate aggregation
CREATE INDEX IF NOT EXISTS idx_jobs_failed_at ON jobs (failed_at) WHERE failed_at IS NOT NULL;

-- Job type and status index for distribution queries
CREATE INDEX IF NOT EXISTS idx_jobs_job_type_status ON jobs (job_type, status);

-- Worker ID index for worker stats aggregation
CREATE INDEX IF NOT EXISTS idx_jobs_worker_id ON jobs (worker_id) WHERE worker_id IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_jobs_completed_at;
DROP INDEX IF EXISTS idx_jobs_created_at;
DROP INDEX IF EXISTS idx_jobs_failed_at;
DROP INDEX IF EXISTS idx_jobs_job_type_status;
DROP INDEX IF EXISTS idx_jobs_worker_id;
-- +goose StatementEnd
