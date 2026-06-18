-- 1. Alter jobs table to support priority, multi-tenancy, and idempotency
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 5;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(100) NOT NULL DEFAULT 'default';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);

-- 2. Create workers table
CREATE TABLE IF NOT EXISTS workers (
    worker_id UUID PRIMARY KEY,
    hostname VARCHAR(255) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL
);

-- 3. Create job_dedup table
CREATE TABLE IF NOT EXISTS job_dedup (
    dedup_key VARCHAR(64) PRIMARY KEY,
    job_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- 4. Create idempotency_keys table
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- 5. Create job_events table
CREATE TABLE IF NOT EXISTS job_events (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    metadata JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

-- 6. Create tenant_limits table
CREATE TABLE IF NOT EXISTS tenant_limits (
    tenant_id VARCHAR(100) PRIMARY KEY,
    max_jobs_per_minute INT NOT NULL,
    max_concurrent_jobs INT NOT NULL
);

-- 7. Add database indexes
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_dedup_key ON job_dedup(dedup_key);
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_recent ON idempotency_keys(created_at) WHERE created_at > NOW() - INTERVAL '24 hours';
