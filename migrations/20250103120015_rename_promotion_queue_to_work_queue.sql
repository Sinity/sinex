-- Migration: Rename promotion_queue to work_queue and add new columns
-- This migration renames promotion_queue to work_queue and adds:
-- - processed_at: TIMESTAMPTZ for TTL policy tracking
-- - failure_reason: TEXT for detailed error information

-- Step 1: Create the new work_queue table with additional columns
CREATE TABLE sinex_schemas.work_queue (
  queue_id                ULID PRIMARY KEY DEFAULT gen_ulid(),
  raw_event_id            ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
  target_agent_name       TEXT NOT NULL REFERENCES sinex_schemas.agent_manifests(agent_name) ON DELETE CASCADE,
  status                  TEXT NOT NULL DEFAULT 'pending', -- Values: 'pending', 'processing', 'succeeded', 'failed', 'failed_retryable'
  attempts                INT NOT NULL DEFAULT 0,
  max_attempts            INT NOT NULL DEFAULT 5,
  last_attempt_ts         TIMESTAMPTZ,
  next_retry_ts           TIMESTAMPTZ,
  error_message_last      TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  processing_worker_id    TEXT,
  processed_at            TIMESTAMPTZ, -- New: When item was successfully processed or permanently failed
  failure_reason          TEXT, -- New: Detailed failure reason for permanent failures
  CONSTRAINT uq_work_queue_event_agent UNIQUE (raw_event_id, target_agent_name)
);

-- Step 2: Copy all data from promotion_queue to work_queue
INSERT INTO sinex_schemas.work_queue (
    queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts,
    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
)
SELECT 
    queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts,
    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
FROM sinex_schemas.promotion_queue;

-- Step 3: Create indexes for efficient queue operations
CREATE INDEX idx_work_queue_pending_tasks ON sinex_schemas.work_queue (status, target_agent_name, next_retry_ts ASC NULLS FIRST, created_at ASC)
WHERE status IN ('pending', 'failed_retryable');

CREATE INDEX idx_work_queue_failed_tasks ON sinex_schemas.work_queue (target_agent_name, status, attempts)
WHERE status = 'failed_retryable';

-- New index for TTL policy cleanup
CREATE INDEX idx_work_queue_ttl_cleanup ON sinex_schemas.work_queue (status, processed_at)
WHERE status IN ('succeeded', 'failed') AND processed_at IS NOT NULL;

-- Step 4: Drop the old promotion_queue table
DROP TABLE sinex_schemas.promotion_queue CASCADE;

-- Step 5: Add comments
COMMENT ON TABLE sinex_schemas.work_queue IS 'Work queue for agents to process raw events for promotion, enrichment, etc.';
COMMENT ON COLUMN sinex_schemas.work_queue.status IS 'Current status: pending, processing, succeeded, failed, failed_retryable.';
COMMENT ON COLUMN sinex_schemas.work_queue.next_retry_ts IS 'If status is failed_retryable, when next attempt should be made.';
COMMENT ON COLUMN sinex_schemas.work_queue.processed_at IS 'Timestamp when item was successfully processed or permanently failed (for TTL policy).';
COMMENT ON COLUMN sinex_schemas.work_queue.failure_reason IS 'Detailed failure reason for items with failed status.';
