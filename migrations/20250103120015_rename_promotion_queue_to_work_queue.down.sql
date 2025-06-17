-- Migration rollback: Restore promotion_queue from work_queue
-- This migration reverses the work_queue changes and restores the original promotion_queue

-- Step 1: Create the original promotion_queue table
CREATE TABLE IF NOT EXISTS sinex_schemas.promotion_queue (
  queue_id                ULID PRIMARY KEY DEFAULT gen_ulid(),
  raw_event_id            ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
  target_agent_name       TEXT NOT NULL REFERENCES sinex_schemas.agent_manifests(agent_name) ON DELETE CASCADE,
  status                  TEXT NOT NULL DEFAULT 'pending', -- Values: 'pending', 'processing', 'failed_retryable'
  attempts                INT NOT NULL DEFAULT 0,
  max_attempts            INT NOT NULL DEFAULT 5,
  last_attempt_ts         TIMESTAMPTZ,
  next_retry_ts           TIMESTAMPTZ,
  error_message_last      TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  processing_worker_id    TEXT,
  CONSTRAINT uq_promotion_queue_event_agent UNIQUE (raw_event_id, target_agent_name)
);

-- Step 2: Copy data back from work_queue, converting new statuses to old ones
INSERT INTO sinex_schemas.promotion_queue (
    queue_id, raw_event_id, target_agent_name, status, attempts, max_attempts,
    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
)
SELECT 
    queue_id, raw_event_id, target_agent_name, 
    -- Convert new statuses back to old statuses
    CASE 
        WHEN status = 'succeeded' THEN 'pending'  -- Completed items go back to pending
        WHEN status = 'failed' THEN 'failed_retryable'  -- Failed items become retryable
        ELSE status  -- Keep pending, processing, failed_retryable as-is
    END as status,
    attempts, max_attempts,
    last_attempt_ts, next_retry_ts, error_message_last, created_at, processing_worker_id
FROM sinex_schemas.work_queue
WHERE status NOT IN ('succeeded', 'failed') OR processed_at IS NULL;  -- Only migrate items that weren't completed

-- Step 3: Recreate original indexes
CREATE INDEX IF NOT EXISTS idx_promo_queue_pending_tasks ON sinex_schemas.promotion_queue (status, target_agent_name, next_retry_ts ASC NULLS FIRST, created_at ASC)
WHERE status = 'pending' OR status = 'failed_retryable';

CREATE INDEX IF NOT EXISTS idx_promo_queue_failed_tasks ON sinex_schemas.promotion_queue (target_agent_name, status, attempts)
WHERE status = 'failed_retryable';

-- Step 4: Drop the work_queue table
DROP TABLE sinex_schemas.work_queue CASCADE;

-- Step 5: Add original comments
COMMENT ON TABLE sinex_schemas.promotion_queue IS 'Work queue for agents to process raw events for promotion, enrichment, etc.';
COMMENT ON COLUMN sinex_schemas.promotion_queue.status IS 'Current status: pending, processing, failed_retryable.';
COMMENT ON COLUMN sinex_schemas.promotion_queue.next_retry_ts IS 'If status is failed_retryable, when next attempt should be made.';