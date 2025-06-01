-- Migration: Create promotion queue table
-- Up Migration

CREATE TABLE IF NOT EXISTS sinex_schemas.promotion_queue (
  queue_id                ULID PRIMARY KEY DEFAULT gen_ulid(),
  raw_event_id            ULID NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
  target_agent_name       TEXT NOT NULL REFERENCES sinex_schemas.agent_manifests(agent_name) ON DELETE CASCADE,
  status                  TEXT NOT NULL DEFAULT 'pending', -- Values: 'pending', 'processing', 'failed_retryable'
  attempts                INT NOT NULL DEFAULT 0,
  max_attempts            INT NOT NULL DEFAULT 5, -- Default, can be overridden by agent config/logic
  last_attempt_ts         TIMESTAMPTZ NULLABLE,
  next_retry_ts           TIMESTAMPTZ NULLABLE, -- For exponential backoff
  error_message_last      TEXT NULLABLE,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  processing_worker_id    TEXT NULLABLE, -- Identifier of worker instance currently processing
  CONSTRAINT uq_promotion_queue_event_agent UNIQUE (raw_event_id, target_agent_name)
);

COMMENT ON TABLE sinex_schemas.promotion_queue IS 'Work queue for agents to process raw events for promotion, enrichment, etc.';
COMMENT ON COLUMN sinex_schemas.promotion_queue.status IS 'Current status: pending, processing, failed_retryable.';
COMMENT ON COLUMN sinex_schemas.promotion_queue.next_retry_ts IS 'If status is failed_retryable, when next attempt should be made.';

-- Index for workers to efficiently pick up pending tasks
CREATE INDEX IF NOT EXISTS idx_promo_queue_pending_tasks ON sinex_schemas.promotion_queue (status, target_agent_name, next_retry_ts ASC NULLS FIRST, created_at ASC)
WHERE status = 'pending' OR status = 'failed_retryable';

-- Index for monitoring tasks that have repeatedly failed
CREATE INDEX IF NOT EXISTS idx_promo_queue_failed_tasks ON sinex_schemas.promotion_queue (target_agent_name, status, attempts)
WHERE status = 'failed_retryable';