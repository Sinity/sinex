-- Migration: Create dead letter queue table
-- Up Migration

CREATE TABLE IF NOT EXISTS sinex_schemas.dlq_events (
  dlq_id                  ULID PRIMARY KEY DEFAULT gen_ulid(),
  failed_event_id         ULID NOT NULL, -- May reference raw.events(id) but could be orphaned
  agent_name              TEXT NOT NULL, -- Which agent/collector failed to process
  source                  TEXT NOT NULL, -- Original event source
  event_type              TEXT NOT NULL, -- Original event type
  failure_reason          TEXT NOT NULL, -- Error message/reason for failure
  error_category          TEXT NOT NULL, -- retryable, permanent, system, user
  retry_count             INT NOT NULL DEFAULT 0,
  failed_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_retry_at           TIMESTAMPTZ,
  next_retry_at           TIMESTAMPTZ, -- For exponential backoff retries
  original_event_payload  JSONB NOT NULL, -- Full original event data
  additional_metadata     JSONB, -- Extra context about the failure
  resolved_at             TIMESTAMPTZ, -- When event was successfully reprocessed or manually resolved
  resolved_by             TEXT, -- How it was resolved: 'reprocessed', 'manual', 'purged'
  CONSTRAINT chk_dlq_error_category CHECK (error_category IN ('retryable', 'permanent', 'system', 'user')),
  CONSTRAINT chk_dlq_resolved_by CHECK (resolved_by IS NULL OR resolved_by IN ('reprocessed', 'manual', 'purged'))
);

COMMENT ON TABLE sinex_schemas.dlq_events IS 'Dead letter queue for events that failed processing and need manual intervention or retry';
COMMENT ON COLUMN sinex_schemas.dlq_events.error_category IS 'Category of error: retryable (temp network), permanent (bad data), system (infra), user (config)';
COMMENT ON COLUMN sinex_schemas.dlq_events.resolved_at IS 'Timestamp when the DLQ entry was resolved (NULL means still pending)';

-- Index for finding events to retry (retryable category with retry time passed)
CREATE INDEX IF NOT EXISTS idx_dlq_retryable_events ON sinex_schemas.dlq_events (error_category, next_retry_at, failed_at)
WHERE resolved_at IS NULL AND error_category = 'retryable';

-- Index for monitoring failed events by agent
CREATE INDEX IF NOT EXISTS idx_dlq_by_agent ON sinex_schemas.dlq_events (agent_name, failed_at DESC)
WHERE resolved_at IS NULL;

-- Index for monitoring by error category
CREATE INDEX IF NOT EXISTS idx_dlq_by_category ON sinex_schemas.dlq_events (error_category, failed_at DESC)
WHERE resolved_at IS NULL;

-- Index for finding events from a specific original event
CREATE INDEX IF NOT EXISTS idx_dlq_by_original_event ON sinex_schemas.dlq_events (failed_event_id);

-- View for current unresolved DLQ events
CREATE OR REPLACE VIEW sinex_schemas.dlq_current AS
SELECT 
  dlq_id,
  agent_name,
  source,
  event_type,
  failure_reason,
  error_category,
  retry_count,
  failed_at,
  last_retry_at,
  next_retry_at,
  (EXTRACT(EPOCH FROM (now() - failed_at))) AS age_seconds
FROM sinex_schemas.dlq_events
WHERE resolved_at IS NULL
ORDER BY failed_at DESC;

COMMENT ON VIEW sinex_schemas.dlq_current IS 'Current unresolved DLQ events with calculated age';