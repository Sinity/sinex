-- Migration: Create dead letter queue table
-- Down Migration

DROP VIEW IF EXISTS sinex_schemas.dlq_current;
DROP TABLE IF EXISTS sinex_schemas.dlq_events;