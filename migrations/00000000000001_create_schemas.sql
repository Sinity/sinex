-- Create database schemas for logical organization

-- Core schema for main event data
CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS 'Core event storage and processing tables';

-- Raw schema for source material and blobs
CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Raw source material and blob storage';

-- Sinex schemas for event payload validation
CREATE SCHEMA IF NOT EXISTS sinex_schemas;
COMMENT ON SCHEMA sinex_schemas IS 'JSON schemas for event payload validation';


-- Metrics schema for analytics
CREATE SCHEMA IF NOT EXISTS metrics;
COMMENT ON SCHEMA metrics IS 'Metrics, analytics, and continuous aggregates';

-- Sinex schema for metrics storage (required by sinex-metrics-lib)
CREATE SCHEMA IF NOT EXISTS sinex;
COMMENT ON SCHEMA sinex IS 'Schema for sinex-metrics-lib compatibility';

-- Synthesis schema for derived events
CREATE SCHEMA IF NOT EXISTS synthesis;
COMMENT ON SCHEMA synthesis IS 'Synthesis configuration and state management';

-- Audit schema for system audit trails
CREATE SCHEMA IF NOT EXISTS audit;
COMMENT ON SCHEMA audit IS 'Audit trails for administrative actions and data changes';

-- Router schema for event routing rules
CREATE SCHEMA IF NOT EXISTS sinex_router;
COMMENT ON SCHEMA sinex_router IS 'Event routing rules and dead letter queues';