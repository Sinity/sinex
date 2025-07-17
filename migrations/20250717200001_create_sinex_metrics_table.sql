-- Create the sinex schema and metrics table for sinex-metrics-lib
-- This provides a dedicated table for the metrics library storage

-- Create sinex schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS sinex;

-- Create the metrics table with the expected structure from sinex-metrics-lib
CREATE TABLE IF NOT EXISTS sinex.metrics (
    id UUID PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    namespace TEXT NOT NULL DEFAULT 'sinex',
    subsystem TEXT NOT NULL,
    CONSTRAINT valid_metric_type CHECK (metric_type IN ('counter', 'gauge', 'histogram', 'summary'))
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_sinex_metrics_name_time 
ON sinex.metrics (metric_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_sinex_metrics_namespace_subsystem 
ON sinex.metrics (namespace, subsystem, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_sinex_metrics_type 
ON sinex.metrics (metric_type);

-- Add comments for documentation
COMMENT ON SCHEMA sinex IS 'Schema for Sinex internal metrics and utilities';
COMMENT ON TABLE sinex.metrics IS 'Metrics data collected by sinex-metrics-lib with Prometheus-compatible structure';
COMMENT ON COLUMN sinex.metrics.metric_name IS 'Name of the metric (e.g., http_requests_total)';
COMMENT ON COLUMN sinex.metrics.metric_type IS 'Type of metric: counter, gauge, histogram, or summary';
COMMENT ON COLUMN sinex.metrics.value IS 'Numeric value of the metric observation';
COMMENT ON COLUMN sinex.metrics.labels IS 'Key-value pairs as JSONB for metric dimensions';
COMMENT ON COLUMN sinex.metrics.namespace IS 'High-level grouping (default: sinex)';
COMMENT ON COLUMN sinex.metrics.subsystem IS 'Functional subsystem within namespace';