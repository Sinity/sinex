-- Enable required PostgreSQL extensions
-- This must be the first migration as other migrations depend on these extensions

-- Enable ULID extension for time-sortable unique identifiers
CREATE EXTENSION IF NOT EXISTS ulid;

-- Enable TimescaleDB for time-series data management
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Enable JSON Schema validation
CREATE EXTENSION IF NOT EXISTS pg_jsonschema;

-- Enable vector operations for embeddings
CREATE EXTENSION IF NOT EXISTS vector;