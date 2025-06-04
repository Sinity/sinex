# Sinex Database Schema

This directory contains database-related documentation for the Sinex project.

## Schema Management

Sinex uses **sqlx migrations** for proper database schema management:

- Migration files are in `/migrations/` directory  
- Run `sqlx migrate run` to apply migrations
- Each migration includes both up and down SQL
- ULID support via pgx_ulid PostgreSQL extension

## Core Schema Overview

### Main Tables

1. **`raw.events`** - Universal event storage:
   - ULID primary keys via pgx_ulid extension
   - Universal event structure with JSONB payloads
   - TimescaleDB hypertable for time-series optimization
   - Comprehensive indexing for query performance

2. **`sinex_schemas.event_payload_schemas`** - Schema registry:
   - JSON Schema definitions for event validation
   - Version tracking and lifecycle management
   - Validation via pg_jsonschema extension

3. **`sinex_schemas.agent_manifests`** - Agent registry:
   - Agent self-registration and capabilities
   - Event subscription and production definitions
   - Health monitoring and status tracking

4. **`sinex_schemas.promotion_queue`** - Event processing queue:
   - Worker-safe concurrent processing
   - Retry logic with exponential backoff
   - Dead letter queue for permanent failures

## Usage

### Database Setup

```bash
# Reset database with latest migrations
./scripts/db_reset.sh

# Or manually:
sqlx migrate run
```

### Development

```bash
# Run migration tests
cargo test --test migration_tests

# Test ULID functionality  
cargo test --test ulid_integration_tests

# Test full pipeline
cargo test --test event_pipeline_integration_tests
```

## Extensions Used

- **pgx_ulid**: Native ULID support with PostgreSQL functions
- **TimescaleDB**: Hypertables for time-series data optimization
- **pgvector**: Vector similarity search capabilities
- **pg_jsonschema**: JSON Schema validation in database

## Event Structure

All events follow a consistent structure:

```sql
raw.events:
- id: ULID (distributed-safe unique identifier)
- source: TEXT (event source/producer)
- event_type: TEXT (specific event type)
- ts_ingest: TIMESTAMPTZ (ingestion timestamp, partitioning key)
- ts_orig: TIMESTAMPTZ (original event timestamp)
- host: TEXT (originating host)
- ingestor_version: TEXT (producer version)
- payload_schema_id: ULID (optional schema reference)
- payload: JSONB (event-specific data)
```