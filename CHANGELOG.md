# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - Core Infrastructure & Test Suite - 2025-01-03

### 🔧 Infrastructure Improvements
- **ULID Implementation**: Added native PostgreSQL ULID support via pgx_ulid extension
- **Database Migrations**: Complete migration to ULID primary keys across all tables
- **Worker System**: Implemented concurrent event processing with SELECT FOR UPDATE SKIP LOCKED
- **Shared Crates**: Created sinex-ulid, sinex-db, and sinex-worker libraries

### 🧪 Comprehensive Testing
- **Migration Tests**: Database migration validation with rollback verification
- **ULID Integration**: Full ULID functionality testing including database roundtrips
- **Concurrency Tests**: Worker safety and deadlock prevention testing
- **Error Handling**: Comprehensive error scenario and recovery testing
- **Schema Validation**: JSON Schema validation testing with pg_jsonschema
- **Agent Management**: CRUD operations and heartbeat functionality testing
- **TimescaleDB**: Hypertable functionality and compression testing
- **Pipeline Integration**: End-to-end event processing pipeline testing

### 🏗️ Build System
- **Flake Updates**: Built pgx_ulid from source, added all required PostgreSQL extensions
- **CI/CD Pipeline**: GitHub Actions with Nix builds, tests, and coverage reporting
- **Test Infrastructure**: Automated test database setup and teardown

### 📊 Monitoring & Metrics
- **Prometheus Integration**: Added metrics collection to worker processes
- **Agent Heartbeats**: Comprehensive heartbeat tracking and monitoring
- **Dead Letter Queue**: Enhanced error tracking and permanent failure handling

## [0.2.0] - Phase 2: Universal Event Substrate - 2024-01-15

### 🚀 Major Features

#### Enhanced Data Model
- **ULID Primary Keys**: Implemented distributed-safe unique identifiers using custom ULID domain
- **Rich Provenance Tracking**: Added host, ingestor_version, ts_orig fields for comprehensive event attribution
- **Schema Registry**: New `sinex_schemas.event_payload_schemas` table with JSON Schema definitions
- **Agent Manifests**: Self-registration system in `sinex_schemas.agent_manifests` table

#### Multiple Ingestors
- **Enhanced Hyprland Ingestor**: 
  - State snapshots every 30 minutes
  - Rich window focus events with workspace context
  - Robust error handling with DLQ support
- **New Kitty Terminal Ingestor**: Command execution tracking via remote control protocol
- **New Filesystem Ingestor**: Real-time file operations with BLAKE3 hashing and inotify

#### Operational Excellence
- **Dead Letter Queues**: Per-agent DLQ with file-based persistence
- **Batch Processing**: Configurable batching for efficient database writes
- **Retry Logic**: Exponential backoff for failed database operations
- **Health Monitoring**: Real-time agent heartbeats and error tracking

### 🔧 Infrastructure

#### Shared Rust Library (`sinex-shared`)
- Unified error handling and retry logic
- Common database operations and connection pooling
- Agent metrics and manifest management
- DLQ implementation with critical failure logging

#### Enhanced CLI (`exo`)
- **Advanced Querying**: Filter by source, event type, host, time ranges
- **Schema Introspection**: `exo schema list/get` commands
- **Agent Management**: `exo agent list/status` for monitoring
- **Multiple Output Formats**: JSON, CSV, YAML, rich tables
- **JQ Integration**: Filter payloads with JQ expressions

### 🛠️ Developer Experience

#### Development Scripts
- `scripts/db_reset.sh`: Complete database reset with Phase 2 schema
- `scripts/run_local_dev.sh`: Start all ingestors with monitoring
- `scripts/stop_local_dev.sh`: Graceful shutdown of all ingestors
- `scripts/tail_agent_logs.sh`: Monitor specific agent logs

#### Enhanced Configuration
- TOML-based configuration for all ingestors
- Environment variable overrides
- Comprehensive defaults with validation

### 📊 Event Types

#### New Event Sources
- `hyprland`: window_focused, workspace_changed, clipboard_changed, state_snapshot
- `terminal.kitty`: command_executed
- `filesystem`: file_created, file_modified, file_deleted, file_renamed
- `sinex`: agent.heartbeat, agent.error, agent.dlq_event_written, schema.change

#### Enhanced Event Structure
```sql
raw.events:
- id: ULID (distributed-safe unique identifier)
- source: TEXT (ingestor identifier)  
- event_type: TEXT (specific event type)
- ts_ingest: TIMESTAMPTZ (ingestion timestamp)
- ts_orig: TIMESTAMPTZ (original event timestamp)
- host: TEXT (hostname of event origin)
- ingestor_version: TEXT (version of ingestor)
- payload_schema_id: ULID (FK to schema registry)
- payload: JSONB (event data)
```

### 🔍 Monitoring & Observability

#### Agent Health Tracking
- Automatic heartbeat emission every 60 seconds
- Error event tracking with severity levels
- DLQ monitoring and critical failure logging
- Agent manifest registration on startup

#### Enhanced Statistics
- Per-source event counts and types
- Average ingestion delays
- Schema usage statistics
- Agent health dashboard in CLI

### 🏗️ Architecture Changes

#### Database Schema Evolution
- Migrated from simple event storage to rich schema registry
- Added agent manifest system for self-registration
- Implemented ULID support for distributed scalability
- Enhanced indexing for query performance

#### Modular Ingestor Design
- Shared libraries for common functionality
- Workspace-based build system
- Consistent configuration patterns
- Unified error handling and DLQ logic

### 📚 Documentation

#### Comprehensive README
- Complete Phase 2 feature overview
- Detailed CLI usage examples
- Development and troubleshooting guides
- NixOS integration examples

#### Per-Ingestor Documentation
- Individual README files for each ingestor
- Configuration options and event schemas
- Performance considerations and limitations
- Installation and usage instructions

---

## [0.1.0] - Phase 1: MVP - 2024-01-01

### Initial Implementation
- Basic Hyprland event capture
- Simple PostgreSQL storage
- Basic CLI querying
- NixOS module foundation

### Features
- Window focus tracking
- Workspace change events
- Basic event storage in `raw.events`
- Simple Python CLI for querying

### Architecture
- Monolithic Rust ingestor
- Direct database writes
- Basic error handling
- Manual configuration management