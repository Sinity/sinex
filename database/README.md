# Sinex Database Schema

This directory contains the database schema definitions for the Sinex project.

## Schema Management (Phase 2)

**IMPORTANT:** For Phase 2, we use a pragmatic approach to schema management:

- All schema definitions are contained in `master_schema.sql`
- This file is idempotent and can be run multiple times
- The NixOS `initDbScript` executes this file to set up the database
- **Formal database migrations (e.g., Sqitch) will be adopted in a future phase** when persistent data needs to be preserved across schema changes

## Phase 2 Schema Overview

### Core Tables

1. **`raw.events`** - The main event storage table with enhanced fields:
   - ULID primary keys for distributed-safe unique IDs
   - Enhanced provenance fields (host, ingestor_version, ts_orig)
   - Schema versioning support via payload_schema_id
   - Comprehensive indexing for query performance

2. **`sinex_schemas.event_payload_schemas`** - Registry of event payload schemas:
   - JSON Schema definitions for each event type
   - Version tracking for schema evolution
   - Active/inactive status for deprecation

3. **`sinex_schemas.agent_manifests`** - Registry of ingestors/agents:
   - Self-registration of all data sources
   - Tracking of produced event types
   - Health monitoring via last_seen_heartbeat

### Event Namespaces

- **`hyprland`** - Window manager events
- **`terminal.kitty`** - Terminal command execution
- **`filesystem`** - File system activity
- **`sinex`** - System operational events (agent.*, schema.*)

## Usage

To reset the database with the latest schema:

```bash
# Drop and recreate the database
sudo -u postgres dropdb sinex
sudo -u postgres createdb sinex -O sinex

# Apply the schema
psql -U sinex -d sinex -f database/master_schema.sql
```

## Development Notes

- ULIDs are implemented as a custom domain with generation function
- All timestamps use TIMESTAMPTZ for proper timezone handling
- JSONB is used for flexible payload storage with GIN indexing
- Foreign key constraints ensure referential integrity between events and schemas