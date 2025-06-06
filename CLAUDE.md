# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

```bash
# Enter development environment (required first step)
nix develop

# Initialize database with migrations
./scripts/db_reset.sh

# Run all tests
cargo test --all-features

# Check system status
./cli/exo.py query --limit 1
```

See the devShell banner for additional commands.

## Architecture Overview

Sinex is an event-driven data capture system with three main layers:

1. **Ingestors** (Rust workspace members): Capture events from various sources
2. **Event Substrate** (PostgreSQL + TimescaleDB): Universal event storage with ULID keys
3. **Query Interface** (Python CLI): Event querying and system introspection

### Key Components

- **Migrations**: sqlx-managed database schema in `/migrations/`
- **Shared Crates**: `sinex-ulid`, `sinex-db`, `sinex-worker` for common functionality  
- **Worker System**: Concurrent event processing with `SELECT FOR UPDATE SKIP LOCKED`
- **Event Schema**: Universal `raw.events` table with JSONB payloads and schema validation

## Core Development Commands

```bash
# Database operations
sqlx migrate run                    # Apply migrations
sqlx migrate add new_feature        # Create new migration
./scripts/setup_test_db.sh         # Test database setup

# SQLX offline cache management
./scripts/update-sqlx-cache.sh     # Update SQLX cache manually
cargo sqlx prepare --workspace     # Regenerate entire cache
nix run .#sqlx-prepare              # Update cache via nix

# Building and testing
cargo build --release              # Build all workspace members
cargo test --test migration_tests  # Run specific test file
cargo test --package sinex-ulid    # Test specific crate

# Development workflow  
cargo watch -x check               # Watch for changes
bacon                              # Continuous testing
```

## SQLX Offline Mode

The project uses SQLX's offline mode for reproducible builds. This means:

1. **Query Cache**: All SQL queries are cached in `.sqlx/` directory
2. **Automatic Updates**: Pre-commit hooks update cache when queries change
3. **CI Verification**: GitHub Actions verify cache is up-to-date
4. **Nix Builds**: Work offline using `SQLX_OFFLINE=true`

### When to Update SQLX Cache

The cache needs updating when:
- You modify any `sqlx::query!()` macros
- You change database schema (migrations)
- You add new SQL queries

### How to Update SQLX Cache

```bash
# Automatic (recommended)
git add .                           # Stage your changes
git commit                          # Pre-commit hook runs automatically

# Manual update
./scripts/update-sqlx-cache.sh      # Updates and stages cache

# Force regeneration
rm -rf .sqlx/                       # Remove existing cache
cargo sqlx prepare --workspace      # Regenerate from scratch
```

### Troubleshooting SQLX

If you see "no cached data for this query":
1. Ensure database is running: `psql $DATABASE_URL`
2. Run migrations: `sqlx migrate run`
3. Update cache: `./scripts/update-sqlx-cache.sh`
4. Commit the `.sqlx/` changes

## Database Schema Architecture

### Core Schemas
- **`raw`**: Immutable event storage (`raw.events` hypertable)
- **`sinex_schemas`**: Schema registry, agent manifests, promotion queue
- **`core`**: Structured data (future: artifacts, entities, relations)

### ULID System
- All primary keys use ULID via `pgx_ulid` PostgreSQL extension
- Provides time-ordered, distributed-safe unique identifiers
- ULID ↔ UUID compatibility for existing tools

### Event Processing Pipeline
1. Ingestors → `raw.events` (immutable storage)
2. Event router → `promotion_queue` (work distribution)  
3. Workers → concurrent processing with retry logic
4. Structured data → `core` schemas (promoted/enriched events)

## Testing Strategy

- **Unit tests**: Standard Rust `#[cfg(test)]` modules
- **Integration tests**: `/tests/*.rs` files using `#[sqlx::test]`
- **Database tests**: Require `TEST_DATABASE_URL` environment variable
- **Property tests**: Use `proptest` crate for schema boundary testing

### Test Database Management
Tests use an ephemeral test database. Set `TEST_DATABASE_URL` or let tests use default:
```bash
export TEST_DATABASE_URL="postgres://sinex_test:testpass@localhost:5433/sinex_test"
```

## Worker System Architecture

### Concurrent Processing Pattern
Workers use PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent task claiming:

```sql
UPDATE promotion_queue 
SET status = 'processing', processing_worker_id = $worker_id
WHERE queue_id IN (
    SELECT queue_id FROM promotion_queue 
    WHERE status = 'pending' 
    ORDER BY created_at 
    LIMIT $batch_size
    FOR UPDATE SKIP LOCKED
)
```

### Retry Logic
- Exponential backoff with jitter
- Configurable max attempts per event type
- Dead letter queue for permanent failures

## NixOS Integration

The project includes a NixOS module for system deployment:

```nix
services.sinex = {
  enable = true;
  systemUser = "username";
  database = { name = "sinex"; user = "sinex"; };
  ingestors.hyprland.enable = true;
};
```

Database initialization uses sqlx migrations automatically.

## Extension Dependencies

The system requires specific PostgreSQL extensions built into the Nix environment:
- **pgx_ulid**: ULID support (built from source)
- **TimescaleDB**: Time-series optimization  
- **pgvector**: Vector similarity search
- **pg_jsonschema**: JSON Schema validation

## Event Types and Schema Validation

All events follow a universal structure stored in `raw.events`:
- Events have optional schema validation via `payload_schema_id` 
- Schema definitions stored in `sinex_schemas.event_payload_schemas`
- Validation enforced by pg_jsonschema CHECK constraints
- Schema versioning supports event evolution over time

## Specification System (`spec/` directory)

The project includes comprehensive specifications that guide development:

### Key Documents
- **`VISION.md`**: Foundational philosophy and conceptual vision
- **`STAD.md`**: System Technical Architecture Document (high-level architectural map)
- **`SADI.md`**: Master index/map of all documentation - **start here**
- **`CDDG.md`**: Claude-Driven Development Guide (AI-assisted TDD methodology)
- **`GLOSSARY.md`**: Definitions of key terms

### Documentation Categories

#### ADRs (Architectural Decision Records) - `spec/docs/adr/`
Record significant architectural choices with rationale:
- **ADR-001**: Primary key strategy (chose pgx_ulid over UUID)
- **ADR-005**: Vector index type (chose pgvector)
- Format: Problem → Options → Decision → Rationale

#### TIMs (Technical Implementation Modules) - `spec/docs/tims/`
Granular implementation specifications organized by domain:
- **`data_substrate/`**: Database schemas, event processing, ULID implementation
- **`ingestors/`**: Desktop, filesystem, application capture strategies  
- **`operations/`**: DevOps, backups, observability setup
- **`cli/`**: Command-line interface design

#### Architectural Modules - `spec/docs/arch_modules/`
Comprehensive domain deep-dives:
- **DataSubstrate_Architecture.md**: Core event storage and processing
- **IngestionArchitecture_And_TelemetrySources.md**: Data capture strategies

### Navigation Tips
1. **Start with `SADI.md`** - master map of all documentation
2. **Check relevant TIMs** before implementing new features
3. **Read ADRs** to understand why certain technical choices were made
4. **All new documentation** should go under `spec/docs/claude/` if needed
5. **TIMs include actual code** - SQL DDL, configuration examples, implementation details

## Important Memories and Notes

- THERE IS NO pg_jsonschema IN THE NIXPKGS
- Always check authentication methods (local socket vs network) before attempting database connections
- When working with Nix packages, check the package structure and available outputs first
- For local PostgreSQL on NixOS, use postgresql:///dbname?host=/run/postgresql