# CLAUDE.md

This file is my persistent memory for working with the Sinex project.

## 🎯 Project Purpose & Architecture

Sinex is an event-driven data capture system that records everything happening on a computer for later analysis.

**Core Flow**: Ingestors → Event Substrate → Workers → Query Interface

- **Ingestors**: Capture events from sources (filesystem, terminals, window managers)
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Workers**: Process events concurrently using `SELECT FOR UPDATE SKIP LOCKED`
- **Query Interface**: Python CLI for exploring captured events

## 🏗️ Key Patterns & Conventions

### SimpleIngestor Pattern
All ingestors implement this trait and let IngestorRuntime handle lifecycle:
```rust
#[async_trait]
impl SimpleIngestor for MyIngestor {
    fn name() -> &'static str { "my-ingestor" }
    fn version() -> &'static str { env!("CARGO_PKG_VERSION") }
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Just capture events - runtime handles heartbeats, retries, DLQ, shutdown
    }
}
```

### Database Patterns
- All primary keys use ULID (time-ordered, distributed-safe)
- Events are immutable once written to `raw.events`
- Schema validation via pg_jsonschema
- Concurrent work distribution via `FOR UPDATE SKIP LOCKED`

### Code Organization
- Consolidate related code - avoid excessive file atomization
- Tests go in categorized subdirectories under `test/`
- Put my working docs in `spec/docs/claude/`
- Clean up obsolete code/files proactively

## 🌟 Memory Bank

- After you finish with your task which involved modifying source code, ensure there is no mess left behind, git commit your changes and only then your job is considered done.

## 📁 Project Map

```
sinex/
├── crate/                    # Core Rust libraries
│   ├── sinex-core/           # RawEvent, errors, constants
│   ├── sinex-db/             # Database models and pooling
│   ├── sinex-ulid/           # ULID ↔ UUID conversion
│   ├── sinex-worker/         # Worker implementations
│   └── sinex-promo-worker/   # Promotion queue worker
├── ingestor/
│   ├── shared/               # Shared utilities (gradually migrating to crate)
│   ├── filesystem/           # Watch file system changes
│   ├── kitty/               # Capture terminal commands
│   ├── hyprland/            # Window manager events
│   └── unified/             # Example multi-source collector
├── config/                   # Example configurations for each ingestor
├── test/                    # Categorized test suites
│   ├── database/            # Schema, migrations, ULID
│   ├── pipeline/            # Event processing, workers
│   ├── agent/              # Manifests, heartbeats
│   └── reliability/         # Error handling, failures
├── migration/              # SQL schema migrations (sqlx)
├── spec/                    # Documentation
│   ├── SADI.md             # Start here - doc index
│   ├── docs/tims/          # Implementation specs
│   └── docs/claude/        # My working area
└── cli/                     # Python query tools
```

## 🛠️ Common Tasks

### Development Setup
```bash
nix develop                      # Always run first - enters dev shell
db setup dev                    # Initialize database
cargo check --workspace         # Verify build
```

### Database Management
```bash
db                              # Show current database
db dev                         # Switch to development database  
db prod                        # Switch to production database
db tmp                         # Switch to ephemeral database (tmp_0)
db tmp_3                       # Switch to ephemeral database 3 (0-9)
db setup [dev|prod]            # Setup/initialize database
db reset                       # Reset current database
db destroy                     # Destroy ephemeral database
db shell                       # Connect with psql to current database
```

### Running Ingestors
```bash
cargo run --bin filesystem-ingestor -- --dry-run
cargo run --bin kitty-ingestor -- --output-file events.json
cargo run --bin hyprland-ingestor
```

### Database Work
```bash
sqlx migrate run                # Apply migrations
sqlx migrate add feature_name   # New migration
psql $DATABASE_URL             # Direct connection

# SQLX cache management (NEW)
nix run .#sqlx-prepare          # Update SQLX cache (replaces old script)
```

### Testing
```bash
# Regular testing
cargo test                      # All tests
cargo test --package sinex-db   # Specific crate
cargo test --test database/     # Test category

# Isolated testing with ephemeral database
db tmp                          # Switch to ephemeral database
cargo test test_full_system_end_to_end -- --ignored  # Run specific test

# Continuous testing
bacon                           # Continuous testing
cargo watch -x test           # Watch mode
```

### Development Environment (NEW)
```bash
nix run .#dev                  # Full interactive development environment (mprocs)
nix run .#dev db-only         # Just setup database
nix run .#dev background      # Start services in background
```

### Monitoring (NEW)
```bash
nix run .#monitor             # Interactive dashboard
nix run .#monitor live        # Live event tail
nix run .#monitor events      # Recent events
```

### Debugging
```bash
./cli/exo.py query --limit 10  # View recent events
./cli/exo.py query --source filesystem --after "1 hour ago"
cargo test -- --nocapture      # See test output
```

## 🗄️ Database Schema

**Core Tables**:
- `raw.events` - Immutable event storage (hypertable)
- `sinex_schemas.event_payload_schemas` - JSON schemas
- `sinex_schemas.agent_manifests` - Registered ingestors
- `sinex_schemas.promotion_queue` - Event processing queue

**Key Types**:
- `RawEvent` - Universal event structure
- `EventSink` - Output abstraction (Database/Log/File/Memory)
- `IngestorRuntime` - Manages ingestor lifecycle

## ⚡ Quick References

### Path Dependencies
```toml
sinex-db = { path = "../../crate/sinex-db" }    # Not src/!
```

### Local PostgreSQL
```
postgresql:///sinex?host=/run/postgresql
```

### Event Types
- `sources::FILESYSTEM`, `sources::TERMINAL_KITTY`, `sources::HYPRLAND`
- `event_type_constants::filesystem::FILE_CREATED`
- `event_type_constants::terminal::COMMAND_EXECUTED`

### Key Crates
- `sinex-core` - Common types all crates use
- `sinex-db` - Database layer
- `sinex-shared` - Ingestor utilities (being split up)

## 📚 Where to Look

- **Architecture Overview**: `spec/STAD.md`
- **Implementation Details**: `spec/docs/tims/`
- **Design Decisions**: `spec/docs/adr/`
- **My Working Notes**: `spec/docs/claude/`

## 🚦 Environment Checks

- Always in nix shell? (`nix develop`)
- Database running? (`psql $DATABASE_URL`)
- Migrations applied? (`sqlx migrate run`)
- SQLX cache current? (`./script/update-sqlx-cache.sh`)

## 💡 Principles

- Events are immutable facts
- Ingestors just capture, workers process
- Use existing patterns before creating new ones
- Clean up as you go - don't let cruft accumulate
- Check the TIMs before implementing features