# CLAUDE.md

This file is my persistent memory for working with the Sinex project.

## 🎯 Project Purpose & Architecture

Sinex is an event-driven data capture system that records everything happening on a computer for later analysis.

**Core Flow**: EventSources → UnifiedCollector → Event Substrate → Workers → Query Interface

- **EventSources**: Individual event capturing components (filesystem, terminals, window managers)
- **UnifiedCollector**: Central coordinator that manages all event sources
- **Event Substrate**: PostgreSQL + TimescaleDB with ULID keys, stores immutable events
- **Workers**: Process events concurrently using `SELECT FOR UPDATE SKIP LOCKED`
- **Query Interface**: Python CLI for exploring captured events

## 🏗️ Key Patterns & Conventions

### EventSource Pattern
All event sources implement this trait for the unified collector:
```rust
#[async_trait]
impl EventSource for MyEventSource {
    type Config = MyConfig;
    const SOURCE_NAME: &'static str = "my_source";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        // Initialize with config from context
    }
    
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Stream events continuously until shutdown
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
- Avoid proliferating around arbitrary ad-hoc scripts, documentation and other such files. There's designated space for such documentation needs you might have - spec/docs/claude

## 🌟 Memory Bank

- After you finish with your task which involved modifying source code, ensure there is no mess left behind, git commit your changes and only then your job is considered done.
- Always examine actual source code, not just documentation which may be outdated

## 📁 Project Map

```
sinex/
├── crate/                    # Core Rust libraries
│   ├── sinex-core/           # EventSource trait, registry, common types
│   ├── sinex-db/             # Database models and pooling
│   ├── sinex-ulid/           # ULID ↔ UUID conversion
│   ├── sinex-collector/      # UnifiedCollector binary
│   ├── sinex-events/         # All event source implementations
│   ├── sinex-worker/         # Worker implementations
│   ├── sinex-promo-worker/   # Promotion queue worker
│   └── sinex-annex/          # Git Annex integration
├── config/                   # Example configurations
├── test/                    # Hierarchically organized test suites
│   ├── unit/                # Unit tests (component isolation)
│   │   ├── core/            # Core library tests
│   │   └── db/              # Database model tests
│   ├── integration/         # Integration tests (component interaction)
│   │   ├── database/        # Database integration tests
│   │   ├── collector/       # Collector integration tests
│   │   ├── worker/          # Worker integration tests
│   │   └── event_sources/   # Event source integration tests
│   ├── system/              # System-level tests (full system validation)
│   │   ├── end_to_end/      # Complete pipeline tests
│   │   ├── external/        # External service integration
│   │   ├── performance/     # Performance and benchmarking
│   │   └── regression/      # Regression tests for specific bugs
│   ├── common/              # Shared test utilities and helpers
│   ├── model/               # Data model tests
│   ├── ulid/                # ULID-specific tests
│   ├── ingestor/            # Event ingestor tests  
│   ├── validation/          # Event validation tests
│   └── adversarial/         # Stress and security tests
├── migrations/              # SQL schema migrations (sqlx)
├── spec/                    # Documentation
│   ├── SADI.md             # Start here - doc index
│   ├── docs/tims/          # Implementation specs
│   └── docs/claude/        # My working area
└── cli/                     # Python query tools
```

## 🛠️ Common Tasks

### Development Setup
```bash
nix develop                      # Always run first - enters dev shell, database setup is automatic
cargo check --workspace         # Verify build
just                            # See available commands
```

### Database Management
The database (`sinex_dev`) is automatically created and migrations applied when entering the nix shell. No manual setup needed!

```bash
just psql                       # Direct database connection
just migrate                    # Apply migrations manually if needed
just migrate-create feature_name # Create new migration

# If you need to reset the database:
dropdb sinex_dev && createdb sinex_dev && just migrate
```

### PostgreSQL Extension Setup
The project requires `pg_jsonschema` extension for JSON Schema validation. Since we use the global PostgreSQL system, install it via:

**Option 1: NixOS System Configuration**
```nix
services.postgresql = {
  enable = true;
  package = pkgs.postgresql_16;
  extraPlugins = with pkgs.postgresql16Packages; [
    # ... other extensions
    # Add pg_jsonschema when available in nixpkgs
  ];
};
```

**Option 2: Manual Installation**
```bash
# Download and install from releases
# https://github.com/supabase/pg_jsonschema/releases
# Follow installation instructions for your PostgreSQL version
```

### Running the Collector
```bash
# Run the unified collector (config logged at startup)
cargo run --bin sinex-collector                    # Run with default config
cargo run --bin sinex-collector -- --dry-run       # Test mode without database
cargo run --bin sinex-collector -- --event-log events.json  # Log to file
cargo run --bin sinex-collector -- --config my-config.toml  # Custom config
cargo run --bin sinex-collector -- --no-db         # Skip database entirely

# Just commands for convenience
just unified                   # Run unified collector (via nix)
just worker                    # Run promotion worker (via nix)
just ingestors-start           # Start both in background
just ingestors-stop            # Stop all running
```

Config loading priority:
1. `--config` command line argument
2. `SINEX_CONFIG` environment variable
3. `unified-collector.toml` in current directory
4. `~/.config/sinex/collector.toml`
5. Built-in defaults (uses DATABASE_URL automatically)

### Database Work
```bash
just migrate                    # Apply migrations
just migrate-create feature_name # New migration
just psql                      # Direct connection

# SQLX cache management
just sqlx-prepare              # Update SQLX cache
just sqlx-check               # Check if cache is up to date
```

### Testing
```bash
just test                       # All tests
just test-unit                  # Unit tests (component isolation)
just test-integration           # Integration tests (component interaction)
just test-system                # System tests (full pipeline validation)
just test-database              # Database-specific tests
just test-collector             # Collector tests
just test-worker                # Worker tests
just test-event-sources         # Event source tests
just test-all                   # Comprehensive test suite
just watch                      # Continuous testing

# Coverage reporting
just coverage                   # Run tests with coverage
just coverage-html              # Generate HTML coverage report
just coverage-lcov              # Generate LCOV format for CI
just coverage-report            # Open coverage report in browser

# Test specific areas
cargo test --test integration   # All integration tests
cargo test --test unit          # All unit tests
cargo test --test system        # All system tests
```


### Debugging
```bash
just query                      # View recent 10 events
just query 50                  # View recent 50 events
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
- `EventSource` - Trait for event capturing components
- `UnifiedCollector` - Central coordinator managing all sources
- `EventRegistry` - Registry of all known event types and their sources

## ⚡ Quick References

### Path Dependencies
```toml
sinex-db = { path = "../../crate/sinex-db" }    # Not src/!
```

### Local PostgreSQL
```
postgresql:///sinex_dev?host=/run/postgresql
```

### Event Types
- `sources::FILESYSTEM`, `sources::TERMINAL_KITTY`, `sources::HYPRLAND`
- Event types defined in `crate/sinex-events/`

### Key Crates
- `sinex-core` - Common types, EventSource trait, registry
- `sinex-db` - Database layer and models
- `sinex-collector` - UnifiedCollector binary and coordination
- `sinex-events` - All specific event source implementations
- `sinex-worker` - Event processing workers
- `sinex-promo-worker` - Promotion queue worker
- `sinex-annex` - Git Annex integration for large files

## 📚 Where to Look

- **Architecture Overview**: `spec/STAD.md`
- **Implementation Details**: `spec/docs/tims/`
- **Design Decisions**: `spec/docs/adr/`
- **My Working Notes**: `spec/docs/claude/`

## 🚦 Environment Checks

- Always in nix shell? (`nix develop`)
- Database running? (automatic in nix shell)
- Migrations applied? (automatic in nix shell)
- SQLX cache current? (`just sqlx-check`)

## 💡 Principles

- Events are immutable facts
- Ingestors just capture, workers process
- Use existing patterns before creating new ones
- Clean up as you go - don't let cruft accumulate
- Check the TIMs before implementing features

## 🔧 Technical Learnings

### SQLX Offline Mode
- SQLX requires `.sqlx/` cache directory for offline builds
- Update cache with: `cargo sqlx prepare --workspace -- --all-targets --all-features`
- Some crates may need individual `cargo sqlx prepare` + merge to workspace
- Cache must be updated when adding new `sqlx::query!` macros
- Missing cache shows as: "SQLX_OFFLINE=true but there is no cached data"

### Nix Build Requirements
- **Critical**: Nix only sees git-tracked files - commit `.sqlx/` and hidden directories
- Untracked/unstaged files are invisible to Nix builds
- "Git tree is dirty" warnings indicate uncommitted changes Nix won't see
- Build failures in Nix that work locally = check git status first

### Debugging Patterns
- Use `just` commands - they have correct flags/environment
- `cargo sqlx prepare` needs `--all-targets --all-features` flags
- Check workspace members individually if commands miss packages
- Recent commits (`git log`) reveal when cache updates are needed