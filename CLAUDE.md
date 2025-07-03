# CLAUDE.md (My Deep Memory)

## What Sinex Is

Sinex captures EVERYTHING happening on a computer as immutable events, stores them in PostgreSQL+TimescaleDB with ULID keys for time-ordering, then processes them asynchronously via workers. It's a "sentient archive" - complete digital activity capture for later analysis.

**Flow**: EventSources → UnifiedCollector → raw.events → WorkQueue → Workers → Analysis

## Core Invariants (NEVER VIOLATE)

1. **Events are IMMUTABLE** - Once in `raw.events`, they never change
2. **All IDs are ULIDs** - Time-sortable, use `sinex_ulid::Ulid` type
3. **Locking pattern**: `SELECT FOR UPDATE SKIP LOCKED` for work distribution
4. **Schema validation**: All events validated via pg_jsonschema before storage
5. **SQLX offline mode**: Must commit `.sqlx/` directory for Nix builds

## Critical Commands

```bash
# ALWAYS FIRST
nix develop                     # Sets up DB, migrations, environment

# Development
cargo check --workspace         # Must pass before anything
just test                      # Run tests
just sqlx-prepare              # Update SQLX cache after queries
git add .sqlx/                 # MUST commit cache

# Running
just unified                   # Start collector
just worker                    # Start promotion worker
RUST_LOG=debug just unified    # Debug mode

# Database
just psql                      # Direct access
just migrate-create name       # New migration
dropdb sinex_dev && createdb sinex_dev && just migrate  # Reset

# Deployment
sudo systemctl restart sinex-update        # Auto pre-flight + deploy
sinex-preflight verify --timeout 120       # Manual verification
```

## Common Pitfalls

1. **Nix build fails but local works** → Check `git status`, commit everything
2. **SQLX offline errors** → Run `just sqlx-prepare` and commit `.sqlx/`
3. **Test failures** → Use `#[sinex_test]` not `#[tokio::test]`
4. **Config not loading** → Priority: CLI args → env → file → defaults
5. **Events not captured** → Check source enabled in config, verify schema

## Project Structure

```
crate/
├── sinex-core/             # Core types: RawEvent, EventSource trait, errors
├── sinex-db/               # Database: models, queries, pool management
├── sinex-collector/        # Main binary: UnifiedCollector coordination
├── sinex-events-fs/        # Filesystem events
├── sinex-events-desktop/   # Desktop events (clipboard, window manager)
├── sinex-events-terminal/  # Terminal events (commands, shell history)
├── sinex-events-system/    # System events (dbus, journal)
├── sinex-worker/           # Worker implementations
├── sinex-preflight/        # Pre-flight verification (7 phases)
test/                       # Tests organized by type (unit/integration/system)
```

## Key Patterns

```rust
// Error handling with context
use sinex_core::ErrorContext;
CoreError::database("failed").with_context("table", "events").build()

// Validation chains
use sinex_core::ValidationChain;
ValidationChain::validate(val, "field").not_empty().min_length(3).into_result()?

// Event creation
let event = RawEventBuilder::new("source", "type", json!({"data": 1}))
    .with_host("myhost")
    .build();

// Testing with transaction isolation
#[sinex_test]
async fn test_x(ctx: TestContext) -> TestResult {
    // Use ctx.pool() not PgPool directly
    insert_event(ctx.pool(), &event).await?;
}
```

## Architecture Details

### EventSource trait
```rust
async fn initialize(ctx: EventSourceContext) -> Result<Self>
async fn stream_events(&mut self, tx: EventSender) -> Result<()>
```

### UnifiedCollector
- Single process coordinating all sources
- Loads config (TOML/NixOS format)
- Spawns EventSource tasks
- Validates events via JSON schemas
- Writes to `raw.events` or logs (dry-run)

### Database Tables
- `raw.events` - Hypertable, ULID primary key, ts_ingest from ULID
- `work_queue` - Processing queue with worker claiming
- `sinex_schemas.*` - Schema registry, agent manifests
- Connection: `postgresql:///sinex_dev?host=/run/postgresql`

### Workers
- Claim work via `SELECT FOR UPDATE SKIP LOCKED`
- Process events asynchronously
- Currently: promotion worker, health monitor

## Event Sources & Types

### Event Type System
Events are defined as Rust types implementing `EventType` trait:
```rust
pub struct FileCreated;
impl EventType for FileCreated {
    type Payload = FileCreatedPayload;
    type SourceImpl = FilesystemWatcher;
    const EVENT_NAME: &'static str = "file.created";
}
```

**NOTE**: EventRegistry is currently manually maintained in `create_registry()` but intended to be auto-generated from these types (see TODO comment).

### Naming Inconsistencies (Technical Debt)
- Sources mix dots/underscores: `terminal.kitty` vs `window_manager.hyprland`
- Event types inconsistent: `file.created` vs `shell.command.executed_atuin`
- Source names don't match crate names: `filesystem` vs `sinex-events-fs`

### Registered Sources
- `filesystem` - File system events
- `terminal.kitty` - Kitty terminal commands  
- `window_manager.hyprland` - Hyprland window manager
- `clipboard` - Clipboard content changes
- `ingestor.atuin_db_reader` - Atuin shell history
- `ingestor.shell_history_reader` - Shell history files
- `ingestor.asciinema_recorder` - Terminal recordings
- `ingestor.scrollback_capture` - Terminal scrollback
- `dbus.monitor` - D-Bus system events
- `journal.monitor` - Systemd journal events

### Event Types by Category
```
# Filesystem (3)
file.created, file.modified, file.deleted

# Terminal/Shell (6)
command.executed
shell.command.executed_atuin, shell.history.command
terminal.asciinema.session_started/ended
terminal.scrollback.captured, terminal.command_output.captured

# Window Manager (13)
window.{focused,opened,closed,moved,title_changed,urgent}
workspace.{changed,created,destroyed}
monitor.{focused,added,removed}
state.snapshot

# Clipboard (2)
clipboard.content.changed, clipboard.selection.changed

# D-Bus/System (12)
dbus.signal, dbus.method_call
system.notification, media.playback.changed
system.power.event, hardware.device.event
session.state.changed, security.policykit.authorization
bluetooth.device.event, network.connection.event
screen.saver.event, storage.mount.event

# Journal (2)
system.journal.entry, system.journal.sync
```

## Testing

### Test Structure
```
test/
├── unit/           # Component isolation tests
├── integration/    # Component interaction tests  
├── system/         # Full pipeline tests
├── common/         # Shared utilities (CRITICAL!)
```

### Test Patterns
```rust
// ALWAYS use #[sinex_test] for DB tests
#[sinex_test]
async fn test_event_insertion(ctx: TestContext) -> TestResult {
    let event = EventBuilder::filesystem()
        .path("/test.txt")
        .created()
        .build();
    
    assert_event_inserted_with_context(ctx.pool(), &event, "test_context").await?;
    ctx.wait_for_work_queue(0).await?;
    Ok(())
}
```

### Test Infrastructure
- **TestContext** - Shared DB pool (2000 connections!), transaction isolation
- **EventBuilder** - Fluent API for test events
- **Timing helpers** - `wait_for_*` functions for deterministic async tests
- **#[sinex_test]** - Wraps test in transaction for auto-rollback

### Running Tests
```bash
just test                     # All tests
just test-unit               # Unit only
just test-integration        # Integration only
just test-database           # DB-specific
just coverage                # With coverage report
cargo test -- --nocapture    # See output
```

## Deployment

### Pre-Flight Verification (7 phases)
1. Database connectivity
2. PostgreSQL extensions (pgx_ulid, pg_jsonschema, timescaledb)
3. Migration dry-run
4. Resource checks (disk, memory, CPU)
5. Config validation
6. Service binary checks
7. Integration tests

### NixOS Module
```nix
services.sinex = {
  enable = true;
  targetUser = "myuser";  # Required!
  
  database = {
    url = "postgresql:///sinex?host=/run/postgresql";
    poolSize = 25;
  };
  
  eventSources = {
    filesystem = true;
    terminal = true;
    windowManager = true;
    clipboard = true;
  };
  
  unifiedCollector = {
    enable = true;
    logLevel = "info";
    dryRun = false;
  };
  
  update = {
    enable = true;
    gracePeriod = 30;
    rollbackOnFailure = true;
  };
};
```

### Systemd Services
- `sinex-unified-collector.service` - Main collector
- `sinex-promo-worker.service` - Event processor
- `sinex-update.service` - Deployment with pre-flight
- `sinex-preflight.service` - Verification runner

### Deployment Flow
1. `systemctl restart sinex-update` triggers deployment
2. Pre-flight verification runs (7 phases)
3. If passes: graceful service restart
4. If fails: automatic rollback
5. Health monitoring throughout

## Advanced Features

- **ValidationChain** - Fluent validation with error accumulation
- **ErrorContext** - Rich errors with chaining
- **ChannelSenderExt** - Enhanced channels with monitoring
- **ConfigExtractor** - Type-safe config access
- **TestContext** - Shared DB pool, transaction isolation
- Schema registry with `EventRegistry`
- Schema validation via pg_jsonschema

See `spec/docs/claude/abstraction_usage_guide.md` for examples.

## Remember

This is a system capturing someone's entire digital life. Reliability is non-negotiable.