# Sinex - Universal Data Capture and Query System

A personal data substrate that captures digital events across devices and modalities, providing a unified query interface for your digital memory.

## 🚀 Quick Start

```bash
# Enter development environment (database setup is automatic)
nix develop

# Run a simple test
cargo run --bin unified-collector -- --dry-run

# Query captured events
./cli/exo.py query --limit 10
```

## 🏗️ Architecture

Sinex follows an event-driven architecture with three layers:

1. **Ingestors** - Capture events from various sources (filesystem, terminals, window managers)
2. **Event Substrate** - PostgreSQL + TimescaleDB stores immutable events with ULID keys
3. **Query Interface** - Python CLI for exploring and analyzing captured data

### Project Structure

```
sinex/
├── crate/                  # Core Rust libraries
│   ├── sinex-core/         # Common types (RawEvent, errors)
│   ├── sinex-db/           # Database models and pooling
│   ├── sinex-ulid/         # ULID implementation
│   ├── sinex-worker/       # Event processing workers
│   └── sinex-events/       # Event type definitions
├── ingestor/              # Event capture implementations
│   ├── shared/             # Shared utilities
│   └── unified-collector/  # Unified multi-source collector
├── config/                 # Example configurations
├── test/                  # Organized test suites
└── cli/                    # Python query tools
```

## 📊 Event Format

All events follow a universal structure:

```json
{
  "id": "01HKJM2Q3R4S5T6U7V8W9X0Y1Z",
  "source": "filesystem",
  "event_type": "file.created",
  "ts_ingest": "2024-01-15T10:30:00Z",
  "ts_orig": "2024-01-15T10:29:59Z",
  "host": "workstation-01",
  "payload": { /* event-specific data */ }
}
```

## 🛠️ Development

### Building & Testing

```bash
just check    # Fast compile check
just test     # Run all tests
just build    # Build everything
just watch    # Continuous testing
```

### Running Ingestors

```bash
# Run the unified collector
just unified                # Run unified collector
just worker                 # Run promotion worker

# Run with options
just unified --dry-run      # Test without database writes
just unified --output-file events.json  # Output to file
just unified --config config/unified-collector.toml

# Manage all ingestors
just ingestors-start        # Start all in background
just ingestors-start --dry-run  # Start all in dry-run mode
just ingestors-stop         # Stop all running ingestors
```

### Database Operations

The database (`sinex_dev`) is automatically created and migrations applied when entering the nix shell.

```bash
# Apply migrations manually if needed
just migrate

# Create new migration
just migrate-create feature_name

# Direct database connection
just psql

# Update SQLX cache after query changes
just sqlx-prepare
```

## 🔧 Configuration

The unified collector automatically loads configuration from (in priority order):
1. `unified-collector.toml` in current directory
2. `~/.config/unified-collector.toml`
3. Built-in defaults (uses DATABASE_URL from environment)

```bash
# Use custom config file
just unified --config my-config.toml

# Configuration is logged at startup
just unified
# [INFO] Configuration loaded:
# [INFO]   Database URL: postgresql:///sinex_dev?host=/run/postgresql
# [INFO]   Enabled sources: [filesystem, terminal, window_manager]
# ...
```

## 🧪 Testing Strategy

Tests are organized by category in `test/`:

- `database/` - Schema, migration, ULID tests
- `pipeline/` - Event processing and worker tests
- `agent/` - Agent manifest and heartbeat tests
- `reliability/` - Error handling and failure scenarios

```bash
just test                    # Run all tests
just test -- --test database/  # Run specific test category  
just test -- --nocapture     # See test output
```

## 🏗️ Key Patterns

### SimpleIngestor Pattern
All ingestors implement a simple trait that focuses only on event capture:

```rust
impl SimpleIngestor for MyIngestor {
    fn name() -> &'static str { "my-ingestor" }
    fn version() -> &'static str { env!("CARGO_PKG_VERSION") }
    async fn capture_events(&mut self, event_tx: mpsc::Sender<RawEvent>) -> Result<()> {
        // Capture logic only - runtime handles lifecycle
    }
}
```

### Database Patterns
- ULID primary keys for time-ordered, distributed-safe IDs
- Immutable events in `raw.events` hypertable
- Concurrent processing with `SELECT FOR UPDATE SKIP LOCKED`
- JSON Schema validation for event payloads

## 📚 Documentation

- `CLAUDE.md` - Development practices and project memory
- `spec/SADI.md` - Documentation index
- `spec/STAD.md` - System architecture
- `spec/docs/tims/` - Implementation details

## 🏗️ NixOS Integration

```nix
{
  services.sinex = {
    enable = true;
    systemUser = "username";
    database = { name = "sinex"; user = "sinex"; };
    ingestors.hyprland.enable = true;
  };
}
```

## 📄 License

MIT License - see LICENSE file for details.