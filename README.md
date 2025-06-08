# Sinex - Universal Data Capture and Query System

A personal data substrate that captures digital events across devices and modalities, providing a unified query interface for your digital memory.

## 🚀 Quick Start

```bash
# Enter development environment (database setup is automatic)
nix develop

# Run a simple test
cargo run --bin filesystem-ingestor -- --dry-run

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
│   └── sinex-worker/       # Event processing workers
├── ingestor/              # Event capture implementations
│   ├── filesystem/         # File system monitoring
│   ├── kitty/             # Terminal command capture
│   └── hyprland/          # Window manager events
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
# Build everything
cargo build --workspace

# Run all tests
cargo test

# Test specific component
cargo test --package filesystem-ingestor

# Continuous testing
cargo watch -x test

# Use just commands
just check    # Fast compile check
just test     # Run tests
just build    # Build all
```

### Running Ingestors

```bash
# Using just commands (recommended)
just filesystem    # Filesystem monitoring
just kitty        # Terminal capture (Kitty)
just hyprland     # Window manager events (Hyprland)
just worker       # Promotion worker

# Or run directly with cargo
cargo run --bin filesystem-ingestor
cargo run --bin kitty-ingestor -- --dry-run
cargo run --bin hyprland-ingestor -- --output-file events.json
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

Each ingestor can be configured via TOML files. Example configurations are in `config/`:

```bash
# Use custom config
cargo run --bin filesystem-ingestor -- --config config/filesystem/development.toml

# View current config
cargo run --bin hyprland-ingestor -- config
```

## 🧪 Testing Strategy

Tests are organized by category in `test/`:

- `database/` - Schema, migration, ULID tests
- `pipeline/` - Event processing and worker tests
- `agent/` - Agent manifest and heartbeat tests
- `reliability/` - Error handling and failure scenarios

```bash
# Run specific test category
cargo test --test database/

# See test output
cargo test -- --nocapture

# The test database is the same as dev database (sinex_dev)
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