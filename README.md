# Sinex - Universal Data Capture and Query System

A personal data substrate that captures digital events across devices and modalities, providing a unified query interface for your digital memory.

## 🚀 Quick Start

```bash
# Enter development environment
nix develop

# Initialize database
./scripts/db_reset.sh

# Run tests
./scripts/test_pipeline.sh

# Test with real data (ephemeral setup)
./scripts/full_system_test.sh auto

# Interactive testing with live monitoring
./scripts/full_system_test.sh interactive

# Check system status
./cli/exo.py query --limit 1
```

See the devShell banner for available commands.

## 🏗️ Architecture

Sinex follows a simple event-driven architecture:

1. **Ingestors** capture events from various sources
2. **Event substrate** stores all events in a universal schema
3. **CLI** provides querying and introspection capabilities

### Core Components

- **Database**: PostgreSQL with TimescaleDB for time-series data
- **Event Schema**: Universal `raw.events` table with JSONB payloads
- **Worker System**: Concurrent event processing with SKIP LOCKED
- **ULID Support**: Distributed-safe unique identifiers

## 📊 Event Format

All events follow a consistent structure:

```json
{
  "id": "01HKJM2Q3R4S5T6U7V8W9X0Y1Z",
  "source": "app.browser",
  "event_type": "page_loaded",
  "ts_ingest": "2024-01-15T10:30:00Z",
  "ts_orig": "2024-01-15T10:29:59Z",
  "host": "workstation-01",
  "payload": { /* event-specific data */ }
}
```

## 🛠️ Development

### Building

```bash
# Build all components
cargo build --all-features

# Run specific tests
cargo test --test migration_tests
cargo test --test ulid_integration_tests
```

### Database Migrations

```bash
# Run migrations
sqlx migrate run

# Create new migration
sqlx migrate add create_new_feature
```

### Testing

The project includes comprehensive testing at multiple levels:

#### Quick Testing
```bash
# Run all tests
./scripts/test_pipeline.sh

# Test specific components
cargo test --package sinex-shared
cargo test --test schema_validation_tests
```

#### Real-World Testing
```bash
# Ephemeral full system test (recommended)
./scripts/full_system_test.sh auto

# Interactive testing with live monitoring
./scripts/full_system_test.sh interactive

# Monitor production system
DATABASE_URL=... ./scripts/live_monitor.sh
```

#### Assumption Validation
```bash
# Detect configuration issues in production data
./scripts/diagnose_assumptions.sh

# Test resilience against common bugs
./scripts/chaos_test.sh

# Test with real system data sources
./scripts/real_world_test.sh
```

**Test Types:**
- Unit tests for validation logic
- Integration tests with real database operations  
- Assumption mismatch detection tests
- Real-time monitoring and validation
- Chaos testing for failure scenarios
- Performance and concurrency testing

## 🔧 Configuration

Configuration varies by component. Check component-specific README files for details:

- `ingestors/hyprland/README.md` - Window manager integration
- `ingestors/kitty/README.md` - Terminal integration
- `ingestors/filesystem/README.md` - File system monitoring

## 🏗️ NixOS Integration

```nix
{
  inputs.sinex.url = "github:sinity/sinex";
  imports = [ sinex.nixosModules.default ];
  
  services.sinex.enable = true;
}
```

## 📋 Project Status

See `CHANGELOG.md` for recent changes and `spec/` directory for detailed specifications.

### Key Features Implemented

- ✅ Universal event schema with ULID support
- ✅ TimescaleDB integration for time-series data
- ✅ JSON Schema validation for event payloads
- ✅ Concurrent worker processing
- ✅ Agent manifest system
- ✅ Comprehensive test suite
- ✅ CI/CD pipeline

## 📚 Documentation

- `spec/VISION.md` - High-level vision and goals
- `spec/docs/` - Technical implementation details
- `CLAUDE.md` - Development practices and invariants

## 📄 License

MIT License - see LICENSE file for details.