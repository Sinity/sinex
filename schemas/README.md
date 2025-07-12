# Sinex Event Schemas

This directory contains the canonical JSON Schema definitions for all Sinex event payloads.

## Directory Structure

```
schemas/
├── v1/                     # Version 1 schemas
│   ├── common/            # Common definitions used across schemas
│   │   └── provenance.json
│   ├── filesystem/        # File system event schemas
│   │   ├── file_created.json
│   │   ├── file_modified.json
│   │   ├── file_deleted.json
│   │   ├── file_moved.json
│   │   ├── dir_created.json
│   │   └── dir_deleted.json
│   ├── shell/             # Shell/terminal event schemas
│   │   ├── command_executed.json
│   │   ├── command_completed.json
│   │   ├── session_started.json
│   │   └── session_ended.json
│   ├── clipboard/         # Clipboard event schemas
│   │   ├── content_copied.json
│   │   └── content_selected.json
│   ├── window_manager/    # Window manager event schemas
│   │   ├── window_opened.json
│   │   ├── window_closed.json
│   │   ├── window_focused.json
│   │   └── workspace_switched.json
│   ├── system/            # System event schemas
│   │   ├── journal_entry.json
│   │   └── state_changed.json
│   ├── scan/              # Scanner event schemas
│   │   ├── scan_started.json
│   │   └── scan_completed.json
│   └── process/           # Process lifecycle schemas
│       ├── process_started.json
│       ├── process_heartbeat.json
│       └── process_shutdown.json
└── v2/                     # Version 2 schemas (backward-incompatible changes)
```

## Schema Management Workflow

1. **Development**: Schemas are generated from Rust structs in `sinex-events` crate
2. **Generation**: CI pipeline runs schema generation on every commit
3. **Validation**: Generated schemas are validated against JSON Schema meta-schema
4. **Compatibility**: Breaking changes require a new major version (v1 → v2)
5. **Deployment**: Schemas are synced to PostgreSQL `sinex_schemas.event_payload_schemas` table

## Schema Versioning

- Schemas follow semantic versioning (e.g., `1.0.0`, `1.1.0`, `2.0.0`)
- Breaking changes require a major version bump and new directory (e.g., `v1/` → `v2/`)
- Non-breaking additions (new optional fields) increment minor version
- Bug fixes increment patch version

## Usage

### For Rust Developers
Schemas are automatically generated from structs with `#[derive(JsonSchema)]` in the `sinex-events` crate.

### For Python Plugin Developers
Reference these JSON files directly to understand the expected event payload structure.

### For Database Validation
Schemas are loaded into PostgreSQL and used for runtime validation via `pg_jsonschema`.