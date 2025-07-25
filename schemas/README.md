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

## Schema Management Workflow (GitOps)

### Current Implementation

1. **Development**: Schemas are maintained as JSON files in this directory
   - Each schema must have a unique `$id` field
   - Follow JSON Schema draft-07 specification
   
2. **CI/CD Pipeline**: `.github/workflows/schema-validation.yml`
   - Validates JSON syntax on every push/PR
   - Tests schema registry table functionality
   - Runs compatibility checks between versions
   
3. **Deployment**: `scripts/deploy-schemas.sh`
   - Syncs schemas from Git to `sinex_schemas.schema_registry` table
   - Handles version activation/deactivation
   - Idempotent - safe to run multiple times
   
4. **Compatibility Checking**: `scripts/check-schema-compatibility.sh`
   - Validates that new versions are backward compatible
   - Fails CI if breaking changes detected without version bump

### Schema Evolution Strategy

1. **Non-breaking changes** (add optional fields): Keep same version
2. **Breaking changes**: Create new version directory (v1 → v2)
3. **Deprecation**: Mark old versions as inactive but keep for reference
4. **Migration**: Document upgrade path in schema descriptions

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

## Technical Implementation Notes

### Schema Registry Table
Schemas are stored in `sinex_schemas.schema_registry` with:
- ULID primary keys for time-ordered identification
- Version tracking with activation flags
- JSON Schema definitions stored as JSONB

### Event Validation
Events reference schemas via `payload_schema_id` foreign key, enabling:
- Runtime validation of event payloads
- Schema evolution tracking
- Type safety across language boundaries

## Future Enhancements (Not Yet Implemented)

These features were considered but represent additional capabilities rather than core requirements:

### Schema Change Eventification
Automatically log schema changes as events in `core.events` for audit trail:
- Would use PostgreSQL trigger on schema registry table
- Create `sinex.schema.definition_changed` events
- Enable tracking schema evolution over time

### Automatic Code Generation
Generate type-safe structs/classes from JSON schemas:
- Rust: Generate structs with serde derives
- Python: Generate Pydantic models
- TypeScript: Generate interfaces
- Would ensure cross-language consistency

### Advanced Schema Tooling
- **Schema Diffing**: Visual/programmatic comparison between versions
- **Migration Scripts**: Auto-generate data migration code for breaking changes
- **Schema Analytics**: Usage metrics, validation failure patterns
- **Schema Composition**: Reference common definitions, inheritance patterns

These enhancements would add value but the current GitOps workflow provides a solid foundation for schema management.