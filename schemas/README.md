# Sinex Event Schemas

> **Source of Truth:** Rust `EventPayload` structs (via `derive(EventPayload)`) define every schema.  
> JSON files under `schemas/` are generated artifacts for GitOps distribution and downstream clients—do **not** edit them by hand.  
> Regenerate them with `./scripts/schema-dev.sh generate` (CI enforces this, similar to `cargo fmt`).

## Directory Structure

```
schemas/
├── v1/
│   ├── registry.json              # Metadata (source, event_type, version, hash)
│   ├── fs-watcher/                # One directory per EventPayload::SOURCE
│   │   ├── file.created.json      # Files are named after EVENT_TYPE
│   │   └── ...
│   ├── canonical.terminal/
│   ├── document-ingestor/
│   └── ...
└── (future versions live beside v1/)
```

## Schema Management Workflow (GitOps)

### Current Implementation

1. **Development**: Schemas are generated directly from the Rust `EventPayload`
   implementations via the `sinex-schema` CLI (see below). Each run rewrites
   `schemas/v1/<source>/<event>.json` plus the accompanying `registry.json`.  
   _Reminder: treat those files as generated output—run the generator rather than editing JSON manually._
   
2. **CI/CD Pipeline**: `.github/workflows/schema-validation.yml`
   - Validates JSON syntax on every push/PR
   - Tests schema registry table functionality
   - Runs compatibility checks between versions
   
3. **Deployment**: `scripts/deploy-schemas.sh`
   - Syncs schemas from Git to `sinex_schemas.event_payload_schemas` table
   - Handles version activation/deactivation
   - Idempotent - safe to run multiple times
   
4. **Compatibility Checking**: `scripts/check-schema-compatibility.sh`
   - Diffs JSON schemas against the base branch and invokes
     `sinex-schema validate` for structural comparisons.

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
Use the helper script to regenerate schemas whenever an `EventPayload` changes:

```bash
./scripts/schema-dev.sh generate
```

Pass `DATABASE_URL=... ./scripts/schema-dev.sh deploy` to push the freshly
generated schemas into Postgres.

### For Python Plugin Developers
Reference these JSON files directly to understand the expected event payload structure.

### For Database Validation
Schemas are loaded into PostgreSQL and used for runtime validation via `pg_jsonschema`.

## Technical Implementation Notes

### Schema Registry Table
Schemas are stored in `sinex_schemas.event_payload_schemas` with:
- ULID primary keys for time-ordered identification
- `source`, `event_type`, and `schema_version` columns that uniquely identify a contract
- JSON Schema definitions stored as JSONB, plus a SHA-256 content hash to detect drift

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
  ```bash
  # Conceptual tool
  sinex-schema diff v1.0 v2.0 --event-type desktop.window_focused
  # Output: Breaking changes detected, migration script generated
  ```
- **Migration Scripts**: Auto-generate data migration code for breaking changes
  - SQL migrations for data transformation (v1.0-to-v1.1.sql)
  - Validation of migrated data against new schema
- **Schema Analytics**: Usage metrics, validation failure patterns
  - Track which schemas are most used
  - Identify common validation errors
  - Visualize schema evolution over time
- **Schema Composition**: Reference common definitions, inheritance patterns
  - Cross-schema reference validation
  - Custom validation functions
  - Conditional schema selection based on event source

### Multi-Tenant Schema Registry (Future Distributed Architecture)
For potential future distributed deployments:
- Per-tenant schema overrides
- Schema federation across instances
- Global vs local schema namespaces

### Integration Points (Planned)
- **OpenAPI spec generation**: Export schemas as OpenAPI definitions
- **GraphQL schema derivation**: Generate GraphQL types from JSON schemas
- **Protocol buffer compatibility**: Bridge to protobuf for binary protocols

These enhancements would add value but the current GitOps workflow provides a solid foundation for schema management.
