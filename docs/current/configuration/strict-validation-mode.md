# Strict Validation Mode

## Overview

Strict validation mode is an optional configuration in `sinex-ingestd` that enforces schema requirements for all incoming events. When enabled, events without registered schemas are rejected at ingestion time.

## Purpose

By default, Sinex follows a flexible approach:

- Events with registered schemas are validated against those schemas
- Events without schemas are accepted but skipped from validation
- This allows rapid prototyping and external event ingestion where schemas evolve independently

Strict mode changes this behavior:

- ALL events MUST have a registered schema
- Events without schemas are rejected with a validation error
- Ensures consistency and prevents schema drift in production environments

## Configuration

### Environment Variable

```bash
export SINEX_INGESTD_STRICT_VALIDATION=true
```

### Configuration File

In `ingestd.toml` or `/etc/sinex/ingestd.toml`:

```toml
strict_validation = true
```

### Default Value

**Default: `false`** (permissive mode - allows schema-less events)

## Behavior

### Permissive Mode (default: `strict_validation = false`)

```
Event arrives → Has schema? ─┬─ Yes → Validate against schema
                             └─ No  → Accept (no validation)
```

- Events without schemas: **ACCEPTED**
- Validation stats: Counted as `no_schema`
- Use case: Development, prototyping, external event sources

### Strict Mode (`strict_validation = true`)

```
Event arrives → Has schema? ─┬─ Yes → Validate against schema
                             └─ No  → **REJECT** with error
```

- Events without schemas: **REJECTED**
- Error message: `Strict validation enabled: event has no registered schema (source=X, event_type=Y)`
- Use case: Production environments, enforcing data contracts

## Error Handling

When an event is rejected in strict mode:

```json
{
  "error": "validation",
  "message": "Strict validation enabled: event has no registered schema (source=fs-watcher, event_type=file.created)",
  "context": {
    "strict_mode": "enabled"
  },
  "operation": "jetstream_consumer.validate_event"
}
```

The event is:

1. **Not persisted** to the database
2. **Not acknowledged** in NATS (will be retried unless max_deliver is reached)
3. Logged as a validation failure

## Observability

### Metrics

Validation statistics are available via `sinex-ingestd` metrics:

```rust
ValidationStats {
    valid: 10250,           // Passed validation
    skipped: 0,             // Validation disabled
    no_schema: 0,           // Would be non-zero in permissive mode
    schema_not_found: 12,   // Schema ID referenced but not found
    invalid: 8,             // Failed schema validation
}
```

In strict mode, `no_schema` should always be **0** (events are rejected before stats are recorded).

### Logs

```
WARN sinex_ingestd::jetstream_consumer: Validation failed for event
  source="custom-ingestor"
  event_type="custom.event"
  error="Strict validation enabled: event has no registered schema"
```

## Use Cases

### When to Enable Strict Mode

✅ **Enable strict validation when:**

- Running in production environments
- Enforcing data contracts between services
- All event types have well-defined schemas
- You want to catch schema drift early
- Regulatory compliance requires schema validation

### When to Keep Permissive Mode

✅ **Keep permissive mode (default) when:**

- Rapid prototyping with evolving event types
- Ingesting events from external sources with independent schema evolution
- Development and testing environments
- Gradual schema rollout (some events have schemas, others pending)
- You want to observe event patterns before defining schemas

## Schema Registration

Before enabling strict mode, ensure all event types have registered schemas:

### Check Current Schema Coverage

```sql
-- Count events by source/type with/without schemas
SELECT
    source,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT payload_schema_id) as schema_count
FROM core.events
GROUP BY source, event_type
HAVING COUNT(DISTINCT payload_schema_id) = 0;
```

### Register Missing Schemas

```rust
use sinex_db::repositories::schema_management::{NewEventSchema, SchemaManagementRepository};

let schema = NewEventSchema {
    source: "my-ingestor".to_string(),
    event_type: "my.event".to_string(),
    schema_version: "v1".to_string(),
    schema_content: serde_json::json!({
        "type": "object",
        "properties": {
            "field1": {"type": "string"},
            "field2": {"type": "number"}
        },
        "required": ["field1"]
    }),
};

pool.schemas().register_schema(schema).await?;
```

## Integration with Schema Validation

Strict mode works in conjunction with `validate_schemas`:

| `strict_validation` | `validate_schemas` | Behavior |
|---------------------|-------------------|----------|
| `false` | `false` | Accept all events, no validation |
| `false` | `true` | Validate events with schemas, accept schema-less events |
| `true` | `false` | Reject schema-less events, accept all events with schemas (no validation) |
| `true` | `true` | Reject schema-less events, validate all others against schemas |

**Recommended production configuration:**

```toml
strict_validation = true
validate_schemas = true
```

## Rollout Strategy

### Gradual Rollout

1. **Development:** Start with both disabled

   ```toml
   strict_validation = false
   validate_schemas = false
   ```

2. **Staging:** Enable validation, keep permissive

   ```toml
   strict_validation = false
   validate_schemas = true
   ```

   - Monitor `no_schema` counts
   - Register schemas for all event types

3. **Production:** Enable strict mode

   ```toml
   strict_validation = true
   validate_schemas = true
   ```

### Monitoring During Rollout

```bash
# Watch validation stats
watch -n 1 "curl -s http://localhost:8080/admin/validation/stats | jq"

# Check for events without schemas (should decrease to 0)
watch -n 5 "psql -c 'SELECT COUNT(*) FROM core.events WHERE payload_schema_id IS NULL'"
```

## Related Configuration

- `validate_schemas`: Enable/disable schema validation (default: `true`)
- `skip_schema_sync`: Skip GitOps schema synchronization on startup (default: `false`)
- Schema cache: `sinex-db` provides a `SchemaCacheRepository` for centralized schema lookups with in-memory caching

## See Also

- [System Operations And Integrity Architecture](../architecture/SystemOperations_And_Integrity_Architecture.md)
- [Schema GitOps Workflow](../workflows/schema-gitops.md)
- [Environment Variables Reference](./environment-variables.md)
