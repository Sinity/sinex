# Node-Side Schema Validation Implementation Plan

**Status:** ✅ Implemented
**Created:** 2025-01-15
**Completed:** 2025-01-17
**Priority:** Medium (optimization, not critical)

## Current Architecture

### How Validation Works Today

```
┌──────────┐         ┌─────────┐         ┌──────────┐
│node │────────▶│  NATS   │────────▶│ ingestd  │
│          │ events  │         │ events  │          │
└──────────┘         └─────────┘         └──────────┘
                                               │
                                               ▼
                                         Load schemas
                                         from DATABASE
                                               │
                                               ▼
                                         Validate events
                                               │
                          ┌────────────────────┴────────────────────┐
                          │                                         │
                          ▼                                         ▼
                     Valid events                            Invalid → DLQ
                     persist to DB                          (dead letter queue)
```

**Current Flow:**
1. **nodes**: Emit events with no validation
2. **NATS**: Transport all events (including invalid ones)
3. **ingestd**: Load schemas from database → validate → reject invalid to DLQ

**Problems:**
- Invalid events consume NATS bandwidth
- Validation errors discovered late (after network hop)
- No early feedback to event producers
- nodes that don't need DATABASE_URL can't validate

## Infrastructure Already in Place

### Schema Broadcast System ✅

**ingestd broadcasts schemas** to `system.schemas.active`:

```rust
// crate/core/sinex-ingestd/src/service.rs
async fn broadcast_active_schemas(
    validator: &EventValidator,
    nats_client: &NatsClient,
) -> IngestdResult<()> {
    let subject = env.nats_subject("system.schemas.active");
    let entries: Vec<SchemaBroadcastEntry> = validator
        .get_available_schemas()
        .into_iter()
        .map(|s| SchemaBroadcastEntry {
            name: s.name,
            version: (*s.version).clone(),
            schema_id: s.schema_id.to_string(),
        })
        .collect();

    let payload = serde_json::to_vec(&entries)?;
    js.publish(subject, payload.into()).await?;
}
```

**nodes subscribe** (always-on as of 2025-01-15):

```rust
// crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs
async fn maybe_start_schema_listener(
    transport: &EventTransport,
) -> nodeResult<Option<Arc<SchemaBroadcastCache>>> {
    let subject = env.nats_subject("system.schemas.active");
    let mut sub = client.subscribe(subject.clone()).await?;
    let cache = Arc::new(SchemaBroadcastCache::default());

    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            match serde_json::from_slice::<Vec<SchemaBroadcastEntry>>(&msg.payload) {
                Ok(entries) => {
                    cache_clone.update(entries).await;
                },
                Err(err) => warn!("Failed to decode schema broadcast"),
            }
        }
    });

    Ok(Some(cache))
}
```

**Cache structure:**

```rust
// crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs
#[derive(Clone, Debug, Default)]
pub struct SchemaBroadcastCache {
    schemas: Arc<RwLock<Vec<SchemaBroadcastEntry>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaBroadcastEntry {
    pub name: String,           // "fs-watcher.file.created"
    pub version: String,        // "1.0.0"
    pub schema_id: String,      // ULID
}
```

## What's Missing

### Problem: Schema Broadcasts Don't Include Schema JSON

**Current broadcast** only has metadata:
```json
{
  "name": "fs-watcher.file.created",
  "version": "1.0.0",
  "schema_id": "01HV7GQZJ7XKQM5V8PQWXY9ZAB"
}
```

**Need for validation**: Full JSON Schema document:
```json
{
  "schema_id": "01HV7GQZJ7XKQM5V8PQWXY9ZAB",
  "source": "fs-watcher",
  "event_type": "file.created",
  "version": "1.0.0",
  "schema_json": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["path", "size"],
    "properties": {
      "path": { "type": "string" },
      "size": { "type": "integer" }
    }
  }
}
```

### What Needs to Be Built

#### 1. Schema Storage in NATS KV (Option A: Recommended)

**Create KV bucket**: `KV_sinex_schemas`

```rust
// Schema storage key format:
// KV_sinex_schemas["schema:{schema_id}"] = full schema JSON
```

**ingestd publishes schemas** to KV:
```rust
async fn store_schemas_in_kv(
    validator: &EventValidator,
    js: &JetStream,
) -> IngestdResult<()> {
    let kv = js.get_key_value("KV_sinex_schemas").await?;

    for schema_info in validator.get_available_schemas() {
        let key = format!("schema:{}", schema_info.schema_id);
        let schema_json = fetch_schema_json_from_db(&schema_info.schema_id).await?;
        kv.put(&key, schema_json.into()).await?;
    }
}
```

**nodes fetch on demand**:
```rust
impl SchemaBroadcastCache {
    async fn fetch_schema_json(
        &self,
        kv: &Store,
        schema_id: &str,
    ) -> Result<JsonValue> {
        let key = format!("schema:{}", schema_id);
        let entry = kv.get(&key).await?;
        Ok(serde_json::from_slice(&entry)?)
    }
}
```

#### 2. Schema Storage via RPC (Option B: Alternative)

**Add gateway endpoint**:
```rust
// GET /schemas/{schema_id}
// Returns: full schema JSON

async fn get_schema(
    Extension(pool): Extension<PgPool>,
    Path(schema_id): Path<Ulid>,
) -> Result<Json<SchemaDocument>, StatusCode> {
    let schema = sqlx::query_as!(
        SchemaDocument,
        "SELECT * FROM core.event_schemas WHERE id = $1",
        schema_id
    )
    .fetch_one(&pool)
    .await?;

    Ok(Json(schema))
}
```

**nodes fetch via HTTP**:
```rust
impl SchemaBroadcastCache {
    async fn fetch_schema_via_rpc(
        &self,
        gateway_url: &str,
        schema_id: &str,
    ) -> Result<JsonValue> {
        let url = format!("{}/schemas/{}", gateway_url, schema_id);
        let resp = reqwest::get(&url).await?;
        Ok(resp.json().await?)
    }
}
```

#### 3. nodeSchemaValidator Implementation

**New file**: `crate/lib/sinex-node-sdk/src/schema_validator.rs`

```rust
use ahash::AHashMap;
use jsonschema::JSONSchema;
use parking_lot::RwLock;
use sinex_core::JsonValue;
use std::sync::Arc;

/// Compiled schema cache entry
#[derive(Clone)]
struct CompiledSchema {
    schema_id: String,
    source: String,
    event_type: String,
    version: String,
    validator: Arc<JSONSchema>,
}

/// Schema validator for nodes (uses NATS broadcasts, not DB)
pub struct nodeSchemaValidator {
    /// Compiled schemas by schema_id
    schemas: Arc<RwLock<AHashMap<String, CompiledSchema>>>,
    /// Lookup: (source, event_type) → schema_id
    lookup: Arc<RwLock<AHashMap<(String, String), String>>>,
}

impl nodeSchemaValidator {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(AHashMap::new())),
            lookup: Arc::new(RwLock::new(AHashMap::new())),
        }
    }

    /// Update cache from broadcast + fetch full schemas from KV/RPC
    pub async fn update_from_broadcast(
        &mut self,
        entries: Vec<SchemaBroadcastEntry>,
        kv: &async_nats::jetstream::kv::Store,
    ) -> Result<usize> {
        let mut new_schemas = AHashMap::new();
        let mut new_lookup = AHashMap::new();
        let mut compiled = 0;

        for entry in entries {
            // Fetch full schema JSON from NATS KV
            let key = format!("schema:{}", entry.schema_id);
            let schema_json = match kv.get(&key).await {
                Ok(Some(entry)) => {
                    serde_json::from_slice::<JsonValue>(&entry)?
                }
                _ => {
                    warn!("Schema {} not found in KV", entry.schema_id);
                    continue;
                }
            };

            // Compile JSON schema validator
            let validator = match JSONSchema::compile(&schema_json) {
                Ok(v) => Arc::new(v),
                Err(e) => {
                    warn!("Failed to compile schema {}: {}", entry.schema_id, e);
                    continue;
                }
            };

            // Parse source.event_type from name
            let parts: Vec<&str> = entry.name.split('.').collect();
            if parts.len() < 2 {
                warn!("Invalid schema name format: {}", entry.name);
                continue;
            }
            let source = parts[0].to_string();
            let event_type = parts[1..].join(".");

            let compiled_schema = CompiledSchema {
                schema_id: entry.schema_id.clone(),
                source: source.clone(),
                event_type: event_type.clone(),
                version: entry.version,
                validator,
            };

            new_schemas.insert(entry.schema_id.clone(), compiled_schema);
            new_lookup.insert((source, event_type), entry.schema_id);
            compiled += 1;
        }

        *self.schemas.write() = new_schemas;
        *self.lookup.write() = new_lookup;

        info!(compiled, "Updated node schema cache");
        Ok(compiled)
    }

    /// Validate event payload
    pub fn validate(
        &self,
        source: &str,
        event_type: &str,
        payload: &JsonValue,
    ) -> Result<()> {
        // Lookup schema_id
        let schema_id = {
            let lookup = self.lookup.read();
            match lookup.get(&(source.to_string(), event_type.to_string())) {
                Some(id) => id.clone(),
                None => {
                    // No schema registered - allow (same as ingestd)
                    return Ok(());
                }
            }
        };

        // Get compiled validator
        let validator = {
            let schemas = self.schemas.read();
            match schemas.get(&schema_id) {
                Some(s) => s.validator.clone(),
                None => return Ok(()), // Should not happen
            }
        };

        // Validate payload
        if let Err(errors) = validator.validate(payload) {
            let error_messages: Vec<String> = errors
                .map(|e| e.to_string())
                .collect();

            return Err(nodeError::Validation(
                format!(
                    "Schema validation failed for {}.{}: {}",
                    source,
                    event_type,
                    error_messages.join("; ")
                )
            ));
        }

        Ok(())
    }

    pub fn schema_count(&self) -> usize {
        self.schemas.read().len()
    }
}
```

#### 4. Integration with EventEmitter

**Modify**: `crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs`

```rust
#[derive(Clone)]
pub struct EventEmitter {
    sender: Arc<EventSender>,
    dry_run: bool,
    validator: Option<Arc<nodeSchemaValidator>>, // NEW
}

impl EventEmitter {
    pub fn new(
        sender: EventSender,
        dry_run: bool,
        validator: Option<Arc<nodeSchemaValidator>>, // NEW
    ) -> Self {
        Self {
            sender: Arc::new(sender),
            dry_run,
            validator,
        }
    }

    pub async fn emit(&self, event: Event<JsonValue>) -> Result<(), nodeError> {
        // Validate before emitting (if validator present)
        if let Some(validator) = &self.validator {
            validator.validate(
                &event.source.to_string(),
                &event.event_type.to_string(),
                &event.payload,
            )?;
        }

        let event_type = event.event_type.clone();
        if self.dry_run {
            info!(
                source = %event.source,
                event_type = %event_type,
                "DRY RUN: Would emit event"
            );
            return Ok(());
        }

        self.sender
            .send(event)
            .await
            .map_err(|_| nodeError::Processing("Event channel closed".to_string()))
    }
}
```

#### 5. Wire Up in StreamProcessorRunner

**Modify**: `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs`

```rust
// After starting schema listener:
let schema_cache = maybe_start_schema_listener(&transport).await?;

// Create validator if schema cache present
let validator = if let Some(cache) = &schema_cache {
    let kv = create_checkpoint_kv(&transport).await?; // Reuse or separate KV
    let mut validator = nodeSchemaValidator::new();

    // Initial load from current cache
    let current_schemas = cache.get().await;
    if !current_schemas.is_empty() {
        validator.update_from_broadcast(current_schemas, &kv).await?;
    }

    // Background updater
    let cache_clone = cache.clone();
    let validator_clone = Arc::new(RwLock::new(validator));
    let validator_for_task = validator_clone.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let schemas = cache_clone.get().await;
            let mut v = validator_for_task.write();
            if let Err(e) = v.update_from_broadcast(schemas, &kv).await {
                warn!("Failed to update node validator: {}", e);
            }
        }
    });

    Some(Arc::new(validator_clone.read().clone()))
} else {
    None
};

// Pass validator to EventEmitter
let event_emitter = EventEmitter::new(event_sender_raw.clone(), dry_run, validator);
```

## Implementation Phases

### Phase 1: Schema Storage (1-2 days)

**Choose**: NATS KV (recommended) or RPC endpoint

**Tasks:**
1. Create `KV_sinex_schemas` bucket in ingestd startup
2. Modify `broadcast_active_schemas()` to also store full schemas in KV
3. Add KV fetch methods to `SchemaBroadcastCache`
4. Test: Verify schemas appear in KV bucket

### Phase 2: Validator Implementation (1 day)

**Tasks:**
1. Create `crate/lib/sinex-node-sdk/src/schema_validator.rs`
2. Implement `nodeSchemaValidator` with:
   - Schema compilation (jsonschema crate)
   - Lookup maps
   - Validation logic
3. Add tests with valid/invalid payloads
4. Test: Unit tests for validation

### Phase 3: Integration (1 day)

**Tasks:**
1. Modify `EventEmitter` to accept optional validator
2. Wire up validator in `StreamProcessorRunner`
3. Add background update task (every 60s)
4. Test: Integration test with schema broadcasts

### Phase 4: Testing & Verification (1 day)

**Tasks:**
1. End-to-end test: node validates before emit
2. Verify invalid events rejected at source
3. Performance test: Validation overhead acceptable
4. Test: No DATABASE_URL needed for validation

## Benefits

**Performance:**
- Reduce NATS bandwidth (invalid events filtered early)
- Reduce ingestd load (fewer events to validate)

**Developer Experience:**
- Immediate feedback on schema errors
- Errors appear in node logs (closer to source)

**Architecture:**
- True edge capability (validation without DB)
- Consistent validation (same schemas as ingestd)

## Tradeoffs

**Complexity:**
- More code in node SDK
- Background updater task
- Schema fetch mechanism (KV or RPC)

**Latency:**
- Schema updates take 60s to propagate
- Initial node startup needs schema fetch

**Reliability:**
- Schema cache can be stale
- Fallback: ingestd still validates (defense in depth)

## Decision: When to Implement

**Implement if:**
- NATS bandwidth is a bottleneck
- Need faster feedback on schema errors
- Many nodes run without DATABASE_URL

**Defer if:**
- Current validation in ingestd is sufficient
- NATS bandwidth not a concern
- Prefer simpler node SDK

## Current Status

**Infrastructure:** ✅ Schema broadcasts working, cache populating
**Validation:** ✅ Fully implemented
**Status:** Complete - all phases implemented

### What's Implemented

1. **NATS KV Schema Storage** (`crate/core/sinex-ingestd/src/service.rs`)
   - `store_schemas_in_kv()` stores full schema JSON in `KV_sinex_schemas` bucket
   - Triggered on schema broadcasts every 5 minutes

2. **NodeSchemaValidator** (`crate/lib/sinex-node-sdk/src/schema_validator.rs`)
   - Edge mode (no DB) and Full mode (with DB fallback)
   - Compiles JSON schemas using `jsonschema` crate
   - Cache-first validation strategy

3. **EventEmitter Integration** (`crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs`)
   - `with_validator()` constructor for validation-enabled emitter
   - Validates payload before sending to NATS

4. **StreamProcessorRunner Wiring** (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs`)
   - `maybe_start_schema_listener()` creates cache + validator
   - Background task updates validator on schema broadcasts
   - Validator passed to EventEmitter

---

**Last Updated:** 2025-01-17
**Related Files:**
- `crate/lib/sinex-node-sdk/src/schema_validator.rs` (NodeSchemaValidator)
- `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs` (cache listener + wiring)
- `crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs` (EventEmitter integration)
- `crate/core/sinex-ingestd/src/service.rs` (schema broadcasts + KV storage)
