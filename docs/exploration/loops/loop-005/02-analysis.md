# Loop 005 - Schema Broadcast Cache Lifecycle

Scope
- Ingestd schema broadcast emission.
- Node runtime subscription and cache/validator updates.

Lifecycle Map

1) Ingestd emits schema broadcasts
- Emission on startup if NATS + DB available.
  - `crate/core/sinex-ingestd/src/service.rs` calls `broadcast_active_schemas()` during initialization.
- Periodic emission on schema reload (every 5 minutes).
  - `crate/core/sinex-ingestd/src/service.rs` `start_schema_reload_task()` ticks every 300s and calls `broadcast_active_schemas()`.
- Broadcast content is metadata only; full schema JSON is stored in NATS KV.
  - `broadcast_active_schemas()` stores schemas in KV via `store_schemas_in_kv()` and publishes metadata to `system.schemas.active`.

2) Node runtime subscribes to broadcasts
- Subscription created during stream processor initialization if NATS and KV are available.
  - `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs` `maybe_start_schema_listener()` subscribes to `system.schemas.active` and opens `KV_sinex_schemas`.
- Listener updates a metadata cache and a schema validator.
  - `SchemaBroadcastCache::update()` stores metadata entries.
  - `NodeSchemaValidator::update_from_broadcast()` fetches schema JSON from KV and compiles validators.

3) Event emission is gated by validator presence
- Event emitter validates payloads when a validator is available.
  - `crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs` `EventEmitter::emit()` calls `NodeSchemaValidator::validate()`.
- In edge mode (the default in `maybe_start_schema_listener()`), missing schemas are hard failures.
  - `crate/lib/sinex-node-sdk/src/schema_validator.rs` returns `Schema not available in cache` in edge mode.

Cache Usage Observations
- `SchemaBroadcastCache` is exposed via `NodeHandles::schema_cache()` but only referenced in tests.
  - Search shows no production usage beyond creation and storage; only `crate/lib/sinex-node-sdk/tests/edge_mode_test.rs` reads it.
- Cache updates replace all entries atomically.
  - `NodeSchemaValidator::update_from_broadcast()` rebuilds the entire cache each broadcast.

Failure Modes
- If a node starts after the last broadcast, it may have no schema cache until the next periodic broadcast.
  - Broadcasts are published to a normal subject (not retained), and subscription does not replay history.
- If `KV_sinex_schemas` is missing, schema validation is skipped entirely (edge mode) with no retries.
  - `maybe_start_schema_listener()` returns `(None, None)` on KV open failure.
- If KV fetch fails for a specific schema ID, that schema is skipped from the cache.
  - `NodeSchemaValidator::update_from_broadcast()` logs and continues per entry.

Findings
- Schema broadcasts are periodic and metadata-only; nodes must combine them with KV fetches to compile validators.
- Validation is strict in edge mode, which can block event emission until a broadcast is received.
- `SchemaBroadcastCache` is currently unused in production code (exposed but not consumed).

Risks
- Nodes can fail to emit events for up to the broadcast interval if they start before receiving a schema broadcast.
- Lost broadcasts or KV availability issues can disable validation or cause repeated schema-missing errors.

Opportunities
- Consider a startup fetch path that reads KV directly without waiting for a broadcast.
- Consider making schema validation soft-fail or configurable during startup to avoid blocking event emission.
