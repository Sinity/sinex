# Loop 013 - Schema Generation Pipeline vs Telemetry Coverage

Scope
- Schema generation tooling (`sinex-schema` CLI) and registry (`schema_registry`).
- Telemetry payload definitions in `sinex-core`.
- Output bundle structure under `schemas/v1`.

Pipeline Summary
1) `sinex-core/src/types/events/schema_registry.rs`
   - Collects all `EventPayload` types via `inventory`.
   - `generate_all_schemas()` returns `(source, event_type, version) -> schema`.
2) `sinex-core/src/types/bin/sinex-schema.rs`
   - `generate_schemas()` writes schemas to `schemas/v1/<source>/<event_type>.json`.
   - Uses `sanitize_component` to make filesystem-safe paths.
   - Writes `registry.json` and optionally syncs to DB.

Telemetry Payload Registration
- Metrics payloads in `crate/lib/sinex-core/src/types/events/payloads/metrics.rs` are annotated with `#[derive(EventPayload)]` and a `#[event_payload(source = "...", event_type = "...")]` attribute.
- This implies they are registered in inventory and should be emitted by `generate_all_schemas()`.

Expected Bundle Layout (examples)
- `schemas/v1/sinex/metric.counter.json`
- `schemas/v1/sinex/metric.gauge.json`
- `schemas/v1/sinex/metric.histogram.json`
- `schemas/v1/sinex/health.status.json`
- `schemas/v1/sinex.gateway/request.stats.json`
- `schemas/v1/sinex.gateway/rate_limit.exceeded.json`
- `schemas/v1/sinex.gateway/replay.stats.json`
- `schemas/v1/sinex.ingestd/stream.stats.json`
- `schemas/v1/sinex.ingestd/assembly.stats.json`
- `schemas/v1/sinex.node/processing.stats.json`

Observed Bundle Contents
- `schemas/v1` does not contain any of the expected telemetry schema files.
- The only `schemas/v1/sinex` match is `process.heartbeat.json`.

Findings
- The schema generator would emit telemetry schemas if run against the current codebase.
- The absence of telemetry schemas in `schemas/v1` suggests the bundle is stale or the generator has not been run since telemetry payloads were added.
- No code-level evidence indicates telemetry payloads are excluded from schema generation.

Risks
- Schema bundles in-repo are out of sync with current `EventPayload` types, which undermines schema validation and DB schema sync.
- Internal telemetry events will continue to bypass schema validation unless schemas are generated and synced.

Opportunities
- Regenerate schemas via `cargo xtask schema generate` to include telemetry payloads.
- Add a guard or CI check specifically for missing `sinex.*` telemetry schemas if they are expected to be validated.
