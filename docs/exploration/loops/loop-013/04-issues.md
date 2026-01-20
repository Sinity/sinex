# Loop 013 - Concrete Issues

1) Schema bundle appears stale for telemetry payloads.
- Telemetry payloads are registered `EventPayload` types, so `schemas/v1` should contain files like `sinex/metric.counter.json` and `sinex.gateway/request.stats.json`.
- These files are absent, implying `schemas/v1` is not in sync with current code.
- Files: `crate/lib/sinex-core/src/types/bin/sinex-schema.rs`, `crate/lib/sinex-core/src/types/events/schema_registry.rs`, `schemas/v1/`.
