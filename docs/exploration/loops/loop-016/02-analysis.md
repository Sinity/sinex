# Loop 016 - Registry.json Coverage vs Telemetry Schemas

Scope
- `schemas/v1/registry.json` entries.
- Self-observation telemetry event types.

Registry Findings
- `registry.json` includes `sinex` process/sensor events (e.g., `process.started`, `process.heartbeat`, `sensor.activated`).
- No entries exist for telemetry sources like `sinex.gateway`, `sinex.ingestd`, or `sinex.node`.
- No entries exist for telemetry event types such as `metric.counter`, `request.stats`, `rate_limit.exceeded`, or `health.status`.

Expected Telemetry Entries (if generated)
- `sinex/metric.counter.json`
- `sinex/metric.gauge.json`
- `sinex/metric.histogram.json`
- `sinex/health.status.json`
- `sinex.gateway/request.stats.json`
- `sinex.gateway/rate_limit.exceeded.json`
- `sinex.gateway/replay.stats.json`
- `sinex.ingestd/stream.stats.json`
- `sinex.ingestd/assembly.stats.json`
- `sinex.node/processing.stats.json`

Findings
- Telemetry schemas are absent from `registry.json`, so the schema bundle is stale relative to the current set of `EventPayload` types.
- The absence is not limited to filesystem files; the registry itself lacks telemetry entries.

Risks
- Schema generation artifacts do not reflect telemetry payloads, undermining schema validation and sync workflows.

Opportunities
- Regenerate schemas to include telemetry payloads and update `registry.json` to match the current `EventPayload` inventory.
