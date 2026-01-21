# Loop 017 - EventPayload Inventory vs Schema Registry Drift

Scope
- `#[event_payload(...)]` occurrences across Rust sources.
- `schemas/v1/registry.json` entries.

Counts
- EventPayload pairs in code (source + event_type): 96.
- Registry pairs in `schemas/v1/registry.json`: 100.
- Missing in registry: 12.
- Extra in registry: 16.

Missing in Registry (examples)
- Telemetry payloads are absent:
  - `sinex/metric.counter`, `sinex/metric.gauge`, `sinex/metric.histogram`, `sinex/health.status`.
  - `sinex.gateway/request.stats`, `sinex.gateway/rate_limit.exceeded`, `sinex.gateway/replay.stats`, `sinex.gateway/pool.stats`.
  - `sinex.ingestd/stream.stats`, `sinex.ingestd/assembly.stats`.
  - `sinex.node/processing.stats`.
- `journald/node.heartbeat` also appears in code but not registry.

Extra in Registry (examples)
- Registry contains several `shell.*` and `terminal.kitty` events not found in the `#[event_payload]` inventory (e.g., `shell.kitty/command.executed`, `terminal.kitty/session.started`).
- `atuin/entry.imported` and some `system/*` historical events are present in the registry without matching code annotations.

Findings
- The registry is out of sync with code: telemetry payloads are missing and several registry entries have no corresponding `EventPayload` annotation.
- The mismatch is not just telemetry; it includes journald and shell/terminal sources.

Risks
- Schema validation and schema sync workflows operate on a stale registry, allowing mismatches between emitted payloads and schemas.

Opportunities
- Regenerate schemas from current code and refresh `schemas/v1/registry.json`.
- Audit registry-only entries to confirm whether they are legacy schemas or should be reintroduced in code.
