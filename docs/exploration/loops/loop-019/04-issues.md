# Loop 019 - Concrete Issues

1) Inventory tooling misses macro-defined payloads.
- `define_event_payload!` expands to `#[event_payload]`, but naive regex scans omit these, causing false schema-drift reports.
- File: `crate/lib/sinex-core/src/types/events/payloads/mod.rs`.

2) Registry contains schemas with no code emitters.
- `journald/satellite.heartbeat` and `system/*_historical` appear only in schema bundles and not in production emission paths.
- Source: `schemas/v1/registry.json` (no matching emitters found).
