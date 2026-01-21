# Loop 022 - Legacy Schema Origins: journald.satellite.heartbeat and system.*_historical

Scope
- JSON schemas: `journald/satellite.heartbeat`, `system/journald.historical`, `system/systemd.units_historical`, `system/udev.device_historical`.
- Code references for these event types.

Schema Inspection
- `journald/satellite.heartbeat` requires `service_name` and allows optional counters (`events_processed`, `memory_usage_mb`, `uptime_seconds`, `git_hash`, `version`).
- `system/*_historical` schemas share a minimal shape: `note`, `scan_type`, and `source` (or `sources` for udev).

Code References
- No production emitters found for these event types in `crate/`.
- Only occurrence is in a proptest regression artifact under tests.

Findings
- These schemas exist in `schemas/v1` and the registry, but no corresponding typed payloads or emitters are present.
- The shapes suggest legacy/system-scan metadata rather than live runtime events.

Risks
- Registry includes schema entries for events that are not emitted, increasing drift and confusion for schema validation.

Opportunities
- Decide whether these schemas are legacy artifacts to prune or need active emitters.
- If external tooling emits these events, document the ownership and ingestion path.
