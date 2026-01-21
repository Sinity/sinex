# Loop 009 - Event Schema Coverage vs Emitted Events

Scope
- Schema-registered event types via `#[event_payload]` in `sinex-core`.
- Event emission and event-type usage sites in nodes and SDK.

Schema-Registered Event Types (sampled)
- Filesystem: `fs-watcher` source with `file.created`, `file.modified`, `dir.discovered`, etc.
  - `crate/lib/sinex-core/src/types/events/payloads/filesystem.rs`.
- System/journald/dbus/systemd/udev: `journald.log_entry.captured`, `systemd.unit.started`, etc.
  - `crate/lib/sinex-core/src/types/events/payloads/system.rs`.
- Process lifecycle: `process.started`, `process.heartbeat`, `process.shutdown`, etc.
  - `crate/lib/sinex-core/src/types/events/payloads/process.rs`.
- Metrics: `sinex` + `metric.*`, `sinex.ingestd` + `stream.stats`, etc.
  - `crate/lib/sinex-core/src/types/events/payloads/metrics.rs`.
- RPC responses: `rpc.content` + `rpc.response`, `rpc.pkm` + `rpc.response`.
  - `crate/lib/sinex-core/src/types/events/payloads/rpc.rs`.

Observed Emission/Usage Patterns
- Most node emitters use typed payloads (`Event::new(payload, ...)`) that correspond to registered schemas (e.g., filesystem, systemd, terminal canonicalization). Example: `crate/nodes/sinex-terminal-command-canonicalizer/src/unified_processor.rs` uses `CanonicalCommandPayload` (schema-defined).
- Health automaton queries event types by string without schema definitions present.
  - `crate/nodes/sinex-health-automaton/src/lib.rs` queries `system.health`, `service.status`, `database.health`, etc., which are not defined in `sinex-core` payloads.
- Heartbeat events are emitted as structured logs with `event_type = "node.heartbeat"` and captured via journald ingestor. A schema exists for `journald.node.heartbeat`.
  - `crate/lib/sinex-node-sdk/src/heartbeat.rs` emits structured log entries.
  - `crate/lib/sinex-core/src/types/events/payloads/system.rs` includes `node.heartbeat` under `journald` source.

Schemas with No Known Emission Sites
- `system.health_summary` payload is defined but no emitter found.
  - `crate/lib/sinex-core/src/types/events/payloads/system.rs` defines `system.health_summary` with source `health-aggregator`.
  - No matching `Event::new` usage found in nodes.
- `rpc.*` response payloads are defined but no emission found.
  - `crate/lib/sinex-core/src/types/events/payloads/rpc.rs` defines `rpc.response` events.
  - No code references emit these payloads.

Findings
- Most production emitters use typed payloads with matching schemas.
- Health automaton expects event types that are not registered with schemas; it may be querying event types that are never emitted or are untyped.
- Some schema payloads appear unused (health summary, RPC response).

Risks
- Health automaton may never see its target events (schema missing or event types not emitted), leading to empty aggregation or misleading output.
- Unused schemas drift from reality and can confuse taxonomy/documentation.

Opportunities
- Align health automaton target event types with actual emitted schemas or add missing payload definitions.
- Remove or implement emitters for `rpc.response` and `system.health_summary` events to keep schema registry accurate.
