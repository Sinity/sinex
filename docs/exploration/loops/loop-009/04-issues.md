# Loop 009 - Concrete Issues

1) Health automaton queries event types without schema definitions
- Evidence: `crate/nodes/sinex-health-automaton/src/lib.rs` queries `system.health`, `service.status`, `database.health`, `filesystem.health`, `network.health`, `process.status`, `system.error`, `service.error`.
- Impact: these event types are not defined in `sinex-core` payloads and may never be emitted, so aggregation may be ineffective or inconsistent.

2) `system.health_summary` schema exists without an emission site
- Evidence: `crate/lib/sinex-core/src/types/events/payloads/system.rs` defines `system.health_summary` (source `health-aggregator`), but no `Event::new` emission found.
- Impact: schema registry includes a payload that is not produced, risking drift between docs and runtime.

3) `rpc.response` schemas exist without an emission site
- Evidence: `crate/lib/sinex-core/src/types/events/payloads/rpc.rs` defines `rpc.content` and `rpc.pkm` response payloads; no emission sites found.
- Impact: schema registry includes unused RPC response event definitions.
