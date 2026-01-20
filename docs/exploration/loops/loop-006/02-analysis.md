# Loop 006 - NATS Subject Namespace Consistency

Scope
- NATS subjects and KV bucket names across core, gateway, and node SDK.
- Environment namespacing via `SinexEnvironment`.

Namespaced Subjects (consistent)
- Replay control uses environment namespacing.
  - `crate/core/sinex-gateway/src/replay_control.rs` uses `env.nats_subject("sinex.control.replay")`.
- Schema broadcasts use environment namespacing.
  - `crate/core/sinex-ingestd/src/service.rs` uses `env.nats_subject("system.schemas.active")`.
- Event ingestion, DLQ, and acquisition subjects use `nats_subject_with_namespace`.
  - `crate/lib/sinex-node-sdk/src/nats_publisher.rs`, `jetstream_consumer.rs`, `acquisition_manager.rs`.
- Node control operations in gateway use environment namespacing.
  - `crate/core/sinex-gateway/src/handlers/nodes.rs` uses `env.nats_subject("sinex.control.nodes.*")`.

Raw Subjects (not environment-namespaced)
- Coordination uses raw `sinex.coordination.*` subjects.
  - `crate/lib/sinex-node-sdk/src/coordination.rs` formats subjects directly (handoff, handoff_ready, failure).
- Self-observation telemetry uses raw `sinex.telemetry.*` subjects.
  - `crate/lib/sinex-node-sdk/src/self_observation.rs` builds `subject_prefix` without `SinexEnvironment`.

KV Bucket Names (not environment-namespaced)
- Schema KV bucket: `KV_sinex_schemas`.
  - `crate/core/sinex-ingestd/src/service.rs` stores schemas in this bucket.
- Checkpoint KV bucket: `KV_sinex_checkpoints`.
  - `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs` creates/opens this bucket.
- Node state KV bucket: `KV_sinex_NODE_STATE`.
  - `crate/core/sinex-gateway/src/handlers/nodes.rs` reads from this bucket.

Findings
- Most event subjects are environment-namespaced via `SinexEnvironment` helpers.
- Coordination and telemetry subjects bypass namespacing and could cross-contaminate environments.
- KV bucket names are global and not environment-specific, which can cause collisions between environments sharing a NATS cluster.

Risks
- Multi-environment deployments on a shared NATS cluster can leak coordination/telemetry between environments.
- KV buckets without namespacing can mix checkpoints or schemas across environments, leading to incorrect validation or control state.

Opportunities
- Introduce environment-aware helpers for coordination and telemetry subjects.
- Consider environment suffixes for KV bucket names or an explicit namespace per environment.
