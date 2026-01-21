# Loop 006 - Concrete Issues

1) Coordination subjects bypass environment namespacing
- Evidence: `crate/lib/sinex-node-sdk/src/coordination.rs` uses `format!("sinex.coordination.{}.*", ...)` directly.
- Impact: coordination messages can cross environments on a shared NATS cluster.

2) Self-observation telemetry subjects bypass environment namespacing
- Evidence: `crate/lib/sinex-node-sdk/src/self_observation.rs` uses `subject_prefix = "sinex.telemetry"` without `SinexEnvironment`.
- Impact: telemetry streams can mix between environments and are harder to isolate.

3) KV bucket names are not environment-namespaced
- Evidence: `KV_sinex_schemas`, `KV_sinex_checkpoints`, `KV_sinex_NODE_STATE` are hardcoded.
- Impact: schema and checkpoint data can collide across environments sharing a NATS cluster.
