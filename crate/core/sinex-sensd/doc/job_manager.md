# Job Manager

`job_manager.rs` coordinates scheduled acquisition jobs. It manages retry
policies, backoff, and sensor readiness checks before dispatching capture tasks.

- Keeps per-sensor state machines so failures do not cascade.
- Emits health metrics consumed by `docs/architecture/SystemOperations_And_Integrity_Architecture.md`.
- Integrates with `material_stream` to enqueue captured material.
