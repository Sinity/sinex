# sensd Service

`service.rs` bundles the job manager, material pipeline, and temporal ledger
into a cohesive service that can be embedded in binaries or invoked by tests.

- Owns the lifecycle of sensor adapters.
- Provides utilities for graceful shutdown and health reporting.
- Emits structured events using the ULID utilities in `sinex-core`.
