# sinex-sensd

`sinex-sensd` is the universal acquisition daemon. It connects to configured
sensors, orchestrates material rotation, and emits events into the ingestion
pipeline.

Major components:

- `config` – runtime configuration for sensor adapters.
- `job_manager` – schedules capture jobs against sensors and enforces retry
  policies.
- `material_stream` / `material_rotation` – manage buffered source material and
  periodic rotation to storage.
- `temporal_ledger` – tracks processed checkpoints to prevent gaps.
- `grpc_server` – optional control plane for remote introspection.

For end-to-end acquisition topology see `docs/architecture/Core_Architecture.md`
and `crate/lib/sinex-satellite-sdk/doc/overview.md`.
