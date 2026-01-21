# Loop 015 - Next Analysis Ideas

- Inspect `schemas/v1/registry.json` to see if telemetry entries are listed but files missing.
- Run `cargo xtask schema generate` to verify telemetry schema output paths.
- Compare `inventory` payload count to schema bundle count to quantify drift.
- Map schema sync logic to ensure telemetry payloads are eligible for DB sync.
