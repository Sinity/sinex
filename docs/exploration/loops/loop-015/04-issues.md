# Loop 015 - Concrete Issues

1) Schema bundle drift is not caused by path sanitization.
- Dotted source/event_type names are preserved by `sanitize_component`, and the bundle already includes dotted directories (e.g., `canonical.terminal`).
- Telemetry schemas are still missing, reinforcing that the bundle is stale.
- Files: `crate/lib/sinex-core/src/types/bin/sinex-schema.rs`, `schemas/v1/`.
