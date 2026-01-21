# Loop 016 - Concrete Issues

1) `schemas/v1/registry.json` lacks telemetry schema entries.
- Expected telemetry payloads are absent from the registry (`metric.*`, `request.stats`, `rate_limit.exceeded`, `replay.stats`, `stream.stats`, `assembly.stats`, `processing.stats`, `health.status`).
- This indicates schema artifacts are stale relative to current `EventPayload` inventory.
- File: `schemas/v1/registry.json`.
