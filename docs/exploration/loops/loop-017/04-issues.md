# Loop 017 - Concrete Issues

1) `schemas/v1/registry.json` missing telemetry payload entries found in code.
- Missing: `sinex/metric.*`, `sinex/health.status`, `sinex.gateway/*` stats, `sinex.ingestd/*` stats, `sinex.node/processing.stats`.
- This indicates schema artifacts are stale relative to `EventPayload` inventory.

2) Registry contains entries with no matching `EventPayload` annotation.
- Examples: `shell.kitty/command.executed`, `terminal.kitty/session.started`, `atuin/entry.imported`.
- These entries may be legacy schemas or require code reintroduction; needs clarification.
