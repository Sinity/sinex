# Loop 017 - Next Analysis Ideas

- Trace registry-only schemas to their corresponding JSON files to confirm whether they are legacy.
- Check feature flags for missing telemetry payloads to confirm inventory registration.
- Run schema generation and compare the regenerated registry to current `schemas/v1/registry.json`.
- Audit `journald/node.heartbeat` emitters and schema presence.
