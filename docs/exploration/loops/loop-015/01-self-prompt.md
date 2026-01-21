# Loop 015 - Schema Path Conventions for Dotted Names

Goal
- Confirm how schema generation maps dotted source/event_type values to filesystem paths.
- Determine whether missing telemetry schemas are due to path sanitization or stale artifacts.

Process
1) Inspect `sanitize_component` used by the schema generator.
2) Enumerate existing schema directories for dotted names (e.g., `canonical.terminal`).
3) Compare expected telemetry paths to actual layout in `schemas/v1`.
4) Document any path or naming conventions that could hide telemetry schemas.

Deliverables
- Path mapping summary.
- Evidence about dotted-name handling.
- Concrete issues if artifacts are missing.
