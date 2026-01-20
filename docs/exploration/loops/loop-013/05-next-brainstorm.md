# Loop 013 - Next Analysis Ideas

- Inspect `sanitize_component` to confirm schema path conventions for dotted event types.
- Determine whether telemetry schemas should be excluded by policy (doc or config).
- Verify `schema generate` output for telemetry payloads in a clean repo state.
- Map `event_payload_schemas` entries in DB to on-disk schemas and detect drift.
- Inventory-based payload registry: count and diff against schema bundle.
