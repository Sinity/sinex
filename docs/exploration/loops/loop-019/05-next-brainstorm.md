# Loop 019 - Next Analysis Ideas

- Query `sinex_schemas.gitops_schema_sources` to attribute legacy schemas.
- Enhance schema inventory tooling to parse `define_event_payload!` macros.
- Audit `EventBuilder` call sites for hard-coded source/event_type strings.
- Check whether any external emitters produce `system.*_historical` events.
