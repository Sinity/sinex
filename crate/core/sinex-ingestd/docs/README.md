# sinex-ingestd

`sinex-ingestd` is the ingestion daemon that receives events from satellites,
validates them, writes them to PostgreSQL, and relays them to streaming sinks.

Key responsibilities:

- Consume JetStream events/materials from satellites and enforce schema validation.
- Persist events and source material through the repositories in `sinex-core`.
- Publish derived data to JetStream so downstream services receive updates.
- Coordinate schema migrations by integrating with `sinex-schema`.

Operational and architectural context lives in
`docs/current/architecture/Core_Architecture.md` and
`docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`.
