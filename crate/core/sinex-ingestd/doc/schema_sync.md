# Schema Synchronisation

`schema_sync.rs` keeps the ingest daemon aware of active schemas by querying
`sinex-schema` tables and caching results locally.

- Runs during startup and periodically to refresh schema metadata.
- Ensures schema IDs and versions are available before ingesting events.
- Coordinates with `sinex-core::types::events` helpers for cache updates.

Cross-reference `crate/lib/sinex-schema/doc/overview.md` when adjusting the sync cadence or
table layout.
