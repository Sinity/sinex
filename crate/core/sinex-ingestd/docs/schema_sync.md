# Schema Synchronisation

`schema_sync.rs` synchronizes payload schemas discovered from Rust `EventPayload`
registrations into `sinex_schemas.event_payload_schemas`, then the validator
loads active schemas from the database.

- Startup path: discover payload schemas from code, upsert/create/update in DB.
- Runtime path: periodic validator reload + broadcast of active schema metadata.
- Broadcast path: metadata is published to `system.schemas.active`; full schema
  documents are stored in NATS KV for node-side schema validator refresh.

Cross-reference `crate/lib/sinex-schema/docs/overview.md` for schema structure.
