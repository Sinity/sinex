# Sinex Macros

`sinex-macros` is the proc-macro crate for Sinex.

It currently exposes one production macro:

- `#[derive(EventPayload)]`

`EventPayload` derive:
- implements `sinex_primitives::events::EventPayload` constants (`SOURCE`, `EVENT_TYPE`, `VERSION`)
- generates fluent `with_<field>(...)` builder-style setters
- registers non-generic payload schemas in the runtime schema registry inventory

```rust
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema, sinex_macros::EventPayload)]
#[event_payload(source = "fs-watcher", event_type = "file.created", version = "1.0.0")]
pub struct FileCreatedPayload {
    pub path: String,
    pub size: u64,
}
```

See `docs/overview.md` for behavior details and limitations.
