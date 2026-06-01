# Sinex Macros

`sinex-macros` is the proc-macro crate for Sinex.

It currently exposes these production macros:

- `#[derive(EventPayload)]`
- `#[derive(SinexConfig)]`

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

`SinexConfig` derive generates env-driven `from_env()` constructors for
configuration structs whose fields map declaratively to environment variables.
It supports infallible and fallible loading, explicit env key overrides,
custom parsers, nested config delegation, defaults, and duration-from-seconds
fields.

See `docs/overview.md` for behavior details and limitations, and
`docs/sinex_config.md` for the configuration derive grammar.
