# Loop 005 - Concrete Issues

1) Schema broadcast cache is unused in production
- Evidence: `SchemaBroadcastCache` is created and stored in `NodeHandles`, but only referenced in tests (`crate/lib/sinex-node-sdk/tests/edge_mode_test.rs`).
- Impact: cached metadata provides no runtime value, and any intended introspection or diagnostics are absent.

2) Nodes rely solely on broadcasts for schema availability; no startup KV fetch
- Evidence: `maybe_start_schema_listener()` only updates caches when a broadcast is received; there is no initial KV scan.
- Impact: nodes can fail validation and refuse to emit events until the next broadcast tick (up to 5 minutes).
