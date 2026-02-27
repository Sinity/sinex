# Stream Node Architecture

Unified node runtime for Sinex.

This module documents the current runtime model used by `sinex-node-sdk`:

- Core interface: `Node` trait (`scan`, `initialize`, `shutdown`, capabilities)
- High-level adapters: `IngestorNodeAdapter` and `AutomatonNodeAdapter`
- Runtime orchestration: `NodeRunner`
- CLI integration: `sinex-node-sdk` `node_entrypoint!` macro

## Architecture Overview

The runtime supports both ingestors and automata through the same `Node` lifecycle:

- `Snapshot` scans for current-state capture
- `Historical` scans for bounded replay/gap-fill
- `Continuous` mode for long-running processing

`NodeRunner` owns transport, checkpoint manager, schema listener, and background workers.

## Checkpoint Types

### External Checkpoints (ingestors)

```rust
Checkpoint::external(
    serde_json::json!({"path": "/var/log/app.log", "offset": 1024}),
    Some("app.log:1024".to_string()),
)
```

### Internal Checkpoints (automata)

```rust
Checkpoint::internal(event_ulid, message_count)
```

## Implementing New Nodes

Most nodes should implement one of:

1. `IngestorNode` (capture from external sources)
2. `AutomatonNode` (derive/process events)

Then expose a binary with `node_entrypoint!(...)`.

For lower-level control, implement `Node` directly.

## Operational Notes

- Checkpoints are persisted via NATS KV (`sinex_checkpoints` bucket).
- Key format is `<node>.<consumer_group>.<consumer>`.
- Automata leader/standby behavior is handled inside runtime processing paths.
- Coordination adapters are optional and should not duplicate built-in automaton leadership.
