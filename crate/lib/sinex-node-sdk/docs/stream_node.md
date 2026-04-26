# Stream Node Architecture

Runtime model for `sinex-node-sdk` nodes.

This module documents the current runtime model used by `sinex-node-sdk`:

- Core interface: `Node` trait (`scan`, `initialize`, `shutdown`, capabilities)
- High-level adapters: `IngestorNodeAdapter` and `DerivedNodeAdapter`
- Runtime orchestration: `NodeRunner`
- CLI integration: `sinex-node-sdk` `node_entrypoint!` macro

## Architecture Overview

The low-level runtime supports snapshot, historical, and continuous scan modes.
High-level traits decide what those modes mean for each node family:

- `Snapshot` scans for current-state capture
- `Historical` scans for bounded replay/gap-fill
- `Continuous` mode for long-running processing

`NodeRunner` owns transport, checkpoint manager, schema listener, and background workers.

Capture ingestors implement `IngestorNode`; they read external material and emit
material-provenance events. Derived nodes implement `TransducerNode`,
`WindowedNode`, or `ScopeReconcilerNode`; they consume confirmed events and emit
synthesis-provenance events.

## Checkpoint Types

### External Checkpoints (ingestors)

```rust
Checkpoint::external(
    serde_json::json!({"path": "/var/log/app.log", "offset": 1024}),
    Some("app.log:1024".to_string()),
)
```

### Internal Checkpoints (derived nodes)

```rust
Checkpoint::internal(event_uuid, message_count)
```

## Implementing New Nodes

Most nodes should implement one of:

1. `IngestorNode` (capture from external sources)
2. `TransducerNode` (stateless event transformation)
3. `WindowedNode` (accumulate events and emit bounded windows)
4. `ScopeReconcilerNode` (maintain per-scope state and reconcile summaries)

Then expose a binary with `node_entrypoint!(...)`.

For lower-level control, implement `Node` directly.

## Operational Notes

- Checkpoints are persisted via NATS KV (`sinex_checkpoints` bucket).
- Key format is `<node>.<consumer_group>.<consumer>`.
- Derived-node leader/standby behavior is handled inside runtime processing paths
  when the configured processing model requires it.
- Coordination adapters are optional and should not duplicate built-in
  derived-node leadership.
