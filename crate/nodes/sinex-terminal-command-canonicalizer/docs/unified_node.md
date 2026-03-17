# Unified Node

`unified_node.rs` implements [`TransducerNode`] — a stateless 1:1 transform that
consumes `command.executed` events, canonicalises them, and emits
`command.canonical` outputs.

## Model Choice: Transducer over `ScopeReconciler`

The original spec suggested `ScopeReconcilerNode`, but the canonicalizer's actual
logic is a pure per-event transform with no accumulated scope state. Transducer is
the correct model because:

- Each input maps to exactly zero or one output (1:1)
- No window accumulation or cross-event state needed
- `ts_orig` is inherited directly from the input event

If future replay invalidation requires session-level scope targeting (e.g.
"recompute all canonicalizations in session X"), this node can be upgraded to
`ScopeReconcilerNode` with `scope_key` derived from `session_id`.
