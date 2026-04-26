# Extensibility Patterns

How to extend Sinex with new event types, nodes, and RPC methods.

## Core Finding: Compositional Completeness

The Sinex architecture exhibits "compositional completeness" - abstractions compose cleanly, and extending the system is "fill in the blanks" work.

## Extensibility Evidence

### 1. EventPayload Pattern

Adding a new event type requires only:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "browser", event_type = "page.visited")]
pub struct PageVisitedPayload {
    pub url: String,
    pub title: String,
    pub duration_ms: u64,
}
```

The derive macro handles:
- Schema registry integration
- JSON schema generation
- Version handling
- Inventory registration for runtime discovery

No migration-chain steps are needed for payload shape changes - `core.events` stores `payload` as JSONB.

### 2. Node Runtime (`Node` + current high-level traits)

The runtime provides complete node lifecycle via `NodeRunner` and adapter traits:
- **Three-phase lifecycle**: Snapshot → Historical → Continuous
- **Associated `Config` type**: Type-safe, deserializable configuration
- **NodeInitContext / NodeRuntimeState**: DB pool, checkpoint manager, event emitter, NATS transport
- **Adapter ergonomics**: `IngestorNodeAdapter` and `DerivedNodeAdapter` remove boilerplate

New capture nodes usually implement `IngestorNode`. New derived nodes implement
`TransducerNode`, `WindowedNode`, or `ScopeReconcilerNode`, then run through the
matching `DerivedNodeAdapter` alias. Binaries use `node_entrypoint!`.

### 3. NATS Subject Routing

Subject pattern: `events.raw.{source}.{event_type}` with collision-free token encoding for each source/type component. New sources automatically get routing without configuration, and JetStream consumers still use the stable wildcard `events.raw.>`.

### 4. NixOS Module

Adding a new node is configuration:

```nix
nodes.browser = {
  enable = true;
  logLevel = cfg.logLevel;
  resources.memory = "256M";
};
```

### 5. Gateway RPC Dispatch

```rust
match method {
    "analytics.event_count_by_source" => handle_event_count_by_source(...),
    "pkm.create_note" => handle_create_note(...),
    "browser.archive_page" => handle_archive_page(...),  // One line to add
    _ => Err(UnknownMethodError { method })
}
```

## Five Properties Enabling Rapid Assembly

1. **Composition over inheritance** - Components connect via events, not class hierarchies
2. **Schema-on-read flexibility** - JSON payloads with validation, not rigid tables
3. **Macro-based boilerplate reduction** - New types get full infrastructure automatically
4. **Event sourcing** - New nodes can replay history without transitional schema branches
5. **Declarative deployment** - NixOS modules are configuration, not code

## Validation Status

The basic cross-section is no longer only a design claim. Current validation is
kept in package tests, proof-carrying runtime scenarios, evidence bundles,
deployment checks, and VM coverage where runtime hardening matters.

When extending the SDK, make the new surface prove its own contract:
- add focused package tests for trait or adapter behavior
- add scenario metadata when the behavior is a runtime proof point
- use evidence artifacts for resource-shape or deployment-readiness claims
- keep generated docs and agent-facing memory aligned with the implemented API
