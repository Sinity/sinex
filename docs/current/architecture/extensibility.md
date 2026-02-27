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

No schema migrations needed - `core.events` stores `payload` as JSONB.

### 2. Unified Node Runtime (`Node` + `IngestorNode`/`AutomatonNode`)

The runtime provides complete node lifecycle via `NodeRunner` and wrapper traits:
- **Three-phase lifecycle**: Snapshot → Historical → Continuous
- **Associated `Config` type**: Type-safe, deserializable configuration
- **NodeInitContext / NodeRuntimeState**: DB pool, checkpoint manager, event emitter, NATS transport
- **Wrapper ergonomics**: `IngestorNodeAdapter` and `AutomatonNodeAdapter` remove boilerplate

New nodes usually implement `IngestorNode` or `AutomatonNode` and use `node_entrypoint!`.

### 3. NATS Subject Routing

Subject pattern: `events.raw.{source}.{event_type}` (subject-safe normalized). New sources automatically get routing without configuration. JetStream consumers use wildcards: `events.raw.>`.

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
4. **Event sourcing** - New processors can replay history without migration
5. **Declarative deployment** - NixOS modules are configuration, not code

## Validation Status

The architecture is sound in design. The remaining validation is proving the wiring works in practice:
- NATS actually delivers
- Ingestd actually persists
- Gateway actually queries

Once the cross-section works, rapid assembly is viable.
