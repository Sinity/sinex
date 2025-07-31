# Sinex Refactoring Plan Phase 4: Unification & Simplification

## Overview
Unify the codebase around a single facade crate (`sinex`) and generic ID system, eliminating redundancy and complexity.

## Tasks (In Order)

### 1. Clean Up ID System Remnants
- [x] Implement generic `Id<T>` type
- [ ] Delete obsolete `define_id!` macro and its module entirely
- [ ] Remove all `{Type}Id` type aliases and definitions
- [ ] Replace remaining uses of `EventId`, `CheckpointId`, etc. with `Id<Event>`, `Id<Checkpoint>`

### 2. Finalize Sinex Facade Crate
- [ ] Update `sinex/Cargo.toml` with complete feature structure:
  ```toml
  [features]
  default = ["standard"]
  core = []  # Just types and events
  standard = ["dep:sinex-db", "dep:sinex-telemetry", "dep:sqlx", "dep:sinex-preflight"]
  satellite = ["standard", "dep:sinex-satellite-sdk", "dep:sinex-nats"]
  services = ["standard", "dep:sinex-services"]
  annex = ["standard", "dep:sinex-annex"]
  test = ["standard", "dep:sinex-test-utils"]
  full = ["satellite", "services", "annex", "test"]
  ```
- [ ] Implement conditional external re-exports:
  - Core: `chrono`, `serde`, `serde_json`
  - With `standard`: `sqlx`, `tokio`, `async-trait`, `tracing`
  - With `satellite`: Additional satellite deps
- [ ] Use wholesale re-exports: `pub use sinex_db::*` etc.

### 3. Redistribute Macros
- [ ] Move `#[with_context]` → `sinex-types/src/error.rs`
- [ ] Move `#[auto_metrics]` family → `sinex-telemetry`
- [ ] Move `#[derive(EventPayload)]` → `sinex-events`
- [ ] Move `db_query!`, `db_transaction!` → `sinex-db`
- [ ] Move satellite derives → `sinex-satellite-sdk`
- [ ] Delete obsolete macros: `event_registry!`, `define_id_type!`
- [ ] Delete `sinex-macros` crate if empty

### 4. Update All Imports
- [ ] Replace all `use sinex_types::*` → `use sinex::*`
- [ ] Replace all `use sinex_events::*` → `use sinex::*`
- [ ] Replace all `use sinex_db::*` → `use sinex::*`
- [ ] Use `sinex::prelude::*` for common imports

### 5. Rename Variables
Apply "have a reason NOT to rename" principle:

**Rename to `id`:**
```rust
// Struct's own ID
struct Event { id: Id<Event> }

// Single ID parameter
fn get_by_id(&self, id: Id<Event>) -> Event

// Clear from context
impl Event {
    fn with_id(self, id: Id<Event>) -> Self
}
```

**Keep prefixed names when:**
```rust
// References another entity
struct EventAnnotation {
    id: Id<EventAnnotation>,      // Its own ID
    event_id: Id<Event>,          // Which event it annotates
}

// Multiple IDs in scope
fn link_events(source_id: Id<Event>, target_id: Id<Event>)

// Cross-entity references
struct EntityRelation {
    id: Id<EntityRelation>,
    from_entity_id: Id<Entity>,   // Keep prefixed
    to_entity_id: Id<Entity>,     // Keep prefixed
}
```

### 6. Fix All Compilation Errors
- [ ] Fix sqlx query macro type annotations
- [ ] Remove placeholder types added during refactoring
- [ ] Ensure all tests pass
- [ ] Reduce warnings to zero where feasible

## Success Criteria
- Zero compilation errors
- Single import source: `use sinex::*`
- Consistent ID naming: `id` unless ambiguous
- No obsolete ID types or macros
- Clean feature-gated facade

## Key Principles
1. **Errors are features** - No backwards compatibility
2. **Locality** - Code lives where it's used
3. **Simplification** - One way to do things
4. **Have a reason NOT to rename** - Default to shorter names