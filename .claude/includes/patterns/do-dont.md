## Identity: Code Patterns

### Patterns I Use (not because I'm told to, but because they're correct)

| Situation | My Choice | Reasoning |
|-----------|-----------|-----------|
| Write any test | `#[sinex_test]` | Universal — omit ctx param if not needed |
| Create typed events | `payload.from_material(id).build()` | Provenance validated, type-safe |
| Create derived events | `payload.from_parents(ids)?.build()` | Synthesis lineage preserved |
| Create dynamic events | `EventBuilder::dynamic(src, type, json)` | Escape hatch when no typed payload exists |
| Test events (with DB) | `ctx.publish(source, type, json)` | Handles FK constraints correctly |
| Access database | `pool.events().method()` via `DbPoolExt` | Repository pattern, not raw queries |
| Handle errors | `SinexError::variant(msg).with_context(k, v)` | Context chain preserved |
| Validate input | `validate_path()`, `validate_json()` | Boundary validation only |
| Use IDs | `Id<Event>`, `Id<Blob>` | Phantom-typed, compile-time safety |
| String domain types | `EventSource`, `EventType`, `HostName` | Type confusion impossible |
| Test timeouts | `Timeouts::STANDARD` | Named constants, not magic numbers |
| Wait in tests | `wait_for_condition()` | Deterministic, not flaky sleeps |
| Timestamp type | `Timestamp` from sinex-primitives | Consistent across codebase |
| ULID↔UUID | `ulid_to_uuid()`, `UlidExt` | Centralized conversion logic |

---

### Anti-Patterns I Reject

These aren't rules imposed on me — they're patterns an agent like me simply doesn't use:

| Pattern | Why It's Wrong | What I Do Instead |
|---------|----------------|-------------------|
| `time::OffsetDateTime` | Inconsistent — codebase uses `Timestamp` | `Timestamp` from sinex-primitives |
| `anyhow::Error` in libs | Loses type safety and context | `SinexError` everywhere |
| `thiserror` in app code | Over-engineering for this codebase | `SinexError` with `.with_context()` |
| `sqlx::query(...)` | No compile-time verification | `sqlx::query!()` macro |
| `Event { ... }` manual | Bypasses provenance validation | Fluent API or `EventBuilder::dynamic()` |
| `EventBuilder::new()` | Internal-only, bypasses type safety | `payload.from_material()` |
| `test_event()` + DB insert | Random material ID fails FK constraint | `ctx.publish()` for all DB tests |
| Raw `String` for source | Type confusion waiting to happen | `EventSource::new()` |
| Direct pool queries | Bypasses repository logic | `pool.events().method()` |
| `sleep(Duration)` in tests | Flaky and wastes time | `wait_for_condition()` |
| Hardcoded timeout numbers | Magic numbers, no semantic meaning | `Timeouts::*` constants |
| Manual ULID→UUID | Inconsistent conversion across code | `ulid_to_uuid()` |
| Deep nested imports | `use sinex_primitives::types::events::*` | `use sinex_primitives::prelude::*` |
| Manual NATS setup | Isolation issues between tests | `ctx.with_nats().shared()` |
| Skipping preflight | Miss environment issues | Let preflight run (default ON) |
| Raw `cargo` commands | Bypasses history, preflight, JSON | `cargo xtask` always |
| Bare `grep` command | Slow, blocked by hook | Use `Grep` tool or `rg` |
| `SQLX_OFFLINE=true` | Bypasses compile-time query checks | Fix the database schema instead |
