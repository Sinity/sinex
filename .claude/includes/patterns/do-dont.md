## DO

| Task | Correct Pattern |
|------|-----------------|
| Write tests | `#[sinex_test]` for ALL tests — omit `ctx` param if not needed |
| Create typed events | `payload.from_material(id).build()` or `payload.from_parents(ids)?.build()` |
| Create dynamic events | `EventBuilder::dynamic(source, type, json).from_material(id, 0).build()` |
| Test events (with DB) | `ctx.publish(source, type, json)` |
| Access DB | `pool.events().method()` via `DbPoolExt` from sinex-db |
| Handle errors | `SinexError::variant(msg).with_context(k, v)` |
| Validate input | `validate_path()`, `validate_json()` at boundaries |
| Use IDs | `Id<Event>`, `Id<Blob>` - phantom-typed |
| String types | `EventSource`, `EventType`, `HostName` |
| Timeouts | `Timeouts::STANDARD` from xtask sandbox |
| Wait in tests | `ctx.wait_for_event_count()` or `WaitHelpers` trait |
| ULID↔UUID | `ulid_to_uuid()`, `UlidExt` from sinex-schema |
| Timestamps | `Timestamp` from sinex-primitives (not `OffsetDateTime`) |
| Error types | `SinexError` everywhere (not `anyhow` in libs) |

---

## DON'T

| Anti-Pattern | Why | Correct Alternative |
|--------------|-----|---------------------|
| `time::OffsetDateTime` | Inconsistent with codebase | `Timestamp` from sinex-primitives |
| `anyhow::Error` in lib code | Loses type safety | `SinexError` from sinex-primitives |
| `thiserror` in application code | Over-engineering | `SinexError` with `.with_context()` |
| `sqlx::query(...)` | No compile-time verification | `sqlx::query!()` macro |
| `Event { ... }` manual | Missing provenance validation | Fluent API or `EventBuilder::dynamic()` |
| `EventBuilder::new()` | Internal-only, bypasses type safety | `payload.from_material()` or `EventBuilder::dynamic()` |
| `test_event()` + DB insert | Random material ID fails FK constraint | `ctx.publish()` for all DB tests |
| Raw `String` for source | Type confusion possible | `EventSource::new()` from sinex-primitives |
| Direct pool queries | Bypasses repository logic | `pool.events().method()` from sinex-db |
| `sleep(Duration)` in tests | Flaky, wastes time | `wait_for_condition()` |
| Hardcoded timeouts | Magic numbers | `Timeouts::*` constants |
| Manual ULID→UUID | Inconsistent conversion | `ulid_to_uuid()` |
| `use sinex_primitives::types::events::*` | Deep nested imports | `use sinex_primitives::prelude::*` |
| Manual NATS setup in tests | Isolation issues | `ctx.with_nats().shared()` |
| Skipping preflight in tests | Miss env issues | `system_test_preflight()` |
| Raw `cargo build/test/check` | Bypasses history, preflight, JSON | `cargo xtask check/test/build` |
| Bare `grep` command | Slow, blocked by hook | Use `Grep` tool or `rg` via xtask |
| `SQLX_OFFLINE=true` | Bypasses compile-time query verification | Fix the database schema instead |
