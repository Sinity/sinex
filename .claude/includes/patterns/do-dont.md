## Identity: Code Patterns

### Patterns I Use (not because I'm told to, but because they're correct)

| Situation | My Choice | Reasoning |
|-----------|-----------|-----------|
| Write any test | `#[sinex_test]` | Universal — omit ctx param if not needed |
| Create typed events | `payload.from_material(id).build()` | Provenance validated, type-safe |
| Create derived events | `payload.from_parents(ids)?.build()` | Synthesis lineage preserved |
| Create dynamic events | `EventBuilder::dynamic(src, type, json)` | Escape hatch when no typed payload exists |
| Test events (with DB) | `ctx.publish(payload)` where payload: `Publishable` | Handles FK constraints correctly |
| Access database | `pool.events().method()` via `DbPoolExt` | Repository pattern, not raw queries |
| Handle errors | `SinexError::variant(msg).with_context(k, v)` | Context chain preserved |
| Validate input | `validate_path()`, `validate_json()` | Boundary validation only |
| Use IDs | `Id<Event>`, `Id<Blob>` | Phantom-typed, compile-time safety |
| String domain types | `EventSource`, `EventType`, `HostName` | Type confusion impossible |
| Test timeouts | `Timeouts::STANDARD` | Named constants, not magic numbers |
| Wait in tests | `wait_for_condition()` | Deterministic, not flaky sleeps |
| Timestamp type | `Timestamp` from sinex-primitives | Consistent across codebase |
| ULID↔UUID | `ulid_to_uuid()`, `UlidExt` | Centralized conversion logic |
| Quick compile check | `xtask check` | ~3s warm, default is compile-only |
| Compile + lint | `xtask check --lint` | ~20s warm, clippy subsumes cargo check |
| Full validation | `xtask check --full` | fmt + clippy + forbidden |
| Background check | `xtask check --bg` | Non-blocking, continue working |

---

### Anti-Patterns I Reject

These aren't rules imposed on me — they're patterns an agent like me simply doesn't use:

| Pattern | Why It's Wrong | What I Do Instead |
|---------|----------------|-------------------|
| `time::OffsetDateTime` | Inconsistent — codebase uses `Timestamp` | `Timestamp` from sinex-primitives |
| `anyhow::Error` anywhere | Codebase uses `color_eyre`, not anyhow | `SinexError` in libs, `color_eyre::eyre::Result` in xtask |
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
| Raw `cargo` commands | Bypasses history, preflight, JSON | `xtask` always |
| `cargo run -p xtask --` | Recompiles xtask first, doubles build time | `xtask` binary directly (on PATH) |
| Bare `grep` command | Slow, blocked by hook | Use `Grep` tool or `rg` |
| `SQLX_OFFLINE=true` | Bypasses compile-time query checks | Fix the database schema instead |
| `xtask test` foreground while any xtask running | Hangs on migration advisory lock — indefinite wait | `xtask test --bg --json` → `xtask jobs wait ID` → `xtask jobs output ID` |
| Running `xtask check` (or anything that invokes cargo) inside a `#[sinex_test]` | Deadlocks: nextest holds cargo target/ lock for its **entire run**; child cargo waits forever. **Enforced**: `ensure_ready()` is a no-op in nextest context; `run_cargo_check/clippy` bail immediately with a clear error. | Use `--help` to verify flag parsing; test logic in unit tests in `check.rs` |
| `xtask check` foreground in parallel | Concurrent cargo invocations compete for target/ lock — all-but-one hang. Migrations now serialized via `flock(LOCK_NB)` (skip-if-locked) | `xtask check --bg` → `xtask jobs wait ID` |
| `some_cmd \| tail -N` on xtask | **Blocked by PreToolUse hook.** tail buffers all output until EOF; if xtask hangs, you see nothing. SIGPIPE when tail exits kills xtask silently | Use `--bg --json`, then `xtask jobs output ID` |
| `xtask check --lint=false` | Old subtractive flag, no longer exists | `xtask check` (default is compile-only) |
| `xtask check --skip-fmt` | Old subtractive flag, removed | `xtask check` (fmt is off by default) |
| `xtask check --forbidden=false` | Old subtractive flag, removed | `xtask check` (forbidden is off by default) |
