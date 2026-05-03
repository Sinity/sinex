## Code Patterns (Use/Don't-Use Quick Reference)

### Event Creation

```rust
// TYPED PAYLOAD (preferred) — source/type from trait constants, compile-time checked
use sinex_primitives::events::payloads::*;

// Material provenance (ingestors — raw source data)
let event = FileCreatedPayload { path: "/file.txt".into(), size: 1024, .. }
    .from_material(source_material_id)
    .build()?;

// Synthesis provenance (automata — derived from other events)
let event = AnalyticsSummaryPayload { .. }
    .from_parents(parent_event_ids)?
    .build()?;

// DYNAMIC PAYLOAD (escape hatch — runtime source/type)
use sinex_primitives::events::{DynamicPayload, builder::EventBuilder};
let event = EventBuilder::dynamic("source", "event.type", json!({..}))
    .from_material(material_id, anchor_byte)
    .build()?;
```

### Error Handling — ALWAYS SinexError

```rust
use sinex_primitives::prelude::*;  // SinexError, Result in prelude

SinexError::validation("Invalid input")
    .with_context("field", "username")
    .with_context("reason", "too short")

// Propagate with context
do_work(input).map_err(|e| {
    SinexError::processing("failed to process data")
        .with_context("input_len", input.len().to_string())
        .with_std_error(&e)
})
```

### Database Access — Repository Pattern

```rust
use sinex_db::DbPoolExt;

pool.events()           // EventRepository
pool.blobs()            // BlobRepository
pool.source_materials() // SourceMaterialRepository
pool.knowledge_graph()  // KnowledgeGraphRepository
pool.state()            // StateRepository
pool.schemas()          // SchemaManagementRepository
pool.schema_cache()     // SchemaCacheRepository
```

### Validation — At Boundaries Only

```rust
use sinex_primitives::validation::core::*;
let safe_path = validate_path(user_input)?;
let safe_json = validate_json(json_string)?;
let normalized = normalize_unicode(input)?;
```

### Privacy Engine

```rust
use sinex_primitives::privacy::{self, ProcessingContext};

let result = privacy::engine().process("export TOKEN=ghp_abc123", ProcessingContext::Command);
if result.any_matched() { /* use result.text (Cow<str>) */ }
if result.suppressed { /* drop the field */ }
// Contexts: Command, Clipboard, WindowTitle, Journal, Dbus, Notification, Document, Metadata
// Strategies: Redact, Encrypt (XChaCha20-Poly1305), Hash (BLAKE3 MAC), Suppress
```

**Coverage:** All ingestors call `privacy::engine()` on their sensitive fields.
`sinex-fs-ingestor` and `sinex-document-ingestor` call `redact_metadata()` with
`ProcessingContext::Metadata` on path-bearing fields (verified: `fs-ingestor/unified_node.rs`,
`document-ingestor/lib.rs`). The previous note about a coverage gap (#555) was stale.

**Open privacy question:** The `Metadata` context only fires the home-prefix collapse rule.
Secret-bearing filenames (e.g. `id_rsa`, `~/.aws/credentials`) receive path-redaction but not
catalog-pattern redaction. Whether that is correct is a privacy-policy question tracked separately.

Automata don't re-invoke the engine; they rely on upstream redaction. Anything that leaks at the
ingestor boundary persists into derived events.

---

### Decision Table: What To Use

| Situation | Use | Not |
|-----------|-----|-----|
| Types, IDs, errors | `sinex_primitives::prelude::*` | Deep nested imports |
| Event ID type | `Id<Event>` (phantom-typed) | Raw `Uuid` |
| Timestamps | `Timestamp` from primitives | `time::OffsetDateTime` |
| Source/type strings | `EventSource`, `EventType` newtypes | Raw `String` |
| Status/health/tier | Domain enums (`OperationStatus`, `HealthStatus`, `DataTier`) | String comparisons |
| Event field values | Typed enums from `events::enums` | Raw strings |
| Error type (libs) | `SinexError::variant().with_context()` | `anyhow`, `thiserror` for ad-hoc enums |
| Error type (xtask) | `color_eyre::eyre::Result` | `anyhow` |
| DB queries | `sqlx::query!()` macro (compile-time checked) | `sqlx::query()` bare string |
| DB access | `pool.events().method()` | Direct `sqlx::query!()` on pool |
| Event creation | `payload.from_material()` or `payload.from_parents()` | `Event { .. }` manual construction |
| Test events with DB | `ctx.publish(payload)` | `test_event()` + manual insert (FK violation) |
| Lazy statics | `std::sync::LazyLock` | `lazy_static!` crate |
| Once cells | `std::sync::OnceLock` | `once_cell::sync::OnceCell` |
| Never type | `!` (feature-gated) | `Infallible` |
| Single-call async closures | `F: AsyncFnOnce() -> T` | `F: FnOnce() -> Fut, Fut: Future` |
| Multi-call async (polling) | `F: Fn() -> Fut, Fut: Future<Output=T>` | `F: AsyncFn() -> T` (breaks Send in spawn) |
| Caller syntax for async | `\|\| async { .. }` | `async \|\| { .. }` (breaks Send) |
| Random | `rand::random::<T>()`, `rand::random_range(range)` | `rng.gen::<T>()` |
| schemars paths | `schemars::SchemaGenerator`, `schemars::Schema` | `schemars::r#gen::..` |
| Test attribute | `#[sinex_test]` | `#[test]` / `#[tokio::test]` (allowlisted: trybuild, proc-macro only) |
| Test location | Per-crate `tests/` directory | Large inline `#[cfg(test)]` modules |
| Test timeouts | `Timeouts::STANDARD` etc. | Magic numbers |
| Test waits | `wait_for_condition()` | `sleep(Duration)` |
| NATS in tests | `ctx.with_nats().shared()` | Manual NATS setup |
| Cargo commands | `xtask` (always) | Bare `cargo` (bypasses history, preflight, JSON) |
| Snapshot updates | `xtask test --update-snapshots` | `INSTA_UPDATE=always cargo nextest ..` |

### Anti-Patterns That Are Enforced

These will cause errors, hangs, or hook rejections:

| Pattern | What happens |
|---------|-------------|
| `cargo run -p xtask --` | Recompiles xtask from source (~30s waste). Use `xtask` binary on PATH |
| `xtask test` foreground while nextest runs | Detected: xtask errors immediately with fix suggestion |
| `xtask check` inside `#[sinex_test]` | Deadlocks: nextest holds cargo target/ lock |
| `some_cmd \| tail -N` on xtask | Blocked by PreToolUse hook. Hides output, can kill xtask |
| Concurrent foreground `xtask check` | Target/ lock contention. Use `--bg` |
| `SQLX_OFFLINE=true` | Bypasses compile-time query checks. Fix the schema instead |
| `std::env::set_var()` without unsafe | Unsafe in edition 2024 |
