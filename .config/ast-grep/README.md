# ast-grep Rule Catalog

Generated from `.config/ast-grep/rules/*.yml`.

Config file: `.config/ast-grep/sgconfig.yml`
Manual scan: `ast-grep scan --config .config/ast-grep/sgconfig.yml .`

Use `xtask check --forbidden` for the public local enforcement surface.
Within xtask automation, `error` severity is blocking; `warning` and `hint` remain advisory.

## Rules

| ID | Severity | Language | Message |
| --- | --- | --- | --- |
| `cargo-command-outside-process` | `error` | `rust` | Spawn cargo via xtask::process helpers, not ad-hoc Command::new("cargo") |
| `dbg-macro` | `error` | `rust` | Debug macro dbg!() found - remove before commit |
| `raw-provenance-literal` | `error` | `rust` | Use Provenance::from_material() / from_synthesis() instead of constructing Provenance::Material/Synthesis directly. Direct struct literals bypass the EventBuilder typestate that enforces XOR provenance. |
| `todo-macro` | `error` | `rust` | TODO macro found in production code |
| `unimplemented-macro` | `error` | `rust` | unimplemented! macro found in production code |
| `anyhow-in-lib` | `warning` | `rust` | Use SinexError instead of anyhow in library code |
| `bare-offset-datetime` | `warning` | `rust` | Use Timestamp wrapper instead of bare OffsetDateTime |
| `chrono-usage` | `warning` | `rust` | Use 'time' crate instead of 'chrono' |
| `color-eyre-in-runtime` | `warning` | `rust` | Keep color_eyre at binary/CLI/test presentation boundaries; use SinexError in shared runtime and library code |
| `context-erasure` | `warning` | `rust` | Error context erasure: use .with_context() instead of .map_err(|_| ...) |
| `double-clone` | `warning` | `rust` | Double clone detected - likely unnecessary |
| `expect-hardcoded` | `warning` | `rust` | Hardcoded expect() message - consider using context |
| `panic-in-lib` | `warning` | `rust` | panic!() in library code - return Result instead |
| `raw-sqlx-query` | `warning` | `rust` | Use sqlx::query!() macro instead of runtime sqlx::query() for compile-time checked queries |

## `cargo-command-outside-process`

- Severity: `error`
- Language: `rust`
- Message: Spawn cargo via xtask::process helpers, not ad-hoc Command::new("cargo")
- Ignore globs:
  - `xtask/src/process.rs`
- Intent:
  Keep cargo spawning centralized in xtask::process::{cargo_command, cargo_tokio_command, ProcessBuilder::cargo}.
  That keeps policy, diagnostics, and future behavior changes behind one seam.

## `dbg-macro`

- Severity: `error`
- Language: `rust`
- Message: Debug macro dbg!() found - remove before commit
- Intent:
  dbg!() is for temporary debugging only.
  Use tracing::debug!() for permanent debug logging.

## `raw-provenance-literal`

- Severity: `error`
- Language: `rust`
- Message: Use Provenance::from_material() / from_synthesis() instead of constructing Provenance::Material/Synthesis directly. Direct struct literals bypass the EventBuilder typestate that enforces XOR provenance.
- Ignore globs:
  - `**/*_test.rs`
  - `**/*_tests.rs`
  - `**/tests/**`
  - `crate/cli/src/commands/report.rs`
  - `crate/lib/sinex-db/src/repositories/events/conversions.rs`
  - `crate/lib/sinex-node-sdk/src/derived_node/adapter.rs`
  - `crate/lib/sinex-node-sdk/src/runtime/stream/handles.rs`
  - `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs`
  - `crate/lib/sinex-primitives/**`
  - `crate/nodes/sinex-system-ingestor/src/dbus_watcher.rs`
  - `crate/nodes/sinex-system-ingestor/src/udev_watcher.rs`
  - `crate/nodes/sinex-system-ingestor/src/unified_journal_watcher.rs`
  - `crate/nodes/sinex-system-ingestor/src/unified_node.rs`
  - `xtask/src/sandbox/**`
- Intent:
  See issue #559. The XOR-provenance invariant is encoded in:
    - EventBuilder typestate (NoProvenance has no .build())
    - serde Deserialize (rejects both-set / neither-set)
    - DB CHECK constraint (defense-in-depth)
  
  Direct `Provenance::Material { .. }` literals outside the defining
  crate skip the typestate guarantee. Use:
    payload.from_material(material_id)
    payload.from_parents(parent_ids)?
  or, for in-place construction of a Provenance value, the helpers in
  `sinex_primitives::events::builder::Provenance`:
    Provenance::from_material(id, anchor_byte, offset_start, offset_end)
    Provenance::from_synthesis(event_ids)  // returns Option (None on empty)

## `todo-macro`

- Severity: `error`
- Language: `rust`
- Message: TODO macro found in production code
- Intent:
  todo!() macros should not be in production code.
  Implement the functionality or remove the code path.

## `unimplemented-macro`

- Severity: `error`
- Language: `rust`
- Message: unimplemented! macro found in production code
- Intent:
  unimplemented!() macros should not be in production code.
  Implement the functionality or return an appropriate error.

## `anyhow-in-lib`

- Severity: `warning`
- Language: `rust`
- Message: Use SinexError instead of anyhow in library code
- Ignore globs:
  - `**/*_test.rs`
  - `**/main.rs`
  - `**/tests/**`
  - `crate/cli/**`
  - `xtask/**`
- Intent:
  SinexError is the project standard for error handling in library code.
  anyhow erases type information and prevents callers from matching error variants.
  Use SinexError::validation(), SinexError::service(), etc. with .with_context().

## `bare-offset-datetime`

- Severity: `warning`
- Language: `rust`
- Message: Use Timestamp wrapper instead of bare OffsetDateTime
- Ignore globs:
  - `crate/cli/**`
  - `crate/core/sinex-gateway/src/handlers/telemetry.rs`
  - `crate/lib/sinex-primitives/src/primitives/timestamp.rs`
  - `crate/lib/sinex-primitives/src/temporal.rs`
  - `xtask/**`
- Intent:
  The codebase uses Timestamp as the canonical time wrapper.
  Consider using Timestamp::now() instead of OffsetDateTime::now_utc()
  where the context expects a Timestamp.

## `chrono-usage`

- Severity: `warning`
- Language: `rust`
- Message: Use 'time' crate instead of 'chrono'
- Intent:
  The codebase standardizes on the 'time' crate for date/time handling.
  Use time::OffsetDateTime, time::Duration, etc. instead of chrono types.

## `color-eyre-in-runtime`

- Severity: `warning`
- Language: `rust`
- Message: Keep color_eyre at binary/CLI/test presentation boundaries; use SinexError in shared runtime and library code
- Ignore globs:
  - `**/*_test.rs`
  - `**/main.rs`
  - `**/tests/**`
  - `crate/cli/**`
  - `xtask/**`
- Intent:
  Shared library and runtime surfaces should return SinexError so callers can
  preserve error class, context, and protocol mapping. color_eyre is still
  acceptable at binary/CLI/devtool presentation boundaries and in tests.

## `context-erasure`

- Severity: `warning`
- Language: `rust`
- Message: Error context erasure: use .with_context() instead of .map_err(|_| ...)
- Ignore globs:
  - `tests/**`
  - `xtask/macros/**`
  - `xtask/src/jobs/mod.rs`
  - `xtask/src/sandbox/timing.rs`
- Intent:
  Using |_| in map_err discards the original error context.
  Use .with_context(|| "message") or .map_err(SinexError::from) instead.

## `double-clone`

- Severity: `warning`
- Language: `rust`
- Message: Double clone detected - likely unnecessary
- Intent:
  Cloning twice in succession is usually unnecessary.
  If you need a clone, one .clone() should suffice.

## `expect-hardcoded`

- Severity: `warning`
- Language: `rust`
- Message: Hardcoded expect() message - consider using context
- Intent:
  expect() with hardcoded messages loses context at runtime.
  Consider using unwrap_or_else with a closure that includes context,
  or use proper error handling with Result.

## `panic-in-lib`

- Severity: `warning`
- Language: `rust`
- Message: panic!() in library code - return Result instead
- Ignore globs:
  - `**/*_test.rs`
  - `**/build.rs`
  - `**/main.rs`
  - `**/tests/**`
  - `xtask/**`
- Intent:
  Library code should not panic. Return a Result<T, E> instead
  to let the caller decide how to handle the error.

## `raw-sqlx-query`

- Severity: `warning`
- Language: `rust`
- Message: Use sqlx::query!() macro instead of runtime sqlx::query() for compile-time checked queries
- Ignore globs:
  - `**/*_test.rs`
  - `**/tests/**`
  - `crate/core/sinex-gateway/src/cascade_analyzer.rs`
  - `crate/core/sinex-gateway/src/handlers/**`
  - `crate/core/sinex-gateway/src/rpc_server.rs`
  - `crate/core/sinex-ingestd/src/config.rs`
  - `crate/lib/sinex-db/src/lib.rs`
  - `crate/lib/sinex-db/src/pool.rs`
  - `crate/lib/sinex-db/src/query_helpers.rs`
  - `crate/lib/sinex-db/src/replay/**`
  - `crate/lib/sinex-db/src/repositories/**`
  - `crate/lib/sinex-node-sdk/src/preflight/**`
  - `crate/lib/sinex-schema/src/main.rs`
  - `xtask/**`
- Intent:
  Compile-time checked queries (sqlx::query!()) catch SQL errors at build time.
  Only use runtime sqlx::query() for truly dynamic queries, session control,
  or advisory locks where the SQL cannot be known at compile time.

