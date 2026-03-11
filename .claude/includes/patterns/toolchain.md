## Rust Toolchain & Language Features

### Nightly Rust (1.95.0-nightly) + Edition 2024

The project runs on **nightly Rust** via Nix flake (`fenix.packages.complete`), using **edition 2024**.
Toolchain updates are controlled — pin changes via `nix flake update fenix`, not automatic.

### Edition 2024 Key Changes

| Change | Impact | Pattern |
|--------|--------|---------|
| `set_var`/`remove_var` are unsafe | All `std::env::set_var()` calls need `unsafe { }` | `unsafe { std::env::set_var(k, v) }` |
| Implicit borrow in patterns | `ref` not allowed when scrutinee is already a reference | Use `.as_ref()` on scrutinee instead of `ref` in pattern |
| Let chains available | `if let A && let B { ... }` syntax works | Use for nested if-let flattening |
| RPIT lifetime capture | `-> impl Trait` captures all in-scope lifetimes | Use `+ use<'a>` to restrict if needed |

### Current Library API Conventions

| Area | Prefer | Avoid |
|------|--------|-------|
| `rand` 0.10 | `rand::random::<T>()`, `rand::random_range(range)`, `rng.random::<T>()`, `rng.random_range(range)` | `rng.r#gen::<T>()`, `rng.gen_range(...)` |
| `schemars` API paths | `schemars::SchemaGenerator`, `schemars::Schema`, `schemars::json_schema` | `schemars::r#gen::...` and older module-path variants |

### Active Nightly Feature Gates

```rust
// sinex-primitives — crate root
#![feature(never_type)]  // `!` as a type (type Err = ! in infallible FromStr)
```

### Stable Features Available (USE FREELY — no feature gate needed)

These features are stable on Rust ≥1.75 and available on our nightly toolchain:

| Feature | Since | What It Enables | Where Used |
|---------|-------|-----------------|------------|
| `#[diagnostic::on_unimplemented]` | 1.78 | Custom compile errors for trait bounds | `EventPayload`, `Publishable`, `AutomatonNode` |
| `async fn` in traits | 1.75 | Native async trait methods without `#[async_trait]` | sinex-db traits |
| `AsyncFnOnce()` | 1.85 | `F: AsyncFnOnce() -> T` instead of `F: FnOnce() -> Fut, Fut: Future<Output=T>` | chaos.rs, progress.rs, preflight_test.rs |
| `std::sync::LazyLock` | 1.80 | `lazy_static!` replacement in stdlib | Privacy detector regexes |
| `std::sync::OnceLock` | 1.80 | `once_cell::sync::OnceCell` replacement | Privacy engine global |
| Let chains | 1.88 + edition 2024 | `if let Some(x) = foo() && x > 5 { ... }` | jetstream_consumer, ingestor_node, dlq_retry, cli |

### Anti-Patterns (things you DON'T need on nightly)

| Don't Do This | Why | Do This Instead |
|---------------|-----|-----------------|
| `#![allow(async_fn_in_trait)]` in NEW code | Lint still fires on nightly 1.95 but is harmless | Existing allows are fine; don't add new ones unless needed |
| `lazy_static!` crate | `std::sync::LazyLock` replaces it | `static X: LazyLock<T> = LazyLock::new(\|\| ...)` |
| `once_cell::sync::OnceCell` | `std::sync::OnceLock` replaces it | `static X: OnceLock<T> = OnceLock::new()` |
| `type Err = Infallible` | Never type `!` is available | `type Err = !` |
| `if let Some(ref x) = &opt` | Edition 2024 implicit borrow | `if let Some(x) = &opt` or `if let Some(x) = opt.as_ref()` |
| `rng.gen::<T>()` / `rng.gen_range(...)` | Legacy pre-0.10 rand style | `rng.random::<T>()` / `rng.random_range(...)` |
| `schemars::r#gen::SchemaGenerator` in new code | Legacy edition-2024 workaround path | `schemars::SchemaGenerator` |
| `std::env::set_var(k, v)` without unsafe | Unsafe in edition 2024 (not thread-safe) | `unsafe { std::env::set_var(k, v) }` |
| `F: FnOnce() -> Fut, Fut: Future<Output=T>` for single-call | `AsyncFnOnce()` available since 1.85 | `F: AsyncFnOnce() -> T` (cleaner, one type param) |
| `F: AsyncFn() -> T` for polling/retry loops | `AsyncFn` futures borrow `&self`, breaking `Send` in spawn contexts | `F: Fn() -> Fut, Fut: Future<Output=T>` (owned future, Send-compatible) |
| `async \|\| { ... }` caller syntax | Creates futures with lifetime-tied borrows, breaks universal `Send` | `\|\| async { ... }` (works with both `Fn() -> Fut` AND `AsyncFn` bounds) |

### Applied Performance Optimizations

| Optimization | Crate | Where |
|-------------|-------|-------|
| SIMD byte scanning in `escape_copy_str` | `memchr` (direct dep of sinex-db) | `sinex-db/src/postgres_copy.rs` |
| Fast integer formatting | `itoa` (direct dep of sinex-db) | `sinex-db/src/postgres_copy.rs` |
