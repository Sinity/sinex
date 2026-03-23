## Rust Toolchain

**Nightly Rust** (1.95.0-nightly) + **Edition 2024** via Nix flake (`fenix.packages.complete`).

### Edition 2024 Rules

| Rule | Pattern |
|------|---------|
| `set_var`/`remove_var` are unsafe | `unsafe { std::env::set_var(k, v) }` |
| Implicit borrow in patterns | `if let Some(x) = &opt` (no `ref` keyword) |
| Let chains | `if let Some(x) = foo() && x > 5 { .. }` |
| RPIT lifetime capture | `-> impl Trait + use<'a>` to restrict |

### Available Stable Features (use freely)

| Feature | Use for |
|---------|---------|
| `async fn` in traits (1.75) | All node/SDK traits (native, no `#[async_trait]` needed) |
| `AsyncFnOnce()` (1.85) | Single-call async closures: `F: AsyncFnOnce() -> T` |
| `LazyLock` (1.80) | Replace `lazy_static!` |
| `OnceLock` (1.80) | Replace `once_cell::OnceCell` |
| `#[diagnostic::on_unimplemented]` (1.78) | Custom compile errors on trait bounds |
| Let chains (1.88 + ed2024) | Flatten nested `if let` |
| `!` never type (nightly, feature-gated) | `type Err = !` in infallible `FromStr` |

### Async Closure Subtlety

| Context | Use | Avoid |
|---------|-----|-------|
| Single-call (consumed) | `F: AsyncFnOnce() -> T` | `F: FnOnce() -> Fut` (verbose) |
| Multi-call/polling loops | `F: Fn() -> Fut, Fut: Future<Output=T>` | `F: AsyncFn() -> T` (breaks Send in spawn) |
| Caller syntax (always) | `\|\| async { .. }` | `async \|\| { .. }` (breaks Send) |

### Performance-Relevant

- SIMD byte scanning via `memchr` in COPY escape path (`sinex-db/src/postgres_copy.rs`)
- Fast integer formatting via `itoa` in COPY serialization
