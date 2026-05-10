## Rust Toolchain

**Nightly Rust** (1.95.0-nightly) + **Edition 2024** via Nix flake (`fenix.packages.complete`).

### ABSOLUTE RULE — Never bare `cargo`

**Every interaction with the Rust toolchain goes through `xtask`. No exceptions.**

This is not "prefer xtask." This is not "use xtask for compilation." Every cargo subcommand
that touches this workspace's source tree, build artifacts, or test surface MUST be invoked
via its xtask wrapper. Reaching for raw `cargo` for any reason — including "I just want to
check something quickly," "I need a feature xtask doesn't expose," or "this isn't really a
build" — is a workflow violation. There is no class of cargo invocation that bypasses this
rule. A PreToolUse Bash hook actively blocks bare `cargo <subcommand>` invocations from
running at all; if you see the block, fix the command, don't try to bypass it.

| Instead of | Use | Notes |
|------------|-----|-------|
| `cargo check ...` | `xtask check ...` | Preflight, history, JSON output, `.sinex/target` |
| `cargo build ...` | `xtask build ...` | Captures diagnostics, proper target dir |
| `cargo test ...` | `xtask test ...` | Nextest wiring, preflight, history |
| `cargo nextest run ...` | `xtask test ...` | xtask test IS nextest under the hood |
| `cargo nextest list ...` | `xtask test --list ...` | Yes, this exists. Pair with `-p <pkg>` to scope. |
| `cargo fmt ...` | `xtask fix` (auto-format) or `xtask check --fmt` (verify) | |
| `cargo clippy ...` | `xtask check --lint` | |
| `cargo bench ...` | `xtask test bench --contracts` | |
| `cargo doc ...` | `xtask docs ...` | |
| `cargo run -p xtask -- <cmd>` | `xtask <cmd>` | The bare-cargo form recompiles xtask from source (~30s waste). |

**Discovering tests / scenarios** (the surface most likely to drift to bare cargo):

- `xtask test --list -p <pkg>` — list nextest-discovered tests in one package.
- `xtask test --list -p <pkg> -E 'test(name)'` — confirm a specific test compiled and registered.
- `xtask test --list-scenarios` — list `#[sinex_test(scenario = ...)]`-tagged tests across the workspace.
- `xtask test -p <pkg> -E 'test(name)' --bg` — run one test in the background; poll with `xtask jobs`.

**Never combine `--workspace` with `-p <pkg>`.** Cargo silently lets `--workspace` override
the package scope, expanding a package-scoped operation into a full workspace rebuild. With a
freshly-invalidated build cache that's a 10+ minute hang that looks like a stuck process. Pick
exactly one scoping flag.

**Bare `cargo` bypasses:** preflight checks, the shared `.sinex/target` directory, xtask
history capture, JSON-formatted diagnostics, the per-test-shape preflight (e.g. e2e binary
prep, runtime infra wiring), and the structured-output contract downstream tooling relies on.
The cost of an `xtask` wrapper over raw cargo is negligible (~0.3s preflight). The cost of
bypassing xtask is invisible drift that surfaces hours later as "why does this hang for 10
minutes?" or "why doesn't my test appear in CI?".

This rule has been violated repeatedly despite being documented. If you catch yourself typing
`cargo` in this repo, stop, delete the line, and re-type `xtask`. If xtask doesn't expose the
surface you want — that's a missing flag, extend xtask. Don't fall back to bare cargo.

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
