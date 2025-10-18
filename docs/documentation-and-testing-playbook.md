# Documentation & Testing Playbook

This playbook captures how Sinex organizes written design context and executable
tests. Treat it as the canonical reference when adding new crates, wiring up
rustdoc, or deciding where tests belong.

## 1. Documentation Principles

- **Single source of truth** – rich design, architecture, and workflow notes live
  next to the code they describe. Inline comments stay short and always point
  toward the canonical Markdown source.
- **Layered narrative** – crate-local documentation owns the immediate “what” and
  “how”, while workspace-level docs under `docs/` provide cross-cutting stories
  (architecture decisions, roadmaps, operations guides). Link between layers
  liberally so readers can climb or descend the stack.
- **Guaranteed discoverability** – every crate must surface its main reference
  material directly through rustdoc so `cargo doc` readers reach the same context
  as someone browsing the filesystem.

## 2. Crate Layout Requirements

1. Create a `doc/` directory in every crate root (`<crate>/doc/`).
2. Add `doc/README.md` that explains the crate’s responsibility, major public
   entry points, and how it interacts with the rest of the system.
3. Write one Markdown file per deep-dive topic or module:
   - Name files after the module or concern (`doc/replay_state_machine.md`,
     `doc/grpc_client.md`).
   - Co-locate diagrams or auxiliary assets under the same directory when needed.
4. Keep narrow, tactical notes (field explanations, algorithm choices) inside the
   Markdown. The corresponding `.rs` file should only retain a short pointer such
   as `//! See crate::doc::replay_state_machine`.

## 3. Rustdoc Inclusion Guide

- At the crate root, include the overview via `#![doc = include_str!("doc/README.md")]`.
- For modules that have companion Markdown, place `#![doc = include_str!("doc/<file>.md")]`
  (or the appropriate relative path) on the module.
- When a module benefits from a short inline summary, keep a 1–2 sentence `//!`
  block in the source above the include. This provides instant context without
  duplicating the long-form documentation.
- Cross-reference global documentation by chaining includes. Example:
  ```rust
  #![doc = include_str!("doc/README.md")]
  #![doc = include_str!("../../docs/architecture/runtime/topology.md")]
  ```
- Use crate-relative paths for intra-crate references and workspace-relative
  paths when pointing to `docs/`. This keeps links stable if the crate moves.

## 4. Workspace Documentation

- The top-level `docs/` directory continues to host architecture books,
  blueprints, historical analyses, and other cross-cutting materials.
- Crate-level Markdown should link upward when additional background already
  exists. For example, a gateway module deep dive can reference
  `../../docs/architecture/data-plane.md` for broader context.
- Update global docs when crate-level changes alter system-wide behaviour; the
  crate doc should highlight the relationship and trust the reader to follow the
  link for the full story.

## 5. Testing Strategy

- **Inline tests** are allowed only for private helpers or code that cannot be
  reached from integration-style tests (e.g., proc-macro parsing utilities). All
  other scenarios belong in dedicated files under `<crate>/tests/`.
- **Crate-level tests** mirror the module or behaviour they cover. For instance,
  `stream_processor.rs` should have a partner test file such as
  `tests/stream_processor.rs`.
- **Shared fixtures** live inside the crate (e.g., `crate::tests::util`) or in
  `sinex-test-utils` when reused across the workspace.
- The workspace `tests/` tree remains reserved for cross-cutting, multi-crate, or
  system validation suites; do not mirror crate-level behaviour there.
- Always invoke tests through `just test` / `cargo nextest run` to match CI and
  ensure `.proptest-regressions` files stay synchronized when property tests
  introduce new seeds.

### 5.1 Test Categories

Sinex maintains dedicated suites for distinct concerns. When adding or updating
tests, align with the existing directory structure:

- `tests/unit/` – fast, isolated component checks.
- `tests/integration/` – cross-component workflows and database interactions.
- `tests/system/` – end-to-end scenarios that exercise the full stack.
- `tests/property/` – property-based tests powered by proptest.
- `tests/adversarial/` – boundary, chaos, and attack surface validation.
- `tests/performance/` – throughput, latency, and resource behaviour.
- `tests/security/`, `tests/concurrency/` – specialised suites for their domains.

### 5.2 Nextest Profiles

Nextest drives all automated runs. The repository defines the following
profiles in `nextest.toml`:

| Profile    | Purpose                                  | Tweaks                                      |
|------------|------------------------------------------|---------------------------------------------|
| `default`  | CI and developer baseline                | `test-threads = num-cpus`, retry once       |
| `fast`     | Quick local feedback                     | 4 threads, shorter slow-timeout             |
| `reliable` | Flake hunting / soak tests               | 2 threads, 3 retries, longer timeouts       |
| `parallel` | Maximum throughput on beefy machines     | `test-threads = num-cpus`, retries disabled |

Select a profile via `cargo nextest run --profile <name>` or using the `just`
aliases (e.g., `just test-fast`, `just test-reliable`).

## 6. Migration Checklist

When touching a crate, run through this list:

1. [ ] `doc/` directory exists with `README.md` and module deep dives.
2. [ ] Crate root uses `#![doc = include_str!(...)]` for the overview and any
       relevant global documents.
3. [ ] Modules include their Markdown counterparts via `#![doc = include_str!(...)]`.
4. [ ] Inline documentation is limited to short summaries or essential notes.
5. [ ] Integration and property tests live under `<crate>/tests/` unless
       exercising private-only helpers.
6. [ ] Inline tests are justified (proc-macro parsing, minimal helper coverage).
7. [ ] Workspace docs are linked when they provide extended rationale.
8. [ ] `just check` and `just test` succeed locally after documentation or test
       moves.

Following this playbook keeps Sinex documentation discoverable and our tests
reliable, while avoiding the drift that comes from scattering long-form context
throughout the source tree.

## 7. Quality Gates & Automation

### 7.1 Linting & Static Analysis

- **Clippy** (`clippy.toml`) enforces architectural rules such as banning raw
  SQL (`sqlx::query`) and discouraging generic `anyhow` errors. Complexity
  thresholds (arguments, lines, cognitive complexity) ensure maintainable code.
- **Unsafe code** is globally denied. If a future change requires it, document
  the justification and safety invariants explicitly.
- **Async hygiene**: linting rejects holding locks across `.await` and other
  unsafe async patterns.

### 7.2 Continuous Integration

GitHub Actions (`.github/workflows/ci.yml`) executes the following stages:

1. **Environment checks** – `nix flake check` keeps the dev shell reproducible.
2. **Formatting & linting** – `cargo fmt`, `cargo clippy`, and the static rules
   above.
3. **Tests** – `cargo nextest run --profile default --workspace` against a
   TimescaleDB-enabled PostgreSQL instance.
4. **SQLx offline validation** – ensures query metadata in `.sqlx/` matches the
   current schema.
5. **Coverage (optional)** – `cargo llvm-cov` can be triggered via `just` when
   deeper analysis is required.

Reference the workflow when adding new steps so local and CI expectations stay
aligned.
