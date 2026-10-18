# Repository Guidelines

This is the Rust rewrite of Sinex. The README describes what works today and
how to run it; `docs/` has a note per implemented feature. The design
lives in the spec repository (`/realm/inbox/spec/sinex-spec/`). The old Sinex
is in Git history and is still the deployed version.

## Structure

The workspace follows the target architecture
(`/realm/project/sinex/analysis/architecture-20261018/sinex-architecture.md`
§2); bodies not written yet are `todo!()` naming their section.

- `crate/sinex-macros`: the `EventPayload` and `DbCheck` derives.
- `crate/sinex-model`: identifiers, the three clocks, record locations,
  equivalence keys, typed payloads (`kinds`), events and batches. No IO.
- `crate/sinex-protocol`: the command table and the producer protocol. No IO.
- `crate/sinex-schema`: the schema declared in Rust and `sinex-schema apply`.
  It has no checked queries, so it builds before any database exists.
- `crate/sinex-store`: admission (the only way events are stored), materials,
  positions, privacy rules, runs, queries. Knows no source by name.
- `crate/sinex-sources`: the parser trait, the drain, shared drivers
  (`AppendFile`, `SqliteRows`, others to come) and one module per source.
- `crate/sinex-automata`, `crate/sinex-analysis`: derivations and analysis as
  pure code.
- `crate/sinex-library`: the media library.
- `crate/sinexd`: the daemon: the writer task, source tasks, `backfill`.
- `crate/sinex-session`, `crate/sinex-capture`: the session service and the
  capture-only node, outside producers over the NATS ingress.
- `crate/sinexctl`, `crate/sinex-offline`: the CLI and MCP server, and
  diagnostics that work while the daemon is down.
- `xtask`: developer commands (`check`, `test`, `db`, `boundaries`), built on
  `devkit` from polylogue-sinex-common. Every run is recorded; `xtask status`
  shows what failed last time and `xtask history` reads more.

`xtask boundaries` enforces which crates may depend on which.

The store compiles its SQL against a real schema (SQLx checked queries; no
offline cache). `xtask db ensure` builds the database for the current schema
version and prints its `DATABASE_URL`; xtask commands set it themselves. The
database is named after a hash of the schema, model and derive sources, so a
schema change gives a new one and only the query-checking crates recompile.

## Development loop

Optimize for fast integration.

- While editing, run focused tests for the change.
- The pre-push hook (`.githooks/pre-push`) runs `cargo fmt --check` and
  `xtask boundaries` (seconds). There is no hosted CI and no required check;
  open a PR and merge it when it is ready.
- Heavier work runs as AgentCTL operations (`agentctl project operations
  sinex`): `check` (fmt, boundaries, clippy against the schema database),
  `test` (all Rust tests under nextest) and `package` (Nix build). Run
  them on batches of merged work, not before every merge, and fix what they
  find afterwards.
- Merge small PRs often. Rebase before pushing; do not keep long-lived
  branches.
- Report what you ran and what you did not. A merge is not evidence of testing.

## Code

- Rust, four-space indent, `rustfmt`.
- Comments state constraints and reasons, briefly. No status, history or spec
  narration in code; Git and Beads hold those.
- Unit tests sit with the code (`#[cfg(test)]` modules, or `<module>/tests.rs`
  when long). A test that needs PostgreSQL calls `devkit::pg::test_database()`
  for its own copy of the schema template; tests run in parallel under
  `xtask test`. CLI and MCP behavior is tested through the public commands,
  not by widening visibility.
- Register only operations that are implemented. Unsupported requests return a
  typed error, never an empty success. Unknown times stay unknown.
- Do not change stored source material; keep what was captured.

## Data

The repository is public: fixtures are synthetic, and credentials, real
captures, private reports and real databases stay out of Git. Reading the
operator's real files on this machine for development and measurement is fine.
