# Repository Guidelines

This is the Rust rewrite of Sinex. The README describes what works today and
how to run it; `docs/` has a note per implemented feature. The design
lives in the spec repository (`/realm/inbox/spec/sinex-spec/`). The old Sinex
is in Git history and is still the deployed version.

## Structure

- `crate/sinex-model`: domain types; no IO.
- `crate/sinex-protocol`: RPC request, reply and error types; no IO.
- `crate/sinex-store`: the PostgreSQL schema and every PostgreSQL connection.
  Only its writer admits events; readers get read-only handles.
- `crate/sinexd`: the daemon: configuration, source collectors (Atuin, zsh,
  Chromium), the RPC server and `sinexd admin schema`.
- `crate/sinexctl`: the CLI client and the MCP server.
- `xtask`: developer commands (`check`, `test`, `scenario`, `db`, `boundaries`),
  built on `devkit` from polylogue-sinex-common. Every run is recorded;
  `xtask status` shows what failed last time and `xtask history` reads more.
- `tests/`: Python end-to-end scenarios that run the real daemon and CLI.

`xtask boundaries` enforces which crates may depend on which.

The admin-only daemon (`cargo build -p sinexd --no-default-features`) builds
without a database and applies the schema. Everything else compiles its SQL
against a real schema (SQLx checked queries; no offline cache). `xtask db
ensure` builds the database for the current schema version and prints its
`DATABASE_URL`; xtask commands set it themselves. The database is named after
a hash of `crate/sinex-store/src/schema`, so a schema change gives a new one
and only the query-checking crates recompile.

## Development loop

Optimize for fast integration.

- While editing, run focused tests for the change.
- The pre-push hook (`.githooks/pre-push`) runs `cargo fmt --check` and
  `xtask boundaries` (seconds). There is no hosted CI and no required check;
  open a PR and merge it when it is ready.
- Heavier work runs as AgentCTL operations (`agentctl project operations
  sinex`): `check` (fmt, boundaries, cargo check), `test` (all Rust tests in an
  owned PostgreSQL), `package` (Nix build) and the end-to-end scenarios. Run
  them on batches of merged work, not before every merge, and fix what they
  find afterwards.
- Merge small PRs often. Rebase before pushing; do not keep long-lived
  branches.
- Report what you ran and what you did not. A merge is not evidence of testing.

## Code

- Rust, four-space indent, `rustfmt`.
- Comments state constraints and reasons, briefly. No status, history or spec
  narration in code; Git and Beads hold those.
- Tests go in separate files (`src/<module>_tests.rs`, `src/<module>/tests.rs`
  or `tests/`); small tests may stay inline. Tests that need PostgreSQL are
  `#[ignore]` and read `SINEX_TEST_ADMIN_URL`, `SINEX_TEST_WRITER_URL`,
  `SINEX_TEST_READER_URL` and `SINEX_TEST_STORE_DIR`; `xtask test` gives each
  one a fresh database. CLI and MCP behavior is tested through the public
  commands, not by widening visibility.
- Register only operations that are implemented. Unsupported requests return a
  typed error, never an empty success. Unknown times stay unknown.
- Do not change stored source material; keep what was captured.

## Data

The repository is public: fixtures are synthetic, and credentials, real
captures, private reports and real databases stay out of Git. Reading the
operator's real files on this machine for development and measurement is fine.
