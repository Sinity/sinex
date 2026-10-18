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
- `crate/sinex-testkit`: test helpers, used only as a dev-dependency:
  per-test PostgreSQL databases cloned from a schema template.
- `xtask`: developer commands (`check`, `test`, `scenario`, `boundaries`).
- `tests/`: Python end-to-end scenarios that run the real daemon and CLI; they
  are being moved into Rust integration tests.

`xtask boundaries` enforces which crates may depend on which.

The admin-only daemon (`cargo build -p sinexd --no-default-features`) builds
without a database and applies the schema. Everything else uses SQLx checked
queries against a live development database; no offline cache. `xtask test`
and `xtask scenario` start one and set `DATABASE_URL`; for a plain build, point
`DATABASE_URL` at a development database with the schema applied.

## Development loop

Optimize for fast integration.

- While editing, run focused tests for the change.
- The pre-push hook (`.githooks/pre-push`) runs `cargo fmt --check` and
  `xtask boundaries` (seconds). There is no hosted CI and no required check;
  open a PR and merge it when it is ready.
- Heavier work runs as AgentCTL operations (`agentctl project operations
  sinex`): `check` (fmt, boundaries, cargo check), `test` (all tests, with
  nextest, in an owned PostgreSQL), `scenario`, and `package` (Nix build). Run
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
  or `tests/`); small tests may stay inline. A test that needs PostgreSQL calls
  `sinex_testkit::database()` for its own database and runs under `xtask
  test`. CLI and MCP behavior is tested through the public commands, not by
  widening visibility. Tests never wait out real default timeouts; configure
  short ones.
- Register only operations that are implemented. Unsupported requests return a
  typed error, never an empty success. Unknown times stay unknown.
- Do not change stored source material; keep what was captured.

## Data

The repository is public: fixtures are synthetic, and credentials, real
captures, private reports and real databases stay out of Git. Reading the
operator's real files on this machine for development and measurement is fine.
