# Sinex Agent Memory

> Personal event-driven capture platform. Rust, NATS JetStream, PostgreSQL (TimescaleDB + pgvector).
> ~464K lines of Rust (1052 `.rs` files under `crate/`, `tests/`, and `xtask/`), 14 workspace members, no trusted production dataset yet. Pre-release
> software for a single AuDHD user. Deployed on `sinnix-prime` with `sinex.enable = true`.

---

## Architecture

@.agent/includes/architecture/_index.md

---

## Agent Identity

@.agent/includes/identity/_index.md

---

## Beads Issue Tracking

This repository uses `bd` (Beads) for durable project task tracking.

- Run `bd prime` when task context, ready work, blockers, or durable project
  memory matter.
- Use `bd ready --json`, `bd show <id> --json`, `bd update <id> --claim --json`,
  and `bd close <id> --reason "..." --json` for tracked work.
- Create linked Beads issues for discovered follow-up work instead of leaving
  markdown TODO lists as the source of truth.
- `bd dolt push` follows the same repo policy as `git push`: push feature
  branches and PR updates proactively after verification, but do not push
  directly to protected/default branches outside the PR flow.

---

## Commands

@.agent/includes/commands/_index.md

---

## Code Patterns

@.agent/includes/patterns/_index.md

---

## Reference

@.agent/includes/reference/_index.md

---

## Contributor Workflow

@CONTRIBUTING.md

---

## Test Matrix

@TESTING.md
