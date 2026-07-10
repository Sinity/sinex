# Cloud agent lane (Claude Code Web / Codex Cloud)

Sinex supports two execution lanes for contributor and agent work:

1. **Local / heavy lane** — sinnix-prime workstation or the Hetzner
   self-hosted runner (`[self-hosted, linux, x64, sinex-heavy]`). Full
   workspace builds, nested-KVM VM tests, live NATS + Postgres, deep
   integration tests.
2. **Cloud lane** — Claude Code Web and Codex Cloud sandboxes. Bounded
   compute, no nested KVM, no shared host services.

This document covers the cloud lane.

## Sandbox budget

Both Claude Code Web and Codex Cloud provision roughly:

- 4 vCPU
- 16 GB RAM
- 30 GB disk
- No nested KVM
- Outbound network, no inbound
- Ephemeral filesystem; setup runs every boot

## Appropriate work

- `xtask check -p <crate>` and `xtask test -p <crate>` for crates listed
  in [`xtask/config/cloud-safe-crates.toml`](../../xtask/config/cloud-safe-crates.toml).
- Formatting and linting through `xtask fix --fmt-only ...` and
  `xtask check --lint ...`.
- Docs, config, schema, and small focused refactors.
- PR repair: rebasing, conflict resolution, response to review comments
  scoped to a single crate.
- With the optional docker sidecar (see below): focused tests against
  ephemeral Postgres / NATS.

## Not appropriate

- The full hosted workspace workflow — exceeds RAM and disk.
- `xtask test vm` — needs nested KVM.
- Full workspace test runs — same reasons; also depends on live infrastructure.
- Anything that expects a system-level NATS or Postgres. Use the
  docker-compose sidecar instead if you need a database.

Route the above to the Hetzner self-hosted runner.

## Sandbox bootstrap

All surfaces run ONE profile-aware, idempotent script:
[`xtask/cloud/bootstrap.sh`](../cloud/bootstrap.sh).

| Surface | Entry point |
| --- | --- |
| Claude Code Web setup | `.claude/setup.sh` (thin wrapper; profile from `SINEX_CLOUD_PROFILE`, default `db`) |
| Claude cached session | `SessionStart` hook (installed into `.claude/settings.local.json` by the bootstrap) runs `bootstrap.sh <profile> --maintenance` — cached snapshots restore files, not running services |
| Codex Cloud setup script | `xtask/cloud/bootstrap.sh <profile>` |
| Codex Cloud maintenance script | `xtask/cloud/bootstrap.sh <profile> --maintenance` (runs after task-branch checkout; also refreshes the `xtask` binary so a cached default-branch build never verifies branch-modified code) |

Codex setup-phase exports do **not** persist into the agent phase; the
bootstrap therefore persists its environment via an idempotent `~/.bashrc`
block and `.claude/settings.local.json`, and prepends `PATH` entries with
growth guards.

### Profiles

- **`static`** — no-compile lanes only (docs, config, prompts, shell
  tooling). No rustup, no builds, no services. Works on any provider.
- **`db`** — the only Rust profile. `xtask` itself depends on `sinex-db`,
  whose `sqlx::query!` macros compile against a live, schema-populated
  PostgreSQL (no offline cache by doctrine), so even `sinex-primitives`
  lanes need a database to *build the frontend*. Bootstrap order is
  load-bearing: database reachable → `schema-apply-bootstrap` (the explicit
  pre-`xtask` chicken-and-egg exception in `sinex-schema`) → `cargo build
  -p xtask` → `cargo-nextest` (required, not best-effort).

### Database strategy (probed, never assumed)

1. **external** — `DATABASE_URL` already answers → use it, provision nothing.
2. **docker** — a working Docker daemon exists → build the in-repo sidecar
   images and `docker compose up -d --build --wait`. The
   `ghcr.io/sinity/sinex-*` tags are **not published** (verified 2026-07-10:
   anonymous pull 403/404), so compose carries `build:` contexts and images
   are built during the internet-enabled setup phase; container caches make
   later sessions cheap.
3. **neither** — hard fail with an explicit blocker. Codex Cloud runs inside
   `codex-universal`, which has **no Docker daemon** — `db` there requires an
   external ephemeral Postgres URL. A native-PG18 apt path (timescaledb +
   pgvector + pg_jsonschema in-sandbox) is not implemented; do not claim it
   until a capability probe passes. Never fall back to `SQLX_OFFLINE`.

The bootstrap also runs an **identity preflight** (`git remote -v`,
`rev-parse HEAD`, optional `SINEX_EXPECTED_REPO` assertion, archived-fork
guard) — the first cloud wave silently ran against an archived fork; lanes
must abort on a mis-bound environment instead of producing stale-base diffs.

### Settings: committed vs sandbox-only

The committed `.claude/settings.json` stays minimal (the `forbid-bare-cargo`
PreToolUse hook only). Sandbox env, permission allowances, and the
SessionStart maintenance hook are generated into
`.claude/settings.local.json` (gitignored) by the bootstrap, so cloud config
never perturbs a local workstation.

### Use xtask in the cloud lane

The `forbid-bare-cargo` hook only blocks bare `cargo` inside the sinex Nix
devshell. Cloud lanes still do all check/test work through `xtask`; direct
`cargo` is reserved for the bootstrap itself (toolchain, schema bootstrap,
xtask build) and for diagnostics when `xtask` cannot start — report that as
a harness failure rather than working around it.

If a fresh shell lacks the environment, `source ~/.bashrc` (the bootstrap's
persisted block) instead of hand-exporting URLs.

See [`xtask/cloud/docker/README.md`](../cloud/docker/README.md) for the
sidecar image build/publish workflow.
