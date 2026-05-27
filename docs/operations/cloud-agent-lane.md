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

- `cargo check -p <crate>` and `cargo test -p <crate>` for crates listed
  in [`xtask/config/cloud-safe-crates.toml`](../../xtask/config/cloud-safe-crates.toml).
- Lint: `cargo fmt --check`, `cargo clippy -p <crate>`.
- Docs, config, schema, and small focused refactors.
- PR repair: rebasing, conflict resolution, response to review comments
  scoped to a single crate.
- With the optional docker sidecar (see below): focused tests against
  ephemeral Postgres / NATS.

## Not appropriate

- `xtask ci workspace` — exceeds RAM and disk.
- `xtask test vm` — needs nested KVM.
- Full workspace `cargo test` — same reasons; also depends on live
  infrastructure.
- Anything that expects a system-level NATS or Postgres. Use the
  docker-compose sidecar instead if you need a database.

Route the above to the Hetzner self-hosted runner.

## Sandbox bootstrap

Both surfaces honour `.claude/setup.sh`:

- Installs Ubuntu apt deps (`pkg-config`, `libssl-dev`, `libdbus-1-dev`,
  `libsystemd-dev`, `protobuf-compiler`, `mold`, `clang`).
- Installs `rustup`; `rustup show` then materialises the toolchain pinned
  by [`rust-toolchain.toml`](../../rust-toolchain.toml).
- Runs `cargo fetch --locked` to pre-warm the registry.
- Installs `cargo-nextest` (best-effort).

Environment defaults are set by `.claude/settings.json`:

| Variable             | Value           | Why                                                        |
| -------------------- | --------------- | ---------------------------------------------------------- |
| `SQLX_OFFLINE`       | `true`          | SQLx macros use the committed `.sqlx/` cache, no live DB.  |
| `CARGO_HOME`         | `/workspace/.cargo` | Survives within the sandbox lifetime.                  |
| `CARGO_TARGET_DIR`   | `/workspace/.target` | Same.                                                  |
| `SINEX_AUTO_INFRA`   | `0`             | Disables autostart of local infra in cloud.                |
| `SINEX_AUTO_STATUS`  | `0`             | Disables status polling daemons.                           |
| `RUSTC_WRAPPER`      | empty           | No sccache; the sandbox cache is local-only anyway.        |

## Database / NATS sidecars

For focused tests that need a real Postgres or NATS, see
[`docker/README.md`](../../docker/README.md) and
[`docker-compose.cloud.yml`](../../docker-compose.cloud.yml). The sidecars
are ephemeral (`tmpfs` data dir for Postgres); each sandbox boot starts
clean.

## SQLx offline cache

The cloud lane builds with `SQLX_OFFLINE=true`. That requires the
operator to run `cargo sqlx prepare --workspace` once on a host with a
live database and commit the resulting `.sqlx/` directory. See
[`docs/sqlx-offline.md`](../sqlx-offline.md) for the full workflow.
