#!/usr/bin/env bash
# Cloud-agent sandbox setup for sinex
# ====================================
#
# Runs once at sandbox boot for Claude Code Web and Codex Cloud
# (both Ubuntu 24.04). Installs system libs sinex builds against,
# bootstraps rustup with the toolchain pinned in rust-toolchain.toml,
# pre-warms the cargo registry, and materializes the repository xtask binary.
#
# Idempotent — re-running is safe.
set -euo pipefail

log() { printf '[setup] %s\n' "$*" >&2; }

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------
# Mirrors the buildInputs sinex's flake.nix declares for the devShell:
# pkg-config + openssl/dbus/systemd headers, protobuf for tonic, mold +
# clang for the linker chain.
log "installing apt packages"
sudo_cmd=""
if [[ $EUID -ne 0 ]]; then
  sudo_cmd="sudo"
fi
${sudo_cmd} apt-get update -qq
${sudo_cmd} apt-get install -y --no-install-recommends \
  pkg-config \
  libssl-dev \
  libdbus-1-dev \
  libsystemd-dev \
  protobuf-compiler \
  mold \
  clang \
  ca-certificates \
  curl \
  git

# ---------------------------------------------------------------------------
# Rust toolchain via rustup (rust-toolchain.toml drives the pin)
# ---------------------------------------------------------------------------
if ! command -v rustup >/dev/null 2>&1; then
  log "installing rustup"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --no-modify-path --default-toolchain none
fi

# shellcheck disable=SC1091
. "${CARGO_HOME:-$HOME/.cargo}/env"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
export CARGO_HOME="${CARGO_HOME:-${repo_root}/.cargo}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/.target}"
export PATH="${CARGO_TARGET_DIR}/debug:${PATH}"

log "resolving toolchain from rust-toolchain.toml"
# `rustup show` triggers install of the channel + components declared in
# the repo's rust-toolchain.toml. Must run from the repo root.
rustup show

# ---------------------------------------------------------------------------
# Cargo pre-warm
# ---------------------------------------------------------------------------
log "fetching crate dependencies"
cargo fetch --locked

log "building repository xtask"
cargo build --locked -p xtask

log "installing cargo-nextest (best-effort)"
cargo install --locked cargo-nextest || log "cargo-nextest install skipped"

# ---------------------------------------------------------------------------
# Sidecar services (Postgres + NATS) for SQLx live validation
# ---------------------------------------------------------------------------
# sinex builds depend on sqlx::query!() macros that validate against a live
# database at compile time. The cloud lane provides this via a docker-compose
# sidecar rather than a committed offline cache. The sidecar lives for the
# sandbox lifetime; `cargo check`/`cargo test` see a real Postgres.
#
# Skip silently if docker is unavailable — agents on docker-less surfaces
# need to surface that themselves rather than fail boot.
cloud_compose="xtask/cloud/docker-compose.yml"
if command -v docker >/dev/null 2>&1 && [[ -f "${cloud_compose}" ]]; then
  log "pulling docker sidecars"
  docker compose -f "${cloud_compose}" pull || \
    log "docker compose pull failed (ignored)"
  log "starting docker sidecars (postgres + nats)"
  docker compose -f "${cloud_compose}" up -d || \
    log "docker compose up failed (ignored — agent must start sidecars manually)"
else
  log "docker unavailable or compose file missing — skipping sidecar boot"
  log "set DATABASE_URL before cargo check or sqlx macros will fail"
fi

# DATABASE_URL / NATS_URL for sqlx macros and integration tests. Exported so
# subsequent `cargo` invocations in this shell pick them up; agents in new
# shells should source this file or re-export.
export DATABASE_URL="${DATABASE_URL:-postgres://sinex:dev@localhost:5432/sinex_dev}"
export NATS_URL="${NATS_URL:-nats://localhost:4222}"
log "DATABASE_URL=${DATABASE_URL}"
log "NATS_URL=${NATS_URL}"

# ---------------------------------------------------------------------------
# Claude Code Web sandbox settings (gitignored, sandbox-only)
# ---------------------------------------------------------------------------
# The committed .claude/settings.json stays minimal so it never perturbs the
# local Nix dev environment. The cloud sandbox needs different cargo paths,
# the resolved DATABASE_URL/NATS_URL, and permission allowances for xtask,
# cargo/rustup/docker. Write them to .claude/settings.local.json, which Claude
# Code reads and merges over settings.json and which .gitignore excludes
# (.claude/*.local.json). The committed forbid-bare-cargo hook is already a
# no-op here because SINEX_DEV_ROOT / IN_NIX_SHELL are unset outside the Nix
# devshell; direct cargo remains available for setup/bootstrap diagnostics,
# while normal check/test/fix flows use xtask.
cat > "${repo_root}/.claude/settings.local.json" <<JSON
{
  "env": {
    "CARGO_HOME": "${CARGO_HOME}",
    "CARGO_TARGET_DIR": "${CARGO_TARGET_DIR}",
    "DATABASE_URL": "${DATABASE_URL}",
    "NATS_URL": "${NATS_URL}",
    "PATH": "${CARGO_TARGET_DIR}/debug:${PATH}",
    "RUSTC_WRAPPER": "",
    "SINEX_AUTO_INFRA": "0",
    "SINEX_AUTO_STATUS": "0"
  },
  "permissions": {
    "allow": [
      "Bash(xtask:*)",
      "Bash(cargo:*)",
      "Bash(rustup:*)",
      "Bash(docker:*)",
      "Bash(docker-compose:*)"
    ]
  }
}
JSON
log "wrote .claude/settings.local.json (sandbox-only Claude Code overrides)"

log "setup complete"
