#!/usr/bin/env bash
# Cloud-agent sandbox setup for sinex
# ====================================
#
# Runs once at sandbox boot for Claude Code Web and Codex Cloud
# (both Ubuntu 24.04). Installs system libs sinex builds against,
# bootstraps rustup with the toolchain pinned in rust-toolchain.toml,
# and pre-warms the cargo registry.
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

log "resolving toolchain from rust-toolchain.toml"
# `rustup show` triggers install of the channel + components declared in
# the repo's rust-toolchain.toml. Must run from the repo root.
rustup show

# ---------------------------------------------------------------------------
# Cargo pre-warm
# ---------------------------------------------------------------------------
log "fetching crate dependencies"
cargo fetch --locked

log "installing cargo-nextest (best-effort)"
cargo install --locked cargo-nextest || log "cargo-nextest install skipped"

# ---------------------------------------------------------------------------
# Optional: pre-pull docker sidecars when docker is available
# ---------------------------------------------------------------------------
# Uncomment if the cloud sandbox has docker and you intend to run
# DB-touching focused tests via docker-compose.cloud.yml.
#
# if command -v docker >/dev/null 2>&1 && [[ -f docker-compose.cloud.yml ]]; then
#   log "pulling docker sidecars"
#   docker compose -f docker-compose.cloud.yml pull || \
#     log "docker compose pull failed (ignored)"
# fi

log "setup complete"
