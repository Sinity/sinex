#!/usr/bin/env bash
# Both Direnv and nix develop select before a command is known. Keep this
# ahead of the host's optional environment setup, which may otherwise seed.
if [ -z "${CARGO_TARGET_DIR:-}" ]; then
  unset SINEX_CARGO_TARGET
  if command -v sinnix-cargo-target >/dev/null 2>&1; then
    if cargo_target="$(sinnix-cargo-target --select-only)"; then
      export CARGO_TARGET_DIR="$cargo_target"
      export SINEX_CARGO_TARGET="$cargo_target"
    fi
  fi
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
fi
