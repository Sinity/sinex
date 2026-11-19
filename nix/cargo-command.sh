#!/usr/bin/env bash
set -euo pipefail

cargo="$1"
shift

# Only the shell's automatic target may be prepared. An inherited override,
# a changed target or another checkout keeps Cargo's original environment.
if command -v sinnix-cargo-target >/dev/null 2>&1 &&
  [ -n "${SINEX_CARGO_TARGET:-}" ] &&
  [ "${CARGO_TARGET_DIR:-}" = "$SINEX_CARGO_TARGET" ]; then
  selected="$(
    unset CARGO_TARGET_DIR
    sinnix-cargo-target --select-only
  )"
  if [ "$selected" = "$CARGO_TARGET_DIR" ]; then
    prepared="$(
      unset CARGO_TARGET_DIR
      sinnix-cargo-target --cargo "$@"
    )"
    if [ "$prepared" != "$CARGO_TARGET_DIR" ]; then
      echo "Cargo target changed while preparing the selected checkout" >&2
      exit 1
    fi
  fi
fi
exec "$cargo" "$@"
