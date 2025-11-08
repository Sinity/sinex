#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "rustc wrapper: missing underlying compiler" >&2
  exit 1
fi

real_rustc="$1"
shift

block=false
if [[ -z "${SINEX_ALLOW_NATIVE_TESTS:-}" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--test" ]]; then
      block=true
      break
    fi
  done
fi

if [[ "$block" == "true" ]]; then
  cat >&2 <<'MSG'
error: direct `cargo test` is disabled for this workspace.
       Use `just test` / `cargo nextest run` instead, or set SINEX_ALLOW_NATIVE_TESTS=1 to bypass.
MSG
  exit 1
fi

exec "$real_rustc" "$@"
