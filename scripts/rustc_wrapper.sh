#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "rustc wrapper: missing underlying compiler" >&2
  exit 1
fi

real_rustc="$1"
shift

sccache_bin="${SINEX_SCCACHE:-}"
if [[ -n "$sccache_bin" && -x "$sccache_bin" ]]; then
  exec "$sccache_bin" "$real_rustc" "$@"
else
  exec "$real_rustc" "$@"
fi
