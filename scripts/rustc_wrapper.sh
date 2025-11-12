#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "rustc wrapper: missing underlying compiler" >&2
  exit 1
fi

real_rustc="$1"
shift

exec "$real_rustc" "$@"
