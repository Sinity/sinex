#!/usr/bin/env bash
set -euo pipefail

# Simple benchmark harness for Sinex builds and SQLx-related steps.
# Run this inside your dev environment (e.g. `devenv shell`) so cargo sees
# the right toolchain and Postgres settings.
#
# Usage:
#   scripts/bench-builds.sh
#   RUNS=5 scripts/bench-builds.sh
#
# You can comment out individual bench() calls below if you only care about
# a subset (e.g. just sqlx-prepare or just nix build).

RUNS="${RUNS:-3}"

repo_root() {
  local dir
  dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  echo "$dir"
}

bench() {
  local label="$1"
  shift

  echo "========== $label =========="
  echo "Command: $*"

  for i in $(seq 1 "$RUNS"); do
    echo "Run $i of $RUNS..."
    local start_ns end_ns dur_ms
    start_ns="$(date +%s%N)"
    if "$@"; then
      end_ns="$(date +%s%N)"
      dur_ms=$(( (end_ns - start_ns) / 1000000 ))
      printf '  Duration: %d ms\n' "$dur_ms"
    else
      echo "  Command failed; aborting further runs for this benchmark."
      return 1
    fi
  done
  echo
}

main() {
  local root
  root="$(repo_root)"
  cd "$root"

  echo "Repository root: $root"
  echo "RUNS=$RUNS"
  if [[ -z "${SINEX_DEVENV_SYSTEM:-}" ]]; then
    echo "NOTE: SINEX_DEVENV_SYSTEM is not set; you probably want to run this inside 'devenv shell'." >&2
  fi
  echo

  # 1) Fast-ish baseline: core correctness checks
  bench "cargo xtask check" \
    cargo xtask check

  # 2) SQLx offline metadata regeneration cost
  bench "cargo xtask sqlx-prepare" \
    cargo xtask sqlx-prepare

  # 3) CI-style pipeline with ephemeral Postgres (migrate + schema + tests)
  bench "cargo xtask ci postgres -- cargo xtask ci workspace" \
    cargo xtask ci postgres -- cargo xtask ci workspace

  # 4) Nix flake build for a single binary (ingest daemon).
  bench "nix build .#sinex-ingestd" \
    nix build .#sinex-ingestd

  # 5) Nix flake build for the full suite (symlinkJoin).
  bench "nix build .#sinex" \
    nix build .#sinex

  echo "Benchmarks complete."
}

main "$@"
