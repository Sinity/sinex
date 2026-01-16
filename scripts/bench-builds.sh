#!/usr/bin/env bash
set -euo pipefail

# Simple benchmark harness for Sinex builds.
# Run this inside your dev environment (e.g. `devenv shell`) so cargo sees
# the right toolchain and Postgres settings.
#
# Usage:
#   scripts/bench-builds.sh
#   RUNS=5 scripts/bench-builds.sh
#
# You can comment out individual bench() calls below if you only care about
# a subset (e.g. just nix build).
#
# For nextest benchmarks, use `scripts/bench-nextest.sh` (wrapper for `cargo xtask bench`).

RUNS="${RUNS:-3}"
NIX_NO_LINK="${NIX_NO_LINK:-1}"

repo_root() {
  local dir
  dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  echo "$dir"
}

format_ms() {
  local ms="$1"
  printf '%d ms' "$ms"
}

ms_stats() {
  local -a values=("$@")
  local n="${#values[@]}"
  if [[ "$n" -eq 0 ]]; then
    echo "n=0"
    return
  fi

  local sum=0 min="${values[0]}" max="${values[0]}"
  for v in "${values[@]}"; do
    sum=$((sum + v))
    if (( v < min )); then min="$v"; fi
    if (( v > max )); then max="$v"; fi
  done
  local avg=$((sum / n))

  # median
  local sorted
  sorted="$(printf '%s\n' "${values[@]}" | sort -n)"
  local mid=$((n / 2))
  local median
  if (( n % 2 == 1 )); then
    median="$(printf '%s\n' "$sorted" | sed -n "$((mid + 1))p")"
  else
    local a b
    a="$(printf '%s\n' "$sorted" | sed -n "${mid}p")"
    b="$(printf '%s\n' "$sorted" | sed -n "$((mid + 1))p")"
    median=$(((a + b) / 2))
  fi

  printf 'n=%d min=%s median=%s avg=%s max=%s' \
    "$n" "$(format_ms "$min")" "$(format_ms "$median")" "$(format_ms "$avg")" "$(format_ms "$max")"
}

bench() {
  local label="$1"
  shift

  echo "========== $label =========="
  echo "Command: $*"
  local -a durs=()

  for i in $(seq 1 "$RUNS"); do
    echo "Run $i of $RUNS..."
    local start_ns end_ns dur_ms
    start_ns="$(date +%s%N)"
    if "$@"; then
      end_ns="$(date +%s%N)"
      dur_ms=$(( (end_ns - start_ns) / 1000000 ))
      durs+=("$dur_ms")
      printf '  Duration: %s\n' "$(format_ms "$dur_ms")"
    else
      echo "  Command failed; aborting further runs for this benchmark."
      return 1
    fi
  done
  echo "Summary: $(ms_stats "${durs[@]}")"
  echo
}

main() {
  local root
  root="$(repo_root)"
  cd "$root"

  echo "Repository root: $root"
  echo "RUNS=$RUNS"
  echo "NIX_NO_LINK=$NIX_NO_LINK"
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "Git: $(git rev-parse --short HEAD) (dirty)"
  else
    echo "Git: $(git rev-parse --short HEAD)"
  fi
  echo "Toolchain: $(rustc --version 2>/dev/null || echo 'rustc not found') / $(cargo --version 2>/dev/null || echo 'cargo not found')"
  echo "Nix: $(nix --version 2>/dev/null || echo 'nix not found')"
  if [[ -z "${SINEX_DEVENV_SYSTEM:-}" ]]; then
    echo "NOTE: SINEX_DEVENV_SYSTEM is not set; you probably want to run this inside 'devenv shell'." >&2
  fi
  echo

  # 1) Fast-ish baseline: core correctness checks
  bench "cargo xtask check" \
    cargo xtask check

  # 2) CI-style pipeline with ephemeral Postgres (migrate + schema + tests)
  bench "cargo xtask ci postgres -- cargo xtask ci workspace" \
    cargo xtask ci postgres -- cargo xtask ci workspace

  # 3) Nix flake build for a single binary (ingest daemon).
  local -a nix_args=()
  if [[ "$NIX_NO_LINK" == "1" ]]; then
    nix_args+=(--no-link)
  fi

  bench "nix build .#sinexIngestd" \
    nix build "${nix_args[@]}" .#sinexIngestd

  # 4) Nix flake build for the full suite (symlinkJoin).
  bench "nix build .#sinex" \
    nix build "${nix_args[@]}" .#sinex

  echo "Benchmarks complete."
}

main "$@"
