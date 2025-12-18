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
  echo "Git: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (dirty)')"
  echo "Toolchain: $(rustc --version 2>/dev/null || echo 'rustc not found') / $(cargo --version 2>/dev/null || echo 'cargo not found')"
  echo "Nix: $(nix --version 2>/dev/null || echo 'nix not found')"
  if [[ -z "${SINEX_DEVENV_SYSTEM:-}" ]]; then
    echo "NOTE: SINEX_DEVENV_SYSTEM is not set; you probably want to run this inside 'devenv shell'." >&2
  fi
  echo

  # 1) Fast-ish baseline: core correctness checks (includes sqlx prepare --check with SQLX_OFFLINE=1)
  bench "cargo xtask check" \
    cargo xtask check

  # 2) SQLx offline vs online compile-time checking
  # Offline: uses .sqlx JSON cache, SQLX_OFFLINE=1
  bench "cargo xtask sqlx-check (offline, .sqlx)" \
    cargo xtask sqlx-check

  # Online: skips .sqlx and talks to a live Postgres, requires DATABASE_URL
  bench "cargo xtask sqlx-check --online (no .sqlx)" \
    cargo xtask sqlx-check --online

  # 3) SQLx offline metadata regeneration cost (writes/refreshes .sqlx)
  bench "cargo xtask sqlx-prepare (regen .sqlx)" \
    cargo xtask sqlx-prepare

  # 4) CI-style pipeline with ephemeral Postgres (migrate + schema + tests)
  bench "cargo xtask ci postgres -- cargo xtask ci workspace" \
    cargo xtask ci postgres -- cargo xtask ci workspace

  # 5) Nix flake build for a single binary (ingest daemon).
  # Offline: current design, uses .sqlx cache and SQLX_OFFLINE=1
  local -a nix_args=()
  if [[ "$NIX_NO_LINK" == "1" ]]; then
    nix_args+=(--no-link)
  fi

  bench "nix build .#sinexIngestd (offline, .sqlx)" \
    nix build "${nix_args[@]}" .#sinexIngestd

  # Online: experimental path using ephemeral Postgres for SQLx at build time
  bench "nix build .#sinexIngestdOnline (online, no .sqlx)" \
    nix build "${nix_args[@]}" .#sinexIngestdOnline

  # 6) Nix flake build for the full suite (symlinkJoin).
  bench "nix build .#sinex (offline, .sqlx)" \
    nix build "${nix_args[@]}" .#sinex

  bench "nix build .#sinexOnline (online, no .sqlx)" \
    nix build "${nix_args[@]}" .#sinexOnline

  echo "Benchmarks complete."
}

main "$@"
