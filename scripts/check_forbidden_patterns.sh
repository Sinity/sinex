#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

FAIL=0

TOKIO_TEST_ALLOW=(
  "crate/lib/sinex-test-utils/macros/src/lib.rs"
  "crate/lib/sinex-test-utils/tests/rstest_integration_example.rs"
  "crate/lib/sinex-test-utils/tests/database_pool_tests.rs"
)

RUST_TEST_ALLOW=(
  "crate/lib/sinex-test-utils/macros/src/lib.rs"
)

SQLX_QUERY_ALLOW=(
  "crate/core/sinex-gateway/src/cascade_analyzer.rs"
  "crate/lib/sinex-core/src/db/repositories/events.rs"
  "crate/lib/sinex-core/src/db/replay/state_machine.rs"
  "crate/lib/sinex-satellite-sdk/src/preflight/database.rs"
  "crate/lib/sinex-satellite-sdk/src/preflight/verification.rs"
  "crate/lib/sinex-test-utils/src/database_pool.rs"
  "crate/lib/sinex-test-utils/src/db_common.rs"
  "crate/lib/sinex-test-utils/src/fixture_generator.rs"
)

SQLX_QUERY_AS_ALLOW=(
  "crate/lib/sinex-core/src/db/repositories/common.rs"
  "crate/lib/sinex-satellite-sdk/src/preflight/database.rs"
)

is_tests_path() {
  [[ "$1" == */tests/* ]] || [[ "$1" == tests/* ]]
}

in_allowlist() {
  local target="$1"
  shift
  local entry
  for entry in "$@"; do
    if [[ "$target" == "$entry" ]]; then
      return 0
    fi
  done
  return 1
}

check_pattern_strict() {
  local label="$1"
  local pattern="$2"
  shift 2
  local allow=("$@")
  local matches
  matches=$(rg --color=never --no-heading --with-filename --line-number "$pattern" --glob '*.rs' 2>/dev/null || true)
  if [[ -z "$matches" ]]; then
    return
  fi

  local line file bad=()
  while IFS= read -r line; do
    file="${line%%:*}"
    if in_allowlist "$file" "${allow[@]}"; then
      continue
    fi
    bad+=("$line")
  done <<< "$matches"

  if ((${#bad[@]})); then
    echo "Forbidden pattern detected ($label):"
    printf '  %s\n' "${bad[@]}"
    FAIL=1
  fi
}

check_pattern_allow_tests() {
  local label="$1"
  local pattern="$2"
  shift 2
  local allow=("$@")
  local matches
  matches=$(rg --color=never --no-heading --with-filename --line-number "$pattern" --glob '*.rs' 2>/dev/null || true)
  if [[ -z "$matches" ]]; then
    return
  fi

  local line file bad=()
  while IFS= read -r line; do
    file="${line%%:*}"
    if is_tests_path "$file" || in_allowlist "$file" "${allow[@]}"; then
      continue
    fi
    bad+=("$line")
  done <<< "$matches"

  if ((${#bad[@]})); then
    echo "Forbidden pattern detected ($label):"
    printf '  %s\n' "${bad[@]}"
    FAIL=1
  fi
}

# Guard against #[tokio::test] in normal crates.
check_pattern_strict "#[tokio::test]" "#\[tokio::test" "${TOKIO_TEST_ALLOW[@]}"

# Guard against plain #[test] (prefer #[sinex_test]); allow macro definitions.
check_pattern_strict "#[test]" "#\[test\]" "${RUST_TEST_ALLOW[@]}"

# Guard against runtime sqlx::query / query_as usage outside allowlist/tests.
check_pattern_allow_tests "sqlx::query(" "sqlx::query\(" "${SQLX_QUERY_ALLOW[@]}"
check_pattern_allow_tests "sqlx::query_as(" "sqlx::query_as\(" "${SQLX_QUERY_AS_ALLOW[@]}"

if [[ "$FAIL" -ne 0 ]]; then
  echo "Forbidden patterns found. See messages above."
  exit 1
fi
