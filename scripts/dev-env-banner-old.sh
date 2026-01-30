#!/usr/bin/env bash
set -euo pipefail

# Skip non-interactive shells
if [[ ! -t 1 ]]; then
  exit 0
fi

# Colors
COLOR_BOLD_CYAN=$'\033[1;36m'
COLOR_RESET=$'\033[0m'
COLOR_DIM=$'\033[90m'
COLOR_GREEN=$'\033[32m'
COLOR_YELLOW=$'\033[33m'
COLOR_RED=$'\033[31m'

# Banner mode: compact (default), full (on error or SINEX_BANNER=full)
BANNER_MODE="${SINEX_BANNER:-compact}"

# State directory for history
STATE_DIR="${SINEX_STATE_DIR:-$HOME/.local/state/sinex}"
HISTORY_DB="${STATE_DIR}/xtask-history.db"

# Quick status checks
check_db() {
  local pg_host="${PGHOST:-localhost}"
  local db_name="${DATABASE_NAME:-${PGDATABASE:-sinex_dev}}"
  if command -v pg_isready >/dev/null 2>&1; then
    if pg_isready -t 1 -h "${pg_host}" -d "${db_name}" >/dev/null 2>&1; then
      echo "ok"
    elif pg_isready -t 1 -h "${pg_host}" >/dev/null 2>&1; then
      echo "warn"
    else
      echo "fail"
    fi
  else
    echo "unknown"
  fi
}

check_nats() {
  local nats_url="${SINEX_NATS_URL:-localhost:4222}"
  nats_url="${nats_url#nats://}"
  if timeout 1 bash -c "echo >/dev/tcp/${nats_url%%:*}/${nats_url##*:}" 2>/dev/null; then
    echo "ok"
  else
    echo "fail"
  fi
}

check_memory() {
  # Returns available memory in GB
  if [[ -f /proc/meminfo ]]; then
    local avail_kb
    avail_kb=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
    echo $((avail_kb / 1024 / 1024))
  else
    echo "0"
  fi
}

get_last_build_status() {
  if [[ -f "${HISTORY_DB}" ]] && command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "${HISTORY_DB}" "SELECT status,
      CASE WHEN julianday('now') - julianday(started_at) < 0.0007 THEN 'just now'
           WHEN julianday('now') - julianday(started_at) < 0.042 THEN cast(round((julianday('now') - julianday(started_at)) * 1440) as int) || 'm ago'
           WHEN julianday('now') - julianday(started_at) < 1 THEN cast(round((julianday('now') - julianday(started_at)) * 24) as int) || 'h ago'
           ELSE cast(round(julianday('now') - julianday(started_at)) as int) || 'd ago'
      END
    FROM invocations WHERE command = 'check' ORDER BY started_at DESC LIMIT 1" 2>/dev/null | head -1
  fi
}

get_last_test_status() {
  if [[ -f "${HISTORY_DB}" ]] && command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "${HISTORY_DB}" "SELECT status,
      CASE WHEN julianday('now') - julianday(started_at) < 0.0007 THEN 'just now'
           WHEN julianday('now') - julianday(started_at) < 0.042 THEN cast(round((julianday('now') - julianday(started_at)) * 1440) as int) || 'm ago'
           WHEN julianday('now') - julianday(started_at) < 1 THEN cast(round((julianday('now') - julianday(started_at)) * 24) as int) || 'h ago'
           ELSE cast(round(julianday('now') - julianday(started_at)) as int) || 'd ago'
      END
    FROM invocations WHERE command = 'test' ORDER BY started_at DESC LIMIT 1" 2>/dev/null | head -1
  fi
}

get_git_info() {
  if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    local branch dirty_count
    branch=$(git branch --show-current 2>/dev/null || echo "detached")
    dirty_count=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
    echo "${branch}|${dirty_count}"
  fi
}

status_symbol() {
  case "$1" in
    ok|success) echo "${COLOR_GREEN}✓${COLOR_RESET}" ;;
    warn|running) echo "${COLOR_YELLOW}⚠${COLOR_RESET}" ;;
    fail|failed) echo "${COLOR_RED}✗${COLOR_RESET}" ;;
    *) echo "${COLOR_DIM}?${COLOR_RESET}" ;;
  esac
}

# Gather status
DB_STATUS=$(check_db)
NATS_STATUS=$(check_nats)
MEM_GB=$(check_memory)
BUILD_INFO=$(get_last_build_status)
TEST_INFO=$(get_last_test_status)
GIT_INFO=$(get_git_info)

# Parse build/test info
BUILD_STATUS="${BUILD_INFO%%|*}"
BUILD_AGO="${BUILD_INFO##*|}"
TEST_STATUS="${TEST_INFO%%|*}"
TEST_AGO="${TEST_INFO##*|}"
GIT_BRANCH="${GIT_INFO%%|*}"
GIT_DIRTY="${GIT_INFO##*|}"

# Determine if we should show full banner
SHOW_FULL=0
if [[ "${BANNER_MODE}" == "full" ]]; then
  SHOW_FULL=1
elif [[ "${DB_STATUS}" == "fail" ]] || [[ "${NATS_STATUS}" == "fail" ]]; then
  SHOW_FULL=1
elif [[ "${TEST_STATUS}" == "failed" ]]; then
  SHOW_FULL=1
elif [[ "${MEM_GB}" -lt 8 ]] && [[ "${MEM_GB}" -gt 0 ]]; then
  SHOW_FULL=1
fi

# Build compact status line
compact_status() {
  local db_sym nats_sym build_sym test_sym line
  db_sym=$(status_symbol "${DB_STATUS}")
  nats_sym=$(status_symbol "${NATS_STATUS}")

  line="sinex ${db_sym} DB ${nats_sym} NATS"

  if [[ -n "${BUILD_STATUS}" ]]; then
    build_sym=$(status_symbol "${BUILD_STATUS}")
    line+=" | build: ${build_sym}"
    [[ -n "${BUILD_AGO}" ]] && line+=" ${BUILD_AGO}"
  fi

  if [[ -n "${TEST_STATUS}" ]]; then
    test_sym=$(status_symbol "${TEST_STATUS}")
    line+=" | test: ${test_sym}"
  fi

  if [[ -n "${GIT_BRANCH}" ]]; then
    line+=" | ${GIT_BRANCH}"
    [[ "${GIT_DIRTY}" -gt 0 ]] && line+=" (${GIT_DIRTY} dirty)"
  fi

  echo "${line}"
}

# Full banner output
full_banner() {
  local HEADLINE_LINE="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  local divider="${COLOR_BOLD_CYAN}${HEADLINE_LINE}${COLOR_RESET}"
  local headline="${COLOR_BOLD_CYAN}   sinex development environment${COLOR_RESET}"
  local toolchain="${SINEX_DEVENV_TOOLCHAIN:-fenix toolchain}"
  local pg_host="${PGHOST:-localhost}"
  local db_name="${DATABASE_NAME:-${PGDATABASE:-sinex_dev}}"
  local nats_url="${SINEX_NATS_URL:-localhost:4222}"

  printf '%s\n' "${divider}"
  printf '%s\n' "${headline}"
  printf '%s\n' "${divider}"

  # Toolchain
  printf '  Toolchain:  %s\n' "${toolchain}"

  # Database
  local db_sym db_msg
  db_sym=$(status_symbol "${DB_STATUS}")
  case "${DB_STATUS}" in
    ok) db_msg="${db_name} ready" ;;
    warn) db_msg="reachable; run 'createdb ${db_name}'" ;;
    fail) db_msg="unavailable on ${pg_host}" ;;
    *) db_msg="unknown" ;;
  esac
  printf '  Database:   %b %s\n' "${db_sym}" "${db_msg}"

  # NATS
  local nats_sym nats_msg
  nats_sym=$(status_symbol "${NATS_STATUS}")
  [[ "${NATS_STATUS}" == "ok" ]] && nats_msg="${nats_url}" || nats_msg="unavailable"
  printf '  NATS:       %b %s\n' "${nats_sym}" "${nats_msg}"

  # Memory
  if [[ "${MEM_GB}" -gt 0 ]]; then
    local mem_sym
    if [[ "${MEM_GB}" -lt 8 ]]; then
      mem_sym="${COLOR_YELLOW}⚠${COLOR_RESET}"
    else
      mem_sym="${COLOR_GREEN}✓${COLOR_RESET}"
    fi
    printf '  Memory:     %b %dGB free\n' "${mem_sym}" "${MEM_GB}"
  fi

  printf '%s\n' "${divider}"

  # Build/Test status
  if [[ -n "${BUILD_STATUS}" ]] || [[ -n "${TEST_STATUS}" ]]; then
    local build_sym test_sym
    if [[ -n "${BUILD_STATUS}" ]]; then
      build_sym=$(status_symbol "${BUILD_STATUS}")
      printf '  Last build: %b %s\n' "${build_sym}" "${BUILD_AGO:-unknown}"
    fi
    if [[ -n "${TEST_STATUS}" ]]; then
      test_sym=$(status_symbol "${TEST_STATUS}")
      printf '  Last test:  %b %s\n' "${test_sym}" "${TEST_AGO:-unknown}"
    fi
  fi

  # Git status
  if [[ -n "${GIT_BRANCH}" ]]; then
    printf '  Branch:     %s' "${GIT_BRANCH}"
    [[ "${GIT_DIRTY}" -gt 0 ]] && printf ' (%d dirty)' "${GIT_DIRTY}"
    printf '\n'
  fi

  printf '%s\n' "${divider}"

  # Quick commands
  printf '%sQuick commands:%s\n' "${COLOR_DIM}" "${COLOR_RESET}"
  printf '  cargo xtask check              # fmt + cargo check\n'
  printf '  cargo xtask test --profile default  # quick tests\n'
  printf '  cargo xtask history            # build/test history\n'
  printf '\n'
}

# Output based on mode
if [[ "${SHOW_FULL}" -eq 1 ]]; then
  full_banner
else
  compact_status
  echo ""
fi
