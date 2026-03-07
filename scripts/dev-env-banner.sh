#!/usr/bin/env bash
set -euo pipefail

# Skip non-interactive shells
if [[ ! -t 1 ]]; then
  exit 0
fi

# Colors
C_CYAN=$'\033[36m'
C_DIM=$'\033[90m'
C_GREEN=$'\033[32m'
C_YELLOW=$'\033[33m'
C_RED=$'\033[31m'
C_RESET=$'\033[0m'

# State directory for history
STATE_DIR="${SINEX_STATE_DIR:-$PWD/.sinex/state}"
HISTORY_DB="${STATE_DIR}/xtask-history.db"
DEV_STATE_DIR="${SINEX_DEV_STATE_DIR:-.sinex}"

# Status symbols
sym_ok="${C_GREEN}✓${C_RESET}"
sym_warn="${C_YELLOW}⚠${C_RESET}"
sym_fail="${C_RED}✗${C_RESET}"
sym_unknown="${C_DIM}?${C_RESET}"

status_sym() {
  case "$1" in
    ok|success|running) echo "$sym_ok" ;;
    warn) echo "$sym_warn" ;;
    fail|failed|stopped) echo "$sym_fail" ;;
    *) echo "$sym_unknown" ;;
  esac
}

# Infrastructure checks
check_postgres() {
  if [ -f "$DEV_STATE_DIR/run/postgres.pid" ] && \
     kill -0 "$(cat "$DEV_STATE_DIR/run/postgres.pid" 2>/dev/null)" 2>/dev/null; then
    echo "ok|unix socket"
  else
    echo "stopped|not running"
  fi
}

check_nats() {
  local nats_port="${SINEX_DEV_NATS_PORT:-4222}"
  if [ -f "$DEV_STATE_DIR/run/nats.pid" ] && \
     kill -0 "$(cat "$DEV_STATE_DIR/run/nats.pid" 2>/dev/null)" 2>/dev/null; then
    echo "ok|port $nats_port"
  else
    echo "stopped|not running"
  fi
}

check_memory() {
  if [[ -f /proc/meminfo ]]; then
    local avail_kb
    avail_kb=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
    local avail_gb=$((avail_kb / 1024 / 1024))
    if [ "$avail_gb" -lt 8 ]; then
      echo "warn|${avail_gb}GB available"
    else
      echo "ok|${avail_gb}GB available"
    fi
  else
    echo "unknown|"
  fi
}

get_last_command_status() {
  local cmd="$1"
  if [[ -f "${HISTORY_DB}" ]] && command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "${HISTORY_DB}" "
      SELECT status,
        CASE
          WHEN julianday('now') - julianday(started_at) < 0.0007 THEN 'just now'
          WHEN julianday('now') - julianday(started_at) < 0.042 THEN
            cast(round((julianday('now') - julianday(started_at)) * 1440) as int) || 'm ago'
          WHEN julianday('now') - julianday(started_at) < 1 THEN
            cast(round((julianday('now') - julianday(started_at)) * 24) as int) || 'h ago'
          ELSE
            cast(round(julianday('now') - julianday(started_at)) as int) || 'd ago'
        END
      FROM invocations
      WHERE command = '$cmd'
      ORDER BY started_at DESC
      LIMIT 1
    " 2>/dev/null | head -1
  fi
}

get_git_status() {
  if command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    local branch dirty_count
    branch=$(git branch --show-current 2>/dev/null || echo "detached")
    dirty_count=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
    echo "${branch}|${dirty_count}"
  fi
}

# Gather status
PG_INFO=$(check_postgres)
NATS_INFO=$(check_nats)
MEM_INFO=$(check_memory)
BUILD_INFO=$(get_last_command_status "check")
TEST_INFO=$(get_last_command_status "test")
GIT_INFO=$(get_git_status)

# Parse info
PG_STATUS="${PG_INFO%%|*}"
PG_DETAIL="${PG_INFO##*|}"
NATS_STATUS="${NATS_INFO%%|*}"
NATS_DETAIL="${NATS_INFO##*|}"
MEM_STATUS="${MEM_INFO%%|*}"
MEM_DETAIL="${MEM_INFO##*|}"
BUILD_STATUS="${BUILD_INFO%%|*}"
BUILD_AGO="${BUILD_INFO##*|}"
TEST_STATUS="${TEST_INFO%%|*}"
TEST_AGO="${TEST_INFO##*|}"
GIT_BRANCH="${GIT_INFO%%|*}"
GIT_DIRTY="${GIT_INFO##*|}"

# Print banner
echo "${C_CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"
echo "${C_CYAN}  sinex development environment${C_RESET}"
echo "${C_CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${C_RESET}"

# Infrastructure
echo ""
echo "Infrastructure:"
printf "  %b Postgres ${C_DIM}%s${C_RESET}\n" "$(status_sym "$PG_STATUS")" "$PG_DETAIL"
printf "  %b NATS ${C_DIM}%s${C_RESET}\n" "$(status_sym "$NATS_STATUS")" "$NATS_DETAIL"
printf "  %b Memory ${C_DIM}%s${C_RESET}\n" "$(status_sym "$MEM_STATUS")" "$MEM_DETAIL"

# Repository state
if [[ -n "$GIT_BRANCH" ]]; then
  echo ""
  echo "Repository:"
  printf "  Branch: %s" "$GIT_BRANCH"
  if [[ "$GIT_DIRTY" -gt 0 ]]; then
    printf " ${C_DIM}(%d dirty)${C_RESET}" "$GIT_DIRTY"
  fi
  echo ""

  if [[ -n "$BUILD_STATUS" ]]; then
    printf "  Build:  %b ${C_DIM}%s${C_RESET}\n" "$(status_sym "$BUILD_STATUS")" "${BUILD_AGO:-unknown}"
  fi
  if [[ -n "$TEST_STATUS" ]]; then
    printf "  Tests:  %b ${C_DIM}%s${C_RESET}\n" "$(status_sym "$TEST_STATUS")" "${TEST_AGO:-unknown}"
  fi
fi

# Quick commands
echo ""
echo "Quick start:"
if [[ "$PG_STATUS" == "stopped" ]] || [[ "$NATS_STATUS" == "stopped" ]]; then
  printf "  ${C_CYAN}sx stack start${C_RESET}       # Start Postgres + NATS\n"
fi
printf "  ${C_CYAN}sx check${C_RESET}              # Format + type check\n"
printf "  ${C_CYAN}sx test${C_RESET}               # Run test suite\n"
printf "  ${C_CYAN}sx history${C_RESET}            # Build/test history\n"

echo ""
