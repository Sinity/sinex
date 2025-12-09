#!/usr/bin/env bash
set -euo pipefail

if [[ -t 1 ]]; then
  IS_TTY=1
else
  IS_TTY=0
fi

# Direnv will occasionally evaluate the environment non-interactively.
# Skip noisy output during those probes.
if [[ ${IS_TTY} -eq 0 ]]; then
  exit 0
fi

HEADLINE_LINE="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ ${IS_TTY} -eq 1 ]]; then
  COLOR_BOLD_CYAN=$'\033[1;36m'
  COLOR_RESET=$'\033[0m'
  COLOR_DIM=$'\033[90m'
  COLOR_GREEN=$'\033[32m'
  COLOR_YELLOW=$'\033[33m'
  COLOR_RED=$'\033[31m'
else
  COLOR_BOLD_CYAN=""
  COLOR_RESET=""
  COLOR_DIM=""
  COLOR_GREEN=""
  COLOR_YELLOW=""
  COLOR_RED=""
fi

divider="${COLOR_BOLD_CYAN}${HEADLINE_LINE}${COLOR_RESET}"
headline_text="${SINEX_DEVENV_HEADLINE:-🚀 SINEX Development Environment}"
headline="${COLOR_BOLD_CYAN}   ${headline_text}${COLOR_RESET}"

pg_host="${PGHOST:-localhost}"
db_name="${DATABASE_NAME:-${PGDATABASE:-sinex_dev}}"
db_status_symbol="${COLOR_RED}✗${COLOR_RESET}"
db_status_message="PostgreSQL unavailable on ${pg_host}"

if command -v pg_isready >/dev/null 2>&1; then
  if pg_isready -h "${pg_host}" -d "${db_name}" >/dev/null 2>&1; then
    db_status_symbol="${COLOR_GREEN}✓${COLOR_RESET}"
    db_status_message="${db_name} ready"
  elif pg_isready -h "${pg_host}" >/dev/null 2>&1; then
    db_status_symbol="${COLOR_YELLOW}⚠${COLOR_RESET}"
    db_status_message="Instance reachable; run 'createdb ${db_name}'"
  fi
fi

toolchain="${SINEX_DEVENV_TOOLCHAIN:-fenix toolchain}"
process_hint="${SINEX_DEVENV_PROCESS_HINT:-devenv up nats ingestd gateway}"
printf '%s\n' "${divider}"
printf '%s\n' "${headline}"
printf '%s\n\n' "${divider}"
printf 'Database:    %b  %s\n' "${db_status_symbol}" "${db_status_message}"
printf 'Toolchain:   %s\n' "${toolchain}"
printf 'Processes:   start via '\''%s'\''\n' "${process_hint}"
printf '%sQuick commands:%s\n' "${COLOR_DIM}" "${COLOR_RESET}"
printf '  xt           # cargo xtask (alias)\n'
printf '  xt sqlx-prepare  # refresh .sqlx after migrations/queries\n'
printf '  xt check     # sqlx check + fmt check + cargo check\n'
printf '  xt test      # nextest (profile=reliable)\n'
printf '  devenv up nats ingestd gateway\n\n'
