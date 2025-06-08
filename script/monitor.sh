#!/usr/bin/env bash
set -euo pipefail

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

log() { echo -e "${BLUE}📊${NC} $*"; }
success() { echo -e "${GREEN}✅${NC} $*"; }
warning() { echo -e "${YELLOW}⚠️${NC} $*"; }
error() { echo -e "${RED}❌${NC} $*" >&2; }

MODE="${1:-dashboard}"

# Load current database from state file
STATE_FILE="$HOME/.sinex_db_state"
if [ -f "$STATE_FILE" ]; then
  DATABASE_URL=$(cat "$STATE_FILE")
  export DATABASE_URL
fi

# Extract database name from URL
get_db_name() {
  if [[ "$DATABASE_URL" =~ postgresql:///([^?]+) ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "unknown"
  fi
}

check_database() {
  local db_name=$(get_db_name)
  if psql "$DATABASE_URL" -c "SELECT 1;" >/dev/null 2>&1; then
    return 0
  else
    return 1
  fi
}

show_dashboard() {
  clear
  local db_name=$(get_db_name)
  echo -e "${BOLD}┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
  echo -e "${BOLD}┃  Sinex Live Dashboard                                      ┃${NC}"
  echo -e "${BOLD}┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫${NC}"

  if check_database; then
    echo -e "┃ 🗄️  Database: ${GREEN}●${NC} $db_name                              ┃"

    local total_events=$(psql "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM raw.events;" 2>/dev/null | xargs)
    local recent_events=$(psql "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '1 hour';" 2>/dev/null | xargs)

    echo "┃ 📈 Total Events: $total_events                                  ┃"
    echo "┃ 🕐 Last Hour: $recent_events                                     ┃"

    echo "┃                                                            ┃"
    echo "┃ 📁 Recent Events by Source:                               ┃"
    psql "$DATABASE_URL" -t -c "
      SELECT '┃   ' || RPAD(source, 15) || ': ' || LPAD(count::text, 8) || '                         ┃'
      FROM (
        SELECT source, COUNT(*) as count
        FROM raw.events
        WHERE ts_ingest > NOW() - INTERVAL '1 hour'
        GROUP BY source
        ORDER BY count DESC
        LIMIT 5
      ) t
    " 2>/dev/null || echo "┃   No recent events                                         ┃"
  else
    echo -e "┃ 🗄️  Database: ${RED}●${NC} DISCONNECTED                           ┃"
    echo "┃                                                            ┃"
    echo -e "┃ ${YELLOW}💡 Run: nix run .#db setup dev${NC}                            ┃"
  fi

  echo "┃                                                            ┃"
  echo "┃ ⌨️  Commands:                                              ┃"
  echo "┃   [r] Refresh   [e] Recent Events   [l] Live Tail         ┃"
  echo "┃   [p] Processes [s] System Stats    [q] Quit               ┃"
  echo -e "${BOLD}┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"
}

show_recent_events() {
  clear
  echo -e "${BOLD}Recent Events (Last 10):${NC}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if check_database; then
    psql "$DATABASE_URL" -c "
      SELECT
        LEFT(id::text, 8) as id,
        source,
        event_type,
        ts_ingest::timestamp(0)
      FROM raw.events
      ORDER BY ts_ingest DESC
      LIMIT 10
    " 2>/dev/null || echo "No events found"
  else
    error "Database not connected"
  fi

  echo ""
  echo "Press any key to return to dashboard..."
  read -n 1
}

live_tail() {
  clear
  echo -e "${BOLD}Live Event Stream (Ctrl+C to exit):${NC}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if check_database; then
    local last_id=""
    while true; do
      local new_events=$(psql "$DATABASE_URL" -t -c "
        SELECT COUNT(*) FROM raw.events WHERE id > '${last_id:-00000000-0000-0000-0000-000000000000}'
      " 2>/dev/null | xargs)

      if [[ "$new_events" -gt 0 ]]; then
        psql "$DATABASE_URL" -c "
          SELECT
            '[' || ts_ingest::timestamp(0) || '] ' ||
            source || ':' || event_type ||
            ' | ' || LEFT(payload::text, 50) || '...'
          FROM raw.events
          WHERE id > '${last_id:-00000000-0000-0000-0000-000000000000}'
          ORDER BY ts_ingest DESC
        " 2>/dev/null

        last_id=$(psql "$DATABASE_URL" -t -c "
          SELECT id FROM raw.events ORDER BY ts_ingest DESC LIMIT 1
        " 2>/dev/null | xargs)
      fi

      sleep 2
    done
  else
    error "Database not connected"
    exit 1
  fi
}

show_processes() {
  clear
  echo -e "${BOLD}System Processes:${NC}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if pg_isready -h /run/postgresql >/dev/null 2>&1; then
    success "PostgreSQL: Running"
  else
    warning "PostgreSQL: Not running"
  fi

  echo ""
  echo -e "${BOLD}Running Ingestors:${NC}"
  ps aux | grep -E "(filesystem|kitty|hyprland)-ingestor" | grep -v grep || echo "No ingestors running"

  echo ""
  echo "Press any key to return to dashboard..."
  read -n 1
}

show_system_stats() {
  clear
  echo -e "${BOLD}System Statistics:${NC}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  echo "💾 Disk Usage:"
  df -h . | tail -n +2

  echo ""
  echo "🧠 Memory Usage:"
  free -h | head -n 2

  if check_database; then
    echo ""
    echo "🗄️  Database Size:"
    psql "$DATABASE_URL" -c "
      SELECT
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
      FROM pg_tables
      WHERE schemaname IN ('raw', 'sinex_schemas')
      ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    " 2>/dev/null
  fi

  echo ""
  echo "Press any key to return to dashboard..."
  read -n 1
}

case "$MODE" in
  dashboard|"")
    trap 'echo "Exiting..."; exit 0' INT
    while true; do
      show_dashboard
      read -n 1 -t 5 key || key=""
      case "$key" in
        r|R) continue ;;
        e|E) show_recent_events ;;
        l|L) live_tail ;;
        p|P) show_processes ;;
        s|S) show_system_stats ;;
        q|Q) exit 0 ;;
      esac
    done
    ;;
  events)
    show_recent_events
    ;;
  live)
    live_tail
    ;;
  *)
    error "Usage: monitor [dashboard|events|live]"
    exit 1
    ;;
esac