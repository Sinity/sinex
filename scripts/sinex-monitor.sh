#!/usr/bin/env bash
# Sinex System Monitor and Diagnostics
set -e

usage() {
  echo "Sinex System Monitor and Diagnostics"
  echo "Usage: sinex-monitor [COMMAND]"
  echo ""
  echo "Commands:"
  echo "  status           Show overall system status"
  echo "  logs [SERVICE]   Show logs for service (hyprland|filesystem|kitty|all)"
  echo "  tail [SERVICE]   Tail logs for service"
  echo "  db               Database connectivity and stats"
  echo "  events [LIMIT]   Show recent events (default: 10)"
  echo "  agents           Show agent status"
  echo "  health           Health check all services"
  echo "  restart SERVICE  Restart specific service"
  echo ""
}

show_status() {
  echo "=== Sinex System Status ==="
  echo ""
  
  # Service status
  echo "[Services]"
  for service in sinex-init sinex-grant-permissions sinex-hyprland sinex-filesystem sinex-kitty; do
    if systemctl is-enabled "$service" >/dev/null 2>&1; then
      status=$(systemctl is-active "$service" 2>/dev/null || echo "inactive")
      case $status in
        active) echo "  ✓ $service: $status" ;;
        *) echo "  ✗ $service: $status" ;;
      esac
    fi
  done
  echo ""
  
  # Database status
  echo "[Database]"
  if systemctl is-active postgresql >/dev/null 2>&1; then
    echo "  ✓ PostgreSQL: active"
    if sudo -u ${SINEX_USER:-sinity} psql "${DATABASE_URL:-postgresql://sinity@localhost/sinex}" -c "SELECT 1" >/dev/null 2>&1; then
      echo "  ✓ Sinex DB: accessible"
    else
      echo "  ✗ Sinex DB: connection failed"
    fi
  else
    echo "  ✗ PostgreSQL: inactive"
  fi
  echo ""
  
  # Recent activity
  echo "[Recent Activity]"
  if command -v exo.py >/dev/null 2>&1; then
    DATABASE_URL="${DATABASE_URL:-postgresql://sinity@localhost/sinex}" python3 exo.py query --limit 3 2>/dev/null || echo "  Unable to query recent events"
  else
    echo "  exo.py not available"
  fi
}

show_logs() {
  local service="$1"
  case "$service" in
    hyprland) journalctl -u sinex-hyprland -n 50 --no-pager ;;
    filesystem) journalctl -u sinex-filesystem -n 50 --no-pager ;;
    kitty) journalctl -u sinex-kitty -n 50 --no-pager ;;
    all|"")
      echo "=== Hyprland Ingestor ==="
      journalctl -u sinex-hyprland -n 20 --no-pager
      echo ""
      echo "=== Filesystem Ingestor ==="
      journalctl -u sinex-filesystem -n 20 --no-pager
      echo ""
      echo "=== Kitty Ingestor ==="
      journalctl -u sinex-kitty -n 20 --no-pager
      ;;
    *) echo "Unknown service: $service"; exit 1 ;;
  esac
}

tail_logs() {
  local service="$1"
  case "$service" in
    hyprland) journalctl -u sinex-hyprland -f ;;
    filesystem) journalctl -u sinex-filesystem -f ;;
    kitty) journalctl -u sinex-kitty -f ;;
    all|"")
      journalctl -u sinex-hyprland -u sinex-filesystem -u sinex-kitty -f ;;
    *) echo "Unknown service: $service"; exit 1 ;;
  esac
}

check_db() {
  echo "=== Database Diagnostics ==="
  echo ""
  
  local db_url="${DATABASE_URL:-postgresql://sinity@localhost/sinex}"
  local user="${SINEX_USER:-sinity}"
  
  # Connection test
  echo "[Connection Test]"
  if sudo -u "$user" psql "$db_url" -c "SELECT version();" 2>/dev/null; then
    echo "✓ Database connection successful"
  else
    echo "✗ Database connection failed"
    return 1
  fi
  echo ""
  
  # Schema check
  echo "[Schema Status]"
  sudo -u "$user" psql "$db_url" -c "
    SELECT 
      schemaname,
      COUNT(*) as table_count
    FROM pg_tables 
    WHERE schemaname IN ('raw', 'sinex_schemas', 'core')
    GROUP BY schemaname
    ORDER BY schemaname;
  " 2>/dev/null || echo "Schema check failed"
  echo ""
  
  # Event counts
  echo "[Event Statistics]"
  sudo -u "$user" psql "$db_url" -c "
    SELECT 
      source,
      event_type,
      COUNT(*) as count,
      MAX(ts_ingest) as latest
    FROM raw.events 
    GROUP BY source, event_type 
    ORDER BY count DESC 
    LIMIT 10;
  " 2>/dev/null || echo "Event statistics unavailable"
}

show_events() {
  local limit="$1"
  [[ -z "$limit" ]] && limit=10
  
  echo "=== Recent Events (last $limit) ==="
  if command -v exo.py >/dev/null 2>&1; then
    DATABASE_URL="${DATABASE_URL:-postgresql://sinity@localhost/sinex}" python3 exo.py query --limit "$limit"
  else
    local db_url="${DATABASE_URL:-postgresql://sinity@localhost/sinex}"
    local user="${SINEX_USER:-sinity}"
    sudo -u "$user" psql "$db_url" -c "
      SELECT 
        ts_ingest,
        source,
        event_type,
        payload::text
      FROM raw.events 
      ORDER BY ts_ingest DESC 
      LIMIT $limit;
    " 2>/dev/null || echo "Unable to query events"
  fi
}

show_agents() {
  echo "=== Agent Status ==="
  if command -v exo.py >/dev/null 2>&1; then
    DATABASE_URL="${DATABASE_URL:-postgresql://sinity@localhost/sinex}" python3 exo.py agent list
  else
    echo "exo.py not available for agent status"
  fi
}

health_check() {
  echo "=== Sinex Health Check ==="
  local issues=0
  
  # Check services
  for service in sinex-hyprland sinex-filesystem sinex-kitty; do
    if systemctl is-enabled "$service" >/dev/null 2>&1; then
      if ! systemctl is-active "$service" >/dev/null 2>&1; then
        echo "✗ Service $service is not active"
        ((issues++))
      fi
    fi
  done
  
  # Check database
  local db_url="${DATABASE_URL:-postgresql://sinity@localhost/sinex}"
  local user="${SINEX_USER:-sinity}"
  if ! sudo -u "$user" psql "$db_url" -c "SELECT 1" >/dev/null 2>&1; then
    echo "✗ Database connection failed"
    ((issues++))
  fi
  
  # Check recent activity
  local recent_events=$(sudo -u "$user" psql "$db_url" -t -c "
    SELECT COUNT(*) FROM raw.events 
    WHERE ts_ingest > NOW() - INTERVAL '1 hour';
  " 2>/dev/null | tr -d ' ' || echo "0")
  
  if [[ "$recent_events" -eq 0 ]]; then
    echo "⚠ No events in the last hour - ingestors may not be working"
    ((issues++))
  fi
  
  if [[ $issues -eq 0 ]]; then
    echo "✓ All systems healthy"
  else
    echo "✗ Found $issues issues"
    return 1
  fi
}

restart_service() {
  local service="$1"
  case "$service" in
    hyprland|filesystem|kitty)
      sudo systemctl restart "sinex-$service"
      echo "Restarted sinex-$service"
      ;;
    *) echo "Unknown service: $service"; exit 1 ;;
  esac
}

case "${1:-status}" in
  status) show_status ;;
  logs) show_logs "$2" ;;
  tail) tail_logs "$2" ;;
  db) check_db ;;
  events) show_events "$2" ;;
  agents) show_agents ;;
  health) health_check ;;
  restart) restart_service "$2" ;;
  help|--help|-h) usage ;;
  *) echo "Unknown command: $1"; usage; exit 1 ;;
esac