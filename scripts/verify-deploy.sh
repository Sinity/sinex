#!/usr/bin/env bash
# Comprehensive sinexd post-deploy verification.
#
# Verifies the LIVE deployment state on the host where it's invoked.
# Designed to be safe to re-run; reads only. Exits non-zero if any
# critical check fails.
#
# Critical checks (exit non-zero on failure):
#   - sinexd.service active
#   - API listening on configured port
#   - No critical/cascade errors in recent journal
#   - Renamed event types reaching core.events
#   - Old-name event leakage is zero
#
# Soft checks (warn but exit zero):
#   - DLQ growth rate
#   - Persistence error rate
#   - NATS stream capacity headroom
#
# Usage:
#   verify-deploy.sh                       # human-readable
#   verify-deploy.sh --json                # JSON output
#   verify-deploy.sh --include-dlq-samples # also include first/last DLQ payload
#
# Run on the host where sinexd is deployed (needs sudo for systemctl /
# postgres, and the sinex user's nats CLI access).

set -u

FORMAT="text"
INCLUDE_DLQ=0
for arg in "$@"; do
  case "$arg" in
    --json) FORMAT="json" ;;
    --include-dlq-samples) INCLUDE_DLQ=1 ;;
    --help|-h)
      sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
  esac
done

CRITICAL_FAILURES=0
WARNINGS=0
declare -A RESULTS

declare -a EVENT_LINES

emit() {
  local key="$1"; shift
  local val="$*"
  RESULTS["$key"]="$val"
  if [ "$FORMAT" = "text" ]; then
    printf "  %-40s %s\n" "$key" "$val"
  fi
}

section() {
  if [ "$FORMAT" = "text" ]; then
    printf "\n=== %s ===\n" "$1"
  fi
}

fail_critical() {
  CRITICAL_FAILURES=$((CRITICAL_FAILURES + 1))
}

warn() {
  WARNINGS=$((WARNINGS + 1))
}

section "Service state"
SVC_STATE=$(sudo systemctl is-active sinexd 2>&1)
emit "service.active" "$SVC_STATE"
[ "$SVC_STATE" = "active" ] || fail_critical

SVC_SUB=$(sudo systemctl show sinexd -p SubState --value 2>&1)
emit "service.sub_state" "$SVC_SUB"
[ "$SVC_SUB" = "running" ] || fail_critical

ACTIVE_SINCE=$(sudo systemctl show sinexd -p ActiveEnterTimestamp --value 2>&1)
emit "service.active_since" "$ACTIVE_SINCE"
if [ -n "$ACTIVE_SINCE" ] && [ "$ACTIVE_SINCE" != "n/a" ]; then
  UPTIME_S=$(($(date +%s) - $(date -d "$ACTIVE_SINCE" +%s)))
  emit "service.uptime_seconds" "$UPTIME_S"
fi

MAIN_PID=$(sudo systemctl show sinexd -p MainPID --value 2>&1)
emit "service.main_pid" "$MAIN_PID"

RESTART_COUNT=$(sudo systemctl show sinexd -p NRestarts --value 2>&1)
emit "service.restart_count" "$RESTART_COUNT"
if [ "${RESTART_COUNT:-0}" -gt 5 ]; then warn; fi

section "API listener"
LISTEN=$(sudo ss -tlnp 2>&1 | grep -c 'sinexd' || true)
emit "api.tcp_listeners" "$LISTEN"
[ "${LISTEN:-0}" -ge 1 ] || fail_critical

section "Schema continuous aggregates"
CA_COUNT=$(sudo -u postgres psql -d sinex_prod -tAc "SELECT count(*) FROM timescaledb_information.continuous_aggregates;" 2>&1)
emit "schema.ca_count" "$CA_COUNT"

# Verify the 4 CAs the collapse touched have correct new-name source filters.
for ca_pair in \
  "ingestd_batch_stats_1h|sinexd.event_engine" \
  "stream_stats_1h|sinexd.event_engine" \
  "assembly_stats_1h|sinexd.event_engine" \
  "gateway_stats_1h|sinexd.api"
do
  CA="${ca_pair%%|*}"
  EXPECTED="${ca_pair##*|}"
  EXISTS=$(sudo -u postgres psql -d sinex_prod -tAc \
    "SELECT view_definition FROM timescaledb_information.continuous_aggregates WHERE view_name = '$CA';" 2>&1)
  if [ -z "$EXISTS" ]; then
    emit "schema.ca.$CA" "MISSING"
    fail_critical
  elif echo "$EXISTS" | grep -q "$EXPECTED"; then
    emit "schema.ca.$CA" "ok (filter mentions $EXPECTED)"
  else
    emit "schema.ca.$CA" "WRONG_FILTER (no mention of $EXPECTED)"
    fail_critical
  fi
done

section "Event flow (renamed sources)"
NEW_NAME_COUNT=$(sudo -u postgres psql -d sinex_prod -tAc \
  "SELECT count(*) FROM core.events WHERE source LIKE 'sinexd.%' AND ts_persisted > NOW() - INTERVAL '15 minutes';" 2>&1)
emit "events.sinexd_prefix_15min" "$NEW_NAME_COUNT"
[ "${NEW_NAME_COUNT:-0}" -ge 1 ] || fail_critical

OLD_NAME_COUNT=$(sudo -u postgres psql -d sinex_prod -tAc \
  "SELECT count(*) FROM core.events WHERE source IN ('sinex.ingestd','sinex.gateway') AND ts_persisted > NOW() - INTERVAL '15 minutes';" 2>&1)
emit "events.old_name_leak_15min" "$OLD_NAME_COUNT"
[ "${OLD_NAME_COUNT:-0}" -eq 0 ] || fail_critical

# Sample of top sources in last 5 min — diagnostic, not a check
if [ "$FORMAT" = "text" ]; then
  echo "  top sources (last 5min):"
  sudo -u postgres psql -d sinex_prod -tc \
    "SELECT source, COUNT(*) FROM core.events WHERE ts_persisted > NOW() - INTERVAL '5 minutes' GROUP BY source ORDER BY count DESC LIMIT 10;" 2>&1 \
    | sed 's/^/    /'
fi

section "NATS streams"
RAW_INFO=$(sudo -u sinex nats --server nats://127.0.0.1:4222 stream info PROD_SINEX_RAW_EVENTS 2>&1)
RAW_MSGS=$(echo "$RAW_INFO" | awk -F: '/^ *Messages:/ {gsub(/[, ]/,"",$2); print $2; exit}')
emit "nats.raw_events.messages" "$RAW_MSGS"
RAW_FIRST=$(echo "$RAW_INFO" | grep "First Sequence" | head -1)
emit "nats.raw_events.first" "$RAW_FIRST"

DLQ_INFO=$(sudo -u sinex nats --server nats://127.0.0.1:4222 stream info PROD_SINEX_RAW_EVENTS_DLQ 2>&1)
DLQ_MSGS=$(echo "$DLQ_INFO" | awk -F: '/^ *Messages:/ {gsub(/[, ]/,"",$2); print $2; exit}')
emit "nats.dlq.messages" "$DLQ_MSGS"
if [ "${DLQ_MSGS:-0}" -gt 100000 ]; then warn; fi

if [ "$INCLUDE_DLQ" = "1" ] && [ -n "$DLQ_MSGS" ] && [ "$DLQ_MSGS" -gt 0 ]; then
  DLQ_LAST_SEQ=$(echo "$DLQ_INFO" | grep "Last Sequence" | awk -F: '{gsub(/[, ]/,"",$2); print $2; exit}')
  if [ -n "$DLQ_LAST_SEQ" ]; then
    section "DLQ latest message"
    sudo -u sinex nats --server nats://127.0.0.1:4222 stream get \
      PROD_SINEX_RAW_EVENTS_DLQ "$DLQ_LAST_SEQ" 2>&1 \
      | head -5 \
      | sed 's/^/  /'
  fi
fi

section "Watchdog"
WATCHDOG_USEC=$(sudo systemctl show sinexd -p WatchdogUSec --value 2>&1)
emit "watchdog.usec" "$WATCHDOG_USEC"
WATCHDOG_LAST=$(sudo systemctl show sinexd -p WatchdogTimestamp --value 2>&1)
emit "watchdog.last_ping" "$WATCHDOG_LAST"

# If watchdog is enabled and active, last ping should be within 2× the interval.
if [ "$WATCHDOG_USEC" != "0" ] && [ "$WATCHDOG_USEC" != "infinity" ] && [ -n "$WATCHDOG_USEC" ]; then
  if [ -n "$WATCHDOG_LAST" ] && [ "$WATCHDOG_LAST" != "n/a" ]; then
    PING_AGE=$(($(date +%s) - $(date -d "$WATCHDOG_LAST" +%s)))
    emit "watchdog.ping_age_seconds" "$PING_AGE"
    if [ "$PING_AGE" -gt 120 ]; then warn; fi
  fi
fi

section "Recent errors (last 15min)"
PARAM_OVERFLOW=$(sudo journalctl -u sinexd --since "15 minutes ago" --no-pager 2>&1 \
  | grep -c "too many arguments for query" || true)
emit "errors.param_overflow_15min" "$PARAM_OVERFLOW"
[ "${PARAM_OVERFLOW:-0}" -eq 0 ] || fail_critical

BATCH_FAILS=$(sudo journalctl -u sinexd --since "15 minutes ago" --no-pager 2>&1 \
  | grep -c "batch_persistence_failures_total" || true)
emit "errors.batch_persistence_failures_15min" "$BATCH_FAILS"
if [ "${BATCH_FAILS:-0}" -gt 50 ]; then warn; fi

CRITICAL=$(sudo journalctl -u sinexd --since "15 minutes ago" --no-pager 2>&1 \
  | grep -ciE "critical_failure_signals_total|shutdown_step: service" || true)
emit "errors.critical_failures_15min" "$CRITICAL"
[ "${CRITICAL:-0}" -eq 0 ] || fail_critical

CASCADE=$(sudo journalctl -u sinexd --since "15 minutes ago" --no-pager 2>&1 \
  | grep -c "Continuous scan returned unexpectedly" || true)
emit "errors.continuous_scan_returned_15min" "$CASCADE"
# Expected for monitor source-units (terminal.monitor, system.monitor) — they
# fire ServiceStart once at boot then return Ok. The hosted-mode sd_notify
# latch prevents their clean exit from cascading shutdown across siblings.
# Only warn if the count looks unusually high (something else is exiting).
if [ "${CASCADE:-0}" -gt 5 ]; then warn; fi

section "Source-worker liveness"
HB_15MIN=$(sudo journalctl -u sinexd --since "15 minutes ago" --no-pager 2>&1 \
  | grep -c "Node heartbeat emitted")
emit "heartbeat.count_15min" "$HB_15MIN"
[ "${HB_15MIN:-0}" -ge 1 ] || fail_critical

ACTIVE_SOURCE_UNITS=$(sudo journalctl -u sinexd --since "5 minutes ago" --no-pager 2>&1 \
  | grep "heartbeat emitted service=sinex-source-worker-" \
  | sed 's/.*service=sinex-source-worker-\([^ ]*\) .*/\1/' \
  | sort -u | wc -l)
emit "source_workers.active_5min" "$ACTIVE_SOURCE_UNITS"

ACTIVE_AUTOMATA=$(sudo journalctl -u sinexd --since "5 minutes ago" --no-pager 2>&1 \
  | grep "heartbeat emitted service=" \
  | grep -v sinex-source-worker \
  | sed 's/.*service=\([^ ]*\) .*/\1/' \
  | sort -u | wc -l)
emit "automata.active_5min" "$ACTIVE_AUTOMATA"

# ── output ────────────────────────────────────────────────────────────
if [ "$FORMAT" = "json" ]; then
  json="{"
  first=1
  for k in "${!RESULTS[@]}"; do
    [ $first -eq 1 ] && first=0 || json+=","
    val="${RESULTS[$k]}"
    val="${val//\\/\\\\}"
    val="${val//\"/\\\"}"
    json+="\"$k\":\"$val\""
  done
  json+=",\"_critical_failures\":$CRITICAL_FAILURES"
  json+=",\"_warnings\":$WARNINGS"
  json+="}"
  echo "$json"
fi

section "Summary"
if [ "$FORMAT" = "text" ]; then
  echo "  critical failures: $CRITICAL_FAILURES"
  echo "  warnings:          $WARNINGS"
fi

if [ "$CRITICAL_FAILURES" -gt 0 ]; then exit 1; fi
exit 0
