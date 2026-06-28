#!/usr/bin/env bash
# sinex-recall — reconstruct what the operator was doing around a moment in time,
# by querying Sinex's captured context (terminal capture + Sinex's derived
# enrichment), unified on real-world occurrence time (ts_orig).
#
# This is the "with Sinex" side of the value demonstration: a single command that
# answers "what was I doing around <T>?" across the full 14-month capture — a
# question that, without Sinex, requires cross-referencing local atuin history by
# hand and has no canonical/entity/session enrichment at all.
#
# Usage:
#   demo/sinex-recall/recall.sh '2025-06-03 16:05:00+00' [WINDOW_MINUTES]
#
# Connection: defaults to the production Sinex Postgres (peer auth as the
# `postgres` role). Override with SINEX_DB_URL for another deployment.
#   SINEX_DB_URL='postgresql:///sinex_prod?host=/var/run/postgresql' demo/.../recall.sh ...
set -euo pipefail

T="${1:?usage: recall.sh '<timestamp>' [window_minutes]}"
MINS="${2:-10}"

run_sql() {
  if [[ -n "${SINEX_DB_URL:-}" ]]; then
    psql "$SINEX_DB_URL" -v ON_ERROR_STOP=1 "$@"
  else
    sudo -u postgres psql -d sinex_prod -v ON_ERROR_STOP=1 "$@"
  fi
}

echo "════════════════════════════════════════════════════════════════════════"
echo " sinex-recall — what was happening around: $T   (±${MINS} min)"
echo " source: Sinex captured context (terminal + derived enrichment), via ts_orig"
echo "════════════════════════════════════════════════════════════════════════"

run_sql -P pager=off -v t="$T" -v mins="$MINS" <<'SQL'
-- Unified, occurrence-time-ordered reconstruction.
-- shell.atuin is duplicated 5-7x in the store (ingestion artifact); dedup on the
-- atuin_history_id so each real command appears once. Derived layers
-- (activity-window, session boundary, entities) are interleaved as context.
WITH ev AS (
  SELECT ts_orig, source, payload,
         payload->>'atuin_history_id' AS hid
  FROM core.events
  WHERE ts_orig BETWEEN (:'t')::timestamptz - ((:'mins')||' minutes')::interval
                    AND (:'t')::timestamptz + ((:'mins')||' minutes')::interval
    AND source IN ('shell.atuin','derived.activity-window',
                   'derived.session-detector')
),
deduped AS (
  -- one row per real shell command; pass derived rows through unchanged
  SELECT DISTINCT ON (CASE WHEN source='shell.atuin' THEN hid ELSE ts_orig::text||source||left(payload::text,40) END)
         ts_orig, source, payload
  FROM ev
)
SELECT to_char(ts_orig,'HH24:MI:SS') AS time,
  CASE source
    WHEN 'shell.atuin' THEN
      '$ '||(payload->>'command_string')
      ||'   ['||COALESCE(payload->>'cwd','?')||' exit='||COALESCE(payload->>'exit_code','?')||']'
    WHEN 'derived.activity-window' THEN
      '— activity window ('||COALESCE(payload->>'primary_source','?')||', '
      ||COALESCE(payload->>'event_count','?')||' events, close='||COALESCE(payload->>'close_reason','?')||')'
    WHEN 'derived.session-detector' THEN '— session boundary'
    WHEN 'entity-extractor' THEN
      '· entity '||COALESCE(payload->>'entity_type','?')||': '||COALESCE(payload->>'raw_name','?')
    ELSE left(payload::text,70)
  END AS reconstruction
FROM deduped
ORDER BY ts_orig;
SQL

echo
echo "── summary ──────────────────────────────────────────────────────────────"
run_sql -P pager=off -tA -F' | ' -v t="$T" -v mins="$MINS" <<'SQL'
SELECT 'distinct commands: '||count(DISTINCT payload->>'atuin_history_id')
  FROM core.events
 WHERE ts_orig BETWEEN (:'t')::timestamptz - ((:'mins')||' minutes')::interval
                   AND (:'t')::timestamptz + ((:'mins')||' minutes')::interval
   AND source='shell.atuin';
SELECT 'working dirs: '||string_agg(DISTINCT (payload->>'cwd'), ', ')
  FROM core.events
 WHERE ts_orig BETWEEN (:'t')::timestamptz - ((:'mins')||' minutes')::interval
                   AND (:'t')::timestamptz + ((:'mins')||' minutes')::interval
   AND source='shell.atuin';
SQL
