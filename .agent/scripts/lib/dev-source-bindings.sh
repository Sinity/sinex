#!/usr/bin/env bash
set -euo pipefail

mode="${1:-status}"
manifest=".agent/dev/dev-source-bindings.json"
runtime_target=".sinex/state/runtime-target.json"
critical_sources="${SINEX_DEVLOOP_CRITICAL_SOURCES:-git-commit-history fs system.journald}"
fresh_last_event_secs="${SINEX_DEVLOOP_SOURCE_FRESH_SECS:-300}"
source_status_timeout_secs="${SINEX_DEVLOOP_SOURCE_STATUS_TIMEOUT_SECS:-12}"
warnings=0

warn() {
  warnings=$((warnings + 1))
  if [[ "$mode" == "review" ]]; then
    printf 'WARN: %s\n' "$*"
  else
    printf '%s\n' "$*"
  fi
}

ok() {
  if [[ "$mode" == "review" ]]; then
    printf 'OK: %s\n' "$*"
  else
    printf '%s\n' "$*"
  fi
}

report_recent_killed_runtime_jobs() {
  if ! command -v xtask >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
    return
  fi

  local jobs_json recent
  jobs_json="$(xtask jobs list --json 2>/dev/null || true)"
  [[ -n "$jobs_json" ]] || return
  recent="$(
    jq -r '
      (.jobs // .data.jobs // [])
      | map(select((.command | tostring | test("(^|/)sinexd$")) and (.status == "killed")))
      | .[:5][]
      | "job=\(.id) invocation=\(.invocation_id) exit=\(.exit_code // "unknown") started=\(.started_at)"
    ' <<<"$jobs_json" 2>/dev/null || true
  )"
  [[ -n "$recent" ]] || return

  warn "recent dev-local sinexd job(s) were killed; runtime-down diagnostics should inspect xtask history before restarting"
  printf '%s\n' "$recent" | sed -n '1,5p'
}

if [[ ! -f "$manifest" ]]; then
  warn "missing $manifest; run xtask infra dev-bindings before dogfood runtime work"
elif [[ ! -f "$runtime_target" ]]; then
  warn "missing $runtime_target; run xtask infra runtime-target before live sinexctl checks"
elif ! command -v sinexctl >/dev/null 2>&1; then
  warn "sinexctl not on PATH; cannot verify dogfood source liveness"
else
  expected_sources="$(jq -r '.bindings[]?.source_id' "$manifest" | sort -u)"
  source_status_stderr="$(
    mktemp "${TMPDIR:-/tmp}/sinex-dev-source-bindings-status.XXXXXX"
  )"
  if command -v timeout >/dev/null 2>&1; then
    source_status="$(
      SINEX_RUNTIME_TARGET_CONFIG="$runtime_target" \
        timeout "${source_status_timeout_secs}s" \
        sinexctl sources status -f json 2>"$source_status_stderr" || true
    )"
  else
    source_status="$(
      SINEX_RUNTIME_TARGET_CONFIG="$runtime_target" \
        sinexctl sources status -f json 2>"$source_status_stderr" || true
    )"
  fi
  if [[ -z "$source_status" ]]; then
    status_error="$(<"$source_status_stderr")"
    if rg -q 'missing field `summary`|missing field summary' <<<"$status_error"; then
      warn "source status decode failed because client/server projection schemas differ; rebuild/restart dev-local sinexd before source-liveness review"
      printf '%s\n' "$status_error" | sed -n '1,4p'
    elif [[ "$status_error" == *"timed out"* || -z "$status_error" ]]; then
      warn "source status unavailable within ${source_status_timeout_secs}s; investigate filtered source-status latency before relying on source-liveness review"
      printf '%s\n' "$status_error" | sed -n '1,4p'
    else
      warn "source status unavailable; dev runtime may be down"
      printf '%s\n' "$status_error" | sed -n '1,4p'
      report_recent_killed_runtime_jobs
    fi
  else
    if [[ "$mode" != "review" ]]; then
      printf 'expected sources: '
      printf '%s\n' "$expected_sources" | paste -sd ',' - | sed 's/,/, /g'
    fi
    while IFS= read -r source_id; do
      [[ -z "$source_id" ]] && continue
      row="$(
        jq -r --arg source_id "$source_id" '
          .payload.sources[]
          | select(.source_id == $source_id)
          | ([.modes[]? | .runtime_observed] | any) as $runtime_observed
          | ([.modes[]? | .runtime_live] | any) as $runtime_live
          | ([.modes[]? | (.recent_output_count // 0)] | add // 0) as $recent_output_count
          | [
              .source_id,
              (.readiness // "unknown"),
              (.continuity // "unknown"),
              ($runtime_observed | tostring),
              ($runtime_live | tostring),
              ($recent_output_count | tostring),
              ((.accepted_binding_count // 0) | tostring),
              (.last_event_at // "unknown")
            ]
          | @tsv
        ' <<<"$source_status"
      )"
      if [[ -z "$row" ]]; then
        warn "configured source missing from source status: $source_id"
        continue
      fi
      IFS=$'\t' read -r _ readiness continuity runtime_observed runtime_live recent_output_count accepted_binding_count last_event_at <<<"$row"
      fresh_last_event=false
      output_state=quiet
      if [[ "$last_event_at" != "unknown" ]]; then
        now_epoch="$(date -u +%s)"
        last_epoch="$(date -u -d "$last_event_at" +%s 2>/dev/null || true)"
        if [[ -n "$last_epoch" && "$now_epoch" -ge "$last_epoch" && $((now_epoch - last_epoch)) -le "$fresh_last_event_secs" ]]; then
          fresh_last_event=true
        fi
      fi
      if [[ "$fresh_last_event" == "true" ]]; then
        output_state=fresh
      elif [[ "$recent_output_count" =~ ^[0-9]+$ && "$recent_output_count" -gt 0 ]]; then
        output_state=recent
      fi
      runtime_state=none
      if [[ "$runtime_live" == "true" ]]; then
        runtime_state=hot
      elif [[ "$runtime_observed" == "true" ]]; then
        runtime_state=observed
      fi
      active=false
      if [[ "$accepted_binding_count" != "0" && ( "$runtime_state" != "none" || "$output_state" != "quiet" ) ]]; then
        active=true
      fi
      if [[ "$mode" == "review" ]]; then
        critical=false
        for critical_source in $critical_sources; do
          if [[ "$source_id" == "$critical_source" ]]; then
            critical=true
            break
          fi
        done
        if [[ "$accepted_binding_count" == "0" ]]; then
          warn "configured source has zero accepted bindings: $source_id readiness=$readiness continuity=$continuity"
        elif [[ "$active" != "true" ]]; then
          if [[ "$critical" == "true" ]]; then
            warn "critical source has no observed runtime or recent output: $source_id readiness=$readiness continuity=$continuity runtime=$runtime_state output=$output_state last=$last_event_at"
          else
            ok "source quiet: $source_id readiness=$readiness continuity=$continuity runtime=$runtime_state output=$output_state recent_output=$recent_output_count last=$last_event_at"
          fi
        elif [[ "$output_state" == "quiet" ]]; then
          ok "source observed: $source_id readiness=$readiness continuity=$continuity runtime=$runtime_state output=$output_state accepted_bindings=$accepted_binding_count last=$last_event_at"
        else
          ok "source active: $source_id readiness=$readiness continuity=$continuity runtime=$runtime_state output=$output_state accepted_bindings=$accepted_binding_count recent_output=$recent_output_count last=$last_event_at"
        fi
      else
        printf '%s\treadiness=%s\tcontinuity=%s\tlast=%s\taccepted_bindings=%s\truntime=%s\toutput=%s\trecent_output=%s\tactive=%s\n' \
          "$source_id" "$readiness" "$continuity" "$last_event_at" "$accepted_binding_count" "$runtime_state" "$output_state" "$recent_output_count" "$active"
      fi
    done <<<"$expected_sources"
  fi
  rm -f "$source_status_stderr"
fi

if [[ "$warnings" -gt 0 ]]; then
  exit 1
fi
