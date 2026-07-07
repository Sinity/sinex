#!/usr/bin/env bash

proof_process_rows() {
  ps -eo pid,ppid,pgid,stat,etime,rss,comm,args --no-headers \
    | awk '
      $7 ~ /^\[/ {next}
      $7 == "xtask" && $0 ~ /xtask jobs/ {next}
      $7 == "xtask" && $0 ~ /(xtask test|xtask check|xtask build|xtask docs|xtask schema|xtask impact|xtask infra smoke)/ {print; next}
      $7 == "cargo" && $0 ~ /(cargo nextest|cargo test|cargo check|cargo build)/ {print; next}
      $7 == "cargo-nextest" {print; next}
      $7 == "rustc" {print; next}
    '
}

proof_process_count() {
  proof_process_rows | sed '/^$/d' | wc -l
}

proof_process_root_count() {
  proof_process_rows | awk '{print $3}' | sort -u | sed '/^$/d' | wc -l
}

serialized_proof_resource_for() {
  local command_line="$*"

  case "$command_line" in
    *"xtask test"*|*"cargo nextest"*|*"cargo test"*)
      printf 'xtask-test/build-dir/sqlx-bootstrap\n'
      ;;
    *"xtask check"*|*"xtask build"*|*"cargo check"*|*"cargo build"*|*"rustc "*)
      printf 'xtask-build/cargo-target\n'
      ;;
    *"xtask schema"*|*"Applying checkout-local schema"*|*"SQLx"*)
      printf 'checkout-local-schema-bootstrap\n'
      ;;
    *"xtask infra smoke"*|*"xtask run core"*|*"sinexd"*)
      printf 'dev-local-runtime-bringup\n'
      ;;
    *)
      printf 'unknown-or-light\n'
      ;;
  esac
}

print_serialized_proof_advice() {
  local command_line="$*"
  local resource
  resource="$(serialized_proof_resource_for "$command_line")"

  printf 'Serialized proof resource: %s\n' "$resource"
  case "$resource" in
    xtask-test/build-dir/sqlx-bootstrap)
      printf 'Advice: do not start another xtask test in this checkout; combine exact filters or run them serially.\n'
      ;;
    xtask-build/cargo-target)
      printf 'Advice: do not overlap check/build/test work sharing this cargo target; use foreground analysis while it runs.\n'
      ;;
    checkout-local-schema-bootstrap)
      printf 'Advice: do not start another SQLx/bootstrap consumer until this proof reaches nextest/cargo or exits.\n'
      ;;
    dev-local-runtime-bringup)
      printf 'Advice: do not start a second dev-local sinexd/runtime bringup; inspect the existing process or logs.\n'
      ;;
    *)
      printf 'Advice: if this is actually compile/test/runtime proof, record the exact xtask command so the resource class is visible.\n'
      ;;
  esac
}

print_proof_process_summary() {
  local mode="${1:-status}"
  local rows count root_count
  rows="$(proof_process_rows)"
  count="$(printf '%s\n' "$rows" | sed '/^$/d' | wc -l)"
  root_count="$(printf '%s\n' "$rows" | awk '{print $3}' | sort -u | sed '/^$/d' | wc -l)"

  if [[ "$mode" == "review" ]]; then
    if [[ "$count" -eq 0 ]]; then
      printf 'OK: no active foreground proof/build processes\n'
    elif [[ "$root_count" -eq 1 ]]; then
      printf 'WARN: one foreground proof lane is active (%s processes); record/poll it before starting another\n' "$count"
      printf '%s\n' "$rows" | sed -n '1,12p'
    else
      printf 'WARN: multiple foreground proof lanes are active (%s process groups, %s processes); stop duplicates or wait before more proof\n' "$root_count" "$count"
      printf '%s\n' "$rows" | sed -n '1,20p'
    fi
  else
    if [[ "$count" -eq 0 ]]; then
      printf 'no active foreground proof/build processes\n'
    elif [[ "$root_count" -eq 1 ]]; then
      printf 'one foreground proof lane active (%s processes); poll or record wait before launching another\n' "$count"
      printf '%s\n' "$rows" | sed -n '1,12p'
    else
      printf 'multiple foreground proof lanes active (%s process groups, %s processes); stop duplicates before adding work\n' "$root_count" "$count"
      printf '%s\n' "$rows" | sed -n '1,20p'
    fi
  fi
}
