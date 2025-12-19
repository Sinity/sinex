#!/usr/bin/env bash
set -euo pipefail

# Benchmark nextest + DB pool concurrency settings.
#
# Goals:
# - Quantify where runtime is spent (compile vs run; slow tests; heavy binaries).
# - Sweep concurrency knobs without permanently editing repo config.
# - Produce stable summaries + raw artifacts for later inspection.
#
# Defaults are deliberately moderate. If you run the full matrix, expect it to take a while.
#
# Usage:
#   scripts/bench-nextest.sh
#   RUNS=3 scripts/bench-nextest.sh
#   PROFILE=fast THREADS_LIST="8 16 24" HEAVY_CAPS="2 4 8" POOL_SIZES="8 16 24" scripts/bench-nextest.sh
#
# Modes:
#   BENCH_MODE=sweeps  (default; three 1D sweeps)
#   BENCH_MODE=matrix  (full cross-product of THREADS_LIST × HEAVY_CAPS × POOL_SIZES)
#
# Safety:
#   BENCH_RESET_DBS=1 BENCH_ALLOW_DROP=1 will drop *only* sinex test DBs between runs:
#   - sinex_test_template_shared
#   - sinex_test_pool_*

RUNS="${RUNS:-3}"
PROFILE="${PROFILE:-fast}"
BENCH_MODE="${BENCH_MODE:-sweeps}"
BENCH_WARMUP="${BENCH_WARMUP:-1}"

BENCH_TARGET="${BENCH_TARGET:-workspace}" # workspace|ingestd|e2e|<cargo-nextest args>

THREADS_LIST="${THREADS_LIST:-}"
HEAVY_CAPS="${HEAVY_CAPS:-2 4 8}"
POOL_SIZES="${POOL_SIZES:-8 16 32}"
SLOT_MAX_CONNECTIONS="${SLOT_MAX_CONNECTIONS:-4}"
CONN_BUDGET="${CONN_BUDGET:-480}"

EAGER_PROVISION="${EAGER_PROVISION:-0}"

MESSAGE_FORMAT="${MESSAGE_FORMAT:-libtest-json-plus}"

BENCH_RESET_DBS="${BENCH_RESET_DBS:-0}"
BENCH_ALLOW_DROP="${BENCH_ALLOW_DROP:-0}"

root() { cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd; }

maybe_direnv_exec() {
  local repo="$1"
  shift
  if command -v direnv >/dev/null 2>&1 && [[ -f "$repo/.envrc" ]]; then
    direnv exec "$repo" "$@"
  else
    "$@"
  fi
}

now_ts() { date -u +"%Y%m%d-%H%M%S"; }

ms_stats() {
  local -a values=("$@")
  local n="${#values[@]}"
  if [[ "$n" -eq 0 ]]; then
    echo "n=0"
    return
  fi
  local sum=0 min="${values[0]}" max="${values[0]}"
  for v in "${values[@]}"; do
    sum=$((sum + v))
    if (( v < min )); then min="$v"; fi
    if (( v > max )); then max="$v"; fi
  done
  local avg=$((sum / n))
  local sorted
  sorted="$(printf '%s\n' "${values[@]}" | sort -n)"
  local mid=$((n / 2))
  local median
  if (( n % 2 == 1 )); then
    median="$(printf '%s\n' "$sorted" | sed -n "$((mid + 1))p")"
  else
    local a b
    a="$(printf '%s\n' "$sorted" | sed -n "${mid}p")"
    b="$(printf '%s\n' "$sorted" | sed -n "$((mid + 1))p")"
    median=$(((a + b) / 2))
  fi
  printf 'n=%d min=%dms median=%dms avg=%dms max=%dms' "$n" "$min" "$median" "$avg" "$max"
}

resolve_threads_list() {
  if [[ -n "$THREADS_LIST" ]]; then
    echo "$THREADS_LIST"
    return
  fi

  local nproc
  nproc="$(command -v nproc >/dev/null 2>&1 && nproc || echo 8)"
  local half=$((nproc / 2))
  if (( half < 2 )); then half=2; fi
  # Keep it small by default: half + full.
  echo "$half $nproc"
}

make_nextest_config() {
  local repo="$1"
  local out="$2"
  local heavy_cap="$3"
  local cfg="$out/nextest-hcap-${heavy_cap}.toml"

  # Replace exactly the db-nats-heavy cap line.
  # (This is safe because we always write to a throwaway config file.)
  sed -E \
    "s/^db-nats-heavy[[:space:]]*=[[:space:]]*\\{[[:space:]]*max-threads[[:space:]]*=[[:space:]]*[0-9]+[[:space:]]*\\}[[:space:]]*$/db-nats-heavy = { max-threads = ${heavy_cap} }/" \
    "$repo/.config/nextest.toml" >"$cfg"

  echo "$cfg"
}

drop_sinex_test_dbs() {
  local repo="$1"
  if [[ "$BENCH_ALLOW_DROP" != "1" ]]; then
    echo "Refusing to drop DBs without BENCH_ALLOW_DROP=1" >&2
    return 1
  fi

  local admin_url="${DATABASE_URL_SUPERUSER:-${SINEX_TESTUTILS_ADMIN_URL:-}}"
  if [[ -z "$admin_url" ]]; then
    # Best-effort fallback: use DATABASE_URL but force db=postgres and user=postgres.
    local base="${DATABASE_URL:-postgresql:///sinex_dev?host=/run/postgresql}"
    admin_url="${base/sinex_dev/postgres}"
    if [[ "$admin_url" != *"user="* ]]; then
      if [[ "$admin_url" == *"?"* ]]; then
        admin_url="${admin_url}&user=postgres"
      else
        admin_url="${admin_url}?user=postgres"
      fi
    fi
  fi

  echo "Dropping sinex test DBs via $admin_url" >&2

  maybe_direnv_exec "$repo" env DATABASE_URL="$admin_url" bash -lc '
    set -euo pipefail
    # Drop pool DBs first, then template.
    psql -v ON_ERROR_STOP=1 -Atqc "
      SELECT datname
      FROM pg_database
      WHERE datname = '\''sinex_test_template_shared'\''
         OR datname LIKE '\''sinex_test_pool_%'\''
    " | while read -r db; do
      echo "DROP: $db" >&2
      # FORCE is available in newer Postgres; fall back if needed.
      psql -v ON_ERROR_STOP=1 -d postgres -c "DROP DATABASE IF EXISTS \"${db}\" WITH (FORCE);" 2>/dev/null \
        || psql -v ON_ERROR_STOP=1 -d postgres -c "DROP DATABASE IF EXISTS \"${db}\";"
    done
  '
}

nextest_target_args() {
  case "$BENCH_TARGET" in
    workspace)
      printf '%s\n' "--workspace"
      ;;
    ingestd)
      printf '%s\n' "--package" "sinex-ingestd"
      ;;
    e2e)
      printf '%s\n' "--package" "sinex-e2e-tests"
      ;;
    *)
      # Advanced: allow passing a raw string of args, e.g.
      # BENCH_TARGET="--package sinex-ingestd --tests"
      # shellcheck disable=SC2206
      local -a arr=($BENCH_TARGET)
      printf '%s\n' "${arr[@]}"
      ;;
  esac
}

run_one() {
  local repo="$1"
  local out="$2"
  local scenario="$3"
  local threads="$4"
  local heavy_cap="$5"
  local pool_size="$6"

  local cfg
  cfg="$(make_nextest_config "$repo" "$out" "$heavy_cap")"

  local -a target_args=()
  while IFS= read -r arg; do
    target_args+=("$arg")
  done < <(nextest_target_args)

  local -a envs=(
    "SINEX_TESTUTILS_POOL_SIZE=$pool_size"
    "SINEX_TESTUTILS_SLOT_MAX_CONNECTIONS=$SLOT_MAX_CONNECTIONS"
    "SINEX_TESTUTILS_CONN_BUDGET=$CONN_BUDGET"
    "NEXTEST_PROFILE=$PROFILE"
  )
  if [[ "$EAGER_PROVISION" == "1" ]]; then
    envs+=("SINEX_TESTUTILS_EAGER_PROVISION=1")
  fi
  if [[ "$MESSAGE_FORMAT" != "human" ]]; then
    envs+=("NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1")
  fi

  local run_dir="$out/runs/${scenario}/t${threads}-h${heavy_cap}-p${pool_size}"
  mkdir -p "$run_dir"

  local -a durs=()
  for i in $(seq 1 "$RUNS"); do
    echo "== $scenario run $i/$RUNS: threads=$threads heavy_cap=$heavy_cap pool=$pool_size ==" | tee -a "$out/bench.log"

    if [[ "$BENCH_RESET_DBS" == "1" ]]; then
      drop_sinex_test_dbs "$repo" | tee -a "$out/bench.log"
    fi

    local out_json="$run_dir/nextest.$(printf '%02d' "$i").jsonl"
    local out_meta="$run_dir/nextest.$(printf '%02d' "$i").meta"

    local start_ns end_ns dur_ms
    start_ns="$(date +%s%N)"
    if maybe_direnv_exec "$repo" env "${envs[@]}" cargo nextest run \
      "${target_args[@]}" \
      --profile "$PROFILE" \
      --config-file "$cfg" \
      --test-threads "$threads" \
      --message-format "$MESSAGE_FORMAT" \
      --color never \
      >"$out_json"; then
      end_ns="$(date +%s%N)"
      dur_ms=$(( (end_ns - start_ns) / 1000000 ))
      durs+=("$dur_ms")
      printf '%s\n' "$dur_ms" >"$out_meta"
      echo "  duration: ${dur_ms}ms"
    else
      echo "FAILED: $scenario threads=$threads heavy_cap=$heavy_cap pool=$pool_size" >&2
      return 1
    fi

    # Snapshot junit if it exists (may be overwritten by subsequent runs).
    if [[ -f "$repo/target/nextest/junit.xml" ]]; then
      cp "$repo/target/nextest/junit.xml" "$run_dir/junit.$(printf '%02d' "$i").xml"
    fi
  done

  local summary
  summary="$(ms_stats "${durs[@]}")"
  echo "Summary ($scenario t=$threads h=$heavy_cap p=$pool_size): $summary" | tee -a "$out/bench.log"

  printf '%s,%s,%s,%s,%s\n' "$scenario" "$threads" "$heavy_cap" "$pool_size" "$(printf '%s' "$summary" | tr ' ' '_')" \
    >>"$out/results.csv"

  if [[ "$MESSAGE_FORMAT" != "human" ]]; then
    python3 - "$run_dir" <<'PY'
import json, sys
from pathlib import Path

run_dir = Path(sys.argv[1])
files = sorted(run_dir.glob("nextest.*.jsonl"))
if not files:
    raise SystemExit(0)

def iter_events(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def get_test_id(ev: dict) -> str:
    # libtest-json-plus: shape is not documented as stable; do best-effort.
    if "name" in ev and isinstance(ev["name"], str):
        return ev["name"]
    if "test" in ev and isinstance(ev["test"], str):
        return ev["test"]
    return "unknown"

def get_exec_ms(ev: dict) -> float | None:
    # libtest-json-plus typically has `exec_time` in seconds as float.
    t = ev.get("exec_time")
    if isinstance(t, (int, float)):
        return float(t) * 1000.0
    # sometimes nested under nextest
    nxt = ev.get("nextest")
    if isinstance(nxt, dict):
        t = nxt.get("exec_time")
        if isinstance(t, (int, float)):
            return float(t) * 1000.0
    return None

def get_binary_id(ev: dict) -> str | None:
    nxt = ev.get("nextest")
    if isinstance(nxt, dict):
        bid = nxt.get("binary_id")
        if isinstance(bid, str):
            return bid
    return None

slow_tests = []
by_binary = {}

for p in files:
    for ev in iter_events(p):
        if ev.get("type") not in ("test", "suite"):
            continue
        if ev.get("event") not in ("ok", "failed", "ignored"):
            continue
        ms = get_exec_ms(ev)
        if ms is None:
            continue
        tid = get_test_id(ev)
        slow_tests.append((ms, tid))
        bid = get_binary_id(ev) or "unknown"
        by_binary[bid] = by_binary.get(bid, 0.0) + ms

slow_tests.sort(reverse=True)
slow_bins = sorted(by_binary.items(), key=lambda kv: kv[1], reverse=True)

out = run_dir / "summary.txt"
with out.open("w", encoding="utf-8") as f:
    f.write("Top slow tests (ms):\n")
    for ms, tid in slow_tests[:20]:
        f.write(f"{ms:9.1f}  {tid}\n")
    f.write("\nTop slow binaries (total ms):\n")
    for bid, ms in slow_bins[:20]:
        f.write(f"{ms:9.1f}  {bid}\n")
PY
  fi
}

sweep_threads() {
  local repo="$1" out="$2"
  local heavy_cap="${FIXED_HEAVY_CAP:-4}"
  local pool_size="${FIXED_POOL_SIZE:-}"
  if [[ -z "$pool_size" ]]; then
    pool_size="$(command -v nproc >/dev/null 2>&1 && nproc || echo 8)"
    if (( pool_size < 8 )); then pool_size=8; fi
    if (( pool_size > 32 )); then pool_size=32; fi
  fi
  local threads_list
  threads_list="$(resolve_threads_list)"
  for t in $threads_list; do
    run_one "$repo" "$out" "sweep_threads" "$t" "$heavy_cap" "$pool_size"
  done
}

sweep_heavy_caps() {
  local repo="$1" out="$2"
  local threads="${FIXED_THREADS:-}"
  if [[ -z "$threads" ]]; then
    threads="$(command -v nproc >/dev/null 2>&1 && nproc || echo 8)"
  fi
  local pool_size="${FIXED_POOL_SIZE:-}"
  if [[ -z "$pool_size" ]]; then
    pool_size="$threads"
    if (( pool_size < 8 )); then pool_size=8; fi
    if (( pool_size > 32 )); then pool_size=32; fi
  fi
  for h in $HEAVY_CAPS; do
    if (( h > threads )); then
      continue
    fi
    run_one "$repo" "$out" "sweep_heavy_caps" "$threads" "$h" "$pool_size"
  done
}

sweep_pool_sizes() {
  local repo="$1" out="$2"
  local threads="${FIXED_THREADS:-}"
  if [[ -z "$threads" ]]; then
    threads="$(command -v nproc >/dev/null 2>&1 && nproc || echo 8)"
  fi
  local heavy_cap="${FIXED_HEAVY_CAP:-4}"
  for p in $POOL_SIZES; do
    run_one "$repo" "$out" "sweep_pool_sizes" "$threads" "$heavy_cap" "$p"
  done
}

matrix() {
  local repo="$1" out="$2"
  local threads_list
  threads_list="$(resolve_threads_list)"
  for t in $threads_list; do
    for h in $HEAVY_CAPS; do
      if (( h > t )); then
        continue
      fi
      for p in $POOL_SIZES; do
        run_one "$repo" "$out" "matrix" "$t" "$h" "$p"
      done
    done
  done
}

main() {
  local repo
  repo="$(root)"

  local out="test-results/bench-nextest-$(now_ts)"
  mkdir -p "$out/runs"

  {
    echo "repo=$repo"
    echo "git=$(git -C "$repo" rev-parse --short HEAD)"
    echo "profile=$PROFILE"
    echo "target=$BENCH_TARGET"
    echo "runs=$RUNS"
    echo "mode=$BENCH_MODE"
    echo "slot_max_connections=$SLOT_MAX_CONNECTIONS"
    echo "conn_budget=$CONN_BUDGET"
    echo "heavy_caps=$HEAVY_CAPS"
    echo "pool_sizes=$POOL_SIZES"
    echo "threads_list=$(resolve_threads_list)"
    echo "eager_provision=$EAGER_PROVISION"
    echo "reset_dbs=$BENCH_RESET_DBS (allow_drop=$BENCH_ALLOW_DROP)"
    echo "uname=$(uname -a)"
  } | tee "$out/meta.txt"

  printf 'scenario,threads,heavy_cap,pool_size,summary\n' >"$out/results.csv"

  if [[ "$BENCH_WARMUP" == "1" ]]; then
    echo "Warmup: compile tests (no-run)..." | tee -a "$out/bench.log"
    local -a target_args=()
    while IFS= read -r arg; do
      target_args+=("$arg")
    done < <(nextest_target_args)
    maybe_direnv_exec "$repo" cargo nextest run "${target_args[@]}" --profile "$PROFILE" --no-run >/dev/null
  fi

  case "$BENCH_MODE" in
    sweeps)
      sweep_threads "$repo" "$out"
      sweep_heavy_caps "$repo" "$out"
      sweep_pool_sizes "$repo" "$out"
      ;;
    matrix)
      matrix "$repo" "$out"
      ;;
    *)
      echo "Unknown BENCH_MODE=$BENCH_MODE (expected: sweeps|matrix)" >&2
      exit 2
      ;;
  esac

  echo
  echo "Wrote results to: $out"
  echo "CSV: $out/results.csv"
  echo "Per-run summaries: $out/runs/**/summary.txt"
}

main "$@"
