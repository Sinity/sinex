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
#   PROFILE=fast THREADS_LIST="8 16 24" POOL_SIZES="8 16 24" scripts/bench-nextest.sh
#
# Modes:
#   BENCH_MODE=sweeps  (default; two sweeps: threads + pool sizes)
#   BENCH_MODE=matrix  (full cross-product of THREADS_LIST × POOL_SIZES)
#   BENCH_MODE=refine  (run sweeps, then a small matrix around best values)
#
# Safety:
#   BENCH_RESET_DBS=1 BENCH_ALLOW_DROP=1 will drop *only* sinex test DBs between runs:
#   - sinex_test_template_shared
#   - sinex_test_pool_*
#
# Key knobs (all via env vars):
# - RUNS: repetitions per combo (default 3)
# - PROFILE: nextest profile (default fast)
# - BENCH_TARGET: workspace|ingestd|e2e|<raw args>
# - THREADS_LIST: explicit threads to try (default: half + full nproc)
# - POOL_SIZES: explicit pool sizes to try (default: "8 16 32")
# - CLEAN_AFTER_USE_LIST: "0 1" compares both cleanup semantics (default: "0 1")
# - EAGER_PROVISION / EAGER_PROVISION_LIST: 0/1 (default: 0)
# - BENCH_MODE=refine: runs sweeps, then a small matrix around the best settings

usage() {
  cat <<'USAGE'
Usage:
  scripts/bench-nextest.sh [--help]

Runs `cargo nextest` benchmarks and writes results under `test-results/bench-nextest-<timestamp>/`.

Common:
  RUNS=3 PROFILE=fast BENCH_TARGET=ingestd BENCH_MODE=refine scripts/bench-nextest.sh
  RUNS=3 PROFILE=fast BENCH_TARGET=workspace BENCH_MODE=sweeps scripts/bench-nextest.sh

Targets:
  BENCH_TARGET=workspace      # `cargo nextest run --workspace`
  BENCH_TARGET=ingestd        # `-p sinex-ingestd`
  BENCH_TARGET=e2e            # `-p sinex-e2e-tests`
  BENCH_TARGET="<raw args>"   # passed to `cargo nextest run` (e.g. "--workspace -E 'not test(/foo/)'")

Bench modes:
  BENCH_MODE=sweeps           # sweep threads, then sweep pool sizes
  BENCH_MODE=matrix           # full cross-product THREADS_LIST × POOL_SIZES
  BENCH_MODE=refine           # sweeps, pick best candidates, then small matrix
    # Advanced: tweak selection size for refine
    # REFINE_TOP_THREADS=3
    # REFINE_TOP_POOLS=3

Key knobs:
  THREADS_LIST="8 16 24"      # if empty, defaults to half+full nproc
  POOL_SIZES="8 16 24"        # pool DB count(s) to try (default: "8 16 32")
  FIXED_POOL_SIZE=24          # for BENCH_MODE=sweeps thread sweep (default: clamped nproc)
  FIXED_THREADS=24            # for BENCH_MODE=sweeps pool sweep (default: nproc)
  CLEAN_AFTER_USE_LIST="0 1"  # compare pool cleanup semantics (default: "0 1")
  EAGER_PROVISION_LIST="0 1"  # compare nextest lazy vs eager pool provisioning (default: "0")
  SLOT_MAX_CONNECTIONS=4      # per-DB sqlx pool max connections
  CONN_BUDGET=480             # total connection budget for all DBs in a run

Failure behavior:
  BENCH_NO_FAIL_FAST=1        # run all tests even after failures
  BENCH_CONTINUE_ON_FAIL=1    # keep benchmarking other combos after a failure

DB reset (dangerous):
  BENCH_RESET_DBS=1 BENCH_ALLOW_DROP=1 scripts/bench-nextest.sh
    Drops only: sinex_test_template_shared and sinex_test_pool_*

Notes:
  This harness intentionally does NOT implement nextest test-group caps anymore.
  If you set HEAVY_CAP / HEAVY_CAPS / VERY_HEAVY_CAP*, the script will error.
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

# Guard against old/removed knobs so we don't silently do nothing.
if [[ -n "${HEAVY_CAP:-}" || -n "${HEAVY_CAPS:-}" || -n "${VERY_HEAVY_CAP:-}" || -n "${VERY_HEAVY_CAPS:-}" ]]; then
  echo "ERROR: HEAVY_CAP* knobs were removed (no nextest test-group caps). Unset them." >&2
  exit 2
fi

RUNS="${RUNS:-3}"
PROFILE="${PROFILE:-fast}"
BENCH_MODE="${BENCH_MODE:-sweeps}"
# This script always captures a dedicated compile duration + log via
# `cargo nextest run --no-run` so run timings are "run-only".

BENCH_TARGET="${BENCH_TARGET:-workspace}" # workspace|ingestd|e2e|<cargo-nextest args>

THREADS_LIST="${THREADS_LIST:-}"
POOL_SIZES="${POOL_SIZES:-8 16 32}"
SLOT_MAX_CONNECTIONS="${SLOT_MAX_CONNECTIONS:-4}"
CONN_BUDGET="${CONN_BUDGET:-480}"

EAGER_PROVISION="${EAGER_PROVISION:-0}"
EAGER_PROVISION_LIST="${EAGER_PROVISION_LIST:-}"
# Legacy single-value knob. Prefer CLEAN_AFTER_USE_LIST (below) for comparing modes in one run.
CLEAN_AFTER_USE="${CLEAN_AFTER_USE:-}"
CLEAN_AFTER_USE_LIST="${CLEAN_AFTER_USE_LIST:-}"

MESSAGE_FORMAT="${MESSAGE_FORMAT:-libtest-json-plus}"
BENCH_NO_FAIL_FAST="${BENCH_NO_FAIL_FAST:-0}"
BENCH_CONTINUE_ON_FAIL="${BENCH_CONTINUE_ON_FAIL:-0}"

BENCH_RESET_DBS="${BENCH_RESET_DBS:-0}"
BENCH_ALLOW_DROP="${BENCH_ALLOW_DROP:-0}"

root() { cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd; }

maybe_direnv_exec() {
  local repo="$1"
  shift
  if command -v direnv >/dev/null 2>&1 && [[ -f "$repo/.envrc" ]]; then
    # Prevent direnv/devenv hooks from treating this as an interactive shell (bench output is noisy).
    # IMPORTANT: env vars must be set *before* `direnv exec` so `.envrc`/devenv sees them.
    SINEX_MOTD_SILENT=1 SINEX_DEVENV_MOTD_ONCE=1 DEVENV_TASKS_QUIET=1 \
      env -u PS1 -u PROMPT -u PROMPT_COMMAND \
      direnv exec "$repo" "$@" \
      2> >(
        # Suppress a known noisy warning when the locally-installed devenv is newer than the pinned input.
        grep -v 'devenv .* is newer than devenv input' >&2 || true
      )
  else
    "$@"
  fi
}

now_ts() { date -u +"%Y%m%d-%H%M%S"; }

dur_ms() {
  local start_ns="$1"
  local end_ns="$2"
  echo $(( (end_ns - start_ns) / 1000000 ))
}

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

resolve_pool_sizes_list() {
  if [[ -n "$POOL_SIZES" ]]; then
    echo "$POOL_SIZES"
    return
  fi
  echo "8 16 32"
}

resolve_clean_after_use_list() {
  if [[ -n "$CLEAN_AFTER_USE_LIST" ]]; then
    echo "$CLEAN_AFTER_USE_LIST"
    return
  fi

  if [[ -n "$CLEAN_AFTER_USE" ]]; then
    echo "$CLEAN_AFTER_USE"
    return
  fi

  # Default: compare both modes in a single benchmark run.
  echo "0 1"
}

resolve_eager_provision_list() {
  if [[ -n "$EAGER_PROVISION_LIST" ]]; then
    echo "$EAGER_PROVISION_LIST"
    return
  fi
  echo "$EAGER_PROVISION"
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

  maybe_direnv_exec "$repo" bash -lc '
    set -euo pipefail
    admin_url="'"$admin_url"'"
    # Drop pool DBs first, then template.
    psql "$admin_url" -v ON_ERROR_STOP=1 -Atqc "
      SELECT datname
      FROM pg_database
      WHERE datname = '\''sinex_test_template_shared'\''
         OR datname LIKE '\''sinex_test_pool_%'\''
    " | while read -r db; do
      echo "DROP: $db" >&2
      # FORCE is available in newer Postgres; fall back if needed.
      psql "$admin_url" -v ON_ERROR_STOP=1 -d postgres -c "DROP DATABASE IF EXISTS \"${db}\" WITH (FORCE);" 2>/dev/null \
        || psql "$admin_url" -v ON_ERROR_STOP=1 -d postgres -c "DROP DATABASE IF EXISTS \"${db}\";"
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
  local pool_size="$5"
  local clean_after_use="$6"
  local eager_provision="$7"

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
  if [[ "$eager_provision" == "1" ]]; then
    envs+=("SINEX_TESTUTILS_EAGER_PROVISION=1")
  fi
  if [[ "$clean_after_use" == "1" ]]; then
    envs+=("SINEX_TESTUTILS_CLEAN_AFTER_USE=1")
  fi
  if [[ "$MESSAGE_FORMAT" != "human" ]]; then
    envs+=("NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1")
  fi

  local run_dir="$out/runs/${scenario}/t${threads}-p${pool_size}-cau${clean_after_use}-eager${eager_provision}"
  mkdir -p "$run_dir"

  local -a nextest_args=()
  if [[ "$BENCH_NO_FAIL_FAST" == "1" ]]; then
    nextest_args+=(--no-fail-fast)
  fi

  local -a durs=()
  for i in $(seq 1 "$RUNS"); do
    echo "== $scenario run $i/$RUNS: threads=$threads pool=$pool_size clean_after_use=$clean_after_use eager_provision=$eager_provision ==" | tee -a "$out/bench.log"

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
      --test-threads "$threads" \
      --message-format "$MESSAGE_FORMAT" \
      "${nextest_args[@]}" \
      --color never \
      >"$out_json"; then
      end_ns="$(date +%s%N)"
      dur_ms=$(( (end_ns - start_ns) / 1000000 ))
      durs+=("$dur_ms")
      printf '%s\n' "$dur_ms" >"$out_meta"
      echo "  duration: ${dur_ms}ms"
    else
      echo "FAILED: $scenario threads=$threads pool=$pool_size clean_after_use=$clean_after_use eager_provision=$eager_provision" >&2
      if [[ "$BENCH_CONTINUE_ON_FAIL" == "1" ]]; then
        printf '%s\n' "FAILED" >"$out_meta"
        printf '%s,%s,%s,%s,%s,%s\n' "$scenario" "$threads" "$pool_size" "$clean_after_use" "$eager_provision" "FAILED" \
          >>"$out/results.csv"
        return 0
      fi
      return 1
    fi

    # Snapshot junit if it exists (may be overwritten by subsequent runs).
    if [[ -f "$repo/target/nextest/junit.xml" ]]; then
      cp "$repo/target/nextest/junit.xml" "$run_dir/junit.$(printf '%02d' "$i").xml"
    fi
  done

  local summary
  summary="$(ms_stats "${durs[@]}")"
  echo "Summary ($scenario t=$threads p=$pool_size cau=$clean_after_use eager=$eager_provision): $summary" | tee -a "$out/bench.log"

  printf '%s,%s,%s,%s,%s,%s\n' "$scenario" "$threads" "$pool_size" "$clean_after_use" "$eager_provision" "$(printf '%s' "$summary" | tr ' ' '_')" \
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

def suite_id(ev: dict) -> str:
    nxt = ev.get("nextest")
    if isinstance(nxt, dict):
        crate = nxt.get("crate")
        tb = nxt.get("test_binary")
        if isinstance(crate, str) and isinstance(tb, str):
            return f"{crate}::{tb}"
        if isinstance(tb, str):
            return tb
    return "unknown-suite"

def test_id(ev: dict) -> str:
    # libtest-json-plus: be defensive.
    name = ev.get("name")
    if isinstance(name, str) and name:
        return name
    name = ev.get("test")
    if isinstance(name, str) and name:
        return name
    return "unknown-test"

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

def binary_from_test_name(name: str) -> str:
    # Names look like: "<crate>::<test_binary>$<test_name...>"
    if "$" in name:
        return name.split("$", 1)[0]
    return "unknown-binary"

slow_tests = []   # (ms, test_name)
slow_suites = []  # (ms, suite_name)
by_binary = {}    # binary -> total ms

for p in files:
    for ev in iter_events(p):
        typ = ev.get("type")
        event = ev.get("event")
        if event not in ("ok", "failed", "ignored"):
            continue

        ms = get_exec_ms(ev)
        if ms is None:
            continue

        if typ == "suite":
            slow_suites.append((ms, suite_id(ev)))
            continue

        if typ != "test":
            continue

        tid = test_id(ev)
        slow_tests.append((ms, tid))
        by_binary[binary_from_test_name(tid)] = by_binary.get(binary_from_test_name(tid), 0.0) + ms

slow_tests.sort(reverse=True)
slow_suites.sort(reverse=True)
slow_bins = sorted(by_binary.items(), key=lambda kv: kv[1], reverse=True)

out = run_dir / "summary.txt"
with out.open("w", encoding="utf-8") as f:
    f.write("Top slow suites (ms):\n")
    for ms, sid in slow_suites[:20]:
        f.write(f"{ms:9.1f}  {sid}\n")
    f.write("\n")
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
  local pool_size="${FIXED_POOL_SIZE:-}"
  if [[ -z "$pool_size" ]]; then
    pool_size="$(command -v nproc >/dev/null 2>&1 && nproc || echo 8)"
    if (( pool_size < 8 )); then pool_size=8; fi
    if (( pool_size > 32 )); then pool_size=32; fi
  fi
  local threads_list
  threads_list="$(resolve_threads_list)"
  local clean_list
  clean_list="$(resolve_clean_after_use_list)"
  local eager_list
  eager_list="$(resolve_eager_provision_list)"
  for eager in $eager_list; do
    for cau in $clean_list; do
      for t in $threads_list; do
        run_one "$repo" "$out" "sweep_threads" "$t" "$pool_size" "$cau" "$eager"
      done
    done
  done
}

sweep_pool_sizes() {
  local repo="$1" out="$2"
  local threads="${FIXED_THREADS:-}"
  if [[ -z "$threads" ]]; then
    threads="$(command -v nproc >/dev/null 2>&1 && nproc || echo 8)"
  fi
  local clean_list
  clean_list="$(resolve_clean_after_use_list)"
  local eager_list
  eager_list="$(resolve_eager_provision_list)"
  for eager in $eager_list; do
    for cau in $clean_list; do
      for p in $(resolve_pool_sizes_list); do
        run_one "$repo" "$out" "sweep_pool_sizes" "$threads" "$p" "$cau" "$eager"
      done
    done
  done
}

matrix() {
  local repo="$1" out="$2"
  local threads_list
  threads_list="$(resolve_threads_list)"
  local clean_list
  clean_list="$(resolve_clean_after_use_list)"
  local eager_list
  eager_list="$(resolve_eager_provision_list)"
  for eager in $eager_list; do
    for cau in $clean_list; do
      for t in $threads_list; do
        for p in $(resolve_pool_sizes_list); do
          run_one "$repo" "$out" "matrix" "$t" "$p" "$cau" "$eager"
        done
      done
    done
  done
}

refine_pick_candidates() {
  local results_csv="$1"
  local top_threads="$2"
  local top_pools="$3"

  python3 - "$results_csv" "$top_threads" "$top_pools" <<'PY'
import csv, sys

path = sys.argv[1]
top_threads = int(sys.argv[2])
top_pools = int(sys.argv[3])

def parse_summary(summary: str):
    if not summary or summary == "FAILED":
        return None
    out = {}
    for part in summary.split("_"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if v.endswith("ms"):
            v = v[:-2]
        try:
            out[k] = int(v)
        except ValueError:
            pass
    return out if "median" in out else None

rows = []
with open(path, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        meta = parse_summary(r.get("summary", ""))
        if not meta:
            continue
        r["median_ms"] = meta["median"]
        rows.append(r)

def pick_unique(sorted_rows, key_field, limit):
    out = []
    seen = set()
    for r in sorted_rows:
        v = r[key_field]
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
        if len(out) >= limit:
            break
    return out

groups = {}
for r in rows:
    key = (r.get("clean_after_use", "0"), r.get("eager_provision", "0"))
    groups.setdefault(key, []).append(r)

for (cau, eager), rs in sorted(groups.items()):
    thread_rows = [r for r in rs if r.get("scenario") == "sweep_threads"]
    pool_rows = [r for r in rs if r.get("scenario") == "sweep_pool_sizes"]
    if not thread_rows or not pool_rows:
        continue
    thread_rows.sort(key=lambda r: r["median_ms"])
    pool_rows.sort(key=lambda r: r["median_ms"])
    threads = pick_unique(thread_rows, "threads", top_threads)
    pools = pick_unique(pool_rows, "pool_size", top_pools)
    print(f"cau={cau} eager={eager} threads={' '.join(threads)} pools={' '.join(pools)}")
PY
}

refine() {
  local repo="$1" out="$2"
  local top_threads="${REFINE_TOP_THREADS:-3}"
  local top_pools="${REFINE_TOP_POOLS:-3}"

  sweep_threads "$repo" "$out"
  sweep_pool_sizes "$repo" "$out"

  echo "Refine: selecting top candidates (threads=$top_threads, pools=$top_pools)..." | tee -a "$out/bench.log"
  local candidates
  candidates="$(refine_pick_candidates "$out/results.csv" "$top_threads" "$top_pools")"
  if [[ -z "$candidates" ]]; then
    echo "Refine: no candidates found (did sweeps fail?)" | tee -a "$out/bench.log"
    return 1
  fi

  printf '%s\n' "$candidates" | tee -a "$out/bench.log" >/dev/null

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    local cau eager threads pools
    # Parse `cau=… eager=… threads=… pools=…` without relying on sed escaping.
    cau="${line#cau=}"
    cau="${cau%% *}"
    eager="${line#*eager=}"
    eager="${eager%% *}"
    threads="${line#*threads=}"
    threads="${threads%% pools=*}"
    pools="${line#*pools=}"

    echo "Refine matrix: cau=$cau eager=$eager threads=[$threads] pools=[$pools]" | tee -a "$out/bench.log"
    for t in $threads; do
      for p in $pools; do
        run_one "$repo" "$out" "refine_matrix" "$t" "$p" "$cau" "$eager"
      done
    done
  done <<<"$candidates"
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
    echo "message_format=$MESSAGE_FORMAT"
    echo "no_fail_fast=$BENCH_NO_FAIL_FAST"
    echo "continue_on_fail=$BENCH_CONTINUE_ON_FAIL"
    echo "slot_max_connections=$SLOT_MAX_CONNECTIONS"
    echo "conn_budget=$CONN_BUDGET"
    echo "pool_sizes=$POOL_SIZES"
    echo "pool_sizes_effective=$(resolve_pool_sizes_list)"
    echo "fixed_pool_size_for_thread_sweep=${FIXED_POOL_SIZE:-<auto(clamped nproc)>}"
    echo "threads_list=$(resolve_threads_list)"
    echo "fixed_threads_for_pool_sweep=${FIXED_THREADS:-<auto(nproc)>}"
    echo "eager_provision_list=$(resolve_eager_provision_list)"
    echo "clean_after_use_list=$(resolve_clean_after_use_list)"
    echo "reset_dbs=$BENCH_RESET_DBS (allow_drop=$BENCH_ALLOW_DROP)"
    echo "uname=$(uname -a)"
  } | tee "$out/meta.txt"

  printf 'scenario,threads,pool_size,clean_after_use,eager_provision,summary\n' >"$out/results.csv"

  # Always record a dedicated compile duration/log (so run timings are comparable and "run-only").
  # This also makes cold-compile vs warm-compile obvious in the log and meta output.
  echo "Compile: cargo nextest run --no-run ..." | tee -a "$out/bench.log"
  local compile_log="$out/compile.no-run.log"
  local -a target_args=()
  while IFS= read -r arg; do
    target_args+=("$arg")
  done < <(nextest_target_args)

  local start_ns end_ns compile_ms
  start_ns="$(date +%s%N)"
  if maybe_direnv_exec "$repo" cargo nextest run "${target_args[@]}" --profile "$PROFILE" --no-run \
    >"$compile_log" 2>&1; then
    end_ns="$(date +%s%N)"
    compile_ms="$(dur_ms "$start_ns" "$end_ns")"
    echo "Compile duration: ${compile_ms}ms" | tee -a "$out/bench.log"
    {
      echo "compile_duration_ms=$compile_ms"
      echo "compile_log=$compile_log"
    } >>"$out/meta.txt"
  else
    end_ns="$(date +%s%N)"
    compile_ms="$(dur_ms "$start_ns" "$end_ns")"
    echo "Compile failed after ${compile_ms}ms; see $compile_log" | tee -a "$out/bench.log"
    {
      echo "compile_duration_ms=$compile_ms"
      echo "compile_log=$compile_log"
    } >>"$out/meta.txt"
    tail -n 120 "$compile_log" >&2 || true
    exit 1
  fi

  case "$BENCH_MODE" in
    sweeps)
      sweep_threads "$repo" "$out"
      sweep_pool_sizes "$repo" "$out"
      ;;
    matrix)
      matrix "$repo" "$out"
      ;;
    refine)
      refine "$repo" "$out"
      ;;
    *)
      echo "Unknown BENCH_MODE=$BENCH_MODE (expected: sweeps|matrix|refine)" >&2
      exit 2
      ;;
  esac

  echo
  echo "Wrote results to: $out"
  echo "CSV: $out/results.csv"
  echo "Per-run summaries: $out/runs/**/summary.txt"
}

main "$@"
