#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/bench-nextest-report.sh [<bench-dir>]

If <bench-dir> is omitted, uses the newest `test-results/bench-nextest-*` directory.

Prints:
  - meta (target/profile/runs)
  - best/worst median per scenario
  - best overall median (across all scenarios)

The bench directory is expected to contain:
  - meta.txt
  - results.csv
USAGE
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

pick_latest() {
  ls -1dt test-results/bench-nextest-* 2>/dev/null | head -n 1
}

bench_dir="${1:-}"
if [[ -z "$bench_dir" ]]; then
  bench_dir="$(pick_latest)"
fi

if [[ -z "$bench_dir" || ! -d "$bench_dir" ]]; then
  echo "ERROR: bench dir not found: $bench_dir" >&2
  exit 2
fi

meta="$bench_dir/meta.txt"
csv="$bench_dir/results.csv"

if [[ ! -f "$meta" || ! -f "$csv" ]]; then
  echo "ERROR: expected $meta and $csv" >&2
  exit 2
fi

python3 - "$bench_dir" "$meta" "$csv" <<'PY'
import csv
import pathlib
import re
import sys

bench_dir = pathlib.Path(sys.argv[1])
meta_path = pathlib.Path(sys.argv[2])
csv_path = pathlib.Path(sys.argv[3])

def read_meta(path: pathlib.Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def parse_summary(summary: str) -> dict[str, int] | None:
    if not summary or summary == "FAILED":
        return None
    # Bench rows store the summary with spaces replaced by underscores.
    parts = summary.split("_")
    out: dict[str, int] = {}
    for part in parts:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        v = v.removesuffix("ms")
        if v.isdigit():
            out[k] = int(v)
    return out if "median" in out else None

meta = read_meta(meta_path)
print(f"bench_dir={bench_dir}")
print(f"git={meta.get('git', '<unknown>')}")
print(f"target={meta.get('target', '<unknown>')} profile={meta.get('profile', '<unknown>')} runs={meta.get('runs', '<unknown>')} mode={meta.get('mode', '<unknown>')}")
print(f"compile_duration_ms={meta.get('compile_duration_ms', '<unknown>')}")
print()

rows: list[dict[str, str]] = []
with csv_path.open(newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        summary = parse_summary(r.get("summary", ""))
        if summary is None:
            continue
        r = dict(r)
        r["_median_ms"] = str(summary["median"])
        rows.append(r)

if not rows:
    print("No successful benchmark rows found.")
    raise SystemExit(0)

def median_ms(r: dict[str, str]) -> int:
    return int(r["_median_ms"])

scenarios = sorted({r["scenario"] for r in rows})
overall_best = min(rows, key=median_ms)
overall_worst = max(rows, key=median_ms)

def fmt_fields(r: dict[str, str]) -> str:
    parts = [f"threads={r.get('threads', '-')}", f"pool={r.get('pool_size', '-')}" if "pool_size" in r else f"pool={r.get('pool_size', r.get('pool', r.get('pool_size', '-')))}"]
    if "heavy_cap" in r:
        parts.append(f"heavy_cap={r.get('heavy_cap', '-')}")
    if "clean_after_use" in r:
        parts.append(f"cau={r.get('clean_after_use', '-')}")
    if "eager_provision" in r:
        parts.append(f"eager={r.get('eager_provision', '-')}")
    return " ".join(parts)

print("Per-scenario best/worst (median_ms):")
for scen in scenarios:
    rs = [r for r in rows if r["scenario"] == scen]
    best = min(rs, key=median_ms)
    worst = max(rs, key=median_ms)
    print(f"  {scen}: best={median_ms(best)}ms ({fmt_fields(best)}) worst={median_ms(worst)}ms")

print()
print(
    "Overall best/worst (across all scenarios):\n"
    f"  best={median_ms(overall_best)}ms (scenario={overall_best['scenario']} {fmt_fields(overall_best)})\n"
    f"  worst={median_ms(overall_worst)}ms (scenario={overall_worst['scenario']} {fmt_fields(overall_worst)})"
)
PY
