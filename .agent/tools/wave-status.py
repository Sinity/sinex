#!/usr/bin/env python3
"""wave-status: per-wave progress board over .beads/issues.jsonl.

Ported from polylogue/.agent/tools/delivery-gate-status.py (2026-07-08) — same
shape (per-bucket closed/wip/ready/blocked counts + a progress bar), adapted
for sinex's sequencing label. DELIBERATE DIVERGENCE from the polylogue source,
stated per .agent/CONVENTIONS.md: polylogue's delivery:<gate> labels are named,
stable milestones with hand-authored exit criteria, so that script hardcodes a
GATES list. Sinex's wave:N labels are numeric and RESTAMPED every sinex-my5
sequencing round (see that bead's notes) — a hardcoded wave list would go
stale within a week, so this script discovers the wave range from the data
instead of hardcoding it, and has no exit-criteria prose to print.

wave:N does NOT mean strict temporal order the way delivery gates do — my5's
own rounds interleave several waves' worth of genuinely-parallel, near-term
work (e.g. the wave:6 LLM-plane lane runs concurrently with wave:1
correctness-core). So this board prints a PRIORITY HISTOGRAM per wave as a
reminder surface, not a computed pass/fail: a P1 stamped on a bead in a wave
that isn't part of the current round's active lanes (cross-check sinex-my5's
latest notes) is worth a second look, but is not automatically wrong — see
sinex-h7cc for the one case this concretely caught (sinex-o6w, UX charter,
was P1 in a non-active wave AND design/meta content — CONVENTIONS.md's own
priority ladder says design/meta/legibility is P3).

Usage: python3 .agent/tools/wave-status.py [--json] [--fresh] [path-to-issues.jsonl]
--fresh runs `bd export -o .beads/issues.jsonl` first (bd updates do NOT
immediately re-export the jsonl; a stale file yields stale counts).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def load(path: Path):
    issues: dict[str, dict] = {}
    deps: list[tuple[str, str, str]] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        d = json.loads(line)
        if d.get("_type") == "issue":
            issues[d["id"]] = d
            for dep in d.get("dependencies") or []:
                deps.append((d["id"], dep.get("depends_on_id"), dep.get("type", "blocks")))
        elif d.get("_type") == "dependency":
            deps.append((d.get("issue_id"), d.get("depends_on_id"), d.get("type", "blocks")))
    return issues, deps


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", nargs="?", default=".beads/issues.jsonl")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fresh", action="store_true", help="bd export first")
    ap.add_argument("--wave", type=int, help="only this wave number")
    args = ap.parse_args()

    if args.fresh:
        subprocess.run(["bd", "export", "-o", args.path], check=True, capture_output=True)

    issues, deps = load(Path(args.path))
    blockers = defaultdict(list)
    for src, dst, kind in deps:
        if kind == "blocks" and dst in issues:
            blockers[src].append(dst)

    def wave_of(d) -> int | None:
        for lab in d.get("labels") or []:
            if re.fullmatch(r"wave:\d+", lab):
                return int(lab.removeprefix("wave:"))
        return None

    by_wave: dict[int, list[dict]] = defaultdict(list)
    unlabeled_open = 0
    for d in issues.values():
        w = wave_of(d)
        if w is None:
            if d.get("status") in ("open", "in_progress"):
                unlabeled_open += 1
            continue
        by_wave[w].append(d)

    waves = sorted(by_wave) if args.wave is None else [args.wave] if args.wave in by_wave else []
    rows = []
    for w in waves:
        beads = by_wave.get(w, [])
        closed = [b for b in beads if b["status"] == "closed"]
        in_prog = [b for b in beads if b["status"] == "in_progress"]
        open_ = [b for b in beads if b["status"] == "open"]
        blocked = [b for b in open_ if any(issues[x]["status"] != "closed" for x in blockers.get(b["id"], []))]
        ready = [b for b in open_ if b not in blocked]
        prio_hist = defaultdict(int)
        for b in beads:
            if b["status"] != "closed":
                prio_hist[b.get("priority", 2)] += 1
        rows.append(
            {
                "wave": w,
                "total": len(beads),
                "closed": len(closed),
                "in_progress": len(in_prog),
                "ready": len(ready),
                "blocked": len(blocked),
                "pct": round(100 * len(closed) / len(beads)) if beads else None,
                "priority_histogram": dict(sorted(prio_hist.items())),
                "ready_ids": sorted(b["id"] for b in ready)[:12],
                "in_progress_ids": sorted(b["id"] for b in in_prog),
            }
        )

    if args.json:
        print(json.dumps({"waves": rows, "unlabeled_open": unlabeled_open}, indent=2))
        return 0

    for r in rows:
        if r["total"] == 0:
            bar = "(no beads)"
        else:
            done = int(round((r["pct"] or 0) / 10))
            bar = "#" * done + "." * (10 - done) + f" {r['pct']:>3}%"
        hist = " ".join(f"P{p}={n}" for p, n in r["priority_histogram"].items())
        print(
            f"  wave:{r['wave']:<3} {bar}  closed {r['closed']:>3} | wip {r['in_progress']:>2} "
            f"| ready {r['ready']:>3} | blocked {r['blocked']:>3}   [{hist}]"
        )
        if r["in_progress_ids"]:
            print(f"      wip:   {', '.join(r['in_progress_ids'])}")
        if r["ready_ids"]:
            print(f"      ready: {', '.join(r['ready_ids'])}")
    if unlabeled_open:
        print(f"\n  {unlabeled_open} open/in_progress beads carry no wave:N label")
    print(
        "\n  Reminder: wave order is NOT strict temporal order here (unlike polylogue's\n"
        "  delivery:* gates) — cross-check a P1 in a high-numbered wave against sinex-my5's\n"
        "  current active-lane notes before assuming it's miscalibrated."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
