#!/usr/bin/env python3
"""bead-lint: mechanical coherence audit of .beads/issues.jsonl, beyond bd-graph-lint.

Ported from polylogue/.agent/tools/bead-lint.py (2026-07-08) — kept close to the
original on purpose; see .agent/CONVENTIONS.md "don't diverge without reason".
Checks already covered by .agent/scripts/bd-graph-lint (blocks-cycles via `bd dep
cycles`, missing-AC on every open bead, exactly-one-wave/-area, wave inversions)
are NOT duplicated here — sinex's graph-lint is already a strict superset of
polylogue's H1/H3/H4/P1/A1/R1 on those axes (numeric wave:N + mandatory area:*
vs polylogue's categorical horizon:{frontier,mid,vision} + optional area). This
tool carries the checks that have no sinex-side equivalent yet:

  D1  no dangling dependency refs
  D2  no dependency cycles among blocks-edges (pure-python reimpl, no `bd` shellout
      required — self-contained cross-check against bd-graph-lint's `bd dep cycles`)
  E1  epic has members: id-prefix children, dep edges, or bead ids named in its text
  E2  epic has a non-empty description (WHY + member map)
  T1  no ephemeral-path ground truth: /realm/inbox/ or /tmp/ cited outside
      provenance/accelerant framing (see `bd memories accelerant-claim-protocol`)
  X1  duplicate open titles (exact, case-folded) — the sinex-a519/sinex-h7cc class
      of bug: a bd invocation error that still inserts a row before aborting
  X2  bead id named in an open bead's text does not exist
  B1  open decision-type bead whose text declares Status: adopted/decided/settled/
      ratified should be closed

Usage: python3 .agent/tools/bead-lint.py [--json] [--fresh] [path-to-issues.jsonl]
--fresh runs `bd export -o .beads/issues.jsonl` first (bd updates do NOT
immediately re-export the jsonl; a stale file yields stale findings).
Exit 1 if any finding, 0 clean. Allowlist: .agent/tools/bead-lint-allow.txt
(lines: CHECK<TAB>bead-id<TAB>reason).
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

EPHEMERAL_RE = re.compile(r"(/realm/inbox/|(?<![\w.])/tmp/)")
# provenance-ish context that legitimizes an ephemeral path mention (mirrors the
# accelerant-claim-protocol memory: packs are accelerant, cited with re-verification
# framing, never bare ground truth)
PROVENANCE_HINTS = (
    "verbatim spec",
    "preserved as",
    "provenance",
    "escrow",
    "was in /realm/inbox",
    "corpus",
    "accelerant",
    "re-verify",
    "packet",
    "handoff",
    "gpt-pro",
    "snapshot",
)
BEAD_REF_RE = re.compile(r"sinex-[a-z0-9]+(?:\.[0-9]+)*")
# structural naming collisions: crate dirs, nix build attrs, and CAS path segments
# share the `sinex-<word>` shape with bead ids but are not bead references.
KNOWN_NON_BEAD_TOKENS = {"db", "cas", "vm"}


def load(path: Path):
    issues, deps = {}, []
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


def text_of(d) -> str:
    return " ".join(str(d.get(k) or "") for k in ("description", "design", "acceptance_criteria", "notes"))


def main() -> int:
    args = [a for a in sys.argv[1:] if a not in ("--json", "--fresh")]
    as_json = "--json" in sys.argv
    path = Path(args[0]) if args else Path(".beads/issues.jsonl")
    if "--fresh" in sys.argv:
        import subprocess

        subprocess.run(["bd", "export", "-o", str(path)], check=True, capture_output=True)
    allow_path = Path(".agent/tools/bead-lint-allow.txt")
    allow = set()
    if allow_path.exists():
        for line in allow_path.read_text().splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and not line.startswith("#"):
                allow.add((parts[0], parts[1]))

    issues, deps = load(path)
    open_ids = {i for i, d in issues.items() if d.get("status") in ("open", "in_progress")}
    findings: list[tuple[str, str, str]] = []

    def add(check: str, bid: str, msg: str):
        if (check, bid) not in allow:
            findings.append((check, bid, msg))

    # D1 dangling deps
    for src, dst, typ in deps:
        if dst not in issues:
            add("D1", src, f"dangling dep -> {dst} ({typ})")

    # D2 cycles among blocks deps (open issues only) — cross-check against `bd dep cycles`
    graph = defaultdict(set)
    for src, dst, typ in deps:
        if typ == "blocks" and src in open_ids and dst in open_ids:
            graph[src].add(dst)
    white, gray, black = 0, 1, 2
    color = defaultdict(int)

    def dfs(n, stack):
        color[n] = gray
        for m in graph[n]:
            if color[m] == gray:
                cyc = stack[stack.index(m):] + [m] if m in stack else [n, m]
                add("D2", n, "blocks-cycle: " + " -> ".join(cyc))
            elif color[m] == white:
                dfs(m, stack + [m])
        color[n] = black

    for n in list(graph):
        if color[n] == white:
            dfs(n, [n])

    # per-issue checks
    titles = defaultdict(list)
    children = defaultdict(int)
    for i in issues:
        if "." in i.removeprefix("sinex-"):
            children[i.split(".", 1)[0]] += 1
    dep_touch = defaultdict(int)  # epics may group members via dep edges instead of id-prefix
    for src, dst, _typ in deps:
        dep_touch[src] += 1
        dep_touch[dst] += 1
    for i, d in issues.items():
        if d.get("status") not in ("open", "in_progress"):
            continue
        titles[d.get("title", "").strip().casefold()].append(i)

        if d.get("issue_type") == "decision" and re.search(
            r"status:\s*(adopted|decided|settled|ratified)", text_of(d), re.IGNORECASE
        ):
            add("B1", i, "decision bead declares adopted/decided/settled/ratified but is still open")
        if d.get("issue_type") == "epic":
            named_members = [r for r in BEAD_REF_RE.findall(text_of(d)) if r != i and r in issues]
            if children[i] == 0 and dep_touch[i] == 0 and not named_members:
                add("E1", i, "epic with no members (no children, no dep edges, no named bead ids)")
            if not (d.get("description") or "").strip():
                add("E2", i, "epic without description")
        blob = text_of(d)
        seen_refs = set()
        for m in BEAD_REF_RE.finditer(blob):
            ref = m.group()
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
            token = ref.removeprefix("sinex-").split(".", 1)[0]
            # id-shaped tokens only: pure-alpha words >=4 chars are English compounds
            if token.isalpha() and len(token) >= 4:
                continue
            if token.isdigit():
                continue
            if token in KNOWN_NON_BEAD_TOKENS:
                continue
            # worktree dirname convention: sinex-pr<N>-fix (see CLAUDE.md worktree
            # placement policy) — a PR number, not a bead id
            if re.fullmatch(r"pr\d+", token):
                continue
            # markdown anchor fragment into a scratch doc (#sinex-r6d8---heading-slug),
            # not a bead cross-reference — the slugifier strips the dot from r6d.8
            if m.start() > 0 and blob[m.start() - 1] == "#":
                continue
            # tolerate .N suffix references to a future child of an existing bead
            if ref not in issues and ref.rsplit(".", 1)[0] not in issues:
                add("X2", i, f"names nonexistent bead {ref}")
        if EPHEMERAL_RE.search(blob):
            low = blob.lower()
            if not any(h in low for h in PROVENANCE_HINTS):
                add("T1", i, "ephemeral path (/realm/inbox or /tmp) cited without provenance framing")

    for t, ids in titles.items():
        if t and len(ids) > 1:
            for i in ids:
                add("X1", i, f"duplicate open title with {[x for x in ids if x != i]}")

    if as_json:
        print(json.dumps([{"check": c, "id": i, "msg": m} for c, i, m in findings], indent=1))
    else:
        by = defaultdict(list)
        for c, i, m in findings:
            by[c].append((i, m))
        for c in sorted(by):
            print(f"[{c}] {len(by[c])} finding(s)")
            for i, m in by[c]:
                print(f"    {i}: {m}")
        print(f"\n{len(findings)} finding(s) across {len(by)} check(s); {len(issues)} issues scanned.")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
