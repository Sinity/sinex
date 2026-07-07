# Sinex Agent Conventions

Repo-local conventions for agents working in this checkout. Always-loaded
operating rules live in `CLAUDE.md` (= `AGENTS.md`); this file holds the
repo-agent conventions that do not need to be in every context window.

**The devloop substrate is Beads.** The former bespoke conductor packet
(`conductor-devloop/`, `DEVLOOP.md`, `devloop-*` scripts, `devloop-contract.json`)
is archived at `.agent/archive/devloop-2026-07/` — see its README for what
subsumed each piece. Do not resurrect packet files or `devloop-*` script names;
the loop is: `bd prime` → `bd ready` → claim → work → PR → close with reasons.

## Directory Shape

```text
.agent/
  README.md          # orientation
  CONVENTIONS.md     # this file
  scripts/           # small repo-agent helpers (non-devloop)
  demos/             # curated demo shelf (see Scratch And Demos)
  dev/               # small tracked dev artifacts (recall proofs, dev bindings)
  inbox/             # external analysis batches awaiting verification
  scratch/           # gitignored thinking space (README + research/ + numbered notes)
  archive/           # retired scaffolds kept as evidence (devloop-2026-07/)
```

## Beads Task Substrate

Beads (`bd`, workspace at `.beads/`) is the durable task substrate for this
repo. It replaces markdown backlogs as the source of truth for cross-slice
work items: ready work, claims, blockers, dependencies, deferred/queued work,
and discovered follow-ups. Run `bd prime` for the workflow context.

Division of responsibility:

- **Beads** owns work items: what exists, what is ready, what blocks what,
  who claimed it, and why it was closed. Anything that should survive the
  current slice becomes a bead (`bd create`), not a bullet in a markdown file.
- **Current focus** is the claimed bead (`bd update <id> --claim`); its notes
  field is the running trail; `bd close <id> --reason "…"` with verification
  commands ends the slice. There is no separate active-loop file.
- **Operator directives** that must survive compaction are beads with the
  appropriate priority/label (`directive`/`campaign`), never a queue file.
- **Backlog and side-research live in beads.** Prioritization is priority
  fields plus deps, reviewed through `bd ready`; a side-research lease is a
  claimed `side-research`-labeled bead whose reconciliation is its close
  reason. Long-form research evidence goes in `.agent/scratch/research/*.md`,
  referenced from the bead body.
- **`bd remember`** holds durable cross-session insights (gotchas, prior-work
  traps, verified constraints). Search with `bd memories <keyword>` before
  re-deriving anything expensive.
- **Session/loop history** is not journaled by hand: polylogue captures agent
  sessions; bead notes capture per-work decisions; the archived operating log
  is the historical corpus for the beads/devloop-as-source work (sinex-pya,
  sinex-hlv, cem.10).

Conventions for bead content:

- Organization is native to beads: capability-plane epics with `parent-child`
  children, priorities, labels, and typed deps — never a mirror of an
  external tracker. When a bead's scope originated in a GitHub issue, cite it
  with `--external-ref=gh-<N>` as provenance only; the bead body carries the
  live remaining scope. Closing a bead never changes GitHub state.
- Discovered work is linked, not orphaned:
  `bd create ... --deps discovered-from:<current-bead>`.
- Use real dependencies (`blocks`) only for true ordering; use `related` for
  soft affinity. Keep `bd dep cycles` clean.
- Label invariants (lint before exporting): exactly one `wave:N` and exactly
  one `area:*` label per open bead; no open bead without acceptance criteria;
  no wave inversions (a bead hard-blocked by a higher-wave bead). NB: in
  `bd list --json` dependency objects use `type`/`depends_on_id`; in
  `bd show --json` they use `dependency_type`/`id` — lint against the list
  shape.
- Priorities: 0 = operator directive/campaign or in-flight recovery,
  1 = data-loss correctness and the current consumption unlock, 2 = normal,
  3 = design/meta/legibility, 4 = far-backlog design notes.
- `bd dolt push` follows the same policy as `git push` (see repo CLAUDE.md).
  NB: no Dolt remote is configured — `.beads/issues.jsonl` in git IS the sync
  surface; ship bead-state deltas in PRs (`chore(beads):`).
- `bd preflight`/`bd doctor` are upstream-tool checklists that do not know
  this repo; ignore them. The real gates are the pre-push hook and xtask.

Execution-grade bar (what makes a bead startable in one read — the target
state for every `ready` bead at priority ≤ 2):

- **Description** states the problem plus the CURRENT verified state; when a
  claim cites `file:line`, date it (line numbers rot — a dated cite tells the
  next agent whether to re-verify). When scope originated in a handoff packet
  or research doc, link it instead of restating.
- **Design** carries the settled DECISION (marked as settled — agents must not
  re-litigate), exact TARGETS (`file:line` or symbol names), known pitfalls,
  and interacting beads by id.
- **Acceptance** is observable and machine-checkable where possible, and names
  the VERIFY commands in repo-native form (`xtask test -p … -E 'test(…)'`,
  `xtask schema strict-diff`, an MCP/sinexctl probe) — not "tests pass".
- **Decision beads** carry an options frame (A/B/C with costs and
  interactions) in design even before the decision; the AC is "recorded
  decision + follow-up beads dep-linked + operator sign-off where authority
  requires it".
- **Reconcile on claim**: the first agent action on claiming a bead is to
  re-verify its claims against master and update the description if the world
  moved (fixes land fast here; a 3-day-old bead can be half-done already).

Do not use TodoWrite/TaskCreate-style ephemeral task lists for anything that
should outlive the turn; local plans are execution checklists only.

## Execution Tactics (distilled from the retired packet)

- **Async verification.** Never idle-wait a heavy proof: launch with `--bg`,
  capture the job id, do light work (reads, searches, doc/bead edits), then
  `xtask jobs wait <id>`. One plain `--bg` per target lock; never nest shell
  background around it.
- **Serial heavy, parallel light.** In one checkout, serialize anything that
  shares the cargo target dir, the dev database, or the dev runtime
  (`xtask test`/`check`/`build`, schema bootstrap, `sinexd` bringup). Parallel
  tool calls are for light work only.
- **Proof ladder.** Prove the changed surface with the narrowest command that
  exercises it while iterating; run the broad gate (`xtask check --full`,
  `xtask test --impact-mode=off --all`) once per publishable phase — see the
  verification cadence in CLAUDE.md.

## Greedy Batch / PR Cadence

Default development unit: one complete bead. If the bead is genuinely too large
for one reviewable branch, widen to the largest coherent phase that can
honestly close a named acceptance-criteria subset with a clear residual matrix.
Do not open a PR for every small projection, helper, or proof artifact merely
because it is mergeable.

Treat widening as the normal policy, not an occasional optimization. When a
chosen bead has several nearby criteria in the same subsystem, gather all of
them into the same manifest before editing. A green narrow proof is a
checkpoint, not a publishing trigger, unless it proves the full bead or the
largest coherent phase available right now.

Prefer a single branch/PR when the work:

- belongs to one bead and one capability claim;
- touches the same shared view/query/rendering substrate;
- can be verified by one focused test family plus one live artifact;
- would otherwise force reviewers and future agents to reconstruct intent
  across several tiny PRs.

Split only when there is a real boundary:

- the bead is too large to review safely as one phase;
- independent parts have different risk, owners, or deployment timing;
- one part is a prerequisite unblocking other active work;
- verification cost or failure isolation would become materially worse;
- a partial PR can close a named bead or named acceptance-criteria phase, not
  just land a convenient substep.

Before publishing, audit the bead acceptance criteria. If the PR does not close
the bead, the body and bead notes must say exactly which criteria are
satisfied, which are deferred, and which follow-up bead owns the remainder.

## Scratch And Demos

`.agent/scratch/` is gitignored thinking space: `README.md`, numbered
grok/audit notes (`NNN-topic.md` — highest NNN is the current entry point),
`research/*.md` for long-form evidence, and inbox dirs for external analysis
batches (`new*/` — verify before trusting). Do not keep active loop state here.

`.agent/demos/` is a current curated shelf, not an append-only dump. Sinex's
canonical local shelf is `.agent/demos/sinex/` with `README.md`,
`SUMMARY_INDEX.json`, `MANIFEST.readable.json`, and preferably
`CURATED_CATALOG.md`. Raw proof payloads are acceptable only inside named demo
packets. Chisel owns portable bundle generation.

## Git Boundary

The tracked `.agent` surface stays small: this file, `README.md`,
`scripts/**`, the small `dev/` artifacts, `inbox/INDEX.md`,
`scratch/README.md`, and the archive. Everything else under `.agent` is
ignored live state or scratch. If a new `.agent` file should survive in git,
allowlist it in `.gitignore` deliberately with a reason in the commit.
