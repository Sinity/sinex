# Sinex Devloop Cold Start

This file is the first stop for a fresh agent told: "continue the devloop setup
in `.agent`". It should be enough to recover the loop without chat history.

## Standing Goal

Conduct the Sinex dogfood/demo devloop indefinitely: choose the highest-value
live-data capability slice, build inspectable artifacts that prove Sinex makes
agents and the operator better at reconstructing real work and machine/personal
context, collapse one-off paths into general acquisition/query/evidence/
projection/rendering substrate, verify on the active store or live local
captures, update the operating log and handoff, and use each loop's evidence to
reprioritize the next slice while keeping development fast.

Dogfooding Sinex on its own development process is instrumental. The goal is
rapid Sinex improvement guided by useful/impressive demos and by systematically
building capabilities Sinex should actually have.

## Minimum Recovery Sequence

Run these from the repo root:

```bash
.agent/scripts/devloop-status
.agent/scripts/devloop-review
```

Use `.agent/scripts/devloop-status --focus` for a very fast context refresh
when you only need current focus, next action, and queued directives. Use
`--quick` when host pressure is high and slower xtask/source inventory would add
friction.

Then read, in order:

1. `.agent/CONVENTIONS.md`
2. `.agent/devloop-contract.json`
3. `.agent/conductor-devloop/INDEX.md`
4. `.agent/conductor-devloop/README.md`
5. `.agent/conductor-devloop/ACTIVE-LOOP.md`
6. `.agent/conductor-devloop/OPERATING-LOG.md` newest entries first
7. `.agent/conductor-devloop/RUNBOOK.md`
8. `.agent/conductor-devloop/PROCESS.md`
9. `.agent/conductor-devloop/TACTICS.md`
10. `.agent/conductor-devloop/VELOCITY.md`
11. `.agent/conductor-devloop/DEMO-RADAR.md`
12. `.agent/inbox/INDEX.md`
13. `.agent/demos/sinex/CURATED_CATALOG.md`

If these disagree, trust live evidence first, then `OPERATING-LOG.md`, then
`ACTIVE-LOOP.md`; update stale state before widening work.

## How To Continue

1. Choose one capability slice, not a broad theme.
2. State the slice contract in `OPERATING-LOG.md`: demo value, reusable
   substrate, proof ladder, non-goals, and first action.
3. Prefer shared algebra over silos: acquisition, query, evidence projection,
   rendering, runtime observability, and dev tooling should be reusable by CLI,
   API, TUI, demos, and future agents.
4. Use existing live evidence: dev-local `sinexd`, source bindings,
   `sinexctl`, Polylogue, Lynchpin, GitHub issues, and the demo shelf.
5. Verify narrowly, create or refresh the inspectable artifact, then commit the
   logical chunk.
6. Run the closing ritual when a slice materially changes demos or handoff:

```bash
.agent/scripts/devloop-refresh-demos
.agent/scripts/devloop-refresh-events
.agent/scripts/devloop-sync
```

7. Update `ACTIVE-LOOP.md` with the next focus and next action.

## Current Working Surfaces

- `.agent/DEVLOOP.md`, `.agent/README.md`, `.agent/scripts/`, and
  `.agent/includes/` are tracked durable scaffold.
- `.agent/CONVENTIONS.md` is the shared Sinex/Polylogue devloop contract:
  active-root semantics, primitive names, focus modes, scratch/demo boundaries,
  and local migration decisions.
- `.agent/devloop-contract.json` is the machine-readable contract used by
  review/status tooling; update it with the prose contract when shared names or
  state semantics intentionally change.
- `.agent/conductor-devloop/` is the canonical active conductor state and packet.
- `.agent/conductor-devloop/ACTIVE-LOOP.md`, `OPERATING-LOG.md`,
  `DEMO-RADAR.md`, `EVENTS.jsonl`, `PHASES.jsonl`, `MANIFEST.md`, and
  `context/**` are local active/generated state; protocol docs in the same root
  are tracked scaffold.
- `.agent/demos/sinex/` is the canonical local demo shelf.
- `.agent/scratch/README.md` is tracked as the scratch routing file.
- `.agent/scratch/`, `.agent/demos/`, and `.agent/artifacts/` are ignored
  local/generated shelves. Keep them useful, but promote durable process rules
  into tracked scaffold files when future agents must inherit them from git.
- `.agent/artifacts/` is the evidence archive shelf; `.agent/inbox/INDEX.md`
  summarizes useful imported inbox/devloop material. Mine them selectively and
  promote conclusions, not raw dumps.
- `.agent/scratch/` is supporting research only: `README.md` plus
  `research/*.md`. Do not put active loop logs, generated proof dumps, baselines,
  or handoff packets there.

## Shared Devloop Primitive Contract

Sinex and Polylogue should expose the same primitive names so agents can
transfer habits between repos:

```text
devloop-status devloop-review devloop-start devloop-checkpoint devloop-log
devloop-focus devloop-demo devloop-baseline devloop-wait devloop-ahead
devloop-meta devloop-handoff devloop-sync devloop-integration devloop-velocity
devloop-refresh-demos devloop-refresh-events
```

`devloop-review` is the local tripwire for this contract. It should warn if
active state leaks back into `scratch/current`, if handoff mirrors or copied
script snapshots reappear, or if retired primitive names such as
`devloop-pulse`, `devloop-proof-budget`, or `devloop-velocity-report` return.

## Important Tactics

- Work while heavy commands run. Use `.agent/scripts/devloop-ahead` and
  `.agent/scripts/devloop-wait` instead of idle waiting.
- Do not start duplicate heavy builds/tests against the same checkout.
- Use `xtask`, not bare cargo.
- Commit local logical chunks proactively; do not push unless asked.
- Put future agent worktrees under `/realm/tmp/worktrees/`, not inside this
  repo's `.claude/worktrees/`. Repo-local worktrees are legacy clutter to drain
  only after dirty branches are preserved or explicitly discarded.
- Treat demos as curated products, not append-only dumps. Regenerate,
  consolidate, caveat, or retire artifacts when that makes the shelf clearer.
- If process friction appears, improve the scaffold or tooling in a concrete
  way, then return to object-level Sinex capability work.

## Common Next Commands

```bash
.agent/scripts/devloop-status
.agent/scripts/devloop-status --focus
.agent/scripts/devloop-status --quick
.agent/scripts/devloop-start "short slice name"
.agent/scripts/devloop-focus Direction Evidence "trigger" "decision"
.agent/scripts/devloop-demo
.agent/scripts/devloop-checkpoint --queue "after current" "switch to Meta focus" "current slice closes"
.agent/scripts/devloop-checkpoint --queue-complete "after current" "promoted into ACTIVE-LOOP.md"
.agent/scripts/devloop-refresh-demos
.agent/scripts/devloop-refresh-events
.agent/scripts/devloop-sync
SINEX_RUNTIME_TARGET_CONFIG=.sinex/state/runtime-target.json sinexctl runtime health -f json
SINEX_RUNTIME_TARGET_CONFIG=.sinex/state/runtime-target.json sinexctl ops dlq list -f json
```

## If Context Was Cleared

Do not ask the operator to restate the whole loop. Run the recovery sequence,
inspect git status and active xtask jobs, read the latest log entries, and
continue from the freshest coherent slice. If the active slice is stale, record
that fact and choose the highest-value next slice from live evidence and the
demo radar.
