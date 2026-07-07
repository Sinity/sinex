# Agent Workspace

This directory holds Sinex agent memory that is useful but not part of the
product source tree.

## Read First

- `DEVLOOP.md` — cold-start guide for a fresh agent told to continue the
  `.agent` devloop setup without chat history.
- `CONVENTIONS.md` — shared Sinex/Polylogue devloop contract: active-root
  semantics, primitive names, focus modes, scratch/demo boundaries, and local
  migration decisions.
- `devloop-contract.json` — machine-readable Schelling point for shared
  primitive names, active packet files, focus modes, ignored/generated state,
  and retired names. `devloop-review` uses it as an executable guardrail.
- `conductor-devloop/` — active conductor/devloop packet. Start with
  `conductor-devloop/README.md` and `conductor-devloop/INDEX.md`.
- `conductor-devloop/context/2026-06-30-conductor-sinex-assimilation.md` —
  assimilation of `/realm/inbox/download/conductor-sinex.md` into the active
  loop.
- `conductor-devloop/OPERATING-LOG.md` — timestamped detailed log for current and
  future conductor loops.
- `conductor-devloop/EVENTS.jsonl` and `conductor-devloop/PHASES.jsonl` —
  generated sidecars from the operating log; use them for machine-readable
  event recall and phase/velocity analysis. `EVENTS.jsonl` is compact by
  design: it stores structured fields plus body excerpt/hash metadata, not a
  duplicate full copy of every log entry.
- `conductor-devloop/SELF-PROMPTS.md` — reusable prompts for resume, slice
  selection, wait-time work, meta repair, and demo curation.
- `conductor-devloop/VELOCITY.md` — time/cadence rubric and acceleration rules.
- `conductor-devloop/TACTICS.md` — async verification and no-idle-wait tactics.
- `conductor-devloop/PROCESS.md` — operational focus modes and triggers for
  shifting attention during the loop.
- `conductor-devloop/RUNBOOK.md` and `conductor-devloop/ACTIVE-LOOP.md` — concrete
  start/end gates and the current loop state.
- `inbox/INDEX.md` — integrated index of useful `/realm/inbox` material and the
  demo-shelf closing ritual.
- `scratch/README.md` — routing index for scratch, archive, research, and
  artifact material.

## Integrated Content Routing

- `.agent/demos/sinex` — canonical Sinex demo shelf. New inspectable demo
  proofs, manifests, and README updates go here. `CURATED_CATALOG.md` is the
  generated first-stop view for externally showable demos; raw proof files are
  allowed only while they are being folded into clearer packets.
- `.agent/conductor-devloop` — canonical active Sinex conductor/devloop
  packet. It is the source of truth for current loop state; it is not mirrored
  into `scratch/current` or `handoff/`.
- `.agent/artifacts/` — ignored evidence/archive shelf for downloaded patches,
  old analyses, GitHub snapshots, lightweight baselines, and other bulky
  material that should not be read on startup.
- `.agent/inbox/INDEX.md` — tracked routing summary for useful material moved
  out of `/realm/inbox`; raw copied session/export bundles should not be
  preserved as duplicate handoff payloads in `.agent`.

Large imported content lives under ignored `.agent/{demos,artifacts}` or
`.agent/scratch/research/`; tracked indexes summarize the routing.

## Git Boundary

Tracked scaffold teaches the loop; ignored live state records this checkout's
current run.

- Track durable instructions and reusable primitives: `.agent/DEVLOOP.md`,
  `.agent/README.md`, `.agent/CONVENTIONS.md`,
  `.agent/devloop-contract.json`, `.agent/scripts/**`,
  `.agent/scratch/README.md`, and the conductor protocol files `README.md`,
  `INDEX.md`, `RUNBOOK.md`, `PROCESS.md`, `TACTICS.md`, `VELOCITY.md`,
  `ADVERSARIAL-REVIEW.md`, and `SELF-PROMPTS.md`.
- Keep active state ignored but canonical in `.agent/conductor-devloop/`:
  `ACTIVE-LOOP.md`, `OPERATING-LOG.md`, `EVENTS.jsonl`,
  `PHASES.jsonl`, `MANIFEST.md`, and `context/**`.
- Keep `.agent/scratch/README.md` as the tracked routing file for supporting
  research. Scratch content beyond that is local ignored research, not startup
  state. Do not reintroduce `.agent/scratch/current`, `.agent/handoff/*`
  mirrors, or copied `conductor-devloop/scripts/` snapshots.
- Keep local evidence archives such as lightweight baselines under
  `.agent/artifacts/`, not under scratch.
- Keep `.agent/demos/sinex/` curated and current, but treat it as a local demo
  shelf unless a particular artifact is deliberately promoted into tracked
  scaffold or docs.

## Stable Includes

- `includes/` — generated/transcluded agent memory used by `CLAUDE.md` /
  `AGENTS.md`. Treat these as curated memory, not scratch clutter.
- `scripts/` — small agent helper scripts:
  - canonical primitive names: `devloop-status`, `devloop-review`,
    `devloop-start`, `devloop-checkpoint`, `devloop-log`, `devloop-focus`,
    `devloop-baseline`, `devloop-wait`, `devloop-ahead`,
    `devloop-meta`, `devloop-handoff`, `devloop-sync`, `devloop-velocity`,
    `devloop-refresh-demos`, and `devloop-refresh-events`.
  - `devloop-status` prints current goal, last log entry, git/runtime/pressure
    state, likely loop-affecting processes, and boundary-time pulse signals.
    Use `devloop-status --focus` for the fastest current-focus/next-action/
    queue refresh, and `devloop-status --quick` when pressure makes xtask/source
    inventory too expensive.
  - `devloop-start "slice"` appends a timestamped start entry.
  - `devloop-checkpoint "title"` appends a reassessment entry.
  - `devloop-focus <from> <to> "trigger" "decision"` records focus transitions
    using the shared modes from `devloop-contract.json`.
  - `devloop-baseline` captures a lightweight local baseline for later
    comparison.
  - `devloop-wait` records a long-running command plus the foreground work that
    should happen before the next poll.
  - `devloop-meta` records process-failure or scaffold-improvement notes.
  - `devloop-handoff` creates a timestamped handoff under
    `conductor-devloop/context/handoffs/`; it must not create root packet
    clutter or a handoff mirror.
  - `devloop-ahead` prints useful foreground work to do while heavy jobs run.
  - `devloop-sync` refreshes generated conductor packet files: event sidecar,
    demo manifests, and packet manifest. It does not copy mirrors.
  - `devloop-refresh-events` regenerates `conductor-devloop/EVENTS.jsonl`
    and `conductor-devloop/PHASES.jsonl` from the operating log for machine
    analysis. It keeps `EVENTS.jsonl` compact with `body_excerpt`,
    `body_line_count`, and `body_sha256` so the full prose source of truth
    remains `OPERATING-LOG.md`.
  - `devloop-refresh-demos` rebuilds the demo manifest, curated catalog, and
    summary index from `.agent/demos/sinex`. Chisel owns portable bundle
    generation; do not create full concatenated readable copies here.
  - `devloop-review` adversarially checks scaffold drift, TODO rot, active jobs,
    and likely process-pressure traps.
  - `devloop-velocity` summarizes recent proof cadence, repeated proof shapes,
    compile/test friction, and relevant resource pressure.
  - `devloop-log "title"` appends a raw timestamped loop entry.

## Generated Or Local Artifacts

- `dev/` — tracked dev/demo support artifacts that are useful to preserve with
  the checkout, including recall-silo-collapse proof output.
- `artifacts/` — bulky raw dumps, media, patches, lightweight baselines, and
  generated proof material that should not be read on startup.

Do not put new loose files at `.agent/` or `.agent/scratch/` root. Put active
loop notes in `conductor-devloop/`, research in `scratch/research/`, durable
rules in tracked scaffold/includes, and generated proof payloads inside named
demo packets or purpose-specific ignored artifact shelves.
