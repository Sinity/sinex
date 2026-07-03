# Shared Devloop Conventions

This is the Sinex copy of the shared Sinex/Polylogue devloop contract. It is a
coordination point, not a transcript. Update it when the primitive names or
state semantics intentionally change.

`.agent/devloop-contract.json` is the machine-readable form of the same
contract. Scripts should read it where practical; prose should explain the
why and edge cases. If they disagree, fix both before treating the convention
as settled.

## Canonical Shape

Use Polylogue's active-state model and Sinex's durable-knowledge model:

```text
.agent/
  README.md
  DEVLOOP.md
  CONVENTIONS.md
  devloop-contract.json
  conductor-devloop/
  includes/
  scripts/
  demos/
  scratch/
  tools/
```

`.agent/conductor-devloop/` is the canonical active loop root. Do not keep
active loop state in `.agent/scratch/current`. Do not maintain a handoff mirror
unless it is a generated, disposable export with a clear source-of-truth marker.

## Required Active Packet Files

The active packet should contain these names:

- `README.md` — how to resume the loop.
- `RUNBOOK.md` — protocol, proof ladder, heavy-job rules, and gates.
- `ACTIVE-LOOP.md` — current slice, accepted warnings, and next action.
- `QUEUE.md` — deferred operator directives and next-after-current obligations.
- `OPERATING-LOG.md` — timestamped decisions, actions, and proofs.
- `PROCESS.md` — focus modes and transition rules.
- `TACTICS.md` — async/heavy-work tactics.
- `VELOCITY.md` — speed/cadence/friction rules.
- `ADVERSARIAL-REVIEW.md` — failure modes and local checks.
- `INDEX.md` — routing guide for the packet.

Recommended generated sidecars:

- `EVENTS.jsonl` — generated from `OPERATING-LOG.md`.
- `PHASES.jsonl` — generated phase/focus subset of `OPERATING-LOG.md` for
  velocity and temporal analysis.
- `MANIFEST.md` — generated packet inventory and script hashes.
- `context/INDEX.md` — generated inventory of supporting context notes.

`EVENTS.jsonl` should expose incomplete historical records instead of hiding
them: when a structured field still contains a literal `TODO`, the generator
marks the event with `incomplete: true` and `incomplete_fields`. `PHASES.jsonl`
must omit incomplete records so velocity/timing views are based on real loop
events, not placeholder templates. `devloop-review` should warn when recent
events regress to incomplete records and may treat older incomplete records as
explicit historical debt.

`EVENTS.jsonl` is a machine sidecar, not a second copy of the operating log.
It should carry structured fields, `body_excerpt`, `body_line_count`, and
`body_sha256`; full prose remains in `OPERATING-LOG.md`. This keeps event
recall useful for later tooling without multiplying giant active-packet files.
`devloop-review` should warn if legacy full-body event records reappear or if
generated sidecars are older than `OPERATING-LOG.md`.

## Canonical Script Names

Every devloop repo should expose these executable primitive names:

```text
devloop-status
devloop-review
devloop-start
devloop-checkpoint
devloop-log
devloop-focus
devloop-baseline
devloop-wait
devloop-ahead
devloop-meta
devloop-handoff
devloop-sync
devloop-integration
devloop-velocity
devloop-refresh-demos
devloop-refresh-events
```

Retired names should not return: `devloop-pulse`, `devloop-proof-budget`, and
`devloop-velocity-report`.

Devloop primitives must be safe by default. A helper that appends conductor
state must either require concrete fields or write explicit "not supplied"
language; it must not emit literal `TODO` placeholders that make
`devloop-review` fail immediately after using the helper.

## Shared Resource Discipline

Sinex proof commands are often not independent even when their test filters are.
In one checkout, serialize:

- `xtask test` invocations, including different packages;
- `xtask build`/`xtask check` invocations that share the cargo target dir;
- checkout-local schema/bootstrap work against the dev database;
- dev-local `sinexd` runtime bringup.

Use parallel tool calls for file reads, searches, source review, artifact
writing, and other light work. For proof work, prefer one combined xtask command
or serial focused invocations. If you intentionally overlap proof lanes, record
why in `OPERATING-LOG.md` and use `xtask history overlap` afterward to measure
whether the overlap actually helped.

## Focus Modes

Use these exact focus modes:

- `Direction`
- `Evidence`
- `Construction`
- `Proof`
- `Artifact`
- `Velocity`
- `Meta`

Material focus changes should be recorded with:

```bash
.agent/scripts/devloop-focus <from> <to> "<trigger>" "<decision>"
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
- **`ACTIVE-LOOP.md`** remains the current-slice projection. A normal slice
  maps to one claimed bead: `bd update <id> --claim` when the slice starts,
  `bd close <id> --reason "..."` when its proof lands. Name the bead id in the
  slice contract.
- **`QUEUE.md`** remains the operator-directive intake channel (scripts parse
  it), but every queued directive is mirrored as a `campaign`/`directive`
  bead at creation time. The bead is the durable copy: the
  slice-closure-is-not-campaign-closure failure (cleanup deleting pending
  operator directives) cannot delete a bead.
- **Backlog and side-research live in beads.** Prioritization is priority
  fields plus deps, reviewed through `bd ready`; a side-research lease is a
  claimed `side-research`-labeled bead whose reconciliation is its close
  reason. Long-form research evidence goes in `.agent/scratch/research/*.md`,
  referenced from the bead body.
- **`bd remember`** holds durable cross-session insights (gotchas, prior-work
  traps, verified constraints). Search with `bd memories <keyword>` before
  re-deriving anything expensive.

Conventions for bead content:

- Organization is native to beads: capability-plane epics with `parent-child`
  children, priorities, labels, and typed deps — never a mirror of an
  external tracker. When a bead's scope originated in a GitHub issue, cite it
  with `--external-ref=gh-<N>` as provenance only; the bead body carries the
  live remaining scope, which is often fresher than the GitHub text. Closing
  a bead never changes GitHub state — that stays a separate explicit act
  under the resolver-keyword discipline in the git workflow rules.
- Discovered work is linked, not orphaned:
  `bd create ... --deps discovered-from:<current-bead>`.
- Use real dependencies (`blocks`) only for true ordering; use `related` for
  soft affinity. Keep `bd dep cycles` clean.
- Priorities: 0 = operator directive/campaign or in-flight recovery,
  1 = data-loss correctness and the current consumption unlock, 2 = normal,
  3 = design/meta/legibility, 4 = far-backlog design notes.
- `bd dolt push` follows the same policy as `git push` (see repo CLAUDE.md).

Do not use TodoWrite/TaskCreate-style ephemeral task lists for anything that
should outlive the turn; local plans are execution checklists only.

## Scratch And Demos

`.agent/scratch/` is not active loop state. Keep it to `README.md` and
`research/*.md` in steady state. Active logs, generated proof dumps, raw exports,
and old handoff packets belong in the conductor packet, a demo packet, or an
ignored artifact shelf.

`.agent/demos/` is a current curated shelf, not an append-only dump. Sinex's
canonical local shelf is `.agent/demos/sinex/`; Polylogue may use
`.agent/demos/` directly. A repo/demo root with demos should maintain:

- `README.md`
- `SUMMARY_INDEX.json`
- `MANIFEST.readable.json`
- preferably `CURATED_CATALOG.md`

Raw proof payloads are acceptable only inside named demo packets when they are
part of an inspectable artifact. Chisel owns portable bundle generation; do not
create duplicated concatenated readable bundles here.

## Handoff Convention

`devloop-handoff` writes context notes under
`.agent/conductor-devloop/context/handoffs/`. A handoff is a snapshot and
pointer set, not a source-of-truth mirror and not a root-level packet file.

Handoff files should avoid TODO placeholders. If a fact is unknown, say which
canonical file or command owns it instead: `ACTIVE-LOOP.md`, `OPERATING-LOG.md`,
`EVENTS.jsonl`, `PHASES.jsonl`, `devloop-status`, or `git status`.

## Packet Size And Clutter Budget

The conductor packet should stay readable as an operating surface, not become a
dumping ground.

- Keep `.agent/conductor-devloop/` root to the named protocol/live-state files
  from this convention.
- Put supporting notes under `context/`, and make sure they are discoverable
  from the generated `context/INDEX.md` plus any more specific context index.
- Treat large context notes as debt: if a single context file grows past roughly
  48 KiB, split it, summarize it into durable scaffold/includes, or move raw
  evidence into a demo/artifact shelf.
- Generated sidecars such as `EVENTS.jsonl` may be large, but must be
  regenerable by `devloop-refresh-events`/`devloop-sync` and should not be
  edited by hand.
- Generated sidecars should not duplicate full source logs. Keep compact
  structured records with stable hashes/excerpts, and return to the source log
  only when full prose is needed.
- Do not add extra `devloop-*` script names casually. Shared primitive names are
  the Schelling surface; repo-specific behavior should usually live behind an
  existing primitive or a non-devloop helper name.

## Git Boundary

The tracked `.agent` surface is intentionally small and contract-driven:

- durable scaffold from `tracked_scaffold` in `.agent/devloop-contract.json`
- explicit small proof/runtime artifacts from `tracked_artifacts`
- executable helpers under `.agent/scripts/**`

Everything else under `.agent` should be ignored live state, generated demo
material, scratch research, or archived evidence. If a new `.agent` file should
survive in git, add it to the contract with a reason instead of relying on an
accidental unignored path.

## Review Warning Acceptance

`devloop-review` warnings are action prompts, not decoration. If a live
condition is intentionally accepted for the current slice, record a timestamped
entry in `OPERATING-LOG.md` with the exact warning and the reason it is safe for
the next action.

Acceptance is narrow and revocable. It may suppress a known process-pressure
warning only while no conflicting heavy build/test process is already running;
review output must still show the live process and resource footprint.

## Active-State Freshness

`ACTIVE-LOOP.md` is the human-readable current-state projection. It must move
when a proof or artifact closes a slice. `devloop-status` prints the latest
operating-log timestamp and the latest active-loop focus-change timestamp;
`devloop-review` warns when a Proof or Artifact log entry is newer than the
last recorded focus transition. Use `devloop-focus` or update the active slice
before moving on, so a context-cleared agent does not resume from a stale
summary.

Refreshing the active slice must update `Current Slice`, `Slice Contract`,
`Current Focus`, and `Next Action` together. A slice checkpoint that changes the
future work but leaves the visible focus trigger/decision from the previous
slice is stale even when the operating log is current.

Keep `ACTIVE-LOOP.md` compact. `Current Slice`, `Slice Contract`, and
`Next Action` are resume projections, not history archives; `devloop-review`
uses `.agent/devloop-contract.json` to warn when those sections exceed the
shared soft line limit.

`QUEUE.md` is the live deferred-directive channel. Use it when the operator
orders sequencing such as "after this, switch to meta" or when a slice creates a
near-future obligation that should survive compaction but should not replace the
current slice yet. It is ignored active state, not durable scaffold. Keep it
short: directive, trigger/condition, status, and next checkpoint. When the
condition fires, either promote the queued item into `ACTIVE-LOOP.md` or mark it
complete/retired with a log entry.

Use the existing checkpoint primitive for queue lifecycle so the shared script
namespace stays stable:

```bash
.agent/scripts/devloop-checkpoint --queue "<title>" "<directive>" "<trigger>"
.agent/scripts/devloop-checkpoint --queue-complete "<title>" "<outcome>"
```

Do not add a separate `devloop-queue` primitive unless the shared contract is
changed in both Sinex and Polylogue.

## Operational Projection Semantics

Shared devloop scripts should expose interpreted state, not raw fields that
look authoritative but require local decoding.

For dev source bindings, use these terms:

- `accepted_bindings`: non-proposed bindings currently present in the
  source-status API. This is catalog/deployment acceptance, not proof that a
  runtime is currently live.
- `runtime=hot`: the runtime mode reports a fresh live heartbeat/output signal.
- `runtime=observed`: the runtime mode has been observed but is not currently
  classified hot. This can still be healthy for quiet sources.
- `runtime=none`: no runtime observation is visible.
- `output=fresh`: source output or last event is within the freshness window.
- `output=recent`: output exists in the inspected window but is not fresh.
- `output=quiet`: no recent output; this is not itself a warning for quiet
  sources with an observed runtime.

Do not print a raw `runtime_live=false` as the headline for an otherwise active
source. Review warnings should be based on the combined binding/runtime/output
state and source criticality.

## Sinex-Specific Decisions

- Keep `.agent/includes/`; durable system knowledge is a Sinex strength.
- Keep `.agent/conductor-devloop/` as the active root.
- Keep `.agent/handoff/` retired. Handoffs, when useful, live under
  `.agent/conductor-devloop/context/handoffs/`.
- Keep `devloop-refresh-events` and `devloop-refresh-demos` as generated-file
  refreshers.
- Keep `devloop-review` as the executable convention tripwire.
