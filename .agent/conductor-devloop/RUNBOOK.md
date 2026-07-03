# Sinex Conductor Runbook

This is the operational protocol for the Sinex dogfood/demo conductor loop.
Follow it before capability work. If deviating, write the reason in
`OPERATING-LOG.md`.

## Non-Negotiable Start Gate

Before starting or resuming a capability slice:

1. Run `.agent/scripts/devloop-status` (includes the beads summary).
2. Run `.agent/scripts/devloop-review`.
3. Read `.agent/conductor-devloop/ACTIVE-LOOP.md`.
4. Read `.agent/conductor-devloop/QUEUE.md`.
5. Run `bd ready` and `bd list --status=in_progress`; if a claimed bead exists,
   it is the presumptive current slice. Confirm the current focus mode,
   trigger, and any queued sequencing directive.
6. If review warns, either fix the warning or record why it is consciously
   accepted for this slice.

Do not start broad build/test/runtime work while unresolved process warnings are
still unexplained.

## Post-Compaction Discipline

Conversation compaction is a lossy state snapshot, not a new operator
instruction. After compaction:

- obey the newest real user message first;
- use the summary only to recover state, proofs, files, jobs, and unfinished
  work;
- do not infer that repeated themes in the summary were re-requested at resume
  time;
- if the summary overweights process/meta/velocity work, spend at most one
  bounded Meta pass to correct the scaffold, then return to the object-level
  slice unless the newest user message says otherwise;
- update `ACTIVE-LOOP.md` if it disagrees with the actual current slice before
  committing or widening work.
- check `QUEUE.md` for deferred operator directives before choosing the next
  slice. A queued directive is not a repeated request from the summary, but it
  is an explicit sequencing obligation if its trigger condition is satisfied.

## Focus State Machine

Use these modes as scopes of attention:

- `Direction`: choose or revise the capability slice.
- `Evidence`: inspect current code/data/runtime/history to ground the slice.
- `Construction`: edit code/config/docs/artifacts.
- `Proof`: verify exactly the claim being made.
- `Artifact`: make the result inspectable outside chat.
- `Velocity`: remove or record friction that slows the next loop.
- `Meta`: audit agent/process failure modes and convert useful corrections into
  executable scaffold, observability, or tripwires.

Every mode switch should have:

```text
Focus: Previous -> Next
Trigger: concrete observation
Decision: what changes now
```

For material focus changes, use:

```bash
.agent/scripts/devloop-focus <from> <to> "<trigger>" "<decision>"
```

This appends a timestamped transition to `ACTIVE-LOOP.md` and
`OPERATING-LOG.md`. Run `devloop-sync` after related packet changes.

## One-Loop Protocol

1. **Direction**
   - Select one capability slice. Start from `bd ready` (highest-priority
     unblocked beads) plus live evidence; a slice that has no bead gets one
     (`bd create`) before construction starts.
   - Claim the bead: `bd update <id> --claim`. Name the bead id in the slice
     contract.
   - State out-of-scope items.
   - Write the slice contract before editing:
     - demo value: what visible operator/agent capability improves;
     - reusable substrate: acquisition, query, evidence projection, renderer,
       or tooling primitive being improved;
     - proof ladder: narrowest proof first, wider proof only when the claim
       widens;
     - non-goals: tempting adjacent work excluded from this loop;
     - first action: the next command/file read/edit, not a vague intention.
   - Append or update `OPERATING-LOG.md`.
   - If the slice creates, refreshes, retires, or caveats a demo artifact,
     update the matching demo bead (`bd list -l demo`).
   - If the operator gives a sequencing directive that should happen after the
     selected slice, record it in `QUEUE.md` instead of relying on chat memory.

2. **Evidence**
   - Gather authoritative current state.
   - Separate occurrence evidence from derived/candidate interpretation.
   - Identify coverage gaps that would make a demo misleading.

3. **Construction**
   - Make the smallest coherent change that advances the slice.
   - Prefer shared acquisition/query/evidence/render substrate.
   - Avoid one-off CLI/report/demo silos.

4. **Proof**
   - Use the proof ladder: source review -> unit/parser -> package -> CLI/dev
     store -> real-data artifact -> broad gate.
   - State what the proof supports and what it does not.

5. **Artifact**
   - Update `/realm/inbox/demos_sinex` or the conductor packet with something
     inspectable.
   - Keep active outputs out of `/realm/inbox/project-artifacts` and
     `/realm/inbox/project-devloops`; those shelves hold archived/downloaded
     inputs after the 2026-06-30 inbox reorg.
   - Include limits/caveats and rerun/inspection instructions.
   - Update the demo bead's status/notes with the artifact path and proof
     state; new demo candidates become demo-labeled beads under the portfolio
     epic with claim/instrument/falsifier/prereq in the design field.

6. **Velocity**
   - Record speedup, drag, next acceleration, and risk.
   - Run `.agent/scripts/devloop-sync`.
   - Inspect `PHASES.jsonl` or run `devloop-velocity` when elapsed time or
     proof churn is the issue; do not rely on memory for phase timing.
   - Run `.agent/scripts/devloop-review` before claiming the loop checkpoint is
     clean.

7. **Meta**
   - Run when the operator corrects process behavior, repeated friction appears,
     or a loop feels vague/stalled.
   - Use `.agent/scripts/devloop-meta "<trigger>"`.
   - Prefer one concrete scaffold/tooling/observability change over a broad
     apology.
   - Ask whether the process failed to generate or update demo artifacts soon
     enough; if yes, update the demo portfolio beads and improve the tripwire.

## Heavy Job Protocol

Before a heavy command:

- name the proof claim;
- choose the narrowest sufficient command;
- prefer `--bg` when the next decision does not require immediate output;
- append job id and expected duration to the log.
- if wrapper/bootstrap, compile, test, docs, or runtime startup may take more
  than one minute, immediately record active wait state:

  ```bash
  .agent/scripts/devloop-wait "<job-or-command>" "<proof-claim>" "<poll-in>" "<mode-task>"
  ```

While it runs:

- do useful foreground work from `.agent/scripts/devloop-ahead`;
- rotate focus deliberately instead of treating the wait as a pause:
  - Proof wait -> Velocity if tooling/resource/time friction is visible.
  - Proof wait -> Artifact if the demo/report can be improved without relying
    on the pending result.
  - Proof wait -> Evidence if adjacent source/runtime/history can sharpen the
    next decision without competing for the build target.
  - Proof wait -> Direction if the pending proof is likely to close the slice
    and the next slice needs prioritization.
- do not start another conflicting heavy job;
- poll at natural boundaries, not continuously.
- if no job id has appeared yet, treat the launcher/wrapper itself as the
  waited-on command and do low-risk docs/scaffold/artifact work until the first
  bounded poll.

If pressure rises or duration exceeds expectation, switch to `Velocity`.

## Dev Runtime Protocol

- For live Sinex proof, use dev-local `sinexd`/Postgres/NATS through `xtask`
  (`xtask infra ...`, `xtask run core ...`). This is the normal fast
  development substrate.
- Keep production/default Sinex services off unless the operator explicitly
  asks for production-state work. Do not start prod PostgreSQL/NATS/sinexd just
  to satisfy a dev artifact.
- When a demo artifact depends on runtime state, prefer fixing/starting the
  dev-local runtime over falling back to a projected artifact.
- Record whether an artifact is live-generated, replayed from a captured
  payload, or fixture-only.
- Capture a lightweight runtime baseline before and after non-trivial runtime
  or resource work:

  ```bash
  .agent/scripts/devloop-baseline "short-label"
  ```

  Baselines live under `.agent/artifacts/live-baselines/` and should be treated as
  local evidence for later comparison, not as product demos.

## Git / Branch Protocol

- Treat this as a long-lived development branch unless the operator asks for a
  new branch or a PR-shaped split.
- Commit logical chunks proactively after focused proof, using staged paths.
  Avoid broad staging sweeps.
- Prefer continuing in this checkout over creating worktrees. Use worktrees only
  when true isolation is needed for concurrent risky edits or a separate agent
  lane; ordinary compile/test wait time is better spent on ahead work in the
  same branch.
- If a compile/test fails after ahead work, diagnose the failure shape and batch
  the fix. Do not treat the possibility of retry as a reason to stop useful
  foreground progress.
- Do not push unless explicitly asked.

## Integration Lane

The long-lived devloop branch is an integration workbench, not the final history
shape. When the branch has accumulated many unrelated slices, keep a parallel
integration lane active enough that work can move to master without archaeology.

Use:

```bash
.agent/scripts/devloop-integration
.agent/scripts/devloop-integration --subagent-prompt
```

The default report shows branch/base/head state, local phase-level plan state,
generated git-stack evidence, PR-shaped branches, replay worktrees, remote PR
branches, and the current ahead commit train. Treat generated `xtask git-stack`
plans as raw evidence, not authority: final PR boundaries require LLM judgment
over product/change intent, dependencies, and verification shape. For the
current branch, target roughly 16-24 total PRs unless a phase proves too risky
and needs subdivision.

Subagents may do read-heavy clustering, claim audits, PR-body preparation,
dependency checks, and dry-run replay scripts. The main devloop owns final
branch creation, conflict resolution, verification, pushing, PR creation, and
cleanup of superseded remote branches.

Before opening or merging a PR, verify the PR head still matches its remote
branch:

```bash
.agent/tools/gh_pr_safety.sh Sinity/sinex <pr-number> <branch-name>
```

## Stop Conditions

Do not continue in the same direction if:

- first evidence contradicts the assumed blocker;
- the artifact would be misleading because source coverage is poor;
- implementation is turning into one-off glue;
- broad verification is being used to avoid choosing a narrow proof;
- the process/log/handoff state is stale enough that a future agent could not
  resume.
- the active slice contract is missing, stale, or no longer matches the work
  being performed.

Switch to `Direction` or `Velocity`, record the trigger, and choose again.

## End Gate

Before ending a turn:

1. `OPERATING-LOG.md` has a filled entry for the work just done.
2. `ACTIVE-LOOP.md` names the next focus and next action.
3. Beads state matches reality: the slice's bead is closed
   (`bd close <id> --reason "..."` citing the proof) or still claimed with the
   blocker recorded; discovered follow-ups exist as beads
   (`--deps discovered-from:<id>`), not as prose bullets; durable insights are
   in `bd remember`.
4. `QUEUE.md` has no satisfied queued directive left unpromoted or unexplained,
   and every live queued directive has its mirror bead.
5. `.agent/scripts/devloop-sync` has refreshed derived conductor files when
   packet state or demo manifests changed.
6. `devloop-review` warnings are either fixed or explicitly accepted.
7. Demo beads touched by the work are updated (artifact path, proof state,
   caveats), or the log says why no demo was implicated.
8. Any running job needed for the request is stopped, completed, or named with
   job id and next poll command.
