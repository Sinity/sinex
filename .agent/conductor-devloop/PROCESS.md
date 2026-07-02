# Conductor Focus Modes

The conductor loop is one agent shifting focus deliberately. These are not
separate identities or permanent hats. They are scopes of attention that should
activate on concrete signals and leave a visible trace in the log or artifact.

## Rule

When the loop starts to feel vague, blocked, or overly absorbed in one detail,
name the current focus mode and the trigger that put you there. If the trigger
is gone, switch focus.

## Focus Modes

### Direction

Owns objective function, sequencing, scope, and tradeoffs. Chooses the next
capability slice, keeps the loop aimed at inspectable real-data artifacts, and
decides when to pivot.

Enter when:

- beginning a loop or resuming after context loss;
- a result contradicts the current plan;
- a task expands into multiple plausible slices;
- more than 10 minutes pass without a concrete capability slice;
- a proof/artifact claim is about to be made.

Ask:

- What is the highest-value demonstrable capability slice now?
- Is this still the fastest path to a real artifact?
- What new demo, demo improvement, or demo retirement does the current evidence
  suggest?
- What should be explicitly out of scope for this loop?

Exit with:

- one selected slice;
- first evidence command or file set;
- operating-log entry if the direction changed.
- `QUEUE.md` update when the operator has specified sequencing after the
  current slice.

### Evidence

Maps the relevant subsystem and prior notes. Produces source/file/issue/log
evidence. Does not turn research into the plan unless it advances the current
capability proof.

Enter when:

- the selected slice depends on uncertain current behavior;
- prior scratch notes disagree;
- a hypothesis is driving the work;
- a live-data claim needs provenance.

Ask:

- What is the raw occurrence/source evidence?
- What is derived/candidate/inferred?
- What coverage gap would make the demo misleading?

Exit with:

- cited files/commands/data sources;
- a narrowed implementation target or a pivot back to Direction;
- caveats to carry into the artifact.

### Construction

Makes scoped changes. Uses existing Sinex patterns and avoids inventing
one-off report/query/render silos.

Enter when:

- evidence has identified an edit boundary;
- the next step is code/config/docs/artifact construction;
- a missing primitive blocks the demo.

Ask:

- Is this a shared substrate improvement or a one-off silo?
- What is the smallest coherent change that moves the capability?
- What proof will validate exactly this claim?

Exit with:

- changed files or generated artifact;
- intended proof command;
- checkpoint if the edit widens beyond the selected slice.

### Proof

Runs targeted checks and live-data proof. Separates fixture proof, active dev
store proof, and operator-seat proof.

Enter when:

- an edit is ready to validate;
- a claim is about to be written;
- a test/build/live command finishes;
- a demo artifact needs evidence.

Ask:

- What claim does this proof actually support?
- Is it fixture-only, dev-store, or live/operator-seat evidence?
- Did the proof fail because of code, stale state, tool friction, or host pressure?

Exit with:

- exact command/output summary;
- updated artifact caveat or acceptance claim;
- continue/fix/pivot decision.

### Artifact

Turns the capability into an inspectable artifact under `/realm/inbox/demos_sinex`
or the current conductor packet. Keeps limitations and caveats visible.
The reorganized archive shelves under `/realm/inbox/project-artifacts` and
`/realm/inbox/project-devloops` are for historical/downloaded inputs, not active
demo output.

Enter when:

- proof exists for a capability slice;
- a demo/report/context pack is the next useful output;
- a skeptical reader needs to inspect the result without chat context.

Ask:

- What can a reader see or rerun?
- Are occurrence, derived, self-observation, and coverage gaps labeled?
- Does the artifact prove a reusable primitive rather than a bespoke report?

Exit with:

- artifact path;
- README/manifest update if needed;
- log entry naming limits and next capability gap.

### Demo Radar

Demo radar is not a separate mode; it is a recurring checkpoint inside
Direction, Artifact, Proof, and Meta. It forces the conductor to ask what
inspectable capability should exist next instead of letting demos become an
afterthought.

Run:

```bash
.agent/scripts/devloop-demo "<trigger>" "<candidate demos>" "<selected demo>" "<artifact action>" "<proof/caveat>" "<next demo question>"
```

Use it when:

- starting or reprioritizing a slice;
- proof shows a capability is real enough to artifact;
- evidence reveals an existing demo is stale or misleading;
- the operator asks about demos, impressive examples, or what Sinex can show;
- before handoff after substantial work.

Exit with:

- a timestamped `DEMO-RADAR.md` entry;
- a selected artifact action, even if that action is "retire/caveat this demo";
- one next demo question that can drive the next Direction pass.

### Velocity

Tracks elapsed time, resource pressure, compile/test cost, stale processes, and
friction. Proposes the single next acceleration move.

Enter when:

- a heavy job starts or exceeds expected budget;
- host pressure is visible;
- progress stalls;
- the same friction recurs;
- a loop ends.

Ask:

- What useful foreground work can run while this waits?
- What process/tooling change makes the next loop faster?
- Is the current proof too broad or duplicated?

Exit with:

- status/pressure snapshot if relevant;
- next acceleration action;
- checkpoint or handoff update.

### Meta

Audits the conductor itself: attention failures, repeated operator corrections,
bad assumptions, stale scaffold, poor waiting behavior, over/under-commitment,
and missed opportunities to automate or observe.

Enter when:

- the operator corrects process behavior, priorities, or interpretation;
- the same friction appears twice;
- a wait or proof loop becomes passive;
- a status report surprises the operator;
- a scaffold/tooling gap caused extra manual work;
- before handoff after a substantial loop.

Ask:

- What did I fail to notice soon enough?
- What trigger should have made me rotate modes earlier?
- Did I invent a constraint, avoid responsibility, or stop at a partial fix?
- Can this be made executable, observable, or checklist-enforced?

Exit with:

- one explicit failure hypothesis;
- evidence for/against it;
- a process/tooling/scaffold change now, or a named reason to defer;
- a tripwire that would catch recurrence.

## Trajectory Adjustment Triggers

Reassess the plan when any of these occur:

- orientation exceeds 10 minutes without a selected slice;
- first evidence contradicts the assumed blocker;
- a build/test/live runtime step exceeds the expected budget;
- host pressure rises enough to slow typing, browsing, or verification;
- an artifact would become one-off glue instead of shared substrate;
- a source family is absent or low quality and would make the demo misleading;
- a different slice becomes clearly more demonstrable with less uncertainty.

## Mode Rotation Policy

The loop should rotate modes whenever the current mode is waiting, stale, or no
longer the highest-leverage use of attention. A rotation is not a context
switch away from the goal; it is how the conductor keeps the goal moving while
some other part of the system is compiling, starting, importing, or being
verified.

Use this table as the default:

| Current condition | Rotate to | Purpose |
| --- | --- | --- |
| Proof is running and may take >1 minute | Velocity | Record wait state, pressure, queue/bootstrap/compile split, and next poll |
| Proof is running but artifact shape is known | Artifact | Improve command trail, caveats, README/manifest, or demo copy |
| Proof is running and artifact may fail | Evidence | Inspect adjacent source/history/runtime without broadening the edit |
| Proof is likely to close the slice | Direction | Pick or prepare the next slice before the result lands |
| Evidence contradicts the plan | Direction | Rechoose scope instead of patching around bad assumptions |
| Construction grows a second concern | Direction | Split or explicitly accept scope expansion |
| Artifact starts becoming bespoke glue | Construction | Promote the shape into shared query/projection/render substrate |
| Demo artifact is stale, weak, or newly possible | Artifact -> Direction | Refresh/caveat it, then decide the next capability slice |
| Repeated friction appears | Velocity | Fix tooling, observability, docs, scripts, or resource setup |
| Host pressure rises | Velocity | Attribute pressure and avoid duplicate heavy work before proceeding |
| Same proof shape is about to rerun | Velocity -> Proof | Run `devloop-velocity`, then rerun only if the claim or changed files justify the cost |
| Operator corrects process/priority | Meta | Identify the missed trigger and improve the loop instead of just apologizing |
| Same kind of correction repeats | Meta -> Velocity | Convert the correction into executable scaffold or observability |
| Operator says "after this, next ..." | Direction -> Meta/target mode when condition fires | Record the directive in `QUEUE.md`; do not let compaction turn it into a repeated immediate request |

Cadence rules:

- Every wait over one minute gets `.agent/scripts/devloop-wait`.
- Every two consecutive polls without a result must rotate mode or record why no
  non-conflicting work exists.
- Every repeated compile-heavy proof shape gets a `devloop-velocity` check
  before the next launch.
- Every focus span over 15 minutes gets a mode check in `OPERATING-LOG.md`,
  even if the decision is to stay in the same mode.
- Every substantial loop gets a demo-radar entry or an explicit reason that no
  demo artifact is implicated.
- Every operator process correction gets a meta-audit entry or an immediate
  scaffold/tooling change.
- Every after-current operator directive gets recorded with
  `.agent/scripts/devloop-checkpoint --queue ...` and completed with
  `--queue-complete ...` when promoted into `ACTIVE-LOOP.md`.
- Do not keep coding through a contradiction. Rotate to Direction first, then
  re-enter Construction only after the slice contract still makes sense.
- Do not keep polishing an artifact after its proof claim is exhausted. Rotate
  to Direction or Velocity.

## Loop Shape

1. `devloop-status`
2. `devloop-review` when resuming, before broad work, or after scaffold edits
3. choose slice and append `devloop-start`
4. Direction -> Evidence until the first edit boundary is clear
5. Construction -> Proof serially around heavy builds, using `TACTICS.md` to avoid
   idle wait time
6. Artifact and demo radar when proof is strong enough to show value, or when an
   existing artifact needs caveat/refresh
7. append `devloop-checkpoint`
8. `devloop-sync` if current conductor files changed
9. write/update `devloop-handoff`
10. decide next slice from evidence

The conductor should not wait for a perfect plan. It should maintain enough
time and evidence awareness to pivot quickly when the current plan is no longer
the fastest route to a real capability artifact.

## Mode Switch Log Shorthand

Use this in `OPERATING-LOG.md` when useful:

```text
Focus: Direction -> Evidence
Trigger: first live query contradicted the assumed blocker
Decision: inspect ActivityWatch extraction before touching renderer
```

## External-Proof Campaigns

A campaign is a bounded goal whose terminal state is an externally legible
artifact — one a stranger with no repo context can read, believe, and
reproduce. Campaigns outrank open-ended substrate slices; enabler substrate
work is in scope only when the specific campaign artifact would be false or
fragile without it.

Current campaign sequence (operator direction; supersede only with recorded
evidence, never delete as duplicate):

1. sinex-recall v2 — multi-source (fs+git+shell+browser) dev-runtime
   reconstruction of one real work window with a committed side-by-side
   baseline arm (raw atuin + git log), stranger-readable README, one-command
   regeneration. Suggested window: the 2026-07-01 afternoon PR-train window
   (externally presentable; ground truth checkable against merged PRs).
2. Production restore — read-only verification plan, then the restore as an
   explicit operator-visible operation, then proof: prod sinexd healthy,
   74M events queryable, one recall query answered from prod data.

Campaign rules:

- **Capabilities may not be silos; demos may.** A demo is a derivative
  product; its packaging is a legitimate one-off. The facts it relies on
  must land as composable capability (query units/fields, shared view
  primitives, projections) the product keeps. Test: after the demo ships,
  can the next differently-shaped question about the same facts be answered
  by composition, without another script?
- **Cold-reader gate.** A campaign artifact reaches terminal state only
  after a fresh agent, given ONLY the artifact directory, can state what it
  proves, name its sample frame and caveats, and reproduce it.
- **Slice closure is not campaign closure.** Committing a bounded slice
  does not retire the campaign; it stays at the top of the priority order
  until its terminal state is recorded.
- **Operator-direction preservation.** Operator-sourced directives, queue
  entries, and backlog items are superseded with recorded rationale in the
  operating log, never deleted as duplicates during sync/cleanup.
