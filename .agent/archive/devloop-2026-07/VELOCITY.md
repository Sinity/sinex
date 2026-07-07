# Devloop Velocity Rubric

Velocity is part of the Sinex product loop. The loop should get faster over
time because every pass removes friction, narrows uncertainty, or improves the
substrate for future passes.

## Time Model

Track these timestamps for each meaningful loop:

- `t0 orient`: conductor state loaded, current aim selected.
- `t1 decision`: specific capability slice chosen.
- `t2 first evidence`: live source/code/runtime evidence gathered.
- `t3 edit start`: first implementation/config/docs edit.
- `t4 first proof`: first targeted verification or live artifact.
- `t5 artifact`: inspectable demo/report/log/handoff produced.
- `t6 reflect`: next slice and process fix recorded.

`PHASES.jsonl` is the generated machine-readable subset of these timings. It is
derived from `OPERATING-LOG.md`; do not edit it by hand. Run
`.agent/scripts/devloop-velocity` to see recent focus/phase transitions, largest
recent gaps, duplicate proof shapes, and recent build/test/check resource cost
in one view.

The useful metric is not just total elapsed time. Watch:

- orientation latency: `t1 - t0`
- evidence latency: `t2 - t1`
- implementation latency: `t4 - t3`
- artifact latency: `t5 - t4`
- reflection latency: `t6 - t5`

## Acceleration Rules

- If orientation takes too long, improve the current index or inbox packet.
- If evidence latency is high, add or fix a query, script, telemetry surface, or
  reusable report.
- If implementation latency is high because of builds, reduce compile fanout,
  fix stale scopes, or split non-build research from serialized verification.
- If a wait exceeds one minute, name the waited-on command/job, proof claim,
  next poll, and mode task. Waiting is loop time, not a pause.
- If `devloop-status` or `devloop-wait` shows a foreground proof lane, do not
  start another compile/test proof in the same checkout until that lane is
  polled, stopped, or proven non-conflicting.
- During waits, cycle modes frequently: Proof -> Velocity/Artifact/Evidence/
  Direction. The purpose is to keep accelerating the loop while preserving the
  pending proof's integrity.
- Track wrapper/bootstrap, queue time, compile time, test runtime, docs
  generation, and runtime startup as separate costs; otherwise the slowest
  part stays invisible and unactionable.
- If artifact latency is high, promote the artifact shape into a shared
  renderer/view primitive instead of hand-writing reports.
- If demo ideation is absent, review the demo portfolio (`bd list -l demo`) before coding more. The loop
  optimizes for rapid Sinex improvement through useful/impressive artifacts, so
  demo candidate generation is velocity work, not polish.
- If two focused proof reruns happen for the same slice, run
  `.agent/scripts/devloop-velocity` before launching the next proof. The
  report makes duplicate proof shapes, phase gaps, compile-dominated tests, and
  IO pressure visible enough to choose batching, source review, or live proof
  instead.
- Before intentionally rerunning a compile-heavy proof shape, run
  `.agent/scripts/devloop-velocity <test|check|build> ...` with the exact
  planned xtask command shape. If it already ran once recently, name what
  changed; if it ran twice, batch remaining edits or switch to live/artifact
  proof unless the claim truly widened.
- If reflection is skipped, the loop is leaking learning; append an operating
  log entry before switching tasks.
- If the next action is fuzzy, run `.agent/scripts/devloop-status` rather than
  relying on memory. Its warning threshold is intentionally low: 10 minutes
  without a fresh log or an active proof wait should force either a focus
  rotation or an explicit checkpoint.
- If xtask reports "Using existing xtask binary; local rebuild is currently
  broken", classify that as wrapper/bootstrap drag, not harmless noise. Fix the
  build break before claiming xtask/tooling behavior changed, or explicitly
  mark the proof as running on the fallback binary.

## Per-Loop Reflection Shape

Record:

- `speedup`: what became faster in this loop.
- `drag`: what still slowed progress.
- `next acceleration`: one concrete change that would make the next loop faster.
- `risk`: what shortcut might have hidden a correctness or provenance problem.
- `meta`: what the agent/process itself failed to catch early enough, and what
  tripwire now catches it.

## Meta-Audit Triggers

Run `.agent/scripts/devloop-meta` when:

- the operator corrects aim, priority, responsibility boundary, or process;
- a wait becomes passive;
- a claim surprises the operator or lacks evidence;
- the same tooling/resource friction appears twice;
- a "temporary" manual workaround is used more than once;
- a status report cannot answer "what is next and why?".

## Current Baseline

As of 2026-06-30, the main drag is not a single code problem. It is the
combination of heavy Rust compile cost, transient background work, duplicate
agent/MCP stacks, Sinex/Polylogue autostarts, and scattered scratch memory.
The first acceleration wins are therefore:

- keep the `.agent` startup packet small and current;
- prevent stale transient build/devloop scopes from consuming RAM/IO;
- keep demos thin over shared primitives rather than one-off reports;
- prefer exact targeted verification, with broad gates only at coherent phase
  boundaries;
- overlap heavy background verification with artifact drafting, adjacent-source
  inspection, log updates, and next-proof preparation;
- run `devloop-review` to catch TODO rot, inbox drift, duplicate agents, and
  surprise background work before it silently slows the loop;
- record time and friction in `OPERATING-LOG.md` so regression is visible.
- keep demo beads fresh enough that new, improved, stale, and retired demo
  artifacts are visible without replaying chat history.
- run `devloop-velocity` as the first response to repeated proof churn;
  do not keep manually polling and re-running tiny checks without a cost view.
- use `devloop-velocity` before the second copy of an exact proof command;
  the fastest proof is often an already-built live CLI command or demo artifact.
- use the foreground proof-lane section in `devloop-status`/`devloop-review` as
  the immediate guard against parallel foreground `xtask test` mistakes.
- use `xtask history overlap latest --command test` after any surprising slow
  focused test; if it reports overlapping invocations, treat the next meta action
  as a serialization/process fix, not as "the tests are just slow."
- surface stale xtask fallback state in `devloop-status` and `devloop-review`;
  otherwise process/tooling edits can appear to be verified while an older
  binary is actually running the loop.
