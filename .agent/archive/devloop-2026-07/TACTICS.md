# Devloop Tactics

This file is the practical loop discipline for moving fast without losing proof.

## Async Verification Rule

When a build/test/check can run without blocking the next decision, run it in
the background and immediately switch to useful foreground work.

Preferred Sinex pattern:

```bash
xtask test -p <pkg> -E 'test(name)' --bg
xtask check --fmt --bg
xtask jobs active
xtask jobs output <id>
```

Do not sit idle watching compilation unless the result is required for the next
edit. Poll at natural boundaries: after a small research pass, after writing an
artifact section, after staging a follow-up patch, or before claiming proof.

## While Heavy Jobs Run

First, if the command may take more than one minute, record the wait explicitly:

```bash
.agent/scripts/devloop-wait "<job-or-command>" "<proof-claim>" "<poll-in>" "<mode-task>"
```

`devloop-wait` records the active foreground proof-lane snapshot. If it shows an
existing `xtask`/`cargo`/`nextest`/`rustc` lane, do not launch another proof
against the same checkout. Poll, stop the stale lane, or switch to non-conflicting
Artifact/Evidence/Direction work.

Then rotate mode deliberately. The default is not "keep waiting"; it is "shift
to the highest-value non-conflicting aspect of the loop."

Pick one mode:

- `Velocity`: analyze wrapper/bootstrap time, queueing, memory/IO, duplicate
  services, missing scripts, bad observability, or compile fanout.
- `Artifact`: update `/realm/inbox/demos_sinex`, command trails, caveats,
  README/manifest, screenshots, or rerun instructions. Treat
  `/realm/inbox/project-artifacts` and `/realm/inbox/project-devloops` as
  archive/input shelves unless intentionally mining old packets.
- `Evidence`: inspect adjacent source/runtime/history, issue context, or
  telemetry that will inform the next edit if the proof fails.
- `Direction`: reprioritize the next slice, update `ACTIVE-LOOP.md`, or write
  a handoff/decision note if the current slice is likely to close.

Useful actions:

- update `OPERATING-LOG.md` with what is being verified and expected duration;
- write the demo README/artifact skeleton with explicit TODO proof slots;
- inspect related call sites and remove one more silo/duplicate path;
- prepare the next narrow verification command;
- read the relevant scratch/research note and extract only actionable findings;
- update `/realm/inbox/demos_sinex` manifest/README if an artifact shape changed;
- check host/process pressure with `.agent/scripts/devloop-status`;
- run `.agent/scripts/devloop-velocity` if this is a repeated proof
  rerun or the current test is known to be compile-dominated;
- draft handoff while details are fresh;
- record an after-current directive with
  `.agent/scripts/devloop-checkpoint --queue "<title>" "<directive>" "<trigger>"`
  instead of relying on chat memory;
- open/code-search the next likely edit boundary, but do not start a conflicting
  heavy build.

If no job id appears during the first bounded poll, classify that as
wrapper/bootstrap latency rather than test/build runtime. Use another light
task or inspect host pressure before retrying.

If xtask says it is using an existing binary because the local xtask rebuild is
broken, treat the rest of the command as potentially stale for tooling/process
claims. Product code can still be checked with the fallback binary when the
surface is unaffected, but xtask behavior changes need the rebuild failure fixed
first.

## Serial Heavy Work, Parallel Light Work

Parallelize:

- file reads, searches, issue/doc lookups, scratch synthesis;
- non-overlapping design/research subagents;
- artifact drafting and verification-log writing.

Serialize:

- full Rust workspace builds;
- `xtask test` invocations in one checkout, even for different packages,
  unless they are explicitly isolated; they share checkout-local schema
  bootstrap, cargo package/build locks, target artifacts, and history capture;
- tests that compile the same large crates or share the same target dir;
- foreground proof lanes shown by `.agent/scripts/devloop-status`;
- live Sinex daemon bringup;
- schema/database work against the same dev DB;
- broad `xtask check --full` / `xtask test --all` gates.

If two focused test proofs are needed, run them serially or combine them into
one xtask invocation when the command surface supports it. Parallelize the
surrounding source review/artifact work instead.

The goal is not fewer checks. The goal is fewer idle minutes, fewer duplicate
expensive checks, and less accidental schema/build contention.

## Proof Ladder

Use the cheapest proof that answers the current question, then climb only when
the claim widens.

Before rerunning the same compile-heavy proof shape, ask for its recent cost:

```bash
.agent/scripts/devloop-velocity test -p sinexctl -E 'test(name)'
.agent/scripts/devloop-velocity check -p sinexctl
.agent/scripts/devloop-velocity build -p sinexctl
```

If the exact shape already ran recently, the default next action is not another
rerun. First decide whether the code changed, the claim changed, or an
already-built live command/demo artifact proves the operator-facing behavior.

1. Source review / exact search: proves shape, call sites, removed silos.
2. Unit or parser test: proves narrow semantics.
3. Package test: proves integration within a crate.
4. CLI command against dev store: proves operator-facing behavior.
5. Demo artifact on live/real data: proves capability value.
6. Broad gate: proves phase readiness, not every tiny edit.

## Greedy Batch Default

Batch toward bead closure by default. The normal loop is:

1. audit the bead acceptance criteria;
2. gather evidence for all remaining criteria;
3. edit the coherent shared substrate in one batch;
4. run focused proof once for the batch;
5. generate one live/demo artifact that exercises the whole claim;
6. publish one PR for the complete bead or coherent phase.

The target branch should be as wide as the bead's natural implementation shape.
If a bead is large, first try to identify the biggest coherent phase that shares
code paths, proof commands, and demo value. Only then decide whether a split is
actually warranted. Do not publish at the first green helper test when the next
acceptance criterion is still in the same subsystem and can be handled with the
same setup.

Avoid turning each small helper, renderer field, or artifact refresh into its
own PR. A small PR is appropriate only when it closes a named bead/phase,
unblocks other active work, isolates genuine risk, or keeps a truly large bead
reviewable. Otherwise, keep working on the same branch until the acceptance
matrix is meaningful.

When tempted to publish a partial slice, ask:

- Would this PR let the bead close, or would it just make the next agent read
  another PR to understand the same feature?
- Can the remaining acceptance criteria be implemented and verified with the
  same test/live-artifact pass?
- Is the split about risk/reviewability, or just because the current diff is
  already green?

If the answer is "already green," keep batching.

If proof churn exceeds the manifest shape, widen the manifest before rerunning:
collect adjacent fixes, docs, artifact refreshes, and Beads updates, then run
one focused proof family. Broad gates remain commit-boundary checks, not
per-substep reflexes.

## Reassessment Triggers

During a long compile/test, reassess instead of waiting if:

- the foreground task finishes and no useful next action is queued;
- memory or IO PSI rises;
- another heavy process appears;
- the build has exceeded the expected budget;
- the expected proof no longer matches the claim.

Then choose: continue waiting, stop stale work, narrow verification, draft
artifact, or pivot.

## Tactical Anti-Patterns

- Waiting silently for a long job without updating the log.
- Running a second broad check because the first one feels slow.
- Starting a second focused `xtask test` through a parallel tool call before
  checking the foreground proof-lane snapshot.
- Treating different `xtask test -p <pkg>` commands as independent just because
  their filters/packages differ.
- Using `--allow-contended-host` before identifying the pressure owner.
- Treating test runtime as dead time instead of artifact/research time.
- Rerunning the same tiny focused test after every micro-adjustment when the
  last run was compile-dominated.
- Ignoring the xtask fallback-binary warning and then trusting the run as proof
  of an xtask/process improvement.
- Writing a one-off demo report while a shared renderer/view primitive is
  obviously the reusable substrate.
- Broadening scope while a verification result that may invalidate the edit is
  still pending.

## Default Loop Cadence

0. Run `.agent/scripts/devloop-review` when resuming or before broad work.
1. Run `.agent/scripts/devloop-status` at natural boundaries: resume, before a
   second proof, after a surprising result, or after 10 minutes without a log
   entry. Treat a warning as a required focus/checkpoint decision, not as noise.
2. Start heavy job only after stating what it proves.
3. Immediately do a light task from "While Heavy Jobs Run."
4. Poll once.
5. If still running, do a second light task or checkpoint.
6. Poll again.
7. If still running and pressure is non-trivial, inspect pressure ownership.
8. Use the result to decide continue, fix, artifact, or pivot.
9. Run `.agent/scripts/devloop-sync` after updating current conductor files.
