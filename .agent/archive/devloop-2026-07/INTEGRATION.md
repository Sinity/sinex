# Integration Plan

Generated: 2026-07-02T11:24:00+02:00

Purpose: integrate the current live dogfood/recovery branch into `master`
through broad, reviewable PR phases while object work continues on the same
checkout. This replaces the stale 349-commit branch plan; the current branch is
much smaller and should not be split into mechanical micro-PRs.

## Current State

- Branch: `feature/runtime/restore-dev-dogfood-catchup`
- Upstream configured locally: `master`
- Base for integration: `origin/master`
- Head: `a6998811f test: split remaining inline test bodies`
- Divergence: `0` behind / `46` ahead of `origin/master`
- Working tree: clean at integration refresh
- Open GitHub PRs for this integration train: #2218, #2219, #2220, #2221, #2222
- Live runtime proof at refresh:
  - dev `sinexd` job `2001018` running
  - runtime health: healthy / serving
  - raw-ingest DLQ: empty, nominal pressure

## Published Stack

Published 2026-07-02T11:27-11:49+02:00. All PRs are normal PRs, not drafts.

1. PR #2218: `feat(dev): restore dogfood source bindings and runtime targets (#2218)`
   - Head: `pr/sinex-devloop-phase/01-dev-dogfood-source-bindings`
   - Base: `master`
   - Commits: `423262a19`..`5506ebc9a`
   - Pre-push proof: `xtask check --changed-strict origin/master`
     checked `sinexd`, `xtask`; passed in 136.063s.
   - Latest visible checks at publish: GitGuardian pass; CodeRabbit pass.

2. PR #2219: `fix(runtime): harden event transport payload admission (#2219)`
   - Head: `pr/sinex-devloop-phase/02-event-transport-admission`
   - Base: `pr/sinex-devloop-phase/01-dev-dogfood-source-bindings`
   - Commits over base: `87eb2e9ba`..`4456ec0d5`
   - Pre-push proof: `xtask check --changed-strict origin/master`
     checked `sinex-db`, `sinexd`, `xtask`; passed in 84.198s.
   - Latest visible checks at publish: GitGuardian pass; CodeRabbit skipped
     because the base is another stack branch.

3. PR #2220: `fix(runtime): recover checkpoints and oversized runtime writes (#2220)`
   - Head: `pr/sinex-devloop-phase/03-runtime-recovery-paths`
   - Base: `pr/sinex-devloop-phase/02-event-transport-admission`
   - Commits over base: `57191dd1c`..`8b5337f62`
   - Pre-push proof: `xtask check --changed-strict origin/master`
     checked `sinex-db`, `sinexd`, `xtask`; passed in 87.176s.
   - Latest visible checks at publish: GitGuardian pass; CodeRabbit skipped
     because the base is another stack branch.

4. PR #2221: `fix(automata): narrow confirmations and script DLQ cleanup (#2221)`
   - Head: `pr/sinex-devloop-phase/04-automata-confirmation-dlq`
   - Base: `pr/sinex-devloop-phase/03-runtime-recovery-paths`
   - Commits over base: `de54386c3`..`359fd8758`
   - Pre-push proof: `xtask check --changed-strict origin/master`
     checked `sinex-db`, `sinex-primitives`, `sinexctl`, `sinexd`,
     `xtask`; passed in 189.314s.
   - Latest visible checks at publish: GitGuardian pass; CodeRabbit skipped
     because the base is another stack branch.

5. PR #2222: `chore(tests): finish runtime presence and inline cleanup (#2222)`
   - Head: `pr/sinex-devloop-phase/05-runtime-presence-test-cleanup`
   - Base: `pr/sinex-devloop-phase/04-automata-confirmation-dlq`
   - Commits over base: `ef1b685e9`..`a6998811f`
   - Pre-push proof: `xtask check --changed-strict origin/master`
     checked `sinex-db`, `sinex-primitives`, `sinex-schema`, `sinexctl`,
     `sinexd`, `xtask`, `xtask-macros`; passed in 201.408s.
   - Additional proof: `extract_inline_tests.py --include-test-directories
     --json` reported zero candidates/skips.
   - Latest visible checks at publish: GitGuardian pass; CodeRabbit skipped
     because the base is another stack branch.

## Publish Discipline

- Normal PRs only. Do not create draft PRs and do not mention draft flow in
  generated process prompts.
- Prefer broad coherent phases over one-commit PRs.
- Publish bottom-up from `origin/master`.
- Keep branch names phase-shaped, not inherited from mechanical commit
  subjects.
- Record exact verification in each PR body. The repo pre-push hook runs
  `xtask check --changed-strict origin/master`; if it fails, fix the tooling or
  code rather than bypassing it.
- Continue local dogfood work on the long-lived branch after publishing; update
  this file as phases merge or are superseded.

## Target PR Train For Current Branch

1. `feat(dev): restore dogfood source bindings and runtime targets`
   - Commits: 1-8 (`423262a19`..`5506ebc9a`)
   - Theme: dev source bindings launch by default, source-binding filters,
     ingest pressure stabilization, self-observation material rotation, current
     rustfmt spillover, runtime target helper restoration, and early xtask test
     extraction needed by the infra changes.
   - Suggested branch:
     `pr/sinex-devloop-phase/01-dev-dogfood-source-bindings`
   - Verification focus: `xtask check --changed-strict origin/master`, plus
     live runtime target/gateway proof if the PR body claims dogfood runtime
     usability.

2. `fix(runtime): harden event transport payload admission`
   - Commits: 9-17 (`87eb2e9ba`..`4456ec0d5`)
   - Theme: reject non-v7 derived parents before DB, keep entity IDs out of
     provenance, accept path-only static directory records, suppress duplicate
     equivalence keys without DLQ, split/attribute oversized NATS batches, and
     guard frame/direct publishes below transport limits.
   - Suggested branch:
     `pr/sinex-devloop-phase/02-event-transport-admission`
   - Verification focus: event-engine/runtime package checks and DLQ duplicate
     suppression evidence.

3. `fix(runtime): recover checkpoints and oversized runtime writes`
   - Commits: 18-28 (`57191dd1c`..`8b5337f62`)
   - Theme: checkpoint file fallback, partial adapter cursor merging, remaining
     control publish guards, long-lived runtime-job preservation, oversized
     intent spooling, checkpoint write bounds, restored self-observation
     reconciliation, and warning-rate reduction.
   - Suggested branch:
     `pr/sinex-devloop-phase/03-runtime-recovery-paths`
   - Verification focus: runtime/checkpoint package checks and live daemon
     stability under dev workload.

4. `fix(automata): narrow confirmation consumers and DLQ operations`
   - Commits: 29-38 (`de54386c3`..`359fd8758`)
   - Theme: narrow confirmed-event consumers for automata, persist
     event-engine heartbeats, read batch latency, bound analytics fan-in,
     stabilize checkpoint consumers, clean migrated peers, add scriptable DLQ
     cleanup, and move the first large inline-test wave.
   - Suggested branch:
     `pr/sinex-devloop-phase/04-automata-confirmation-dlq`
   - Verification focus: automata/runtime/event-engine checks and `sinexctl ops
     dlq` scriptability proof.

5. `chore(tests): finish runtime presence and inline-test cleanup`
   - Commits: 39-46 (`ef1b685e9`..`a6998811f`)
   - Theme: source-status count bounds, bootstrap-free xtask help probes,
     hosted source binding restart, active runtime module projection, premature
     self-observation DLQ fix, canonical split-module naming, and bulk
     extraction of the remaining literal inline test bodies.
   - Suggested branch:
     `pr/sinex-devloop-phase/05-runtime-presence-test-cleanup`
   - Verification focus: extractor inventory zero candidates/skips, runtime
     modules present, raw-ingest DLQ empty, and focused package check job
     `2001020` or a fresh equivalent.

## Immediate Next Action

1. Watch PR #2218 substantive review/check state first because it is the bottom
   stack PR and the only PR based directly on `master`.
2. Merge bottom-up only after checks/review are acceptable: #2218 -> #2219 ->
   #2220 -> #2221 -> #2222.
3. After a PR merges, retarget or refresh the next stacked PR if GitHub does not
   update it automatically.
4. After the stack lands, prune the five `pr/sinex-devloop-phase/*` branches
   and mark this integration packet closed.
5. Continue object work on `feature/runtime/restore-dev-dogfood-catchup` while
   watching PR feedback; do not let the integration lane block dogfood runtime
   work unless a substantive review/check failure appears.
