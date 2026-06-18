## Intent

What is this PR trying to achieve?

## Source

- Closes:
- Related:
- Derived from doc / report / note:

## Scope

What is intentionally in scope here?

## Why This Shape

Why is this the right implementation or contract shape instead of plausible alternatives?

## Non-Goals / Deferred Work

What is explicitly not being solved here?

## Verification

List the checks you ran and the exact commands when useful. GitHub-hosted
Actions are manual-only for this repository's no-spend posture, so local
verification recorded here is the default review gate unless a workflow is
explicitly invoked.

- [ ] `xtask check --changed-strict origin/master` (drift guard — required for any PR
      touching Rust source unless `WIP:` in the title; auto-runs via the
      `.githooks/pre-push` hook installed by the devshell)

```bash
# commands here
```

## Acceptance Criteria Drift

For each acceptance-criterion checkbox in the linked issue, mark whether
this PR satisfies it as written, defers it to a follow-up issue, or
discovered it was misframed. Silent AC dropping is the failure mode
this section exists to prevent (see the 2026-04-26 audit cycle —
several "closed" issues had landed PRs that satisfied only the easy
ACs without recording the deferrals).

```
- [ ] AC #1 ("...") — ✅ satisfied / ⏭ deferred to #N / ❎ misframed (see body)
- [ ] AC #2 ("...") — ...
```

If the issue has no AC list (e.g. a research / design ticket), state
that explicitly: "no AC list — issue closes on producing the decision
recorded in this PR body."

## Impact

- Schema / database:
- Deployment / operations:
- Docs / canon:
- Security / privacy:

## Output Kind

If this PR adds or changes a durable output, operator-visible view, generated
artifact, proposal/judgment path, operation record, projection row, or event
payload, name its `OutputKind` from `sinex_primitives::output_kind` and link the
registry/doc entry. A new canonical event must explain why the output is not a
projection row, artifact, proposal, judgment, operation record, or ephemeral
view.

- OutputKind:
- Registry/doc reference:

## Sinex Guardrails

- [ ] Existing shared abstractions were reused instead of adding local shims.
- [ ] Query/persistence paths still flow through the typed `sinex-*` layers rather than ad-hoc escape hatches.
- [ ] Provenance, replay, and identifier semantics remain honest.
- [ ] New output-producing boundaries declare or reference their `OutputKind`.
- [ ] Schema or generated artifacts were updated if this change requires them.

## Follow-Ups

Anything that should become a separate issue after this lands.

## Checklist

- [ ] This branch is tied to an issue or an explicit source document/report.
- [ ] The PR body explains scope and non-goals clearly.
- [ ] Verification is recorded honestly.
- [ ] Docs / operational impact is called out if relevant.
- [ ] Remaining follow-up work is split out instead of hidden.
