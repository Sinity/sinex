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

## Sinex Guardrails

- [ ] Existing shared abstractions were reused instead of adding local shims.
- [ ] Query/persistence paths still flow through the typed `sinex-*` layers rather than ad-hoc escape hatches.
- [ ] Provenance, replay, and identifier semantics remain honest.
- [ ] Schema or generated artifacts were updated if this change requires them.

## Follow-Ups

Anything that should become a separate issue after this lands.

## Checklist

- [ ] This branch is tied to an issue or an explicit source document/report.
- [ ] The PR body explains scope and non-goals clearly.
- [ ] Verification is recorded honestly.
- [ ] Docs / operational impact is called out if relevant.
- [ ] Remaining follow-up work is split out instead of hidden.
