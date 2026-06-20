# CI Policy

GitHub-hosted workflows are manual-only in this repository. Local `xtask`
verification recorded in the PR is the default review gate unless a workflow is
deliberately invoked with `workflow_dispatch`.

## Required Local Checks Per PR Type

Sinex's default PR gate is intentionally affected and impact-shaped. Broader
workspace passes are phase-boundary checks, not a tax on every small PR.

### Every PR

- **`xtask check --changed-strict origin/master --allow-contended-host`** —
  Required for Rust/API changes. This is the pre-push drift guard and records
  the changed-file to affected-package result.
- **Focused `xtask test ...` evidence** — Required when behavior changes. Use
  exact filters or affected-package tests that exercise the changed surface, and
  record the command in the PR body.
- **Generated-surface checks only when touched** — Run `xtask docs
  command-reference --check`, `xtask docs schema-bundle --check`, or
  `xtask docs check` when the PR changes command/schema/docs-generation
  surfaces.
- **`xtask schema strict-diff` only when schema/contracts changed** — Live
  schema drift belongs to schema or payload-contract PRs, not ordinary test or
  runtime refactors.

### Broad / Phase-Boundary Gate

- **`xtask check --full`** — Broad local compile/lint/forbidden-pattern gate.
- **`xtask test --impact-mode=off --all`** — Deliberate full local package test
  pass when a PR needs phase-boundary breadth instead of the default affected
  loop.

### PRs Touching Database Schema

- **`xtask schema strict-diff`** — Verify live schema drift after applying schema
  changes.
- **`xtask docs schema-bundle --check`** — Verify the checked-in contract bundle.
- **`xtask check --full`** and **`xtask test --impact-mode=off --all`** — Run the
  broad local gates before merge when schema edits affect runtime behavior or
  generated contracts.

### PRs Touching NixOS / Deployment

- **`xtask test vm --category smoke`** — NixOS VM smoke tests. Not in the
  default GitHub Actions gate; run manually before merge.

## Merge Criteria

1. All required local gates pass, or the equivalent manual GitHub workflow is
   green when deliberately invoked.
2. PR template filled out: Summary, Problem, Solution, Verification.
3. Acceptance Criteria Drift section completed (mark each AC as satisfied,
   deferred, or misframed).
4. At least one human review approved (for solo development: self-review
   with a 24-hour cooling-off period before merge).
5. No unresolved automated review findings (CodeRabbit, Copilot, proof packs).
   False positives must be explicitly noted in a PR comment.
6. Branch is up to date with `master` (rebased, not merged).

## Heavy / Chaos Suite Policy

- **Heavy tests** (`xtask test --heavy`): Run manually before merging PRs that
  touch COPY protocol, batch insert routing, CAS store/lookup, or JetStream
  consumer logic. Heavy tests use larger datasets and stress resource limits.
  Not run in the default CI gate due to runtime and memory constraints.
- **Trybuild compile-fail tests**: Run with `xtask test --debug --heavy -p
  <package> -E '<filter>'` when a PR edits trybuild runners or stderr fixtures.
  The debug profile serializes the selected compiler tests and avoids the
  default nextest timeout failure that happens when several cold trybuild nodes
  compile the same dependency graph concurrently.
- **Chaos / VM integration tests** (`xtask test vm --category integration`):
  Run manually before merging PRs that touch deployment topology, service
  restart behavior, or replay/cascade logic. Requires a working NixOS VM
  test environment.

## CI Lane Summary

| Lane | When To Run | Approximate Runtime |
|------|---------|-------------------|
| `check (full)` | Local before merge; manual workflow if requested | ~3-5 min |
| `test (postgres, workspace)` | Local broad/phase gate; manual workflow if requested | ~8-12 min |
| `docs (check)` | Local before merge when generated surfaces may drift | ~30 sec |
| `schema (contract drift)` | Local before merge when schema/payload contracts change | ~1 min |
| `schema (bootstrap)` | Local for schema changes; manual workflow if requested | ~2 min |
| `test (heavy)` | Local/manual for heavy-risk surfaces | ~15-30 min |
| `test (vm smoke)` | Local/manual for NixOS/deployment changes | ~5-10 min |
| `test (vm integration)` | Local/manual for restart/replay/cascade changes | ~15-30 min |

## Skipping CI

Do not skip CI. If a lane fails, fix the root cause — do not bypass with
`--no-verify` or force-push to skip hooks. If a lane is genuinely flaky,
open an issue with the failure evidence and work around it transparently.
