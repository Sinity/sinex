# CI Policy

GitHub-hosted workflows are manual-only in this repository. Local `xtask`
verification recorded in the PR is the default review gate unless a workflow is
deliberately invoked with `workflow_dispatch`.

## Required Local Checks Per PR Type

All PRs must pass the following before merge:

### Every PR

- **`xtask check --full`** — Compile + lint (clippy, rustfmt, forbidden patterns,
  AST-grep structural rules).
- **`xtask test`** — Default affected-package nextest loop for ordinary local
  iteration. Use the workspace gate below when the PR needs CI-parity breadth.
- **`xtask docs check`** — Generated docs drift detection (CLAUDE.md transclusions,
  schema bundle, command reference).
- **`xtask ci compat --base master`** — Schema-contract drift check against the
  default branch. Despite the command name, this is not an N-1 runtime
  compatibility gate.

### Broad / Phase-Boundary Gate

- **`xtask ci postgres -- xtask ci workspace`** — CI-parity local workspace lane.
  This applies schema, checks contract tables, runs dependency/lint validation,
  enforces workspace cleanliness, runs e2e tests first, then runs the rest of
  the workspace with e2e excluded.

### PRs Touching Database Schema

- **`xtask ci postgres -- xtask ci schema-only`** — Full schema bootstrap and
  apply cycle.
- **`xtask ci postgres -- xtask ci workspace`** — Run this broad gate before
  merge when schema edits affect runtime behavior or generated contracts.

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
