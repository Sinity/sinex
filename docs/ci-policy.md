# CI Policy

## Required Checks Per PR Type

All PRs must pass the following before merge:

### Every PR

- **`xtask check --full`** — Compile + lint (clippy, rustfmt, forbidden patterns,
  AST-grep structural rules). Equivalent to the `check (full)` CI lane.
- **`xtask test`** — Default nextest test loop: unit, integration, property-based,
  and scenario-tagged tests. Equivalent to the `test (postgres, workspace)`
  CI lane.
- **`xtask docs check`** — Generated docs drift detection (CLAUDE.md transclusions,
  schema bundle, command reference). Equivalent to the `docs (check)` CI lane.
- **`xtask ci compat --base master`** — Schema compatibility check against the
  default branch. Equivalent to the `schema (compat)` CI lane.

### PRs Touching Database Schema

- **`xtask ci postgres -- xtask ci schema-only`** — Full schema bootstrap and
  apply cycle. Equivalent to the `schema (bootstrap)` CI lane.
- **`xtask ci postgres -- xtask ci workspace`** — Postgres-backed workspace
  lane (schema apply, contract tables, dependency/lint validation, workspace
  cleanliness, package test surfaces).

### PRs Touching NixOS / Deployment

- **`xtask test vm --category smoke`** — NixOS VM smoke tests. Not in the
  default GitHub Actions gate; run manually before merge.

## Merge Criteria

1. All required CI lanes green.
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

| Lane | Trigger | Approximate Runtime |
|------|---------|-------------------|
| `check (full)` | Every push | ~3-5 min |
| `test (postgres, workspace)` | Every push | ~8-12 min |
| `docs (check)` | Every push | ~30 sec |
| `schema (compat)` | Every push | ~1 min |
| `schema (bootstrap)` | Schema changes | ~2 min |
| `test (heavy)` | Manual | ~15-30 min |
| `test (vm smoke)` | Manual | ~5-10 min |
| `test (vm integration)` | Manual | ~15-30 min |

## Skipping CI

Do not skip CI. If a lane fails, fix the root cause — do not bypass with
`--no-verify` or force-push to skip hooks. If a lane is genuinely flaky,
open an issue with the failure evidence and work around it transparently.
