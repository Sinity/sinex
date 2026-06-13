# CI Policy

Most GitHub-hosted workflows are manual-only in this repository. The exception
is the **compile gate** (`compile-gate.yml`), which runs automatically on every
`pull_request` and is a required status check — a non-compiling branch cannot
merge. All other workflows require deliberate `workflow_dispatch` invocation.

## Required GitHub Status Checks (branch protection)

These checks must be green before a PR can merge:

| Check | Workflow | Trigger |
|-------|----------|---------|
| `xtask check --full --all` | `compile-gate.yml` | every pull_request |
| CodeRabbit | external | every pull_request |
| GitGuardian | external | every pull_request |
| `xtask schema strict-diff` | `schema-strict-diff.yml` | PRs touching schema paths |

> **Branch protection note:** after adding a new required check, the repo owner
> must navigate to Settings → Branches → master branch protection rule and add
> the check's exact job name (`xtask check --full --all`) to the required status
> checks list.

## Warm-Cache False-Pass Risk (deletion / refactor PRs)

`xtask check` can false-pass locally when a warm `CARGO_TARGET_DIR` masks a
cross-crate break from a deleted symbol. The implementing agent in PR #1749
reported a passing `xtask check -p sinexd` while the worktree's code did not
compile — the agent's `CARGO_TARGET_DIR` pointed at the main checkout's warm
cache, not the worktree's actual compiled artifacts.

**Before merging any deletion or refactor PR:**
- Run `xtask check --full --all` with `CARGO_INCREMENTAL=0` from a clean or
  isolated target dir to rule out warm-cache false-passes.
- From a worktree agent, always override `CARGO_TARGET_DIR` to a
  worktree-dedicated path:
  ```bash
  cd <worktree> && nix develop --command env \
    CARGO_TARGET_DIR=/var/cache/sinex/sinity/wt-<tag>/target \
    CARGO_INCREMENTAL=0 \
    xtask check --full --all
  ```
  A result in 0.3s means the wrong tree was checked; a real check takes minutes.
- The CI compile gate (`compile-gate.yml`) always runs with `CARGO_INCREMENTAL=0`
  and a fresh checkout, so a CI green is authoritative even when local signal
  was unreliable.

## Required Local Checks Per PR Type

All PRs must pass the following local gates before merge (the CI compile gate
catches compile errors independently, but local verification should still run
before pushing to avoid unnecessary CI cycles):

### Every PR

- **`xtask check --full`** — Compile + lint (clippy, rustfmt, forbidden patterns,
  AST-grep structural rules).
- **`xtask test`** — Default affected-package nextest loop for ordinary local
  iteration. Use the workspace gate below when the PR needs CI-parity breadth.
- **`xtask docs check`** — Generated docs drift detection (CLAUDE.md transclusions,
  schema bundle, command reference).
- **`xtask schema strict-diff`** — Live schema drift check against the
  checkout-local dev stack.

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
- **Chaos / VM integration tests** (`xtask test vm --category integration`):
  Run manually before merging PRs that touch deployment topology, service
  restart behavior, or replay/cascade logic. Requires a working NixOS VM
  test environment.

## CI Lane Summary

| Lane | When To Run | Trigger | Approximate Runtime |
|------|-------------|---------|-------------------|
| `compile gate (check --full --all)` | **Required** — every PR | pull_request (auto) | ~5-15 min cold |
| `check (full)` | Local before merge; also the compile gate lane | local / pull_request | ~3-5 min warm |
| `test (postgres, workspace)` | Local broad/phase gate; manual workflow if requested | workflow_dispatch | ~8-12 min |
| `docs (check)` | Local before merge when generated surfaces may drift | local | ~30 sec |
| `schema (contract drift)` | PRs touching schema sources | pull_request (path-scoped) | ~2 min |
| `schema (bootstrap)` | Local for schema changes; manual workflow if requested | workflow_dispatch | ~2 min |
| `test (heavy)` | Local/manual for heavy-risk surfaces | local | ~15-30 min |
| `test (vm smoke)` | Local/manual for NixOS/deployment changes | local | ~5-10 min |
| `test (vm integration)` | Local/manual for restart/replay/cascade changes | local | ~15-30 min |

## Skipping CI

Do not skip CI. If a lane fails, fix the root cause — do not bypass with
`--no-verify` or force-push to skip hooks. If a lane is genuinely flaky,
open an issue with the failure evidence and work around it transparently.
