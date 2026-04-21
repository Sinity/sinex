# Contributing

## Development Environment

Work inside the project devShell.

```bash
cd /realm/project/sinex
direnv allow   # one-time setup; afterward the devShell loads automatically on cd
```

If you are not using `direnv`, enter the same environment manually:

```bash
nix develop
```

## Workflow Surface

`xtask` is the default automation entrypoint for local development. Use:

```bash
xtask --help
xtask <command> --help
xtask --list-commands --json
```

Use raw `cargo` only for low-level Rust workflows that are not already exposed
through `xtask`. Infrastructure orchestration, status, verification, docs, and
agent-facing helpers should go through `xtask`.

`xtask` is a development-plane tool, not the production control plane. Live
runtime operation belongs to `sinexctl`, while host activation proof belongs to
NixOS activation checks and VM tests. Keep these responsibilities explicit; see
[`docs/architecture/runtime-target-boundaries.md`](docs/architecture/runtime-target-boundaries.md).

## Planning, Issues, and Source Documents

Large or pre-planned work should not live only in scratch notes or chat history.
Use a GitHub issue or an explicit source document/report before implementation
starts.

Use the issue templates intentionally:

- `Feature or Change` for concrete implementation slices with a defined outcome
- `Cleanup or Refactor` for simplification, unification, or removal of awkward
  parallel paths
- `Research or Decision` for architectural questions, comparisons, or
  recommendation work before implementation
- `Bug or Regression` for wrong or regressed behavior with a repro and severity

Treat scratch notes as input material, not as the project-facing planning
surface, when the work is:

- cross-cutting across crates/services/docs
- likely to span multiple sessions or PRs
- architectural enough that alternatives and non-goals need to stay explicit

The templates imply a workflow that should be followed explicitly:

1. Pick the right issue type.
2. Record scope, invariants, non-goals, and verification up front.
3. If the work starts as investigation, use `Research or Decision` first, then
   spin out implementation issues after the shape is settled.
4. When opening a PR, link it to an issue or cite the source document/report.
5. Split deferred work into follow-up issues instead of burying it in PR text or
   scratch notes.

In practice, that means:

- a cross-cutting redesign should usually begin as an issue before code changes
- a PR should not be the first place where the intended shape, constraints, and
  verification story become visible
- “Derived from doc / report / note” in the PR template is not filler; use it to
  preserve provenance when the branch is driven by a scratch note, audit, or
  design memo

The PR template also implies these review norms:

- state the problem, not just the patch
- explain why this shape won over plausible alternatives
- record exact verification honestly
- call out impact on schema, operations, docs, and security/privacy when relevant
- turn real deferred work into explicit follow-up issues

## Generated Agent Docs

`AGENTS.md` is generated output. Do not edit it directly.

When you change `CLAUDE.md` or any transcluded include, regenerate the local
agent surface:

```bash
xtask docs agents
```

The devShell also regenerates `AGENTS.md` on entry. The file is gitignored and
should be treated as a checkout-local artifact.

The generated xtask docs surfaces can be refreshed and verified with:

```bash
xtask docs sync
xtask docs check
```

## Verification Baseline

The canonical test/verification matrix lives in [TESTING.md](TESTING.md). Keep
workflow/entrypoint guidance here, and keep concrete verification commands there
so the same command sets are not maintained twice.
