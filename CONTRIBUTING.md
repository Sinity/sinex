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

## Acceptance-criteria honesty

The PR template's `## Acceptance Criteria Drift` section is not optional
when the linked issue carries an AC list. For each AC item, mark whether
this PR satisfies it as written, defers it to a follow-up issue, or
discovered the AC was misframed.

This requirement exists because of a recurring failure mode (caught in
the 2026-04-26 audit cycle): an issue with five AC items closes on a PR
that satisfies three structurally and silently drops the two
measurement-under-load items. Future readers — including future-you —
treat the issue as "done" and rebuild on top of a partial foundation.

Concrete shape:

```
- [ ] AC #1 ("Lag and parent fan-in observed under prod traffic") — ⏭ deferred to #561
- [ ] AC #2 ("Schemas registered for every emitter") — ✅ satisfied
- [ ] AC #3 ("Heavy-lane scenario asserts on lag bound") — ⏭ deferred to #561
```

If the issue has no AC list (research / design / cleanup tickets often
don't), say so explicitly: "no AC list — issue closes on the decision
recorded in this PR body."

The cost of this section is two minutes of writing. The cost of
skipping it has been entire audit cycles re-doing forensic work on
issues that "closed."

## Closure verification commands

When closing an issue (or merging the PR that closes it), the closing
comment must include the **commands a future reader can run themselves
to verify the claim**, not just an assertion that it landed.

This requirement exists because of a recurring failure mode (caught in
the 2026-05-11 audit cycle): closing comments described work that lived
only in a working tree and never reached `master`. The most severe case
was issue #1081 — the closing comment claimed "44K-line delete, all
legacy crate directories deleted" against zero actual commits. A second
case attributed issue #987 to "PRs #998–#1002" when PR #1000 in that
range had `state=CLOSED, mergedAt=null`. Both fabrications survived for
days because future agents trusted the closing comments without
re-verifying against `master`.

Verification commands you might include:

```bash
# File-deletion claim
git log --all --diff-filter=D -- <claimed-deleted-path>

# Commit SHA / PR landing claim
git show <sha> --stat
gh pr view <N> --json state,mergedAt --jq .

# Symbol existence / wiring claim
grep -rn "<symbol>" crate/ --include="*.rs"

# Behavior claim (test that exercises the AC)
xtask test -p <pkg> -E 'test(<name>)'
```

The commands embedded in a closing comment are not just documentation —
`xtask verify closure <N>` parses the closing comment / issue body and
actually runs each command, reporting per-command exit codes. Run it
locally before closing:

```bash
xtask verify closure 1081
```

The `.github/workflows/verify-closure.yml` workflow exposes the same
command on demand (`gh workflow run verify-closure.yml -f issue_number=N`)
so any reviewer or future reader can re-run the closure verification
without checking out the repo.

Two antipatterns to avoid:

- **"Closed by PRs #X–#Y" range claims** without verifying each PR in
  the range actually merged and contributed to the AC. If five PRs
  collectively address the AC list, name each PR's contribution
  individually. Range notation makes one closed-but-not-merged PR
  invisible.
- **"Status: done" comments listing work** that exists only in a working
  tree. If you're describing what *will* land, say so. If you're
  describing what *did* land, the verification commands prove it.

## Source-unit promotion gate (ingestors)

A new ingestor is not promotable until its
[`SourceUnitDescriptor`](crate/lib/sinex-primitives/docs/source_unit.md)
is filled in and the declared proof obligations pass. The descriptor is
the executable form of the contract — identity, emitted
`(source, event_type)` pairs, checkpoint family, privacy tier, runtime
shape, horizons, retention, proof obligations, occurrence identity.

Register with `register_source_unit!` in the ingestor crate's
`src/lib.rs`. See `crate/nodes/sinex-terminal-ingestor/src/lib.rs` for
the canonical example.

This is a hard gate, not a convention: a PR adding a new ingestor
without a descriptor should be sent back. The descriptor compiles
against typed fields, so a missing or malformed one fails at build
time.

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
