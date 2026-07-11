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

### Pre-push drift guard

The repo carries a pre-push hook at `.githooks/pre-push` that runs scoped
drift guards against `origin/master` before allowing a push:

- `xtask docs schema-bundle --check` when payload/schema/bundle paths changed
- `xtask check --changed-strict` when Rust source paths changed

The schema-bundle guard catches stale checked-in payload schemas. The
changed-strict guard catches the class of regression where a PR's diff compiles
in isolation but references APIs that changed on master (the failure mode
behind PR #1268 and several others).

The devshell auto-installs this hook on first entry. To install manually:

```bash
git config core.hooksPath .githooks
```

Emergency bypass (force-push during recovery, etc.):

```bash
SINEX_SKIP_DRIFT_GUARD=1 git push ...
```

Document each bypass in the PR body.

### I/O pressure during development

Heavy compilation is itself the dominant source of build-time I/O: a full
`xtask check --lint` over the ~464K-LOC workspace writes a large volume of
rustc/clippy artifacts, and with incremental builds enabled and sccache disabled
in the devshell that artifact churn — not the running stack — is what drives host
I/O during a build. The live sinex stack adds a baseline (self-telemetry) on top,
which can compound host pressure (#1556), but it is a secondary contributor;
measure before blaming it.

Mitigations already applied:
- `test-threads = 12` in `.config/nextest.toml` (down from 18; cuts rustc fan-out I/O)
- `CARGO_TARGET_DIR` is routed to `/cache/sinex/<checkout>/...` via the devShell
  (NVMe-backed), keeping compilation output off the main filesystem.

### Worktree isolation and CARGO_TARGET_DIR

When a worktree agent inherits the orchestrator's devshell environment, its
`CARGO_TARGET_DIR` points at the **main checkout's** warm build artifacts rather
than the worktree's own cache. Without a guard, `xtask check` in the worktree
compiles against those stale artifacts and false-passes in under a second — exactly
the mechanism behind #1749 merging non-compiling code.

xtask now self-corrects this automatically:
- `workspace_target_dir_for` detects a `/var/cache/sinex/<user>/<HASH>/...` path
  whose `<HASH>` does not match the active workspace (workspace-hash mismatch).
- On mismatch it prints a one-line `[xtask] WARNING: CARGO_TARGET_DIR=...` to
  stderr and silently overrides the value to the worktree-correct target dir.
- The corrected value is explicitly exported to every cargo subprocess via
  `apply_cargo_env_policy_std/tokio`, so it wins over the raw inherited env.
- Arbitrary user-set paths that are neither a `/var/cache/sinex/<hash>` shape
  nor inside another checkout remain respected verbatim.

If you see the WARNING, it confirms xtask caught and corrected the leak. A real
compilation against the worktree tree takes minutes, not 0.3 s. If a check
returns in under a second claiming success from a worktree, the env fix may not
have fired — verify with:

```bash
CARGO_TARGET_DIR=/var/cache/sinex/sinity/SOMEOTHERHASH000/target xtask check -p xtask
# Should print the WARNING and then do a real compile.
```

When a check/test run is slow on a contended host:
- xtask reports host pressure as an advisory signal; it does not refuse to
  start checks or tests because of live PSI alone. Use
  `xtask analytics pressure --top-io` and
  `xtask analytics pressure --top-swap` to attribute pressure before changing
  workload shape.
- Clippy over the whole workspace can exceed the default 600s cargo timeout under
  load. The symptom is `cargo timed out after 600s`, not a code error — raise the
  ceiling with `SINEX_CARGO_TIMEOUT=1800` rather than assuming lock contention.
  (Empirically a full `check --lint` completes around io.full avg10 ~50% with the
  daemon still running; the timeout, not the daemon, is the usual blocker.)

Stopping the live daemon is a last resort, worthwhile only once you have measured
it as the contributor. On `sinnix-prime` it is a **system** service:

```bash
sudo systemctl stop sinexd     # free the daemon's baseline I/O
sudo systemctl start sinexd    # restart afterwards
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
[`xtask/docs/runtime-target-boundaries.md`](xtask/docs/runtime-target-boundaries.md).

## Planning, Beads, and Source Documents

> **GitHub Issues are retired as of 2026-07-10.** Beads (`bd`) is the sole
> durable task substrate for this repo. GitHub issue forms and the automatic
> issue-closure workflow have been removed; `.github/issue-operating-model.md`
> is only a superseded pointer. `bd prime` gives current workflow context;
> `bd create --type <task|bug|epic|...>` replaces "open an issue."

Large or pre-planned work should not live only in scratch notes or chat history.
Create a bead (`bd create`) or cite an explicit source document/report before
implementation starts.

Target-vision-derived claims that may steer architecture, scope, naming, or
verification obligations should also be checked against
[`target-vision-claim-ledger.md`](.github/target-vision-claim-ledger.md).
The ledger records whether a claim is raw, tracked, implemented, verified,
semantic debt, superseded, or rejected. Do not copy full target-vision prose
into a bead; summarize the claim, link the source, and keep beads as the
implementation authority.

Pick the right bead `issue_type` intentionally — mirrors the retired issue
kinds:

- `task` / `feature` for concrete implementation slices with a defined outcome
- `task` (cleanup-scoped) for simplification, unification, or removal of
  awkward parallel paths
- `task` (research-scoped, `design` field carries the decision) for
  architectural questions, comparisons, or recommendation work before
  implementation
- `bug` for wrong or regressed behavior with a repro and severity
- `epic` for campaigns/umbrellas spanning multiple beads

Treat scratch notes as input material, not as the project-facing planning
surface, when the work is:

- cross-cutting across crates/services/docs
- likely to span multiple sessions or PRs
- architectural enough that alternatives and non-goals need to stay explicit

The workflow that should be followed explicitly:

1. Pick the right bead type.
2. Record scope, invariants, non-goals, and verification (`description`,
   `design`, `acceptance` fields) up front.
3. If the work starts as investigation, record the decision in `design` once
   settled, then spin out implementation beads (`depends_on`/`parent-child`)
   after the shape is settled.
4. When opening a PR, cite the bead id or the source document/report.
5. Split deferred work into follow-up beads instead of burying it in PR text or
   scratch notes.

In practice, that means:

- a cross-cutting redesign should usually begin as a bead before code changes
- a PR should not be the first place where the intended shape, constraints, and
  verification story become visible
- "Derived from doc / report / note" in the PR template is not filler; use it to
  preserve provenance when the branch is driven by a scratch note, audit, or
  design memo

The PR template also implies these review norms:

- state the problem, not just the patch
- explain why this shape won over plausible alternatives
- record exact verification honestly
- call out impact on schema, operations, docs, and security/privacy when relevant
- turn real deferred work into explicit follow-up beads

## Acceptance-criteria honesty

The PR template's `## Acceptance Criteria Drift` section is not optional
when the linked bead carries an `acceptance` field. For each AC item, mark
whether this PR satisfies it as written, defers it to a follow-up bead, or
discovered the AC was misframed.

This requirement exists because of a recurring failure mode (caught in
the 2026-04-26 audit cycle, back when this was GitHub-issue-native): an
issue with five AC items closed on a PR that satisfied three structurally
and silently dropped the two measurement-under-load items. Future readers
— including future-you — treat the item as "done" and rebuild on top of a
partial foundation. The same failure mode applies equally to beads.

Concrete shape:

```
- [ ] AC #1 ("Lag and parent fan-in observed under prod traffic") — ⏭ deferred to sinex-abcd
- [ ] AC #2 ("Schemas registered for every emitter") — ✅ satisfied
- [ ] AC #3 ("Heavy-lane scenario asserts on lag bound") — ⏭ deferred to sinex-abcd
```

If the bead has no `acceptance` field (research / design / cleanup tickets
often don't), say so explicitly: "no AC list — bead closes on the decision
recorded in this PR body."

The cost of this section is two minutes of writing. The cost of
skipping it has been entire audit cycles re-doing forensic work on
tickets that "closed."

## Closure verification commands

When closing a bead (`bd close <id> --reason "..."`, or merging the PR that
closes it), the close reason must include the
**commands a future reader can run themselves to verify the claim**, not
just an assertion that it landed.

This requirement exists because of a recurring failure mode (caught in
the 2026-05-11 audit cycle, back when this was GitHub-issue-native):
closing comments described work that lived only in a working tree and
never reached `master`. The most severe case was issue #1081 — the
closing comment claimed "44K-line delete, all legacy crate directories
deleted" against zero actual commits. A second case attributed issue #987
to "PRs #998–#1002" when PR #1000 in that range had `state=CLOSED,
mergedAt=null`. Both fabrications survived for days because future agents
trusted the closing comments without re-verifying against `master`. The
same discipline applies to bead `close_reason` text now.

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

`xtask verify closure <bead-id>` reads the structured Bead via
`bd show <bead-id> --json`, validates the Bead state and complete AC
dispositions, then executes the manifest commands. Numeric GitHub issue ids
are rejected. Put this table in the Bead's `close_reason`; rows follow the
order of the Bead's `acceptance_criteria` field:

```markdown
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command | Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- |
| AC-1 | runtime | replay integration test | replay survives restart | xtask test -p sinexd -E 'test(replay_restart)' | - | Satisfied |
| AC-2 | contract | deferred provider | owned by sinex-abcd | - | sinex-abcd | Deferred |
```

Use exactly `Satisfied`, `Deferred`, or `Misframed`. Every original AC needs
one ordinal row. A satisfied non-doc row needs a runnable command; a docs row
needs a command or named artifact; a deferred row needs a follow-up Bead id.
The verifier executes commands from the manifest or from a `Verification` shell
block and fails closed on missing/malformed Beads JSON, an open Bead, incomplete
AC coverage, invalid evidence, or command failure.
There is no GitHub Actions replacement: Beads has no GitHub close event, so the
verifier is a local pre-close/review gate.

Two antipatterns to avoid:

- **"Closed by PRs #X–#Y" range claims** without verifying each PR in
  the range actually merged and contributed to the AC. If five PRs
  collectively address the AC list, name each PR's contribution
  individually. Range notation makes one closed-but-not-merged PR
  invisible.
- **"Status: done" comments listing work** that exists only in a working
  tree. If you're describing what *will* land, say so. If you're
  describing what *did* land, the verification commands prove it.

## Agent Docs

`CLAUDE.md` is a single self-contained file (no transclusion) and `AGENTS.md`
is a committed symlink to it — every agent framework reads the same bytes with
no render step. Edit `CLAUDE.md` directly; keep it dense and move long-form
material to `docs/architecture.md`, `docs/glossary.md`, or the owning
`crate/**/docs/` file.

The generated xtask docs surfaces can be refreshed and verified with:

```bash
xtask docs sync
xtask docs check
```

## Verification Baseline

The canonical test/verification matrix lives in [TESTING.md](TESTING.md). Keep
workflow/entrypoint guidance here, and keep concrete verification commands there
so the same command sets are not maintained twice.
