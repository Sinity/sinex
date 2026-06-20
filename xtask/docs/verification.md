# Verification Workflow

Sinex verification should stay executable, narrow, and attached to the behavior
owner. `xtask` is the conductor: it starts infrastructure, selects packages,
renders generated surfaces, and aggregates release or closure reports. It should
not become a second truth ledger for invariants that ordinary Rust tests,
integration tests, schema strict-diff, or generated-surface checks can own.

## Verification Map

| Surface | Behavior owner | Gate or command | Notes |
| --- | --- | --- | --- |
| Changed Rust/API surfaces | affected-package resolver plus package checks | `xtask check --changed-strict <base-ref>` | Release readiness runs this as `changed-strict`. |
| Test-scope decisions | impact planner explain output plus the selected `xtask test` invocation | `xtask impact explain --json`; `xtask test ...` | Record this in PRs when an affected run, exact filter, or full `--impact-mode=off --all` choice is material to the review. The explain output is evidence for the chosen test scope, not a separate proof ledger. |
| Impact-plan coverage audits | impact planner sampled skipped-test audit | `xtask impact audit --sample-skips N` | Run for impact-planner or verification-policy changes, and for closeout audits where skipped affected-test coverage is part of the claim. Keep the sample bounded in PRs; broader audits belong to phase-boundary verification. |
| Forbidden architecture drift | `xtask lint-forbidden` plus ast-grep catalog | `xtask check --forbidden` | Keep this to coarse forbidden patterns, structural lint rules, and deployment-boundary scans. Do not add product proof ledgers here. |
| Dependency duplicate docs | dependency command tests | `xtask test -p xtask -E 'test(test_dependency_hygiene_doc_matches_duplicate_classifier)'` | The duplicate-classification doc check lives with `xtask deps` behavior tests, not `lint-forbidden`. |
| Closure claims in issues | issue body/comment evidence parser | `xtask verify closure <issue>` | Extracts executable verification commands and acceptance matrices from GitHub issue text. |
| Release readiness | release contract/report command | `xtask release-readiness --run-required-checks` | Emits claims, non-claims, caveats, artifacts, and required check results. |
| Event admission and event-engine runtime | `sinexd` event-engine tests | `xtask test -p sinexd -E 'test(admission|event_engine)'` | Use focused integration tests for NATS, DLQ, admission, material assembly, schema sync, and runtime behavior. |
| Source catalog drift | `sinexd` source catalog integration test | `xtask test -p sinexd -E 'test(source_catalog_artifact_matches_inventory)' --allow-contended-host` | The generated NixOS source catalog is rendered from the linked Rust source inventory and compared to the checked-in artifact. |
| Privacy catalog loading | privacy command/runtime | `xtask privacy catalog --format json` | This proves the catalog loads. Destination enforcement is covered by focused privacy/disclosure tests. |
| Replay invalidation recovery | `ops.start` projection-rebuild recovery path | `xtask test -p sinexd -E 'test(ops_start_projection_rebuild_recovers_pending_replay_invalidation)'` | Proves pending replay scope invalidation metadata can be drained through a durable operation instead of relying solely on post-commit NATS publish. |
| Database schema drift | schema strict-diff | `xtask schema strict-diff` | Owns DB shape and migration drift against the checkout-local development database. |
| Command reference drift | docs generated-surface command | `xtask docs command-reference --check` | Owns the checked-in command reference against the live clap tree. |
| Payload schema bundle drift | docs generated-surface command | `xtask docs schema-bundle --check` | Owns the checked-in payload schema bundle against the Rust registry. |
| Phase and perf manifests | `xtask verify plan` and `xtask verify perf` | `xtask verify plan --check`; `xtask verify perf ...` | Orchestration and artifact emission only. Product/resource invariants should move to ordinary tests or explicit perf contracts when possible. |
| Trybuild compile-fail runners | owning crate trybuild fixtures | `xtask test --debug --heavy -p <package> -E 'test(<runner>)'` | Keep individual fixtures and stderr files. Use the debug profile for edited runner/fixture verification so cold trybuild nodes run serially instead of timing out under parallel nextest execution. |

## Simplification Landed

The dependency-hygiene duplicate-vocabulary guard was removed from
`xtask lint-forbidden`. It was a docs/product coherence check embedded in a
forbidden-pattern scan. The live invariant now sits beside the `xtask deps`
behavior it relies on: `test_dependency_hygiene_doc_matches_duplicate_classifier`
loads `xtask/docs/dependency-hygiene.md`, runs `xtask deps duplicates --json`,
checks the current `direct_workspace` / `transitive_upstream` vocabulary, and
rejects a stale zero-direct-debt claim when the live duplicate report disagrees.

The release-readiness gate also names the source-catalog drift test directly.
That keeps the checked-in `nixos/modules/source-catalog.generated.json` artifact
owned by the `sinexd` inventory renderer instead of a decorative release note.

## Release Gate Residual List

Fixed now:

- `source-catalog-drift` is a release-readiness required check and points at the
  ordinary `sinexd` integration test that owns the Rust-to-Nix catalog seam.
- Dependency duplicate-doc drift is no longer part of `lint-forbidden`; it is
  covered by the dependency command test suite.

Follow-up issue needed:

- #1792 should eventually replace the narrow source-catalog release check with a
  complete source/package gate once package modes have EventContract,
  AdmissionPolicy, ResourceBudgetSpec, debt views, operations, fixtures, and
  generated catalog coverage.
- Privacy destination enforcement remains owned by focused runtime/CLI tests; do
  not replace those with catalog-load proof.
- #1735 must keep command catalog, help, completion, ViewEnvelope, API, and TUI
  DTO checks tied to generated-surface or focused Rust tests.
- Replay/archive invalidation recovery is covered by the `ops.start`
  projection-rebuild recovery test. Broader replay changes should add focused
  tests at the replay/debt/operation boundary they modify.

Blocked by named human decision:

- None in this cleanup. The remaining items above already have owning issues.

Intentionally out of scope:

- Resource-budget and package-completeness gates that depend on the new #1899,
  #1900, #1901, and #1902 primitives are not invented here. They should land as
  executable v0 mechanisms before release readiness claims them.

Unsafe due to verification failure:

- None recorded in this map. Record host-pressure refusal separately from code
  failures, and keep durable commands repo-native through `xtask`.

## Command Patterns

Use focused gates before broad ones:

```bash
xtask impact explain --json
xtask impact audit --sample-skips 20
xtask test -p xtask -E 'test(test_dependency_hygiene_doc_matches_duplicate_classifier)'
xtask test -p xtask --lib -E 'test(release_readiness)'
xtask test -p sinexd -E 'test(source_catalog_artifact_matches_inventory)' --allow-contended-host
xtask docs command-reference --check
xtask docs schema-bundle --check
xtask schema strict-diff
xtask release-readiness --target rc-local --base-ref origin/master --run-required-checks
```

Use the heavy debug profile for edited trybuild runners or stderr fixtures:

```bash
xtask test --debug --heavy -p sinex-primitives -E 'test(source_contract_compile_failures)'
```

Use broad gates only after the focused owner tests are green:

```bash
xtask check --changed-strict origin/master --allow-contended-host
xtask check --full --allow-contended-host
xtask test --impact-mode=off --all
```
