# Issue 2039 Verifiability Closure Map

This records the closure boundary for #2039. The issue began as a
projectwide test-quality audit and deliberately accumulated many coherent
test/harness slices. It is closeable when the audit's own weak-test and
false-green concerns are exhausted, and when remaining product capability work
has a live owner instead of being hidden inside the test-harness issue.

The current boundary is:

- #2039 owns verifiability mechanics: false-green prevention, skipped/ignored
  classification, closure evidence semantics, VM/runtime evidence states,
  production-path obligations, parser/admission/privacy fixtures, CLI/API
  output-shape checks, strict-diff blind spots, and typed/stringly boundary
  tests.
- #1043 owns unfinished media capture capability: acquisition runners, local
  model execution, live/on-demand modes, raw-media lifecycle policy wiring,
  media operations, resource budgets, replay/invalidation, and full
  destination disclosure for modes that do not exist yet.
- #1469 owns unfinished email capability: complete staged mailbox modes,
  Gmail/IMAP scheduled/live sync, body/thread/attachment projections, provider
  auth/rate-limit/cursor behavior, attachment fetch/storage policy, and full
  destination disclosure for modes that do not exist yet.

## Acceptance Matrix

| Acceptance criterion from #2039 | Current state |
| --- | --- |
| Audit and classify ignored/skipped/soft-skip tests under `xtask/tests`, `crate/*/tests`, `tests/workspace`, `tests/e2e`, and `tests/vm-suite` | Satisfied. Ignored tests use explicit `heavy:`, `long:`, or `external:` reasons, and `xtask check --forbidden` rejects bare or ambiguous ignored tests. Ordinary content/material tests no longer depend on implicit `git-annex`; the legacy annex roundtrip remains explicit `external:` coverage. |
| Closure verification maps every AC to behavior evidence and rejects source-grep or prose-only evidence for behavior claims | Satisfied by `xtask verify closure` and `xtask verify plan` Closure Evidence Manifest support. Satisfied non-doc rows reject grep-only/source-text-only evidence, invalid matrix statuses, and missing required artifacts. |
| Harness result models expose pass/fail/skipped/inconclusive/evidence-missing states where runtime prerequisites or fault injection matter | Satisfied by the VM-suite typed outcome stream, `VM_OUTCOME_SUMMARY`, `xtask vm` progress classification, zero-outcome failure handling, and the chaos/concurrency upgrades that report missing observations instead of manufacturing passes. |
| Tests can declare required evidence collectors/probes for DB, NATS, process, logs, source material, output contracts, and proof artifacts | Satisfied by the ordinary sandbox evidence collector model, phase evidence manifests, VM required/optional evidence outcomes, production-path obligation harness, replay fake-runtime await helpers, and output-contract tests. Missing required evidence fails or records evidence-missing; optional unavailable evidence is visibly skipped. |
| Closure and phase verification consume AC-to-evidence manifests tying criteria to behavior surface, commands, artifacts, evidence kind, and skip/defer status | Satisfied. Phase manifests have `evidence_manifest` rows, closure verification parses `Closure Evidence Manifest` tables from Bead `close_reason` text, and validation enforces status vocabulary and non-grep behavior evidence. |
| Harness helpers replace repeated ad hoc runtime polling with typed observations for readiness, process exit/kill, NATS delivery, DB/material insertion, log emission, CLI/API output envelopes, replay/archive/invalidation, and disclosure destinations | Satisfied for the weak-test audit scope by replay fake-runtime typed results, live gateway env guards/readiness failure propagation, VM fault-state classifiers, source-status runtime observation caveats, production-path obligations, and CLI/API output envelope tests. Product-specific helpers for future media/email acquisition modes belong with #1043/#1469 when those modes exist. |
| Parser/admission/privacy fixture suites express positive, negative, malformed, sensitive, replay/idempotency, and disclosure expectations as fixture contracts | Satisfied for current runnable surfaces by production-path fixtures, parser/admission tests, privacy policy disclosure tests, DLQ/query-card disclosure fixtures, and source package completeness checks. Future fixture sets for unimplemented media/email modes are #1043/#1469 product acceptance, not remaining #2039 harness debt. |
| Replay/RPC/live helper tasks propagate fake-runtime failures to parent tests with concrete context | Satisfied. Replay-control and workspace replay fake runtimes return typed task results and parent await helpers distinguish panics, timeouts, receive/decode/build/insert/publish failures, missing material roots, and final progress failures. |
| Schema strict-diff/convergence tests cover declared blind spots as detected, warned/unsupported, or intentionally visible behavior | Satisfied by strict-diff coverage for inline CHECK drift, FK action drift, TimescaleDB policy/chunk drift, orphan-column allowlists, rogue live columns, and strict-diff output/report behavior. |
| Media #1043 has tests beyond parser units for current transcript/OCR surfaces | Satisfied for currently implemented staged/parser and disclosure surfaces: production-path media obligations exercise recording, transcription run, transcript segment, screenshot/video/capture-session, OCR run, and OCR segment obligations; media disclosure tests cover event cards, query snippets, DLQ previews, transcript/OCR text, window titles, raw material refs, source paths, and model logs. Remaining acquisition/live/model/replay/resource capability is owned by #1043. |
| Email capture/parser coverage is current for any claimed staged email surface | Satisfied for current staged surfaces: RFC822, Maildir, and MBOX production-path obligations run through the shared package harness; email disclosure tests cover export redaction/caveats for subject, recipients/Bcc, raw material refs, operation scope, and preview; provider failure/rate-limit/source-coverage tests expose Gmail/IMAP runtime caveats where implemented. Remaining Gmail/IMAP sync, full body/thread/attachment behavior, and live provider capability are owned by #1469. |
| Package completeness tests no longer assert closed-issue missing refs as steady state for accepted/runnable modes | Satisfied. Accepted/runnable package modes prove EventContract, AdmissionPolicy, operation, coverage/debt, resource/transport, and export/fetch/rebuild refs where those contracts apply. Closed-issue missing-ref expectations were removed or converted into current invariants. |
| Source/catalog tests distinguish artifact drift from runtime behavior | Satisfied. Generated catalog drift remains an artifact check, while behavior is covered by package completeness, production-path obligations, source-status actions/caveats, operation refs, privacy coverage, and source-material lifecycle tests. |
| `sinexctl` command/format coverage exercises command families and declared machine formats, including unsupported-format failures | Satisfied by the format registry/Clap leaf exact-coverage test, command path preservation tests, `--list-formats` machine catalog tests, unsupported format rejection tests, NDJSON item-shape tests, DOT trace renderer tests, YAML/JSON envelope tests, and validation fixtures over MCP/API surfaces. |
| Typed/stringly boundaries have behavior tests for RPC methods, CLI enum-like values, field paths, output envelopes, error codes, and machine round trips | Satisfied by typed RPC method catalogs, `sinexctl` format registry and MCP schema tests, privacy action/matcher/scope parsing tests, telemetry/runtime pressure typed DTO tests, output envelope validation, DLQ/error shape tests, and command/RPC/MCP cross-surface catalog checks. |
| xtask history/diagnostic/fix/idempotency invariants have lightweight non-ignored coverage while expensive subprocess/property tests remain explicit heavy gates | Satisfied. Lightweight adapter coverage now protects workspace/history/idempotency contracts; expensive workspace subprocess/property tests remain `heavy:` gates with visible ignore reasons and are routed through `xtask test --heavy`. |
| Closed issue refs in tests are audited | Satisfied for #2039-relevant stale ownership. Closed issue references that remain in tests/docs are historical provenance, current architecture anchors, or non-#2039 product ownership notes. Current email and media test comments point to live #1469/#1043 owners where product capability remains unfinished. |
| Ceremonial tests that only assert source text, renamed/deleted spellings, exact internal list names, or generated-list absence are removed or rewritten | Satisfied for the audit scope. The remaining checks protect public command catalogs, generated artifact drift, documented operator surfaces, or machine output contracts rather than memorializing implementation churn. |

## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command / artifact | Status |
| --- | --- | --- | --- | --- | --- |
| ignored-skipped-tests | harness | forbidden scan and ignored-test taxonomy | Ambiguous ignored tests are rejected; current ignored tests are categorized as `heavy:`, `long:`, or `external:`. | `xtask check --forbidden`; `TESTING.md`; `xtask/src/commands/lint_forbidden.rs` | Satisfied |
| closure-evidence-semantics | harness | closure/phase verification | Closure and phase verification parse AC-to-evidence manifests and reject grep-only/source-text-only behavior claims. | `xtask verify plan --check`; `xtask/src/commands/verify.rs`; `xtask/config/phase-verification.json` | Satisfied |
| vm-runtime-evidence | runtime | VM-suite and `xtask vm` | VM reports typed pass/fail/skip/inconclusive/evidence-missing outcomes and hardened chaos/concurrency scenarios require observed faults before pass claims. | `tests/vm-suite/src/**`; `xtask/src/commands/vm.rs` | Satisfied |
| replay-runtime-propagation | replay | replay-control/workspace replay tests | Fake source runtime task failures propagate to parent tests with receive/decode/build/insert/publish/progress context. | `crate/sinexd/src/api/replay_control/tests/**`; `tests/workspace/tests/replay_end_to_end_test.rs` | Satisfied |
| strict-diff-blind-spots | schema | strict schema drift tests | Declared strict-diff blind spots have negative/visible behavior coverage instead of comments-only acceptance. | `crate/sinex-schema/src/strict_diff.rs` | Satisfied |
| production-path-fixtures | parser/admission | source production-path harness | Empty obligations fail; current media/email staged surfaces run through package-mode obligations. | `crate/sinexd/tests/sources/production_path.rs`; `crate/sinexd/tests/sources/production_path/{media,email}.rs` | Satisfied |
| media-current-surface | privacy/disclosure | current media transcript/OCR surfaces | Current staged media/parser/disclosure paths cover admission obligations, cards/snippets, DLQ previews, model logs, window titles, raw material refs, and caveats. | `crate/sinexd/src/api/handlers/dlq.rs`; `crate/sinexd/src/event_engine/policy.rs`; #1043 for unfinished capability | Satisfied |
| email-current-surface | privacy/disclosure | current email staged/provider surfaces | Current staged email surfaces cover production-path obligations, export disclosure, provider runtime caveats, and projection debt. | `crate/sinexd/tests/sources/production_path/email.rs`; `crate/sinexd/tests/api/ops_handlers_test.rs`; `crate/sinexd/src/api/handlers/source_status.rs`; #1469 for unfinished capability | Satisfied |
| cli-api-output-shapes | cli/api | `sinexctl` and MCP/API outputs | Registry-wide command/format coverage, unsupported-format failures, NDJSON item shapes, DOT/YAML/JSON envelopes, and MCP schema tests protect declared machine surfaces. | `crate/sinexctl/src/main.rs`; `crate/sinexctl/tests/validation_test.rs`; command/format tests | Satisfied |
| product-capability-residuals | issue routing | media/email unfinished modes | Remaining media/email work is product implementation owned by #1043/#1469, not residual #2039 harness debt. | `gh issue view 1043`; `gh issue view 1469` | Tracked elsewhere |

## Remaining Work Policy

Do not reopen #2039 for generic "more tests" discoveries. Route concrete gaps
to the owning surface:

- media capture behavior, acquisition, live/on-demand modes, model execution,
  raw-media lifecycle, replay/invalidation, and media resource budgets -> #1043;
- email staged/provider/live behavior, body/thread/attachment projection,
  Gmail/IMAP auth/cursor/rate-limit behavior, attachment fetch/storage, and
  email resource budgets -> #1469;
- new closure-verification semantics -> the closure/verification command
  surface;
- new CLI/API format drift -> the affected command/API surface;
- new VM/runtime fault-evidence gaps -> the VM/runtime scenario surface;
- new parser/admission fixture gaps for an implemented package mode -> that
  package mode's issue or a fresh narrowly scoped bug.

This keeps #2039 from remaining open as an infinite umbrella while preserving
the full product scope of #1043 and #1469.
