# Issue 1963 Cleanup Closure Map

This records the concrete closure boundary for #1963. The issue is a cleanup
grab bag, not an infinite permission slip: it can close when the original
acceptance criteria are accounted for, merged cleanup phases have removed the
parent's unique actionable scope, and any remaining work has a narrower owner.

## Acceptance Matrix

| Acceptance criterion from #1963 | Current state |
| --- | --- |
| Package-local or package-mode event/admission/resource/debt authoring direction | Satisfied by the source-package authoring work that made package modes feed typed material lifecycle, transport semantics, capability refs, skeleton metadata, package completeness, and source-status actions from shared runtime-binding/package-mode truth. Future package-specific capture work belongs on its capture issue, not this parent. |
| Package-completeness authoring loop improvements | Satisfied by focused package/mode completeness flags and aliases plus source-skeleton/package-completeness docs that put the strict package-mode loop before skeleton generation. |
| Operator surface overlap decision map or consolidation PRs | Satisfied by `docs/architecture/operator-surfaces.md` and its `sinexctl` grouping map, plus the merged ops/source-status/debt/runtime-pressure cleanup phases. Future command moves should follow the map and prove DTO/format coverage. |
| At least one physical modularization slice for a large stable file/impl | Exceeded. Completed slices include primitives view DTO modules, event persistence helpers, several xtask history DB clusters, `sinexd` ops handlers, `sinexd` RPC server helpers, `sinexctl ops`, and `xtask history` command families. |
| `sinexctl` root grouping review or explicit keep/fold map | Satisfied by the operator-surface grouping map. The decision is to keep Sinex operator-shaped while folding write/recovery leaves under `ops`, source posture under `sources`, and runtime status/telemetry under `runtime` when DTO/format coverage proves the move. |
| xtask proof-sprawl cleanup direction | Satisfied by the current xtask cleanup direction: proof-like surfaces remain ordinary Rust tests, generated-surface checks, or checkout-local orchestration. The history DB and history command splits remove the largest concrete xtask history/proof sprawl surfaces without inventing a second proof framework. `xtask verify closure` remains the explicit Bead closure checker and is not replaced by ad hoc proof ledgers. |
| Privacy/disclosure and resource-budget guardrails recorded in relevant issues or docs | Satisfied by the source-package template, disclosure/resource-budget docs, source-status typed material lifecycle/transport/resource rows, and the operator-surface rule that privacy/disclosure stays field-, material-, destination-, and operator-policy controlled. |

## Remaining Work Policy

Do not add more #1963 PRs for generic cleanup discoveries. If a concrete gap is
found after this closure batch, route it narrowly:

- package-specific capture behavior -> that capture issue;
- source/package authoring mechanics -> the source package or package-mode issue;
- operator command movement -> the command's owning surface issue, using the grouping map above;
- runtime/provenance semantics -> the primitive/schema/runtime issue that owns the semantic boundary;
- test harness or closure verification behavior -> the test-harness issue.

This keeps #1963 from reopening every time a large file or stale phrase is
noticed.
