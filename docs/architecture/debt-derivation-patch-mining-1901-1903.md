# Debt and Derivation Patch Mining (#1901/#1903)

## Context

The remaining external patch files for #1901 and #1903 were reviewed as source
material, not as authoritative diffs. Current master already uses the unified
`DebtListView` model with `DebtKind::{Capture, Admission, Projection}` rows.
Future work must preserve that shared surface.

## Accepted

- Keep `DebtListView` as the public operator debt surface.
- Treat capture, admission, and projection debt as providers of typed rows in
  the same finite view envelope.
- Classify `views.debt_list` as an `EphemeralView` output kind.
- Let derivation/invalidation code feed projection-debt rows into the unified
  debt surface.
- Let source coverage material/event buckets feed capture-debt rows into the
  unified debt surface.

## Rejected

- Do not introduce separate public `AdmissionDebtListView`,
  `CaptureDebtListView`, or `ProjectionDebtListView` DTO families.
- Do not add separate `dlq.admission_debt.view` or projection-debt CLI/RPC
  roots when `ops debt list` can carry the same rows.
- Do not reopen #1903's derivation primitives as a parallel projection-debt
  model; derivation metadata should remain a source of debt rows, not a second
  public debt API.

## Patch File Decisions

- `1901-debt-views-turbo-closure.patch`: mined for row vocabulary and operator
  action ideas; rejected as a direct patch because it creates separate public
  debt DTO families and roots.
- `1901-admission-debt-v0.patch`: mined for DLQ-to-admission-debt behavior;
  rejected as a direct patch because current `ops debt list` already owns the
  unified view.
- `sinex-1903-derivation-projection-debt-full.patch`: mined for derivation
  impact/debt linkage; rejected where it creates standalone projection-debt
  list DTOs.
- `sinex-1903-projection-debt-addon.patch`: mined for derivation-trigger rows;
  rejected where it duplicates the public debt view.
- `sinex-derivation-spec-v0.patch`: mostly superseded by current derivation
  primitives; remaining useful work is registry and documentation alignment.

## Applied Slice

The mined slice registers and documents `views.debt_list`, updates the package
completeness gate wording to point at unified debt providers, and wires
`sinexctl ops debt list --include-capture` so material-without-events and
events-without-material coverage buckets become `DebtKind::Capture` rows in the
existing `DebtListView`.

## Next Slice

Move provider composition behind the gateway/RPC surface once the operator CLI
semantics are settled. Preserve the single `DebtListView` payload rather than
creating provider-specific envelopes.
