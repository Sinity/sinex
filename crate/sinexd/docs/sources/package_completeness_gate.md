# SourcePackage completeness gate (#1792)

`sinexd export-package-completeness` emits the package/mode report used by
source and capture work. The report is generated from compiled Rust inventories
and generated projections, not from a hand-maintained proof ledger.

Authoring truth comes from:

- `SourceContract` and `SourceRuntimeBinding` inventory;
- parser inventory and source factory registry;
- event payload schema inventory;
- EventContract registry;
- AdmissionPolicy registry;
- generated source catalog;
- generated source privacy coverage matrix.

The report is keyed by package id and mode id. A source/capture PR should cite
the relevant row, then explain which blocking missing entries it removes. It
should not paste a separate proof checklist.

## Commands

```bash
sinexd export-package-completeness
sinexd export-package-completeness --strict
sinexd export-package-completeness --output path/to/report.json
sinexd export-package-completeness --package terminal.atuin-history
sinexd export-package-completeness --package terminal.atuin-history --mode terminal.atuin-history --strict
```

`--strict` fails when accepted modes have blocking missing requirements.
Proposed and typed manual rows remain visible but do not block strict mode.

Use the package/mode filters during authoring. They render the same report
schema, but scoped to the row that a source PR is actually changing. Mode ids
are package-local, so `--mode` requires `--package`. The long forms
`--package-id` and `--mode-id` remain available for scripts; the shorter
`--package` and `--mode` names are the preferred interactive loop.

## Status Rules

`accepted` means the mode is intended to run and has no blocking missing
requirements.

`proposed` means the runtime binding is marked proposed. Proposed rows are
metadata/review rows, not runnable support. Their missing fields are surfaced as
non-blocking diagnostics.

`manual` means the report has evidence that the mode is intentionally outside
the normal local source factory path, such as an external producer, in-process
emitter/projection, or parser-only dispatch row.

`incomplete` means a would-be accepted or unbound mode is missing one or more
blocking code-owned references or projections.

## Policy Boundaries

Package labels are not privacy authority. Disclosure must remain field,
material, destination, operator-context, and policy-owner scoped. The gate may
report that a mode lacks observable disclosure policy references, but it must
not silently hide, delete, redact, or withhold data.

Resource behavior is runtime pressure control. It may bound queues, batch work,
defer work, create debt, and expose actions. It must not silently change event
meaning, drop material, censor fields, or bypass admission/disclosure policy.

## Closure Residuals Surfaced By The Gate

The gate intentionally keeps accepted-intent modes incomplete when remaining
mechanisms are code-owned elsewhere but not yet consumed by package rows:

- coverage and debt view refs from the unified debt/coverage surfaces;
- operation refs for package-mode operator actions;
- disclosure refs where EventContract/package metadata do not yet carry the
  destination-policy evidence directly;
- `fixtures_and_tests` per-mode fixture ownership.

Those are executable report fields, not detached checklist items. Follow-up
issues should add code-owned refs or inventory rows that remove the matching
blocking entries.

Material lifecycle and transport semantics are currently caveat-producing
fields: they expose missing typed package-mode policy without making
`--strict` fail by themselves. They remain architectural cleanup targets, but
release readiness should describe them as caveats unless the gate is changed to
make accepted rows block on them.
