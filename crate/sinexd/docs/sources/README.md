# sinexd Sources Documentation

This directory owns source and staged-source runtime docs for
`sinexd::sources`.

## Core Documents

- `staged_source_parser_substrate.md` - source material, input-shape adapter,
  parser, and source substrate direction.
- `adding_staged_export_parser.md` - implementation guide for staged personal
  export parsers.
- `historical_backfill_runtime_plane.md` - historical scan runtime proof shape.
- `source_drain.md` - source drain, material finalization, and gap
  evidence contract.
- `evidence_lanes.md` - source-material occurrence/snapshot role model.
- `sqlite_evidence_lane.md` - SQLite row-stream plus snapshot evidence decision.
- `integration_authority.md` - external adapter authority categories.
- `source_capture_package_template.md` - required fields and per-mode contract
  every new source/capture package issue follows.
- `package_completeness_gate.md` - #1792 report shape, status rules, strict
  gate, and source/capture issue consumption guidance.
- `polylogue_material_protocol.md` - public byte-backed Polylogue producer
  contract and fail-closed manifest rules.

## Coverage error semantics

The `sinexctl sources status` coverage summary reports the fraction of declared source contracts with evidence debt. `coverage_error_basis_points` is `coverage_error_sources * 10,000 / total_sources`, using integer floor division. A source counts once even when it has multiple gap reasons. `coverage_error_kinds` retains the per-reason counts, including readiness, continuity, and explicit gap evidence.

This source-count denominator is a contract denominator. It answers whether Sinex has evidence for each declared source, not how many real-world records an upstream system produced. A source with no runtime binding, no material, no events, an unobserved bridge, or stale runtime evidence is reported as debt or unknown evidence; silence is never counted as successful capture. Source-specific denominators such as journald sequence ranges, filesystem notification overflow, provider cursor lag, and expected-rate models remain separate probe work and must carry an explicit observed, missing, stale, unsupported, or unknown state before they affect a record-rate estimate.
