# Implementation roadmap

## Slice 0 — docs and fixtures

Land this design program under `docs/design/ux-mk3`. Add fixture JSON for runtime snapshots, event cards, source readiness, timeline viewport, and operation runs. No runtime behavior change.

## Slice 1 — View DTO spine

Implement shared view DTOs in primitives/CLI-visible crates:

- `ViewEnvelope`
- `SinexObjectRef`
- `ActionAvailability`
- `PrivacyStateView`
- `CaveatView`
- `EventCardView`

Add tests that CLI/TUI/MCP projections carry `generated_at`, refs, caveats, redaction metadata, and action states.

## Slice 2 — Event Inspector in TUI

Upgrade `sinexctl tui --tab events` from a one-column recent-events list into a two/three-pane event workbench. Use current event query/explain/trace/source calls.

Acceptance:

- select event
- inspect payload and raw JSON
- copy event id/json/query/trace/explain/source anchor
- show privacy/caveat/source chips
- show disabled reasons
- preserve selection while refreshing

## Slice 3 — Now + runtime snapshot convergence

Make `status`, `now`, and TUI dashboard consume a shared runtime snapshot builder. Reduce drift between shortcut commands and TUI rendering.

## Slice 4 — Timeline browser

Implement #1025 as a query-backed temporal browser. Start with event buckets and selected event inspector before advanced lane rendering.

## Slice 5 — Source readiness cockpit

Build a source room over `sources readiness`, `continuity`, `drift`, `explain-gap`, `coverage`, `list`, and `show`.

## Slice 6 — Operations room

Represent replay, ops, state snapshot, DLQ, and lifecycle runs as operation objects with preview/confirm/audit grammar.

## Slice 7 — Context composition prototype

Prototype a read-only/context-basket surface that can select event/source/document refs and render a draft context manifest. Keep it target-labeled until #1095 backing exists.

## Slice 8 — Moment and Explore target surfaces

Only after event/timeline/source work is real, implement Moment Search (#1110) and Explore Bridge (#1062) as evidence artifacts over view DTOs.
