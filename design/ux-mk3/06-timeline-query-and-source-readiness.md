# Timeline, query, and source readiness

## Timeline browser

The timeline should be implemented as a temporal query/view, not a decorative chart. It should answer:

- what happened in this interval?
- which sources were active?
- which sources were stale, private, redacted, missing, or replaying?
- what selected event anchors this window?
- what query/filter created the current view?

Important lanes:

- shell / terminal
- filesystem
- browser
- desktop/window manager
- documents
- AI/chat / Polylogue bridge
- derived/automata
- ops/replay/lifecycle
- private/redacted/gap overlays

Timeline states:

- loading first window
- loading adjacent window
- empty interval
- query narrowed to zero results
- partial source coverage
- private-mode suppressed interval
- replay overlay
- late evidence changed window
- stale source unit
- disconnected gateway

## Query builder/results

The query UI should mirror CLI semantics. It should show:

- generated command equivalent
- generated JSON request
- active filters: source, event type, text/payload, time range, limit, sort
- pagination cursor/window
- result count and caveats
- result rows rendered as EventCardView
- aggregation vs event-list mode clearly separated

## Source readiness cockpit

This should be a workbench, not a health grid. For each source unit/material family, show:

- readiness status and reason
- latest event time
- latest source material and anchor
- continuity gaps
- drift reports
- parser/material registration state
- privacy constraints
- cost/freshness/retention caveats when available
- repair or explain commands

Backed command surfaces include `sources readiness`, `sources continuity`, `sources drift`, `sources explain-gap`, `sources coverage`, `sources list`, and `sources show`.

## Continuity gap explainer

A gap should show:

- time range
- affected source unit/material
- expected vs observed coverage
- candidate causes: private mode, source not running, parser drift, permissions, host sleep, material missing, late replay, retention
- evidence supporting each cause
- next commands: explain-gap, source readiness, drift list, stage/show material, replay preview

The user should leave the view knowing whether Sinex lacks evidence, hid evidence, failed to capture evidence, has not parsed evidence yet, or has not implemented that source.
