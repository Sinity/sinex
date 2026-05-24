# TUI workbench and event object system

## Current TUI

The current TUI is a useful seed: tabs, status bar, refresh, simple gateway data fetching, recent events, nodes, and DLQ. It should not be discarded. It should be upgraded into a workbench with the same low-dependency terminal-native posture.

## Workbench shell v2

Target layout:

```text
┌ Sinex Evidence Workbench ─ runtime target ─ generated_at ─ stale/private chips ┐
│ nav: Now Timeline Events Sources Ops Context Tasks Semantics MCP Settings      │
├──────────────────────────────┬──────────────────────────────┬─────────────────┤
│ list / lanes / table         │ selected object / reader      │ inspector/actions│
│ query/filter bar             │ payload/source/provenance     │ caveats/raw/copy │
└ status: gateway · source freshness · keymap · command equivalent · errors ────┘
```

## Minimum useful event inspector

The Event Inspector is the key deliverable. It should include:

- header: event id, source, type, time, severity/privacy/caveat chips
- summary line that is human-readable but never replaces raw payload
- payload renderer with type-aware rows and JSON toggle
- source material anchor, parser/source unit, timing quality
- provenance/trace tree
- related events and descendants when available
- domain projections, e.g. task/document/semantic labels if derived
- copy menu with success/failed/disabled feedback

## Copy menu

Core copy variants:

- copy event id
- copy short citation
- copy JSON
- copy payload JSON
- copy reselect query
- copy trace command
- copy explain command
- copy source material anchor
- copy MCP/resource call when available
- add to context selection basket when target context composition exists

Every disabled copy option should explain why, for example “source material anchor unavailable: event was admitted without material ref.”

## Keyboard details

Recommended baseline:

- `j/k` and arrows: move selection
- `enter`: open selected object
- `esc`: back/close panel
- `/`: search/filter
- `?`: command palette/help
- `r`: refresh
- `t`: trace
- `e`: explain
- `s`: source material
- `y`: copy menu
- `a`: action menu
- `c`: add to context selection basket when available
- `p`: privacy/caveat detail

## Little details that make the workbench feel real

- Show `generated_at` and “data stale by …” in the footer.
- Render “no data” differently from “no matching data”, “source unavailable”, and “target-only”.
- Persist selection while refreshing when possible.
- Keep raw ids copyable even when pretty names are truncated.
- Use source family labels, but keep raw `event.source` visible.
- When privacy/redaction is active, show policy/session/cause without leaking content.
