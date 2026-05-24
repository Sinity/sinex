# Claude output review and extraction

## What the ZIP contains

The incoming package contains two related design canvases: an MK1.1-style board and a larger MK2-style board. The bigger board is the useful one. It has 24 boards across deployment/runtime, evidence, trust/privacy, composition, domain projection, agent/filesystem projections, state matrix, components, surface parity, and roadmap.

The file inventory confirms it is a static React/Babel design canvas, not a repo-ready artifact:

```text
Sinex Workbench MK2.html
index.html
lib/colors_and_type.css
lib/design-canvas.jsx
lib/tui/*.jsx
mk2/src/*.jsx
src/*.jsx
uploads/sinex-ux-mk1-pack ...
uploads/sinex-ux-mk2-pack ...
```

The duplicated uploaded packs inside `uploads/` should not be carried forward as canonical source. MK3 extracts the ideas and provides a clean standalone program.

## Keep

The Claude board is strongest in these areas:

- terminal-native visual language: dense panes, monospace rhythm, state badges, command equivalents
- event fixtures with copy actions, raw JSON, trace, source material, related events, and context-pack affordances
- source readiness / material detail / gap explainer as first-class views
- runtime and deployment readiness as a product surface rather than afterthought
- state matrix, component library, and surface parity boards
- moment/context surfaces framed as evidence composition instead of chat

## Change

Several details must be corrected before implementation:

- `sinexctl context pack new` should be shown as target/proposed. Current `context` is a resumption shortcut, not a context-pack subcommand.
- Current TUI is Dashboard/Nodes/Events/DLQ. Workbench shell/timeline/event inspector are target implementation slices over the existing TUI, not current reality.
- Some fixture source names are presentation aliases. UI should expose both family label and raw source string, e.g. `filesystem` plus `fs-watcher`, not invent a canonical `fs.inotify` if the payload says otherwise.
- Moment search, Context Pack Composer, Explore Bridge, SinexFS, advanced privacy workflows, and semantic shadow lane workflows must carry target/proposed state until backing RPCs and DTOs exist.
- The React/CDN/Babel artifact is fine for Claude Design, but the repo should receive static docs, contracts, issue bodies, and eventually Rust/TUI visual smoke fixtures.

## Drop

Do not keep:

- the embedded `uploads/` copies of older generated packs
- demo-only version strings as factual runtime claims
- target features rendered as enabled without disabled reasons
- any UI that bypasses authority surfaces for replay, snapshot, privacy, lifecycle, or parser promotion

## Resulting MK3 position

MK3 is not merely prettier. It gives Claude's visual breadth an implementation spine: view DTOs, action availability, state grammar, issue bodies, visual-smoke fixtures, and current/target labels.
