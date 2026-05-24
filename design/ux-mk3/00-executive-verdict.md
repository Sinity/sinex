# Executive verdict

Claude Design's latest Sinex output is useful, but it should be treated as an input deck, not a product spec. Its strengths are visual breadth, concrete fixtures, and an improved sense of terminal-native product feel. Its weaknesses are current/target blending, invented-or-target command affordances shown as too live, and a delivery format that is a demo artifact rather than a repo-ready design program.

MK3 turns that output into a grounded Sinex UX program:

1. The primary surface is still **TUI-first**, because the current repository has `sinexctl`, gateway RPC, query/explain/trace/context/now/source operations, and an existing Ratatui dashboard. A web SPA is not the next forcing function.
2. The design object is not “dashboard cards”. It is a shared view layer over evidence: runtime snapshots, event cards, source readiness, material anchors, privacy/caveat state, timeline windows, operation runs, context packs, and agent projection resources.
3. Current features must be visually separated from target features. `sinexctl tui`, `now`, `context`, `query`, `trace`, `explain`, `sources readiness`, `sources continuity`, `sources drift`, `sources explain-gap`, documents, tasks, semantic lanes, privacy private-mode, replay/ops/state, and current MCP/CLI surfaces are real substrate. Moment search, context packs, staged material explore/propose/simulate/promote, SinexFS, richer privacy workflows, and many intelligence features are target/proposed unless backed by code.
4. The first implementation should create a DTO and action spine, not a huge screen rewrite. Once `EventCardView` and `ActionAvailability` exist, CLI, TUI, MCP, and future web/SinexFS projections can converge instead of multiplying incompatible renderers.

The strongest near-term outcome is a working TUI where events are readable objects with copy/trace/explain/source/raw actions and honest caveats. That would make Sinex feel like a real system to a human operator.
