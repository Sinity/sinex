# Codebase grounding

This pack was grounded against the attached Sinex repository snapshot and material exports.

## Current substrate

The repository already has enough UI substrate to justify an implementation wave:

- `crate/cli/src/commands/tui.rs` implements a Ratatui dashboard with Dashboard, Nodes, Events, and DLQ tabs; refresh; basic navigation; loading/error states; and recent events from gateway queries.
- `sinexctl now` fetches gateway health, node list, recent activity, and automata status, then renders table/json/yaml.
- `sinexctl context` answers “what was I doing?” by querying recent events, grouping by source, and rendering concise source summaries.
- `sinexctl query`, `trace`, and `explain` form the read path for events, provenance, and payload detail.
- `sinexctl sources` already covers stage/list/show/coverage/annotate/archive/continuity/readiness/drift/explain-gap.
- The RPC catalog contains read/write/admin metadata for coordination, events query, sources, privacy private-mode, replay, ops, lifecycle, documents, tasks, semantic lanes, LLM budget/router surfaces, and more.
- `docs/design/operator-ux-convergence.md` already calls for shared runtime/event view models, output helpers, command catalog, RPC descriptors, and projection alignment across CLI/TUI/MCP/SinexFS.

## Current TUI reality

The current TUI is intentionally modest. It is a dashboard, not a workbench. The implementation currently renders:

- Dashboard overview: gateway version, healthy nodes, recent event count, DLQ count
- Nodes list: heartbeat, type, leader flag
- Events list: recent events, timestamp/source/type/snippet
- DLQ stats

MK3 designs the next TUI as an evolution of this file, not as proof that those richer boards already exist.

## Important command and RPC grounding

Concrete command surfaces to use in design copy:

- `sinexctl now`
- `sinexctl context --since 2h`
- `sinexctl query ...`
- `sinexctl explain <event-id>`
- `sinexctl trace <event-id>`
- `sinexctl tui --tab events`
- `sinexctl sources list|show|coverage|continuity|readiness|drift|explain-gap`
- `sinexctl privacy private-mode status|enable|disable`
- `sinexctl replay ...`, `sinexctl ops ...`, `sinexctl state snapshot ...`, `sinexctl dlq ...`

Use target/proposed labels for:

- moment search UI over #1110
- context pack composer over #1095
- staged material explore/propose/simulate/promote over #1062
- SinexFS over #1121
- advanced privacy audit/export/delete/redact over #1072 unless implemented
- richer semantic shadow lane workbench over #1109/#1346

## Design consequence

Sinex should not have separate JSON shapes for CLI, TUI, MCP, future web, and future SinexFS. MK3 therefore starts with a `ViewEnvelope`/`SinexObjectRef`/`ActionAvailability` spine, then projects it into the current and future surfaces.
