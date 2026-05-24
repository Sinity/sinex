# Canonical design program: Sinex Evidence OS Workbench

## North star

Sinex should make an event-native personal/runtime system legible. It should show what happened, why the system believes it, what raw material supports it, what is missing, what is private, what derived interpretation exists, and what safe action is available next.

The interface should feel like an operator-grade evidence workbench, not a generic dashboard or chatbot.

## Product loops

### Runtime loop

Verify that the system is alive, fresh, private-mode-compatible, and source-complete enough to trust. This loop uses health, nodes, automata, recent activity, source readiness, DLQ, replay/ops, and snapshot status.

### Evidence loop

Find and read events. Navigate time, query by source/type/text, inspect payloads, trace provenance, open source material anchors, compare raw vs projected values, copy citations, and understand caveats.

### Coverage loop

Explain what is missing: source gaps, stale collectors, parser drift, private-mode suppression, redaction, late evidence, replay overlays, timing-quality warnings, and unsupported material shapes.

### Composition loop

Select events, documents, source materials, caveats, and timeline windows to build inspectable context. Moment windows and context packs are artifact-shaped composition tools, not hidden prompt generation.

### Authority loop

Perform risky work through explicit authority: preview, dry-run, confirm, execute, monitor, audit, rollback/restore where applicable. Replay, snapshot restore, lifecycle tombstone, privacy deletion, parser promotion, and DLQ mutations all live here.

## Primary surfaces

1. First-run / deployment readiness
2. Now command center
3. TUI workbench shell
4. Timeline browser
5. Event inspector
6. Query builder/results
7. Source readiness cockpit
8. Source material detail / explore bridge
9. Continuity gap explainer
10. Privacy and authority room
11. Context resumption
12. Moment search
13. Context pack composer
14. Operations room
15. Task/domain projections
16. Semantic shadow lanes
17. Agent projection / MCP
18. SinexFS and filesystem projection
19. State matrix and component library

## Current/target badges

Every board, component, action, and payload section should carry one of four states:

- **current**: backed by code/RPC/CLI today
- **near**: implementable over current DTOs with modest adapter work
- **target**: requires open issue work or new DTO/RPC contracts
- **blocked**: dependent on authority, privacy, schema, parser, or deployment decision

## Design principles

1. Show evidence before interpretation.
2. Show caveats before confidence.
3. Keep raw and projected forms one keystroke apart.
4. Never hide privacy/redaction state.
5. Never show dangerous actions without preview/confirm/audit.
6. Make disabled actions useful by explaining the missing backing surface.
7. Treat agents as projections over the same view models, not special users with separate semantics.
8. Prefer terminal-native density and keyboard speed, but keep the object model usable for future web/filesystem surfaces.
