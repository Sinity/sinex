# sinex-pkm-automaton

Personal Knowledge Management (PKM) automaton that transforms entity and
relation events into curated knowledge graph updates.

- Observes entity creation/update/delete events.
- Produces derived relations, tags, and narrative insights.
- Maintains checkpointed state using `sinex-node-sdk` helpers.
- Binary entrypoint uses the unified `processor_main!` runner (`src/main.rs`) to start the `StatefulStreamProcessor` implementation in `src/lib.rs`.

See `docs/current/architecture/UserInteraction_And_Query_Architecture.md` and
`crate/lib/sinex-node-sdk/docs/overview.md` for the broader workflow.
