# sinex-pkm-automaton

Personal Knowledge Management (PKM) automaton that transforms entity and
relation events into curated knowledge graph updates.

- Observes entity creation/update/delete events.
- Produces derived relations, tags, and narrative insights.
- Maintains checkpointed state using `sinex-satellite-sdk` helpers.

See `docs/architecture/UserInteraction_And_Query_Architecture.md` and
`docs/architecture/satellite-implementation.md` for the broader workflow.
