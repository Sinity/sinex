# sinex-search-automaton

The search automaton keeps the search index fresh by reacting to event streams
and scheduling reindexing work.

- Consumes content and PKM events that affect search.
- Dispatches indexing jobs via `sinex-services` search APIs.
- Maintains checkpointed state for incremental updates.

See `docs/architecture/UserInteraction_And_Query_Architecture.md` and
`crate/lib/sinex-satellite-sdk/doc/overview.md` for overarching design.
