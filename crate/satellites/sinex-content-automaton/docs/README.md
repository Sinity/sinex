# sinex-content-automaton

The content automaton analyses textual and media-rich events to derive
classifications and metadata. It builds on the shared automaton pattern from
`sinex-satellite-sdk`.

- Watches gateways and ingestion events that carry rich content.
- Generates derived events with sentiment, keywords, and other features.
- Uses the shared annex and database adapters for retrieval.

See `docs/current/architecture/UserInteraction_And_Query_Architecture.md` and
`crate/lib/sinex-satellite-sdk/docs/overview.md` for higher-level context.
