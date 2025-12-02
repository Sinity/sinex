# sinex-terminal-command-canonicalizer

This satellite normalises terminal command events so query interfaces can reason
about equivalent command lines.

- Consumes raw terminal events from `sinex-terminal-satellite`.
- Canonicalises commands (resolving aliases, collapsing spacing, normalising
  environment references).
- Emits canonical command events with provenance metadata.

See `docs/current/architecture/UserInteraction_And_Query_Architecture.md` and
`crate/lib/sinex-satellite-sdk/docs/overview.md` for context.
