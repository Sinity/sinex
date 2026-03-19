# sinex-terminal-command-canonicalizer

This node normalises terminal command events so query interfaces can reason
about equivalent command lines.

- Consumes raw terminal events from `sinex-terminal-ingestor`.
- Canonicalises commands (resolving aliases, collapsing spacing, normalising
  environment references).
- Emits canonical command events with provenance metadata.

See `README.md#architecture` and
`crate/lib/sinex-node-sdk/docs/overview.md` for context.
