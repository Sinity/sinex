# Entity Resolution

**Status:** dissolved into issue tracking. The substantive contract
that lived here — load-bearing "resolver never merges on its own;
candidates flow through proposal/judgment/finalizer" invariant, the
`core.entities` person shape + alias-relation pattern, the two
candidate signals (trigram name similarity + temporal co-occurrence),
visible confidence thresholds, manual resolution CLI sketch, privacy
inheritance rule, and the boundaries list — now lives in [issue #1087
(feat(intelligence): activate entity and relation automata as consumer
substrate)](https://github.com/Sinity/sinex/issues/1087) as a design
comment.

Originating design issues `#467` (contact entity resolution) and
`#474` (entity model taxonomy) are closed. `#1087` is the live tracking
issue; the shadow-lane registry + diff that resolves competing model
generations is `#1346`.

The proposal/judgment/finalizer substrate that mediates every merge is
owned by `docs/architecture/proposal-judgment-finalizer.md`.

**Related:** `docs/architecture/proposal-judgment-finalizer.md`,
`docs/architecture/inference-decision-metadata.md`,
`docs/architecture/knowledge-boundaries.md`.
