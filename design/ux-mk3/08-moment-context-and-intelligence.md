# Moment, context, and intelligence surfaces

The intelligence layer should start as evidence composition, not a chatbot.

## Context resumption current surface

`sinexctl context` is already useful: it answers “what was I doing?” over recent events and groups by source. The TUI should expose this as a fast “resume session” view:

- recent source summaries
- last event per source
- time window selector
- caveats: stale, private, no events, redacted, source gap
- jump to event/timeline/query
- copy context summary

## Moment search target

Moment search expands seed hits into evidence windows. The UI should show:

- query text and filters
- seed hits
- window policy
- candidate windows
- evidence roles: seed, support, contradiction, caveat
- scores: relevance, density, diversity, temporal alignment, caveat penalty
- coverage joins: private gaps, source not ready, timing low, replay pending
- inspectable evidence list

Moment candidates are not truth. They are ranked evidence windows.

## Context Pack Composer target

A context pack is an inspectable artifact for humans and agents. It should show:

- intended task/model/use
- selected refs and excerpts
- omitted evidence and reasons
- redactions and privacy policy
- stale/unavailable evidence
- caveats
- token/byte budget
- provenance manifest
- copy/export variants
- regeneration strategy

A context pack should never be a hidden prompt blob. It must remain traceable to Sinex object refs and caveats.

## Grounded intelligence layer

Useful near-term intelligence features:

- summarize selected event window with caveats
- compare two source windows
- explain why a moment ranked high
- suggest missing sources for a context pack
- identify contradiction/caveat evidence
- produce an agent handoff manifest with refs, not just prose

Do not implement autonomous action as the first intelligence layer. Readable evidence composition is enough to deliver value.
