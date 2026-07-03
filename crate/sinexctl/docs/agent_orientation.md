# Sinex Agent Orientation

Sinex is a personal evidence substrate. It captures observations as events,
keeps their provenance explicit, and lets later interpreters rebuild derived
understanding from the same source material instead of treating old
interpretations as permanent truth.

## First Model

Every event is one of two kinds:

- Material provenance: the event interprets a byte range from a registered
  source material. The source material is the ground truth.
- Derived provenance: the event is computed from parent events. The parents are
  the ground truth.

Those two provenance forms are mutually exclusive. If an event has neither, or
both, it is invalid.

## Time

Use the three clocks deliberately:

- `ts_orig`: when the observed thing happened in the source domain.
- `ts_coided`: when Sinex created this interpretation. It comes from the UUIDv7
  event id, so replay creates a new `ts_coided`.
- `ts_persisted`: when PostgreSQL stored the row.

Ask "what happened then?" with `ts_orig`. Ask "when did Sinex understand this?"
with `ts_coided`.

## Identity

The event id identifies an interpretation, not the real-world occurrence.
Replay intentionally creates new event ids. Occurrence identity lives in the
material coordinates: source material plus byte anchor, and sometimes an
equivalence key for object-level deduplication.

## How To Query

Start with these tools:

- `sinex_query` for typed query-unit expressions across events, sources, debt,
  operations, and runtime health.
- `sinex_search_events` for event-card search with redacted previews.
- `sinex_context_pack` for compact agent context. Treat project scoping caveats
  as meaningful; do not silently assume a path was scoped correctly.
- `sinex_trace_lineage` when a claim needs ancestry, descendants, or material
  links.
- `sinex_source_readiness`, `sinex_source_continuity`, and
  `sinex_source_gap_explain` when a missing or stale source might change the
  answer.

Query expressions support bounded event windows and explicit ordering. A common
time-window shape is:

```text
events where ts_orig >= '2026-07-02T12:00:00Z' and ts_orig < '2026-07-02T13:00:00Z' order by ts_orig asc limit 100
```

Use returned refs and ids in follow-up calls. Prefer resolvable evidence over a
free-text summary.

## Honesty Rules

Gaps are first-class. If a tool reports caveats, stale sources, redacted
samples, approximate scoping, empty read models, or disabled raw text, preserve
that in your answer. A useful Sinex answer says both what the evidence supports
and what the substrate cannot yet prove.

Do not infer raw private content from redacted previews. Do not treat derived
events as source truth unless their lineage supports the claim. Do not treat a
missing event as proof that nothing happened until source coverage is checked.

## First Contact Checklist

1. Call `sinex_orient` if you have not used Sinex in this session.
2. Call `sinex_context_pack` for the current task or project, then read caveats.
3. Use `sinex_query` or `sinex_search_events` for the actual evidence slice.
4. Use `sinex_trace_lineage` before making a strong claim about why something is
   true.
5. Include refs, ids, and caveats in the answer or handoff.
