# Entity Resolution

Status: design contract for the entity-resolution surface. Tracked as #467
(contact entity resolution / person dedup across platforms) and #474 (entity
model taxonomy/resolution/confidence). This record locks the algorithm shape,
confidence levels, and how resolutions become proposals rather than direct
mutations.

Person dedup across communication platforms is the canonical case: the same
human shows up as an IRC nick, a Facebook display name, a Reddit username, an
email address, and a Signal contact. Sinex needs one canonical entity with
weighted aliases, not five disconnected records — but it must reach that
state without silently merging things that are merely similar.

## What This Owns

- The `core.entities` shape for persons and the alias-relation pattern.
- The candidate-resolution algorithm (name similarity, temporal co-occurrence).
- Confidence levels and their visible thresholds.
- The boundary that resolution candidates are proposals, not mutations.
- The manual resolution CLI surface (`sinexctl graph entity ...`).

## What This Does Not Own

- The general knowledge-graph entity model. That is #474 — this record
  inherits its taxonomy and confidence vocabulary.
- The proposal/judgment/finalizer substrate that promotes a candidate to a
  merge. Lives in `docs/architecture/proposal-judgment-finalizer.md`.
- Per-platform contact ingestion. IRC, Messenger, Reddit, Wykop, email each
  have their own source-domain records and emit contact-bearing events; this
  layer reads those events.
- Non-person entities (organizations, projects, places). Same machinery
  conceptually, but specific resolution heuristics differ.

## Data Model

`core.entities` rows with `entity_type = 'person'`:

| Column | Meaning |
| --- | --- |
| `id` | Stable UUID |
| `entity_type` | `'person'` |
| `name` | Display name from one source context |
| `canonical_name` | User-set or auto-inferred canonical label |
| `aliases` | JSONB array of all known aliases |
| `properties` | JSONB with per-source IDs: `irc_nicks`, `reddit_users`, `facebook_names`, `email_addresses`, etc. |
| `confidence_score` | Numeric, see Confidence Levels below |

Aliases that are themselves distinct entities link to the canonical one via
`core.entity_relations(from, to, relation_type = 'ALIAS_OF', properties)`,
where `properties` records the source platform, the alias string, and a
per-link confidence.

The alias-relation pattern lets a low-confidence link survive in the graph
without collapsing the canonical record, and lets the user inspect *why*
two records are believed to be the same person.

## Resolution Sources

Two automated candidate signals, both intentionally weak on their own:

### Name Similarity

Trigram similarity over `canonical_name` (Postgres `pg_trgm`):

```sql
SELECT id, name, similarity(canonical_name, $1) AS sim
FROM core.entities
WHERE entity_type = 'person'
  AND similarity(canonical_name, $1) > 0.6
ORDER BY sim DESC
LIMIT 5;
```

When a new contact entity is created from an inbound event, the candidate
query runs, and one `entity.resolution_candidate` proposal is emitted per
strong match. The proposal carries the new entity id, the candidate
canonical entity id, the similarity score, and the evidence event id.

### Temporal Co-occurrence

If entity A (IRC nick "Eliezer") and entity B (Facebook "Eliezer
Yudkowsky") both appear in communication events within short time windows
with the same conversational partner, that is a soft signal — not enough
for auto-merge, but enough to lift a name-similarity candidate from
"possible" to "likely".

Co-occurrence runs as a derived automaton over communication events; its
output also lands as a proposal, not as a direct merge.

## Confidence Levels

| Confidence | Meaning | Action |
| --- | --- | --- |
| `1.0` | User manually confirmed | Display as resolved |
| `0.8 – 0.99` | Strong name match (same platform, identical canonical name) | Show as "likely same"; auto-emit proposal |
| `0.5 – 0.79` | Trigram similarity match | Show as "possible match"; emit proposal at lower priority |
| `< 0.5` | Low confidence | Keep as separate entity; no proposal |

These thresholds are visible defaults, not invariants. Per-platform
adjustments (e.g. Reddit usernames are unique within Reddit; identical
usernames are stronger than identical real-name strings) live in the
per-source heuristics.

## Resolution Becomes Proposal, Not Mutation

The load-bearing rule: the resolver **never merges** entities on its own.
A merge is a destructive operation that absorbs aliases and redirects all
event references — wrong merges are difficult to unwind.

Every candidate flows through the proposal/judgment/finalizer substrate:

- The resolver emits an `entity.resolution_candidate` proposal with kind
  `entity.merge`, target = canonical entity id, candidate payload describing
  the merge, evidence chain (the events that produced the candidate), and
  confidence.
- A judgment (user via CLI/TUI, or a deterministic policy) accepts,
  rejects, or modifies the merge.
- The finalizer applies the accepted merge: absorbs aliases, redirects
  references, emits the canonical merge event with provenance to the
  proposal and judgment.

This is the same pattern documented in
`docs/architecture/proposal-judgment-finalizer.md`. Resolution candidates
are not a special path; they ride the shared substrate.

## Manual Resolution CLI

```bash
# Show all entities matching a name
sinexctl graph entity search "Eliezer"

# Link an alias to a canonical entity (manual, high confidence)
sinexctl graph alias add "Eliezer" --source irc --entity "person:EliezerYudkowsky"

# Inspect or judge a pending resolution candidate (proposal substrate)
sinexctl graph candidate list
sinexctl graph candidate judge <proposal-id> --accept

# Merge two entities (still goes through finalizer)
sinexctl graph entity merge <from-id> <into-id>

# Inspect the contact network for a person
sinexctl graph entity network "person:EliezerYudkowsky" --depth 2
```

Every command that mutates canonical state is mediated by the finalizer —
the CLI is a producer/consumer of proposals and judgments, not a direct
graph mutator.

## Privacy

Person entities are sensitive: they correlate behaviour across platforms.

- Per-source identifiers in `properties` (Reddit usernames, email addresses,
  IRC nicks) inherit the privacy class of their source.
- Resolution proposals must not leak alias content into broad-scope context
  packs or search results without the same redaction the source events get.
- The resolver runs locally; no external entity-resolution services.

## Open Questions

- Whether `entity.resolution_candidate` should be a distinct proposal kind
  in the substrate, or whether it reuses the generic `entity.merge` kind.
  Default expectation: one kind (`entity.merge`), with the "this is just a
  candidate" status carried by the proposal's `status` field.
- How replay handles a model upgrade in the trigram threshold. Per
  `proposal-judgment-finalizer.md` replay rules, a regenerated proposal
  preserves prior judgments; raising the threshold may simply drop a
  candidate, and any prior judgment becomes historical.
- Whether co-occurrence proposals should auto-supersede pending name-only
  proposals against the same pair, or coexist. Default: supersede,
  preserving the older as evidence.

## Boundaries

- Do not auto-merge entities, regardless of confidence.
- Do not store entity-resolution decisions outside the proposal/judgment
  substrate.
- Do not bypass the privacy class of the underlying source when displaying
  resolution evidence.
- Do not let confidence thresholds become invisible policy. They live in
  documented config and the proposal payload.

**Related:** `docs/architecture/proposal-judgment-finalizer.md`,
`docs/architecture/inference-decision-metadata.md`,
`docs/architecture/knowledge-boundaries.md`,
issues #467, #474.
