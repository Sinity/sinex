# Querying evidence

`sinexctl query 'QUERY'` uses the same language as the daemon's query API and
MCP tool. A bare word searches events. A named table or alias selects its
rows, followed by conditions and stages:

```text
commands where exit_code != 0 sort when desc
materials where size > 1000
archive where reason = replay
```

The [parser](../crates/sinex-model/src/query/parse.rs),
[tables and fields](../crates/sinex-model/src/query/tables.rs) and
[compiler](../crates/sinex-store/src/query/compile.rs) own the language.
`events`, `archive`, `materials`, `runs`, `sources`, `operations` and
`failures` are the query tables. Aliases select produced event kinds and
expose their useful fields; for example `commands` reads fused commands,
while `events where event_type = command.executed` reads source commands.
[Architecture](architecture.md#interfaces) owns the alias and paging rules.

Conditions support comparisons, text search, Boolean combinations and
parentheses. `payload.FIELD` addresses a payload field; `^` on a path matches
that path and its descendants by whole path components. A field's declared
type checks its values. Invalid fields, enum values and unsupported requests
return errors. Missing values stay unknown.

Quoted text is a phrase, and a trailing `*` requests a word prefix. Search
uses the store's read model; the stored event and source citation remain
unchanged. Sorting has a stable identity tie breaker. Explicit limits select
a smaller answer; page size controls how much of that answer is returned at
once.

## Time and provenance

`when`, `seen` and `written` distinguish the source time, reading time and
commit time. Relative time expressions bind to the question's resolved
clock, zone and logical day start. Unknown source time stays unknown.
`near (QUERY) within DURATION` joins by time; `during (QUERY)` selects within
spans. These are temporal joins. Polylogue's `near:` instead ranks semantic
similarity.

`recall last` selects the latest recorded shell session and reads all sources
inside its recorded span. `--cwd` scopes this form to a directory; it does
not invent an activity span when no timed shell session is known. Reference
resolution accepts event, record and equivalence-key citations as described
in [vocabulary](vocabulary.md#storing-and-reading). Walks follow an event's
parents or children; a direct event reference identifies one reading, while
a record or key reference can resolve a newer reading after replay.

`as of` reads the readings visible at the requested commit position or time,
including the recorded visibility changes. It does not run today's parser
backwards over bytes. Hiding removes a reading from ordinary views and
counts while keeping its evidence; replay preserves the hide. A source
deletion instead stays queryable with `state = removed`; add `state = present`
to omit those readings.

## Answers

The [answer type](../crates/sinex-store/src/query.rs) contains `table`,
`query`, `read_as`, `resolved`, `columns`, `rows`, `total` and `page`.
`resolved` records the clock, zone, day start and commit position. Follow
`page.cursor` until it is null to read the entire answer. A continuation is
bound to its original question and access; when prior visible rows change,
the query must be asked again. Every page uses one read-only transaction.

[Output masking](architecture.md#interfaces) applies after access checks.
Queries read through the caller's class grants; the presence of a row does
not grant access to its protected payload. [Query tests](../crates/sinex-store/src/query/tests.rs)
and [API tests](../crates/sinexd/src/api/tests.rs) exercise ordering, temporal
joins, citations, visibility, grants and continuation.
