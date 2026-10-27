# Queries

CLI, MCP and HTTP run the same query text. `sinexctl query '…'` returns rows,
the total, resolved clock and commit position, and a cursor. `--page` changes
transport size; `limit` in the query changes the answer. `read_as` explains the
question that ran. Existing filters, time joins, grouping, sorting, provenance
walks and `as of` compose as stages. Keywords ignore letter case, and spaces
around the colon in `query:name` are optional.

## Typed payload fields and completion

Payload paths use the Rust event registry, including nested objects, optional
fields and timestamp fields. Filter an event type to select its schema:

```sh
sinexctl query 'events where event_type = command.executed and payload.exit_code > 0'
sinexctl query 'events where event_type = command.executed sum(payload.duration_ns)'
sinexctl query-complete 'events where event_type = command.executed and payload.dur'
```

Completion returns field names with their types. A path shared by several
schemas can be used without an event-type filter when its declared types
agree. Unknown paths and conflicting types are errors. `sum` and `avg` need
numbers; `min` and `max` need ordered scalar values. An unknown value remains
null. Numeric payload fields sort numerically.
Text, boolean, numeric and timestamp comparisons keep their declared types.
Text-list comparisons match any member; `!=` excludes every matching member.
Unknown lists remain unknown, and an empty list has no matching member.

## Empty time buckets

`fill` follows the aggregates in a time-bucketed grouping:

```sh
sinexctl query 'commands where when in last 7d group by day(when) count fill'
sinexctl query 'commands where when in last 7d group by day(when), cwd count fill where count > 0'
```

The grouping needs one time-bucket key and a finite range on its time field.
A lower bound is required; an omitted upper bound resolves to the query's
`now`. Calendar buckets use the configured zone and logical day start.
Dimension combinations come from the groups that were recorded in the range.
Recorded groups remain in the answer, including states whose start precedes
a recall window. Recall bounds intersect any narrower filter on `when`; other
bucket fields need their own bounds. Every filled row has `coverage`: `recorded`
for a group with records, or `no_data` for an unrecorded bucket. All aggregates
in unrecorded buckets are null. A run's lifetime alone does not establish that
a source could have recorded throughout the bucket. Sorting, filtering, further
grouping and paging operate on the filled rows.

## Marks and citations

The operator can mark now, or a moment being recalled:

```sh
sinexctl mark 'Investigate the test failure' --at 2026-09-14T08:01:00Z --cwd /src/tide
sinexctl recall mark
sinexctl recall 'mark 2 ±5m'
```

Marks are `operator.mark` records, stored with their request bytes, actor and
recording time. The marked time has operator quality. A client may supply its
observed directory, command, application and title. No event IDs are needed at
mark time. Recall selects the newest visible mark by marked time, with ID as
the tie breaker, and returns its note and context at one commit position.
`mark 2` selects the second newest. Recall also resolves command and focus
links by equivalence key and sessions by containing time. An absent or
unavailable mark is explicit.

Citations work through `show` and event citations through provenance commands:

| Form | Resolves to |
|---|---|
| `sinex:ev/ID` | One event reading, live or archived |
| `sinex:record/MATERIAL/OFFSET[/TYPE[/OUTPUT]]` | The live reading at stored coordinates |
| `sinex:key/SOURCE/TYPE/KEY` | The live reading with that equivalence key |
| `sinex:at/2026-09-14T08:01:00Z/shell.session` | The live output whose declared span contains that instant |
| `sinex:window/FROM/TO` | A recall query over the half-open absolute window |

`at` citations use an absolute RFC3339 time and the registered spanning event
type. They resolve again after replay or changed bounds. No matching visible
output is an error; several matching outputs require an event or record
citation. Shell session bounds include their last recorded command; other
spans exclude their upper bound. A window citation is a query, so it cannot be
used as the single event of a hide, trace or excision request.

## Saved questions with parameters

```sh
sinexctl query-save in-directory 'commands where cwd ^ $directory since $period count' \
  --parameter directory:text --parameter period:duration
sinexctl query 'query:in-directory(directory="/src/tide", period=7d)'
sinexctl query-list
```

`query:NAME` follows Polylogue's saved-query naming. Parameters are explicit
named scalar arguments. Types are `text`, `integer`, `number`, `bool`, `time`
and `duration`. Every declared parameter must be used and supplied exactly
once. Parameters stand in value positions, never in field names or clauses;
a quoted value containing query punctuation stays one value. The ordinary
parser and schema checker validate fields and stages with typed placeholder values when saving, then bind the supplied values before execution.
Later clauses operate on the saved question's result, including grouped rows.

A saved definition is a `query.saved` event keyed by name and author. Saving
again under that name and author replaces its live reading through normal
admission. Visible definitions from several authors with the same name are
ambiguous when invoked without an author. Use `query:NAME@AUTHOR` to select one,
for example `query:in-directory@operator(directory="/src/tide", period=7d)`.
`query-list` includes each definition with its author in `by`. Quote a selector
containing spaces: `query:"shared@another author"`. The recorded author is the
caller's coarse actor (`operator` or `agent`), rather than an individual identity.
Hidden definitions are unavailable, and a changed definition invalidates a
continuation of its previous question. Marks and saved definitions follow the
guarded class used for authored requests.

## Context, timeline and source evidence

```sh
sinexctl context
sinexctl context --since 30m --until 2026-09-14T09:00:00Z
sinexctl timeline 'where source = terminal.atuin-history since 1d'
sinexctl query 'sources fields source, continuity, gaps, drift, record_failures'
```

`context` reads the past two hours by default. An explicit end anchors a
lookback duration; absolute start and end times also work. Its answer is recall,
including states whose spans overlap the window, a source-by-source
account and the latest visible observation from each source. The digest is
independent of the page size, so a busy source does not crowd the others out.
Recall digests apply the question's row filters, joins and limits before any
grouping. `timeline` reads ordinary event queries in time order. Both preserve
paging and query metadata on every surface.

Source evidence stays in the `sources` query table. `continuity` says whether
recorded live run lifetimes are available, or coverage is unknown. `gaps` counts
holes between those lifetimes, merging overlapping runs; zero does not establish
coverage outside them. The `gaps` table pages through each gap's source, next
run, start, end and reason. `drift` reports a status and count of visible events
with parser-recorded unknown fields at the selected commit position. Without
such evidence it says `unknown`; it does not infer that an input schema stayed
unchanged. The `drift` table pages through those events with the ordinary event
fields. `record_failures` counts parse failures; the `failures` table supplies
their material, offset and reason. All views apply the caller's visibility and
normal output masking.

```sh
sinexctl query 'gaps where source = desktop.clipboard sort start'
sinexctl query 'drift where source = health.samsung fields id, when, payload'
sinexctl query 'failures where source = health.samsung'
```
