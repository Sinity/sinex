# Query

Sinex's query catalog and what its compiler adds: tables and aliases, typed payload
fields, clocks, the logical day as Sinex uses it, units and fill, joins, read context,
paging and budgets, saved queries and kept results, citation forms, and `read`, `trace`
and `related`. The grammar, spelling, AST semantics, three-valued logic, time
expressions, sealed cursors and the shared query laws belong to the common
[`query-language`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md)
crate; how the CLI, MCP and HTTP carry a query is in [interfaces](interfaces.md); what a
record and a citation identify is in [events](events.md#identity).

The same query text runs on every surface. An answer carries its rows, the total, the
resolved read context (clock, zone, commit position), a continuation, and `read_as`, the
question read back as a sentence:

```sh
sinexctl 'commands where cwd ^ /src/tide AND when in last 7d sort when desc'
sinexctl explain 'shell-sessions where cwds ^ /src/tide sort start desc'
sinexctl find rate limit
```

## Tables

Source and binding are distinct identities in every table; a source's totals join
through its registered bindings. The generated field list is
[reference/query-tables](reference/query-tables.md).

| Table | One row is |
|---|---|
| `events` | one live reading: `id`, `event_type`, `source`, `host`, `when`, `time_quality`, `seen`, `written`, `origin`, `material`, `offset`, `output`, `parents`, `producer`, `producer_version`, `run`, `text`, `key`, `class`, `commit_seq`, `written_by`, `payload` |
| `archive` | an archived reading: the event fields plus `archived`, `reason`, `operation`, `archived_seq`; history is implicit, so there is no pointer to a replacing reading |
| `tombstones` | a tombstone skeleton: event ID, source, type, `ts_orig`, time, reason, operation, material and anchor |
| `materials` | a stored material: `id`, `binding`, `parser`, `parser_version`, `hash`, `size`, `status` (`live`, `archived`, `tombstoned`), `origin_path`, `host`, `part`, `previous_part`, `events`, `first`, `last` |
| `runs` | a producer execution: `id`, `producer`, `producer_version`, `host`, `started`, `ended`, `status`, `events`, `failed`, `refused` |
| `operations` | a user act: `id`, `kind`, `author`, `via`, `scope`, `started`, `outcome`, `affected` |
| `sources` | one source and binding, with the [status fields](interfaces.md#the-sources-relation) |
| `inputs` | one input of a binding (a file, an export, an archive member): records read, events made, suppressed, matched to another input, record errors with the first unparsed form |
| `gaps` | one gap interval of a binding: `binding`, `from`, `to`, `state` (`gap`, `unknown`, `estimated`), `reason`, `missing_count` (known or unknown) |
| `shapes` | one observed payload shape of a binding over an interval: fields added, removed and retyped against the declaration and the previous shape |
| `failures` | one refused or failed record: material, anchor, binding, source, reason |
| `duplicates` | one ambiguous fusion pair and its judgment state ([fusion](derivations.md#fusion)) |
| `queries` | one saved query definition |
| `result:<id>` | a kept result, read as a relation |

```sh
sinexctl 'gaps where binding = desktop.clipboard sort from'
sinexctl 'shapes where source = health.samsung'
sinexctl 'failures where source = health.samsung'
```

### Aliases

Aliases are payload projections over registered, produced event types: `commands`,
`shell-sessions`, `activity-sessions`, `episodes`, `intervals`, `now`, `figures`,
`focus`, `visits`, `plays`, `items`, `media`, `assignments`, and one for each
[higher-layer](higher-layer.md) kind once its producer exists. An alias exists only with
its producer, so an alias never answers from nothing. Every duration projection states
its unit. `kinds [KIND]` lists which aliases expand to which event types.

### Typed payload fields

Payload paths come from the kind registry, including nested objects, optional fields and
timestamp fields. Filtering on an event type selects its schema:

```sh
sinexctl 'events where event_type = command.executed AND payload.exit_code > 0'
sinexctl 'events where event_type = command.executed sum(payload.duration_ns)'
```

- A path shared by several schemas can be used without an event-type filter when its
  declared types agree. Unknown paths and conflicting types are errors positioned in
  the query.
- `sum` and `avg` need numbers; `min` and `max` need ordered scalar values. Numeric
  payload fields sort numerically.
- Text, boolean, numeric and timestamp comparisons keep their declared types. A
  comparison on a text list matches any member; `!=` excludes every matching member.
  An empty list has no matching member.
- An unknown value stays unknown (null), and an unknown list stays unknown.
- Completion offers field names with their types at each position.

## Clocks and the logical day

- Every event has three clocks: `when` is event time (may be unknown), `seen` reading
  time, `written` commit time. Commit position orders visibility
  ([time](events.md#time)).
- Unknown time stays unknown and appears in a labelled section of an answer, never at an
  invented midnight.
- **One logical day** (a named zone and a start hour) is used by every day-grain read:
  `group by day(when)`, `recall DAY`, the brief, Today and every derived day product. Its
  semantics are the common `DaySetting`; its key and default are
  [deployment configuration](operations.md#logical-day). The zone is validated when
  the daemon starts; there is no fallback to UTC.
- Today, the brief and the reader offer the derived wake view (`since woke`) beside the
  logical day, which is shown first; the wake view never changes `group by day`.
- Ranges are half-open. Dates, weeks and logical days resolve once per answer under the
  selected zone. Elapsed durations and civil periods stay distinct across DST; a
  repeated hour carries its offset; a nonexistent hour is never invented.

## Units

- Every numeric column carries what it measures and its unit. Counts, seconds, bytes,
  ratios and physiological measures are never combined.
- Unknown exit codes are outside a success denominator. Averages are computed from sums
  and known counts.

## Fill

`fill` after the aggregates of a time-bucketed grouping returns every bucket of a finite
range:

```sh
sinexctl 'commands where when in last 7d group by day(when) count fill'
sinexctl 'commands where when in last 7d group by day(when), cwd count fill where count > 0'
```

- The grouping needs one time-bucket key and a finite range on its time field. A lower
  bound is required; an omitted upper bound resolves to the answer's `now`. Calendar
  buckets use the logical day's zone and start.
- Every bucket carries a coverage state from the one
  [source-evidence path](interfaces.md#the-sources-relation): `observed`, `partial`,
  `recorded_unknown_coverage`, or `missing` with its reason. Run lifetime alone never
  establishes coverage.
- A zero is shown only where the opportunity to observe is established; otherwise the
  aggregates of an unrecorded bucket are unknown.
- The answer states whether dimension combinations are the observed combinations in the
  range or a supplied domain.
- Sorting, filtering, further grouping and paging operate on the filled rows.

## Joins and relations

- `near`, `before`, `after`, `during` and `containing` keep one outer row when any inner
  match exists and return the nearest match and its signed offset. Ties break
  deterministically; an all-pairs result declares its changed grain.
- Point-in-span and span overlap are distinct. Open intervals are provisional;
  coarse-precision records carry their precision.
- An inner relation with its own read-context stage runs at that context or is refused
  as incompatible; it is never accepted and ignored.
- Shared material and captured ancestors are found through provenance, never time
  proximity.
- Sequences (`seq`), `exists` subqueries, attached child relations and `distinct` over
  declared identity behave as the shared crate's laws state.

## Read context, paging and budgets

- **An answer resolves once:** `now`, the zone, the day start, the query text and its
  bindings, schema and definition versions, grants and the commit position. Every page
  shares them.
- `as of POSITION` reads visibility at that commit, including archive, restore, replay
  and tombstone ([as-of reads](lifecycle.md#as-of-reads)).
- Continuations are sealed to the question and the caller-visible context. A changed
  definition, grant or incompatible change refuses with `stale_cursor` and says how to
  restart. Totals count rows after semantic stages.
- **Four budgets stay apart:**

| Budget | Belongs to | Changes the answer |
|---|---|---|
| semantic limit (`limit`) | the question | yes |
| page size (`--page-size`) | the transport | no |
| presentation bytes | the display | no |
| deadline (`--deadline`) | the execution | no |

- A presentation budget is measured in the destination's encoded bytes (or tokens, with
  a declared tokenizer revision), counting notices, metadata, citations and cut markers.
  It cuts whole items with an omission count and a continuation.
- A deadline miss is `timeout` naming a narrower query, never an empty success.
  `--deadline` raises it without limit.
- Graph walks, source details and `sql` output page or stream at the execution
  boundary. There is no depth cap and no fetch of a whole result before slicing, because
  a cap would truncate valid answers.
- `sql` is read-only SQL on the reader role over the documented relations, under the
  same output rules as every output; the reader role cannot reach withheld fields or keys
  ([privacy](privacy.md)).

## Saved queries

A query can be saved, named and invoked inside another query. Ruling, 2026-07-09:
"saving a query in a database, and returning that as a result -- with proper ids …
THIS. THIS IS WHAT I MEANT"
(cc-queued:3b0038ec-234c-44b8-bb88-fe222b44fe0f@2026-07-09T20:06Z).

```sh
sinexctl 'commands where cwd ^ $directory since $period count' \
  save in-directory --parameter directory:text --parameter period:duration
sinexctl 'query:in-directory(directory="/src/tide", period=7d)'
sinexctl 'queries'
```

- **A saved query has a content identity,** `sinex:query/<hash>` over its canonical AST
  and declared parameter types. A name is a separate authored record pointing at an
  identity; renaming or re-pointing a name never changes the identity, and saving again
  under the same name and author re-points it.
- Definitions are authored records (a request material plus a `query.saved` event)
  with the [computed author](interfaces.md#computed-authorship). Definitions of one name
  by different authors are ambiguous until qualified by author (`query:NAME@AUTHOR`) or
  by identity; resolving a collision never requires archiving someone else's record.
- **Parameters** are named scalars of type `text`, `integer`, `number`, `bool`, `time`
  or `duration`. They stand in value positions only, never for fields, tables or clause
  structure; a quoted value holding query punctuation stays one value. Every declared
  parameter is used and supplied exactly once. Missing, duplicate, unused and wrong-type
  arguments, recursion and incompatible stages are refused.
- Saving checks fields and stages with typed placeholder values; invocation binds the
  supplied values into the parsed question before execution. A parameter that selects an
  event type leaves the schema unknown at save time, so payload fields whose types differ
  across schemas need a fixed event-type selection.
- Relative times resolve at invocation and stay fixed for the continuation.
- Later stages operate on the saved question's result, including grouped rows. Nested
  references record composition edges, which take part in version checks and completion.
- An archived definition is unavailable, and a changed definition invalidates the
  continuation of the previous question.

## Kept results

- `QUERY keep` stores the answer as a derived event keyed `query:<hash>` plus its
  resolved read context: its rows by reference, its total, its context and its author.
  It is cited as `sinex:result/<id>` and read as the relation `result:<id>` inside
  another query, where it reproduces at its read context.
- Any answer that is cited, delivered (a brief, a signal delivery, a context pack) or
  produced by a recipe is stored the same way. An ordinary interactive read is not.

## Citation forms

Sinex citations always carry the `sinex:` prefix, so they never collide with Polylogue's
references. What each identity means is in [events](events.md#citations); the forms are:

| Form | Names | Across replay |
|---|---|---|
| `sinex:ev/<id>` | one reading, live or archived | does not follow; preserves what was cited |
| `sinex:record/<material>/<offset>[/<type>[/<output>]]` | the current reading at those record coordinates | follows replay of the same coordinates |
| `sinex:key/<source>/<type>/<key>` | the current reading under a source record key (the key encoded reversibly) | follows replay and re-import |
| `sinex:at/<RFC3339>/<type>` | the live output of a registered spanning type that contains an instant | resolves to the current span; several matches are an error |
| `sinex:window/<from>/<until>` | a bounded recall question over a half-open absolute window, not an event | a question; mutations and `trace` refuse it |
| `sinex:material/<id>[@<range>]` | a stored material and optionally a byte, page or record range, with its content hash | stable |
| `sinex:query/<hash>` | a saved query | stable |
| `sinex:result/<id>` | a kept result | stable |

- A shell session's span includes its last recorded command; other spans exclude their
  upper bound.
- **Derived claims carry both** their exact reading and their current record or key
  reference where one exists, plus the material range where relevant.
- **Resolution** happens under the caller's grants and returns a typed state: `live`,
  `archived`, `tombstoned`, `changed` (the current reading differs from the cited one),
  `ambiguous` (with candidates), `withheld`, `unavailable` or `removed_at_source`. A
  known form that cannot be resolved reports its form and reason, never an empty result,
  and never falls back to searching other object types.
- One resolver serves every user of these forms: Polylogue, the wiki, briefs, context
  packs and the reader.

## Read, trace, related

- **`REF read`** returns any object in full:
  - an event: its payload under the output rules, three clocks, origin, producer and
    version, its state (live, archived or tombstoned) and archive history, and usable
    citations;
  - a material: digest, length, input and origin, part sequence, parser and version,
    status, record accounting, and a continuation over its events;
  - a run: producer and version, lifetime, progress, failures, refusals, events;
  - an operation: author, via, scope, outcome and affected references.
- **`REF trace [--down] [--bytes]`** walks provenance up to the stored byte range, or
  down to descendants. Each event appears once and distinct branches are kept;
  unavailable, withheld, archived and tombstoned links are named; continuation reaches
  any depth. `--bytes` prints the stored range on a terminal only.
- **`REF related`** lists events sharing a native key (URL, path, commit, thread) with
  the reference, naming the matching field and the key's domain.

## AI conversation text

Sinex does not index the text of AI conversations; Polylogue holds it. When Polylogue
runs on Sinex, the Polylogue daemon evaluates a mixed query: names are bound from the
parsed query with the `pl.` and `sx.` prefixes, the Sinex parts are compiled for and run
by Sinex through its API, results join on references and time, and the answer reports
the position it read in each product and its lag. Sinex callers reach AI text through
that route ([cross-system queries](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md#cross-system-queries)).

## Laws

- **paging**: page sizes 1 and 50 and `--jsonl` give the same ordered rows; inserts,
  replays and archives between pages either keep the selected view or refuse
  `stale_cursor`.
- **presentation-budget**: a presentation budget counts the encoded destination
  including metadata and citations; whole items are cut with an omission count; the
  stored product is unchanged.
- **budgets-stay-apart**: changing page size, presentation budget or deadline never
  changes the selected rows or the total; only the semantic limit does.
- **timeout-not-empty**: a query past its deadline ends `timeout` naming a narrower
  query, never an empty or partial success.
- **no-depth-caps**: traces deeper than 100 levels and graphs wider than a page are
  complete through continuation.
- **citations-across-replay**: `ev` preserves the cited reading across replay; `record`
  and `key` follow; `at` over two spans is ambiguous; a window refuses mutation; a known
  form that cannot resolve reports its state.
- **saved-identity**: two definitions with equal canonical AST share one
  `sinex:query/<hash>`; renaming keeps the identity; a kept result is queryable as
  `result:<id>` and reproduces at its read context.
- **parameters-are-values**: a parameter never binds a field, table or clause; missing,
  duplicate, unused and wrong-type arguments and recursion are refused.
- **logical-day-agrees**: across 23- and 25-hour days, a host zone different from the
  configured one, and a changed boundary, every day-grain surface agrees; the zone never
  falls back to UTC.
- **unknown-is-not-zero**: a failed-read interval inside a live run, an unchanged poll
  and a never-read binding give `missing`, `observed` and `unknown` respectively in fill,
  status, recall and alerts.
- **units-never-mix**: every numeric column states its unit, an expression combining
  different units is refused, and an unknown exit code never enters a success
  denominator.
- **inner-context-honoured**: an inner relation with its own read-context stage runs at
  that context or is refused; it is never silently run at the outer context.
- **sql-under-output-rules**: `sql` returns the same rule effects as every other surface
  and cannot read a withheld field or key.
