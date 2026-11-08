# Interfaces

Every way to reach Sinex: the operation table and its transports, callers and their
powers, the `sinexctl` CLI, MCP, status, signals and stall alerts, `follow`, and the
latency budgets. The query catalog and its behaviour are in [query](query.md); recall
and agent arrival in [recall](recall.md); Today and the reader in
[web](web.md). The shared mechanics (the `Operation` trait, request and answer
envelopes, the failure-to-exit-and-HTTP table, latency classes and deadlines, SSE
framing, the status figure format) are the common
[`command-surface`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/command-surface/README.md)
crate's, including the answer contract every surface uses (envelope, section status,
value states, budget fitting, errors); the signal rule schema, evaluator and route
function are common
[signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/signals/README.md).

## Principles

- **One question, one answer, every surface.** One parsed question, one resolved read
  context and one server-side answer serve the CLI, MCP, HTTP, `follow` and the
  reader. Page size, presentation budget and deadline never change what was selected.
- **One entry point for questions.** The query is the entry point; a new capability
  arrives as a table, an alias, a stage or a verb on rows, never as a top-level
  command that restates a query. (ruling 2026-05-25, cc-queued:c2404522-7bba-4de3-936d-e08deb14262c@2026-05-25T00:40Z).
- **Agents are primary users** and get no fewer powers than the operator; what
  distinguishes the operator is authorship, which the daemon computes. (ruling 2026-06-01, journal 2026-06-01 06:54).
- **Legible and honest.** Every answer says what was observed, what was derived, what is
  unknown and what was left out of the display. A quiet source is not a failed one; a
  running process is not evidence of capture; a command with no recorded end is "end
  not recorded", never "running"; proximity is not provenance.
- **Captured text is data.** Every result holding captured text carries the data
  notice, delimits payload text per row and escapes control sequences. What is masked,
  hashed or withheld is decided only by the operator's configured rules, applied
  identically on every output ([output parity](privacy.md#output-parity)).
- **Nothing unasked without a rule.** No streaks, scores, gamification or unsolicited
  behavioural advice; anything Sinex surfaces unasked comes from a declared signal rule.

## Operation table

- **One typed operation table** declares every command once. The CLI parser, the socket
  and HTTP routes, the MCP tools (with `outputSchema` generated from the answer types),
  help, completion and the shipped agent skill are generated from it. A command missing
  from the table fails closed.
- **Every feature reachable through MCP is in the CLI**, and the reverse, unless the
  table records a reason. (ruling 2026-07-29, cc-queued:d6f4b0c3-840b-47b6-bd57-5f1bd6c9c7d4@2026-07-29T01:12Z).
- **The write path.** Handlers hold only the read pool. Every write (stage, `record`,
  note, duplicate decision, curation, saved query, signal rule, archive request) goes to the one
  [admission function](admission.md) as a request material plus events.
- The command list will be rendered into [planned reference/commands.md](reference/README.md#planned-pages).

## Transports

- `sinexd` serves a Unix socket and an HTTP listener carrying one typed request
  protocol: `POST /v1` with a tagged request, `GET /v1/commands` for the table's
  metadata, SSE for `follow`. There are no dotted method aliases.
- The listen address and authentication mode are deployment configuration (default
  loopback with a peer-uid check; a token file for any other address) set through the
  NixOS module, the config file or environment variables
  ([configuration](operations.md#configuration)). Phone or remote access is that
  configuration, not a product feature.
- **Requests are strict.** Unknown fields are refused. A request that changes state
  (archive, restore, replay, tombstone, stage, recompute) has no default scope: an
  omitted scope is an error, never "everything".
- **Health** is its own route: up or down, without internal error text, separate from
  capture health.
- **Version.** The server reports its version; the client warns on a mismatch.
- **The daemon is required.** Ordinary commands go through `sinexd`. Only the declared
  offline diagnostics (`verify`, `status --offline`, `store verify --offline`) read the
  database directly through the reader role, and completion works from the published
  catalog file. (ruling 2026-09-14, cc:652ae14f-a036-496b-abfa-42e7574c9ec9:b8a68d64-772f-4d8c-8e95-435ce601d245).

## Failures

One `Failure { kind, message, details, position }`, mapped once to an exit code and an
HTTP status by the shared table in `command-surface`. Sinex uses the shared kinds and
adds one:

| Kind | Raised when | Exit |
|---|---|---|
| `stopped_early` | an archive, restore, replay, recompute, stage or tombstone operation did not finish | 5 (degraded); never reported as ok |

- MCP returns typed failures, never a success with empty rows, for a refusal, an
  unknown capability or a timeout.
- A refusal after the first committed batch of a lifecycle operation ends
  `stopped_early` naming what moved; `resume OPERATION` continues it
  ([lifecycle](lifecycle.md)).

## Callers

### One command set

The operator and agents hold the same command set; there are no ordered access tiers.
(ruling 2026-07-03, cc-queued:30f5e650-7033-4423-9f91-2b66ebd1e6b2@2026-07-03T13:20Z).

- **Grants are per-credential configuration**, seeded with defaults by the NixOS
  module, never code constants. Which output a caller sees is decided by the output
  rules, which are the same for every caller ([privacy](privacy.md)); nothing is
  refused because the caller is an agent. Privacy rules may be written by any author;
  status lists those written by anyone but the operator as non-operator changes.
- The credential gates grants; it is not an identity.
- **An act reserved to the operator is an explicit configured rule with its reason**,
  listed in configuration and in `help`, never inferred from the token. The seeded
  reserved acts:

| Act | Rule | Reason |
|---|---|---|
| confirming a tombstone's cascade preview | any caller requests and previews; the operator confirms interactively (default; open operator question) | tombstoning is the one irreversible deletion ([tombstone](lifecycle.md#tombstone)) |
| `same`/`distinct` duplicate decisions that govern fusion | every caller records decisions; the operator's govern, and a per-family setting admits others' | duplicate decisions shape every fused record ([fusion](derivations.md#fusion)) |

### Computed authorship

- Every write records `written_by`: `author` (`operator`, `agent:<session>`,
  `agent:<origin>`, `script:<unit>`, `unattributed`) and `via` (`cli`, `mcp`, `web`,
  `hook`, `daemon`, `import`, `desktop`).
- **No request field names an author.** The client reports its harness identity (the
  variables supported harnesses already set: `CLAUDE_CODE_SESSION_ID`,
  `CODEX_THREAD_ID`, `GEMINI_CLI`, `HERMES_SESSION_ID`; for MCP, the harness
  connection), and the daemon decides:
  - the nearest harness wins;
  - `operator` only through the operator's own channels with no harness identity
    (an interactive terminal without harness variables is the operator);
  - `script:<unit>` from the calling process's cgroup unit;
  - otherwise `unattributed`.
- No job runner is recognised by name. A launcher that forwards its caller's harness
  variables keeps the caller's session.
- `events where written_by.session = S` lists everything one agent session wrote.

### Agent writes are visible and undoable

- Status lists the last week's agent writes by session and kind.
- **`undo --session S [--dry-run]`** reverts every write one session made, listing it
  first and recording each reversal with its own author. It archives the session's
  notes, duplicate decisions, curation, saved queries, signal rules and `record` outputs; archives
  what the session's stages admitted; and undoes the session's replays by the ordinary
  [replay undo](lifecycle.md#undo), possible at any later time. A tombstone confirmed in
  the session is listed as not revertible. A second undo restores what the first
  archived.

### Derived conclusions: `record`

`record KIND PAYLOAD --parent REF…` (a `sinex_write` row over MCP) writes an agent's derived
conclusion: a registered kind, a payload matching its schema, at least one parent
citation, the computed author, and the producer `agent:<session>` with its model as
producer version. It passes ordinary admission and is withdrawn by its author's archive
act or by session undo. Programs that derive continuously use the ingress as
[outside handlers](producers.md#outside-consumers-and-handlers), not `record`.

### Agent guidance shipped with Sinex

- Sinex ships a skill text **generated from the operation table**: what Sinex holds, the
  core operations, example queries with their `read_as`, the data notice, and the rule
  "cite event IDs". Sinex's NixOS module installs it into each harness's skill
  directory; `sinexctl skill print` prints it for any other installation. (ruling 2026-10-18, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:12f1a067-cc5b-41b7-9209-297099d6b0c1).
- The MCP `instructions` field carries its summary; each tool description is short and
  says when to use the tool.
- Context reaches agents without their asking, through Polylogue's session-start brief
  ([agent arrival](recall.md#agent-arrival)).

## CLI

`sinexctl` is product UI, not developer tooling. (ruling 2026-02-12, claude.ai:7abe2ed3-2651-4e23-a23a-48a61c82bb85:019c5366-a07e-7623-9477-9d8a857b0aa3).

### Grammar

- **One grammar rule, shared with Polylogue**
  ([Polylogue's grammar](https://github.com/Sinity/polylogue/blob/master/docs/interfaces.md#grammar)):
  the first argument is a command name, otherwise the start of a query, which needs a
  signal (a field, a table, a reference, `find`, or one argument holding whitespace or
  query punctuation). An unsignalled first word is refused within the local budget
  with the nearest command and the `find WORDS` form.
- **Verbs act on rows after a query:** `QUERY read`, `QUERY trace`, `QUERY follow`,
  `QUERY keep`, `QUERY save NAME`, `QUERY archive`, `QUERY replay`; `REF read`,
  `REF trace`, `REF related`.
- **There is no `query`, `search` or `timeline` command.** Bare words in a signalled
  query already search, and a timeline is the default rendering of an event query
  sorted by time. `recall` is a command because it assembles several queries into one
  answer ([recall](recall.md)); the current `context` and `brief` verbs fold into it.
- **Groups, not hyphens:** `library add|import|save` (curation acts on a query or
  reference: `QUERY tag add`, `QUERY judge SCALE`; [library](library.md#surfaces)),
  `privacy rules|rule|class|verify`, `study`, `experiment`, `forecast`, `place`
  ([personal evidence](evidence.md#surfaces)), `sources [NAME] | discover`,
  `stage list|show`, `parser try`, `parsers`, `hosts`, `placement`, `kinds [KIND]`,
  `store verify`. Rules, saved queries and lanes are authored records written through
  `write` (`write signal:NAME …`). Decided target (2026-11-01): this replaces the
  current `library tag|untag|set|…`, `analysis` and `signal add|…` groups.
- **Export is `read --to`.** `QUERY read --to DIR` writes the bundle atomically with its
  manifest: the question, its resolved read context, the selected records, references to
  the material bytes they came from, what was left out, and the replay recipe.
  `--to FILE` writes one document. There is no `export` verb. Results produced outside
  from a bundle come back as attributed derived events through `record` or the ingress,
  never as edits.
- **No aliases.** Each name is fixed once; [vocabulary](vocabulary.md) holds every
  command word and flag.

### Alignment with Polylogue's CLI

Each product's concepts are its own; the CLI mechanics are shared through
`command-surface` and `query-language`:

- the grammar rule above, with Sinex's own catalog; `|` between stages and the rest of
  the query-language spelling;
- the exit table, the request envelope with deadline classes and lifting, the answer
  envelope and SSE framing;
- the grammar-position completer, a catalog file published by the daemon, and shell
  registration scripts;
- global flags with the same names and meanings: `--json`, `--jsonl`, `--ids`,
  `--page-size N`, `--cursor C`, `--fields`, `--why`, `--deadline MS`; Sinex's own
  target flag is `--endpoint`;
- space-separated command groups, and MCP names joined with `_`;
- the same words for the same mechanical acts: `read` (one thing rendered), `follow`
  (a stream of new rows), `trace`, `changes --since POS`, `explain`, `sql`, `status`,
  `keep`, `save`, `judge` (write an evaluative record), `compare` (a read), `verify`,
  `mcp`, `completions`, `setting`, and `read --to` for writing out. A vocabulary check
  compares both products' tables.

Not shared, because they are Sinex's own concepts: the lifecycle verbs (archive,
restore, replay, tombstone, stage), library curation, privacy rules and personal
evidence. **Recall** means the same act in both products: build a pack about a subject
from what is already held. Sinex's verb is `recall SUBJECT`; Polylogue's is its `recall`
recipe. **Brief** is only Polylogue's session-start recipe; Sinex's part of it is the
here pack.

### Lifecycle and staging verbs

The semantics are [lifecycle](lifecycle.md)'s and [staging](materials.md#staging)'s;
the surfaces are:

- `stage PATH… [--parser NAME…] [--export-date DATE] [--account NAME] [--follow]`,
  answering with the operation and the inventory of every staged member at once, and
  with `--follow` ending with each product's accounting and `stopped_early` when it did
  not finish ([staging](materials.md#inventory-and-accounting)); `stage-list
  [--active]` and `stage-show OPERATION` read stages later. With the daemon stopped,
  `sinexd admin stage` runs the same command with its own writer;
- `archive REF | QUERY | --material ID… | --run ID | --source S [--after T] [--before T]`
  and `restore` over the same scopes or `--operation OP` (the chains that operation
  archived), each refusing when any part cannot move; `--compare` prints the cascade and
  what would refuse and changes nothing;
- `undo OPERATION [--compare]` archives what the operation made live and restores what it
  archived, for an archive, a restore, a replay or an undo;
- `resume OPERATION` continues an archive, restore, tombstone, undo, replay or stage that
  ended `stopped_early` or whose daemon stopped mid-run;
- `replay QUERY [--parser NAME] [--compare]` and its undo, with `--compare` showing per-field changes; `recompute DERIVATION [--compare]` and
  `recompute DERIVATION --switch` ([versions](derivations.md#versions));
- `tombstone` over the same scopes as `restore`, only on archived chains; `--compare`
  previews the cascade, and the act is reserved to the operator
  ([one command set](#one-command-set));
- `backup hold` and `backup release HOLD`, called by the deployment's backup tool
  around a copy ([backup](operations.md#backup-and-restore));
- each records an operation with its computed author, and the preview is shown on
  every surface before a state change runs.

Archive and restore take something out of view and bring it back; tombstone is the
one irreversible deletion; a refused material is read again by replay.

### Output

- Typed printers per operation, from its answer type. Modes: a table (default on a
  terminal), `--json` (one complete document), `--jsonl` (rows streamed after a
  documented metadata frame), `--ids`. An answer rendered as text (a pack, a panel,
  `explain missing`) prints that text once by default; its JSON document is printed only
  with `--json`.
- Colour only on a terminal with `NO_COLOR` unset. Colour and weight encode status only
  (ok, stale, missing, provisional, error), never source identity. Provisional rows look
  provisional; unknown is printed "unknown", never blank. Widths count display columns.
  Local times carry their offset when ambiguous. Captured control characters are
  escaped.
- Long output on a terminal streams into `$PAGER`, fetching later pages only as the
  pager reads.
- **Errors** are one line of what failed and what to do, positioned in the query, with a
  runnable correction when one exists; with `--json`, an error object on stderr. An
  empty result says why: nothing matched, the source was not observed, or the rows are
  outside the caller's grants.
- `--deadline` has no upper bound; `--deadline none` lifts it.

### Completion, picker and references

Completion and fuzzy finding are part of the product. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:dad489d4-a1b8-48bd-bbc0-26262f34b262).

- **Completion runs in the CLI process from a catalog file `sinexd` publishes**: tables,
  aliases, fields per event type from the kind registry, sources, hosts, recent event
  types, saved queries. The daemon rewrites it atomically after committed batches. It
  works with the daemon down and never waits on it. The grammar-position completer is
  the shared `query-language` function; MCP `completion/complete`, HTTP and the reader's
  query box call the same function.
- `completions SHELL` prints registration scripts for bash, zsh and fish.
- **One embedded picker** (`nucleo` matching) opens only when stdin and stdout are
  terminals and no machine format or harness is present. Otherwise an ambiguous
  reference refuses (exit 2) with the candidates and the exact non-interactive call.
  `pick` prints chosen references for shell widgets.
- **References:** full IDs; unique prefixes of at least 8 characters; `@N`, `@N.M`,
  `@N-M`, `@*` from the previous list in this terminal (a row file holding the list's
  query, read context and next cursor, so `next` continues it); `last`; the
  [citation forms](query.md#citation-forms). Over MCP, `@N` names the agent session's
  previous result. `@` rewriting applies only to reference and query arguments.
- There is no REPL and no full-screen terminal interface: the shell with completion,
  `@N`, the pager and the picker is the interactive loop, and the [reader](web.md) is
  the exploration surface.

### Help and orientation

- Help is generated from the table; no command is hidden; each entry says what it does
  and one usable next step, and its examples run as tests.
- `explain QUERY` prints the query read back as a sentence (`read_as`), its field
  phrases and the resolved read context.
- `kinds [KIND]` lists event types with counts, schemas, time quality and the aliases
  that expand to them; `kinds export DIR` writes JSON Schemas for outside producers.
- `setting` prints the effective configuration and where each value came from.
- **Bare `sinexctl` renders [Today](web.md#today)**; `status` stays explicit.

## MCP

- `sinexctl mcp` (stdio, on `rmcp`) advertises the operations the table marks for MCP,
  with strict argument schemas and `outputSchema`, grouped into tools by the common
  grouping rule.
- **Eight tools** (decided target, 2026-11-01). Agents bypass a long flat tool list by
  reaching for raw SQL, so Sinex groups its rows into eight tools named like
  Polylogue's:

| Tool | Rows |
|---|---|
| `sinex_status` | daemon, sources summary, hosts, attention, budget |
| `sinex_query` | every unit (sources, inputs, coverage, items, encounters, figures, studies, …), `sort by rank(scale)`, `judge next`, `signal:NAME backtest`, the `wait` option |
| `sinex_read` | any reference: an event, a material range, an item, a pack's expand handle, a study result; `parser try` (a dry run that admits nothing) |
| `sinex_trace` | the provenance walk |
| `sinex_recall` | every recipe: resume, moment, day, period, here, project, compare, today, study, experiment |
| `sinex_write` | notes, curation, comparisons, scales, rules and watches, saved queries and lanes, instruments, studies, forecasts, places |
| `sinex_act` | stage, replay (compare by default), archive, restore, tombstone, undo, library add, import, save, open and viewed, experiment acts, notify, ack, snooze |
| `sinex_explain` | syntax, units, fields, a query's reading, metrics, methods, missing cells |

  `sinex_sql` stays a local operator tool, not an agent tool. The deployed flat tools
  (`sinex_brief`, `sinex_hide`, `sinex_unhide`, the `sinex_library-*`, `sinex_analysis-*`,
  `sinex_forecast-*` and `sinex_privacy-*` tools and `sinex_comparisons`) fold into these
  rows; hide and unhide are the lifecycle's archive and restore.
- Every answer uses the common answer contract: the text part is the rendering, rows
  appear as JSON only with `format: json`, and a budget cuts whole lines with counts of
  what was left out and expand handles.
- `follow` is available as a resumable subscription of `sinex_query`, with the same positions as SSE.
- Output rules, the data notice and per-row delimiting apply exactly as on HTTP and the
  CLI. `@N` and `since last_seen` are per agent session; one client's state never
  affects another's.
- A query that would exceed MCP's deadline is refused with its estimate and the CLI
  form, never answered differently.
- MCP never returns raw material bytes.

## Status

### The status view

One status view covers:

- the daemon, writer, bus and ingress backlog, and database;
- per source, its run state and coverage;
- per derivation, its position, lag, pending version switch and compare result, and last
  failure ([versions](derivations.md#versions));
- fusion's ambiguity counts;
- replays, stages and recomputes in progress;
- **refusal figures**: the refusal rate per source and kind, and kinds whose every
  material is refused;
- failures; archive size and growth; disk free and days until full (warning below 14
  days);
- the last week's agent writes by session;
- outside consumers' positions and lag ([outside handlers](producers.md#outside-consumers-and-handlers)).

Every figure uses the shared figure state and verdict of `command-surface`. Sinex's
domain fields keep their own values beside it, each with a stated mapping to the verdict:

| Field | Values |
|---|---|
| `last_run` | `new`, `repeats`, `nothing`, `not_run` |
| gap state | `gap`, `unknown`, `estimated` |
| input state | `capturing`, `backlogged`, `quiet`, `stalled`, `held`, `unavailable(cause)`, `excluded(reason)`, `finished` ([input states](sources.md#input-states)) |
| coverage | `complete`, `gap(reason)`, `unknown`, `outside_span`, `not_yet`, `excluded`, `unavailable(cause)`, `held` ([completeness](sources.md#completeness)) |

A run that found only repeats is healthy; a staged export is never stale; a missing cell
carries its reason. Status reads self-observation events, so nothing is recounted on the
request path. There are no scheduled verify, scrub or check runs, because invariants
hold by construction: `store verify` is on demand, reports mismatches and repairs
nothing. `status --offline` reads positions, the last commit and the writer lock's
holder through the reader role while the daemon is down.

### The sources relation

- `sources` is the one source status relation and `sources NAME` the one detail view.
  Gaps, inputs and payload shapes are the query tables `gaps`, `inputs` and `shapes`
  ([tables](query.md#tables)), read and paged like any query, so there are no separate
  gap, continuity, drift or readiness commands.
- Fields: `source`, `binding`, `shapes`, `version`, `event_types`, `purpose`,
  `exclusion` and its reason, `state`, `last_run`, `expectation`, `silent_after`, `every`,
  `expires_after` and the remaining margin, `coverage`, `behind`, `rate`,
  `refused_rate`, `shape_changed`, `days_with_data_30d`, `gaps_30d`, `last_success`,
  `last_error`.
- **One source-evidence path** classifies sources for fill, panels, recall, status and
  alerts: durable poll outcomes (success, unchanged, failure), cursor positions, gap
  records with their missing count, record failures and shape observations, read under
  the answer's read context. Never source-name prefixes, process liveness, run lifetime
  alone, or earliest and latest event alone.
- A never-read binding is never healthy; an unchanged successful poll is never a stall.
- `sources NAME` gives per-input accounting: records read, events made, suppressed,
  matched to another input, record errors with the first unparsed form. An input whose
  records did not become events shows as such instead of reading as complete.
- `sources [FILTER] [--days N] [--host H]` renders the Sources grid: one row per source,
  one cell per day in the coverage vocabulary, and the state with its reason ("STALLED
  since Tue 12:28 · database busy on every poll"). Its footer names derivation lag,
  failures, stored-unparsed materials and disk runway.

### The status bar

`status --bar` emits one JSON line on change, for a desktop bar or dashboard to
consume:

- the **capture stripe**, coloured by the most invasive capture running now: green for
  files only, amber for screen, red for keyboard and screen; audio and accessibility
  count with screen. It is a capture indicator, part of capture status;
- the count of quiet sources;
- passive signals not yet seen;
- per binding, `producing_now` (an event admitted in the last 5 s).

### Polylogue on Sinex

When Polylogue runs on Sinex, status shows both sides: what Polylogue has sent and Sinex
committed, the confirmed-stream position Polylogue has applied, and its lag. While Sinex
is unreachable, pending counts are `not_measured` and `up_to_date: false` is shown,
never success ([Polylogue primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md)).

## Signals

A **signal** is a standing query with a declared delivery rule. It is the only way Sinex
surfaces anything unasked. The rule schema, the evaluator, the route
function, the interrupt budget, inhibition, outcomes, `wait`, `backtest` and desktop
notifications are common
[signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/signals/README.md);
its conformance cases run in Sinex. What Sinex supplies:

- its catalog for rule queries;
- `value` targets (a file or a loopback URL) and `action` commands, named in
  configuration;
- occurrences keyed by citation (record coordinates, source record key or derived key,
  never the reading), so replay and re-import never fire a rule again;
- the attention inputs: presence, focus with the window-to-session join, deep work and
  breakpoints from the [higher layer](higher-layer.md).

### Rules, deliveries and acts

- **Declared, never inferred.** A rule is an authored record written by the operator or
  an agent through `write` (`write signal:NAME …`, or `QUERY watch`) and runs as
  written. Status lists rules written by others since the operator last looked;
  disabling one is an authored revision.
- **Passive by default:** a delivery appears in the inbox, Today's Signals pane, the
  status bar and the day pack. **Interrupting** only for rules marked so, under the
  one interrupt budget.
- **Event kinds:** `signal.rule` (an authored rule revision), `signal.delivery` (one
  handoff by the host deliverer, derived from the rule or forwarded report that fired
  and the readings its rows were computed from) and `signal.outcome` (what happened to
  a delivery afterwards: an outlet's attempt, shown, cleared, closed), plus
  `signal.report` for evaluator reports another product forwards. Decided target, not
  built: `notice.submitted` from producers and `instrument.defined` for elicitation.
  The evaluator and host policy checkpoint is daemon state, not events. Every delivery
  stores its route decision, holds and attention, so "why did this fire" and "why
  didn't it interrupt" are both queries.
- **Acts:** `delivery:ID open|ack|snooze 1h`, `quiet until 14:00 | quiet off`.
  `QUERY wait [--for D] [--timeout T] [--until-empty]` blocks until the query first
  matches or empties, recording nothing; `signal:NAME backtest since:30d` replays a rule
  over stored history with the route decisions it would have received, and writes
  nothing.
- **Three deliveries, one mechanism:** a notice, a value, an action. An action names
  only a command already in configuration, runs outside `sinexd` with the triggering
  citation as its last argument, and its result returns as ordinary events.

### The host deliverer

Sinex is the one deliverer on its host. A product whose grant has `forward_signals`
(Polylogue on Sinex) sends its evaluator's reports (firings, cleared conditions, rule
revisions and withdrawals, progress) in a signals frame through the producer path; each
report is stored as a `signal.report` naming its authenticated producer and family. A
report about any product but that family is refused, a cleared report takes effect only
on that product's deliveries, and the `sinex` family cannot forward. Sinex's host policy takes
them in beside its own rules' reports and routes their firings under the host's
attention, budget and quiet hours; a forwarded current-state firing is held until its
producer reports being up to date. A forwarded value or action is recorded not sent,
because its configured outlet belongs to its own product. Polylogue shows only its own
"Sinex blocked" notice directly, because the path to the deliverer is what is broken.

- **Outlets.** An interrupting decision becomes a desktop notification through the
  common deliverer (`notifications.bus_address`, or the session bus) and, for a notice
  sent away while the operator is away, the configured `notifications.away_command`.
  A value is written to its configured file or loopback URL, and an action runs its
  configured command. Each attempt is recorded `unknown` and committed before the
  outlet runs, so an interrupted attempt is never run twice; an outlet still running at
  `outlet_timeout_ms` may have acted and stays unknown.
- **Deliveries cite what fired.** A delivery's parents are the rule's reading (or the
  forwarded report) and the readings each matched row was computed from when it was
  first seen: the reading itself, or every reading a group selected. Replaying one of
  those readings archives the delivery with the rest of the chain, like any derived
  reading.
- **A firing whose basis changed is withdrawn.** When a cited reading is archived (by a
  replay or a lifecycle step) or the privacy policy changes between selecting a row and
  handing its firing off, the handoff is withdrawn: no delivery is recorded and no
  outlet runs, only an outcome `not_sent` with reason `basis_changed`. Its evidence was
  read under a policy or from readings that no longer hold, so it is never certified
  under the current ones. The evaluator retracts the firing and the rules are read
  again: if the rule still finds the occurrence, it fires afresh under the same
  delivery identity, citing the readings selected now; a threshold, change or daily
  firing fires again only on a new crossing, change or day.
  The current build checks the cited readings against the lifecycle. Policy-change
  withdrawal belongs to the output-time privacy design and is not built.
- **Any producer may notify** (decided target, not built). A credentialed producer submits a `notice.submitted` fact
  (title, body, reference or URL, an interrupt request, a `key` for grouping and
  replacement, an optional `clears` key); it is a delivery of the implicit rule
  `producer:<name>` with author class `producer`, inside the budget and passive unless
  it asks to interrupt. `sinexctl notify --title T [--body B] [--ref REF|--url U]
  [--key K] [--clears K] [--interrupt]` is the CLI for scripts, so a host's scripts, job
  runners and CI reports share one budget, grouping, quiet hours and inbox instead of
  calling a notification command themselves.
- **Own notifications are excluded** (decided target, not built). A notification captured by Sinex's D-Bus source
  whose hints carry a product's delivery identifier is marked as a signal origin, and no
  rule reads it.
- `sinexctl attention` prints presence, quiet, the budget, held notices and the last 24
  hours of interruptions.

### Shipped rules

| Condition | Ships | Inhibits (host scope) |
|---|---|---|
| the daemon stalled | enabled, interrupting | capture stalled, source unreadable, runaway volume, refusal-rate spike |
| Sinex blocked (admission cannot accept producer input) | enabled, interrupting | |
| a source unreadable | enabled, interrupting | |
| capture stalled ([completeness](sources.md#completeness)) | enabled, interrupting; `critical` only where captured data is being lost now | |
| expiry race | enabled; interrupting when the margin is below one poll period | |
| a host out of contact past its deadline; spool runway under one day ([hosts](hosts.md)) | enabled, interrupting | |
| a kind whose every material is refused; refusal-rate spike | enabled, interrupting | |
| disk runway below 14 days | enabled, passive (a warning in status) | |
| disk runway below 3 days | enabled, interrupting | runaway volume |
| new input, parser arrival, stage finished, spool runway under the out-of-contact deadline, link rot on a saved item | enabled, passive | |
| the resume digest: operator activity returns after a gap and the resume pack is delivered as a notice | disabled; passive when enabled | |

Everything else is passive. Tripwires and prospective memory are [notes](recall.md#notes)
with triggers; none is enabled by shipping an example. An inhibiting rule folds the
notices of the rules it names into its own notification while it is open. A late fact
(older than the rule's `fresh_within`) is delivered passively. "Digest" is not a rule
kind: the day pack and Today's Signals pane are the digest.

No rule starts or resumes an agent session (default; open operator question): there is
no agent-starting delivery, and the deployment's action commands include none that
start or resume an agent, because a run would spend quota with no one acting.

### Elicitation

Self-report prompts are deliveries of rules with `deliver: ask` and an instrument
([elicitation](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/signals/README.md#elicitation)).
The answer is a [note](recall.md#notes) with the delivery as parent and its latency;
`sinexctl delivery:ID answer 4` and `sinexctl note --value mood=3` answer from the CLI,
and notification actions answer with one click. Nothing is asked until the operator
decides which prompts may be sent (default; open operator question).

### Stall alerts

- The stall rule fires from the source's declared expectation (`silent_after` or `every`) and its
  durable poll outcomes. It tells apart an unchanged successful poll, an event-driven
  source with no recent events, an unavailable input, a parser rejection, an excluded
  source, source lag, a host out of contact within its deadline, and an actual missed
  expected read. It never fires from low counts alone.
- An alert carries its evidence, the last success, the affected interval and a concrete
  next action, and stays visible in status and the day pack until recovery clears it.

## Follow

- `QUERY follow` (SSE over HTTP, a subscription over MCP) is a signal without a delivery
  rule: the same standing evaluator delivered to the open connection, `on_new` by
  default, `--for` and `--what` as in rules. Nothing is recorded.
- The initial data notice follows initialization of the subscription's watch and
  commit cursor. A matching commit admitted after that notice is beyond the initial
  cursor floor and is delivered once.
- Event follows wait for the search read model's durable position, including queries
  without an explicit text predicate. Projection lag delays delivery. Read-model
  completion wakes subscriptions, and each captured batch stays within its lower
  exclusive and upper inclusive commit positions across every page.
- Each message carries its commit or stream position as the SSE `id`. A reconnect with
  `Last-Event-ID` receives everything after it, from the confirmed stream or
  PostgreSQL, including lifecycle changes (archive, restore, replay, tombstone) that
  affect matching rows. A position whose history is unavailable refuses with a named
  gap; it never silently starts now.
- Messages report withheld payloads as withheld; keepalives are not capture evidence.
  Grouped, aggregate and ranked subscriptions use `on_change` semantics.
- Programs that act on every committed fact subscribe to the confirmed stream as
  [outside handlers](producers.md#outside-consumers-and-handlers).

## Latency budgets

Budgets are p95, end to end through the real binary with a warm daemon. Each operation
belongs to one class of the shared `command-surface` classes; Sinex's budgets per class:

| Class | Sinex operations | Budget |
|---|---|---|
| local | help, version, grammar completion, refusals of malformed input | ≤ 15 ms |
| status | `status`, the first screen of bare `sinexctl`, `status --bar` | ≤ 100 ms |
| completion | each keystroke, daemon up or down | ≤ 50 ms |
| read | `REF read`, indexed lists | ≤ 100 ms to the first screen |
| search | bare-word queries, `recall` | ≤ 250 ms to the first page |
| aggregate | grouped and joined queries | ≤ 400 ms by default; a timeout names a narrower query; `--deadline none` lifts it |

Journey budgets on top:

- the morning view (bare `sinexctl`, Today) renders in under 1 s;
- resuming work takes at most two commands or interactions to reach evidence;
- a prepared pack is read in ≤ 20 ms, a moment pack on request in ≤ 250 ms, a closed day in ≤ 100 ms, a week in ≤ 400 ms and a month in ≤ 1 s;
- `note` commits in under 1 s;
- the machine section answers within Polylogue's 100 ms deadline;
- confirmed delivery to `follow` and outside handlers arrives under 5 s after a row is
  queryable;
- a stall alert arrives within the source's `silent_after` or `every` plus one evaluator tick and
  measured delivery delay.

A miss is recorded with its measurement, never met by relaxing the number. Measurement
is paired benchmark runs with hardware-independent figures (rows scanned, bytes encoded,
memory per page); no test asserts elapsed time ([benchmarks](testing.md#benchmarks)).
