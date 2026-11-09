# Derivations

Every computation from admitted evidence upward runs as a derivation: a declared computation whose outputs are stored events admitted through the [one admission path](admission.md). This page owns the derivation model (shapes, declarations, versions, identity, time, the payload every output carries), fusion of records seen by several sources, intervals, documents and tags, and the contract for derivations that call a model. The products built on this model (presence, episodes, figures, packs, narratives) are in [the higher layer](higher-layer.md). The event hierarchy, record identity and the producer version recorded on every event are in [events](events.md); the states an output moves through are in [the lifecycle](lifecycle.md).

## Principles

- **Every level is a stored event.** A derived event sits above its parents with exact parent links, so it is citable, archivable and derivable from, and the finest grain below it is kept ([the hierarchy](events.md#hierarchy)). There is no second analysis database, no separate result lifecycle and no "insights" stage. (ruling 2026-08-31, cc-queued:62b956ba-3ea3-4693-8d71-9a36321c37a9@2026-08-31T05:41:13Z).
- **Materialize when the inputs are admitted, not when someone asks.** A derivation computed on request names in its declaration why it cannot be maintained correctly as inputs arrive. (ruling 2026-06-27, cc-queued:1944721d-4531-41be-8093-13454b62ebd6@2026-06-27T17:03:47Z).
- **Never make things up.** Unknown time stays unknown; missing coverage is never a measured zero; a time overlap does not establish ownership, authorship, causation or attention. There are no free-floating confidence numbers or scalar scores: a method states its evidence, denominator and measured precision instead. (ruling 2026-03-27, cc-queued:14ea730a-aa86-433a-9bba-8be973977d5c@2026-03-27T08:47:07Z).
- **Labels need construct validity.** A derived label or detection ships only when its declaration says what it measures, from which structured fields, with which denominator, and what agreement evidence supports it. (ruling 2026-03-27, cc-queued:14ea730a-aa86-433a-9bba-8be973977d5c@2026-03-27T22:55:51Z).
- **No sampling.** Nothing in the derived layer samples; were sampling ever needed for a stream, it would be deterministic so that replay reproduces it.

## Two shapes, one runner

- A **per-event derivation** maps one admitted input to zero or more outputs.
- A **keyed derivation** computes the complete output set of one key (a period, a scope, an episode) from the inputs it selects, including intervals that overlap its period.
- Derivations read only committed events, in commit order. Inside `sinexd` they read PostgreSQL and admit directly; outside, they are durable subscribers of the [confirmed stream](producers.md#the-confirmed-stream) and send their outputs back through the ingress. Both obey every rule on this page.
- Paged readers choose event identities and apply their row and byte budgets before fetching payloads. A separate materialized fetch loads payloads for that ID batch once before joining its page metadata. Its inclusive UUID bounds exclude chunks and compressed row groups outside the selected ID range; exact membership excludes gaps inside that range. A sparse batch can span unselected row groups inside its bounds, so the row budget alone does not bound decompression work.
- The daemon does the I/O; the `sinex-derivations` crate computes from supplied values, so every computation is testable without a database.
- Admission commits a derivation's outputs, the replacement of the key's previous output set, the derivation's saved position, its read ranges and the downstream dirty marks in one transaction.
- A per-event pass that finds no event of its kinds up to the head it read at starts its next pass from that head, so an idle derivation reads only new commits. The saved position moves only with admitted outputs; after a restart, or when a version switch resets it, the next pass starts from the saved position. Compressed chunks wholly before a pass's start are skipped by their commit-position range.
- Dirty keys are marked in the same transaction as the inputs that dirty them. There is no separate invalidation stream.
- Recomputing a derivation is idempotent: the same live inputs give the same outputs by key and payload.

### When a key recomputes

- A dirty key recomputes at most once per writer group, however many inputs in the group dirtied it.
- A key whose time window is still open (an hour or day in progress) is **provisional**. Its value is held as one current row per key in the derivation's provisional table, overwritten at each recompute, and readable and citable as provisional. It is admitted as an event only at window close, and again whenever a later input dirties the closed key. Superseded provisional values are not kept (default; open operator question).
- Keys dirtied by a lifecycle operation in progress recompute after its last batch.

The `analysis.products` derivation reads admitted `analysis.request` questions,
selected daily figures and `source.coverage` observations. It stores panels,
recipe answers, findings and daily context packs. An empty or not-estimable answer
retains its request as a parent. Questions reuse their canonical identity; changed
inputs replace the complete output set. The selected coverage retains the latest
read status per source binding and host, including failure reasons, alongside the
declared intervals that overlap the question. A running process or a successful
read without declared temporal bounds does not establish observed coverage.

A figure or coverage observation dirties only the stored questions whose panel
period it falls in; a question about another day stays clean. Interactive recall
is a read at one commit position, not a derivation ([recall](recall.md)).

## Declarations

Each derivation declares:

- its exact input event types and predicates (a kind can be an event type narrowed by payload field values, so a key never reads a whole type to use a sliver of it), its output types, its settings and its dependencies;
- its producer version;
- `on_version: recompute | keep_history` ([versions](#versions));
- `materialize: at_admission` (the default) or `on_request` with its reason;
- for each output label or detection: what it measures, its fields, its denominator and its agreement evidence.

Registration is validated at start. Unknown types, dependency cycles and unimplemented operations refuse to start the runner, naming the derivation and the reason. Derivations run in dependency order.

## Reads outside a key's period

A keyed derivation reads outward until its semantic stop: a genuine gap, a matching closing transition, or the end of stored evidence.

- It records the ranges it actually read for each key, empty ranges included: scope, kinds and the commit-position interval read. These ranges are also how a keyed aggregate cites its parents ([payload](#the-common-payload-contract)). Any later insertion, change of time or scope, replacement or lifecycle transition intersecting those ranges marks the key dirty.
- Before clearing a key, the runner checks for changes since the read in the key's newly computed ranges, not only its former ones. A dirty mark raised during a computation survives that computation's commit.
- There is no fixed lookback, neighbour count, day edge, event count or maximum duration. A polling horizon may stop periodic refresh, but it never makes a later closer irrelevant: a closer arriving a month late still changes the key.

## Versions

Every event records the producer version of the parser or derivation that made it ([producer versions](events.md#producer-versions-runs-and-manifests)). For derivations that version governs history as follows.

- **A new version is compared before it switches.** `recompute DERIVATION --compare` runs the new version against the live outputs on real data and reports what would change (counts per key, examples, keys added and removed), writing nothing. Switching is a separate, attributed operation recorded in the [operations log](lifecycle.md#operations-and-actors).
- **Each derivation declares what a switch does to history.** `recompute` replays its whole history through the dirty-key path, archiving the old outputs as a set with their chains; `keep_history` keeps outputs made by the old version and applies the new one from the switch onward. Readers see the producer version on every output, and an answer spanning outputs of several versions says so.
- **Nothing recomputes at daemon start.** A build carrying a new version computes only new and dirty keys with it, and answers spanning both versions disclose the mix. History is recomputed, for `recompute` derivations, only by the switch, after its compare. Status shows each derivation's pending switch and its compare result.
- **A changed setting is a new version.** A change to anything a derivation reads as configuration (a profile parameter, the logical-day boundary, a fusion rule file) is a version change of that derivation and follows the same rules.

`recompute DERIVATION --compare` stores a comparison ID tied to the installed build, its logical-day settings, and the input and lifecycle positions used throughout the comparison. A comparison that covered only a time range cannot select a version. `recompute DERIVATION --switch ID` consumes a full comparison of that same declaration and build; changed inputs refuse the switch and require another comparison. The writer records the caller and comparison in `producer_switch`, selects the version and, for `recompute`, archives its live output chains before scheduling the complete new output set. A `keep_history` declaration preserves those chains and its read position. Startup records available producers and leaves stored keys and positions unchanged. `status` lists selected and available versions. Runs record the full source commit and dirty flag; a build lacking that identity refuses producer execution.

Stored products disclose the producers that actually made their selected inputs and ancestors,
grouped by source, run manifest and build, and material parser version. A shared
daemon run does not replace a source parser's version. Missing historical run or
manifest metadata remains an explicit null entry with its event count.

## Identity of derived records

- A derived record's key is computed from its parents' keys or offsets plus its window or scope; never from the producer version, event IDs, commit order or text alone. Typed opaque fields are encoded as tuples, never joined by punctuation. (Record keys in general: [identity](events.md#identity).)
- A new version's output for the same key is a new reading of the same record; the previous reading is its history.
- Request-made derived events (a recipe answer, a kept query result, a finding produced for a question) are keyed `query:<hash>` over the canonical question plus its resolved read context, and recompute like any keyed aggregate.
- Ties break by stable keys: material digest, anchor, output index or source record key, then genuine source order where the source records one. Never commit order, arrival order or event ID, all of which replay changes.
- A late first member, bridge, split or merge can change keys. Every affected owner is recomputed and its full set replaced. No alias redirects an old group to an arbitrarily chosen new one: a current query may return several replacement groups, and an exact citation of the old reading stays historical.

## Derivations and the lifecycle

The states, archive, restore, replay and tombstone are defined in [the lifecycle](lifecycle.md); what they mean for derived outputs:

- **Whole chains move together.** When an input is archived for any reason (replay, a newer reading, a version switch, the operator's archive), every output that cites it is archived in the same transaction ([whole chains](lifecycle.md#whole-chains-move-together)). No live output cites an archived or tombstoned event.
- **Keyed aggregates re-derive.** Archiving one input of a keyed aggregate (a day's figures, an episode, a fused group) archives the aggregate and marks its key dirty; the derivation then produces a new reading from what remains live. A keyed output whose inputs are unchanged is re-admitted as a new reading over live parents, never kept over archived ones.
- **Per-event outputs check their parent at admission.** An output whose parent was archived while it was being computed is refused by admission, and its key is dirtied again.
- **Restore re-derives.** Restoring a chain whose root is a keyed aggregate restores nothing stored: its keys are marked dirty and recompute from what is live, once, after the restore's last batch.
- **Tombstoned events are never parents.** A derivation never selects a tombstoned event; a new reading of the same record, by replay or re-import, is an ordinary input.
- Derived subtrees replay like anything else ([replay](lifecycle.md#replay)).

## Time

- **Derived event time is the derivation's to declare.** Each derivation defines its outputs' `ts_orig` from its inputs, never "now". A pure mapping keeps its parent's time quality, unknown included. Processing time stays separate ([time](events.md#time)).
- **One logical day.** Every day-grain product uses the one logical-day setting shared with Polylogue ([`DaySetting`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md); key and default in [operations](operations.md#logical-day)). Day- and hour-grain records state the boundary and zone they were cut with. Changing the setting is a version change of every day-grain derivation and recomputes them all under the one new setting.
- **Periods are half-open UTC instants.** The two instances of a repeated autumn hour are distinct periods; a nonexistent spring hour is not invented.
- **The wake view sits beside the logical day.** `day.wake` events ([rest and the wake view](higher-layer.md#rest-and-the-wake-view)) let Today, the brief and narratives offer "since you woke" and a wake-to-wake view. They never redefine the logical day, never partition calculation and never change `group by day`. The logical day is shown first; the wake view is offered beside it.
- A malformed negative duration is a failed input with its evidence, never clamped to zero.

## The common payload contract

Every stored output carries:

- a registered typed payload and its key;
- three clocks, source and producer version;
- its parents, complete and never truncated. A per-event output names its exact parent event IDs. A keyed aggregate cites its parents as the read ranges it recorded, each a scope, kinds and commit-position interval whose members are exactly the live events in it at that position; `event_children` holds one row per range, and the lifecycle walk from an event finds the aggregates whose ranges contain it;
- coverage where the output is a measure: which sources could observe, over which interval, in which state (`observed`, `partial`, `recorded_unknown_coverage`, or `missing` with a reason);
- for a profile-driven output, the profile name and version and its effective parameters;
- `provisional: true` on a provisional value while a trailing window may still change.

Counts are checked non-negative integers; durations are integer nanoseconds; overflow is an explicit failure, never saturation. Lists without semantic order are sorted and deduplicated. References in payloads complement parent links and never replace them. A product with no event parent (an empty or not-estimable answer) cites the request material that asked for it, never a fabricated parent.

## Fusion

Fusion joins readings of the same occurrence seen by several sources, for a source family whose definition is written. It is a derived layer over source records, which stay live. The base case is separate records with no fusion: where no family is defined, consumers read each source's records directly and say that counts across sources are per source. (ruling 2026-10-28, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3ae3257e-e70a-433a-be97-d1222670c284).

Handling may differ per family, because no single rule is right for every pair of sources. Fusing one shell command seen through zsh history, Atuin, Kitty and other sources into one derived layer is the motivating use.

### Records, sources and association

Five things are kept apart:

| Thing | What it is |
|---|---|
| source record | evidence, as one source saw it |
| reading | an interpretation of a source record ([records and readings](events.md#records-and-readings)) |
| association | a proposal that records from different sources concern the same occurrence |
| resolved output | an association represented under a stated policy (a fused record) |
| count | outputs counted under a declared unit and scope |

- **Within one source**, a record's source record key recognises the same record seen again, and a newer changed reading replaces the live one through [replay](lifecycle.md#replay). That is admission's work, not fusion's.
- **Across sources**, readings never replace each other. Two readings of one field-keyed record from different sources or export families (a calendar event in two exports, a contact in two accounts) are separate records unless a defined family groups them.
- A family gets association only once its definition is written: what counts as one item, the scope that keeps hosts and accounts apart, exact shared identifiers first, and time-window matching only where nothing better exists, labelled as inference. Where capture can carry a shared token (one execution ID seen by both the shell and the terminal), capture it instead of matching.
- Matches do not chain: A~B and B~C does not make A~C. A resolved output's key identifies its group under its policy, not a lasting occurrence.
- Association is idempotent: recomputing it over the same live records and duplicate decisions gives the same groups.

### What fusion is not

- **It is not deduplication.** Every member stays live and is named as a parent of the fused record; nothing is suppressed. Suppressing a byte-identical re-read is [admission's](admission.md) job.
- **It never merges AI sessions.** Polylogue sends resolved session objects ([primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md)).

### Rules are data

- Rules live in a declaration file over a closed set of strategies:
  - **window**: match within a declared time window, closest first;
  - **fallback**: a wider window, accepted only when the match is unique;
  - **absorb**: a refresh from the same observer extends a span instead of starting a new one;
  - **identity extractors**: exact shared identifiers in a declared namespace.
- The file is `crates/sinex-derivations/src/fusion/families.json`, built into the daemon. Each family declares its members in preference order (an event type and, where several sources write that type, the one source it reads), the payload fields each member supplies under the fused kind's field names with an optional normalisation (`trim`, `non_empty`, `before_colon`), the identifier namespaces each member carries, its scope fields, its match fields, its windows, and whether agents' duplicate decisions govern.
- A family whose identity alone decides declares the fields its keys are made of and how far apart in time its copies are expected to be; its key reads every copy of that identity whatever its time. Every other family is keyed by the hour its groups start in.
- The file may hold only families with a written definition, each writing the fused kind its definition names. It is validated against the kind registry at start; an invalid or undefined family refuses to start the daemon, naming the family and the reason.
- Each family's version covers its declared version and its declaration's text, so a rule change is a [version change](#versions) of that family only.

### Matching

- Exact shared identifiers in the declared namespace win over time.
- A fused group holds at most one observation per source. Scope (host, seat, desktop, browser profile, account) partitions matching: two hosts or two desktops never match through time alone.
- Normalisation never merges different meanings: whitespace inside quotes is significant in commands; distinct clipboard selections stay distinct; equal notification text alone never collapses repeated notifications; a missing Message-ID never makes all such mail one message.
- Repeated records from one observer are distinct unless an absorb rule establishes continuity.
- An unmatched observation stays a source record only; there are no one-member fused records. A consumer of a fused family reads the fused records plus the family's unmatched source records, and counts each occurrence once.
- Unknown-time observations stay queryable and are ineligible for time matching.
- Unbounded spans use [overlap reads](#reads-outside-a-keys-period), not a fixed margin.

### Basis, ambiguity and duplicate decisions

Ambiguity is never silently resolved; the operator decides what a rule cannot.

- **A fused record is the association record.** It cites every member by its stable record citation (never a reading ID: a native ID, the record's source record key, or its location when it has no key) with how it joined (`lead`, `identifier` with its namespace, `window` or `fallback` or `absorb` with the time distance, or `decision` with its author), and records its basis: `identifier` (an exact shared ID), `rule` (a strategy matched) or `operator` (a duplicate decision decided it); plus `ambiguous: n` when it chose among n candidates, the citations of the competitors, and the family's version.
- **Considered pairs** are `fusion.considered_pair` events, written by the fusion pass with its fused records in the same batch, so they move with them: every pair a choice among several candidates touched, and every pair a duplicate decision names. A row holds the pair's record citations, their sources, the strategy, the candidate count, the time distance, whether the pair is joined, and the decision state (`undecided`, `same`, `distinct`, with its author, plus standing decisions that do not govern). They are queryable as the `duplicates` alias.
- **`same A B` and `distinct A B` are duplicate decisions**, authored records: a request material plus a `curation.duplicate` event, the one `duplicate` kind the library's duplicate review also writes. It carries the authored-record header (root, revision, author, channel, time, `revises`, `withdraws`), the pair by record citation, and the decision, and follows the shared authored-record rules: currency by the descendant rule, and one uniqueness slot per unordered pair and author, so a caller's next decision on a pair revises its current one. Nothing is replaced by write order or time; an author whose current decisions on a pair disagree has no standing decision until one revision revises both. Because decisions cite records rather than readings, a decision survives replay of either side and changes to the rules, and it never archives a member. `same` is refused for records of different scopes, and a decision for records farther apart than one fusion key reads (its window plus twice its fallback) or, for a family keyed by identity, of different identities.
- **`withdraw-decision A B`** writes a withdrawal record that withdraws the caller's current decision on the pair; fusion then decides the pair by its rules again.
- **The operator's decisions govern.** Agents' decisions are recorded with their evidence and shown in `duplicates`; they govern only for families whose declaration admits agent decisions, and only when they agree. The configuration is the same for every caller, and every caller can record a decision ([callers](interfaces.md#callers)).
- Status reports ambiguity per family: undecided pairs, decided pairs, and fused records with `ambiguous > 1`.
- **Outputs of removed derivations are archived.** When the daemon starts, every registered derivation the build no longer has, such as the fusion derivations removed with the families that had no written definition (focus, visits, plays, clipboard, notifications, rest, posts, conversations), has its readings archived by one lifecycle operation (actor `sinex`), and its registration then ends. This happens once: `undo` reverses the archive, and a later start leaves it reversed. A derivation turned off by configuration is still in the build and keeps its readings. Nothing is deleted.

### The fused kinds

Two families are defined and built (default; open operator question):

| Output | Inputs | Content |
|---|---|---|
| `command.fused` | the shell integration's `command.reported`, Atuin's rows, the shells' history files, and kitty's `cmd_end` reports holding a token; other history databases join only when defined against their actual sources | exact command text after outer trimming, host, start, optional cwd, session, duration, exit and execution token, members with how each joined, basis, all parents |
| `mail.fused` | Gmail, IMAP, Maildir, mbox and Outlook copies of one account | account, Message-ID, headers, labels, members with how each joined, basis |

- **Commands** are scoped by host. The shell integration Sinex ships mints one execution token (a UUIDv7 of the command's start) per command; its report and kitty's record of the command's end both carry it, so they join by identifier (namespace `execution`) whatever their time distance: a key reads a token's partners however far from its hour, and a record holding a token dirties the hours of the record that started it, found at the token's mint time. Every partner lies within the token's lifetime, from its mint time to the end the report records (`token_length`, the report's `duration_ns`), widened by the family's window; a read for a token's partners is an indexed range that stops there, so a token without a partner costs one bounded read. Kitty's records join only by token, never by time, and never count as a command of their own: one without a token is not a command occurrence, and one whose report is lost leaves the command to the Atuin row. A record of a command's end is timed at the end while the fused record is timed at the start, so a consumer reading the later window would otherwise count the command twice. The report also records the Atuin row started for the execution, which joins it by identifier (namespace `atuin`) within one key's reach. Otherwise commands match closest first within 2 s, and within 60 s only when the match is unique. A command's hour is the hour of its first start.
- **Mail** is scoped by account and keyed by account and Message-ID: copies join by Message-ID within the account whatever dates their stores give them; no Date-header agreement is required. Consumers read a day beyond their period for mail.
- Every other family is built once it is defined against its actual sources. Until then window focus, page visits, media plays, clipboard copies and notifications are separate source records.

## Intervals

One `state.interval` derivation, with declared rules for adjacent transitions, observed spans, and matching start and stop.

- An interval carries a stable start-record key, state kind, subject, start, optional end, labels and attributes, and **every transition whose fields affect it as a parent**, including same-state refreshes that changed a label.
- Scope is the observation's original host and session; systemd units add boot, manager and unit. The host that ran the derivation never supplies missing observation scope.
- A next different focus or workspace closes the previous interval in its scope. A matching unit stop closes only that unit; a repeated start closes the previous run as a restart; a stop without a start invents no interval; a boot boundary stops a stop from closing a previous boot's unit. A boot fence records `fenced_at` while leaving `ended_at` unknown, so it bounds the earlier state without claiming uptime until the new boot. Fenced intervals are absent from `now`; their unknown duration remains null.
- Genuine suspend or hibernate cuts open desktop intervals; resume alone does not establish a new focus. Transitions come from the source's typed journal and logind events, never from log text containing "Started".
- Equal-time transitions order by recorded sequence, then stable record identity.
- The `intervals` and `now` query aliases show an open end as unknown and the evidence's age separately.
- Repairs of malformed source records (ActivityWatch heartbeats, for example) belong to the source's parser, not to this derivation.

## Documents and tags

### Documents and citable chunks

- `document.parsed` and `document.chunk` derive from admitted note text, extracted document text (`document.text` over stored bytes: PDF pages, plain text, Markdown, HTML, DOCX, ODT) and captured terminal output. A command with no captured output produces no chunks.
- Computation reads the stored record, never the original path; a moved or vanished file changes nothing.
- Document metadata: stable record and part identity, format, title, parsed front matter and its status, wikilinks, text length, body range, chunk count, optional source time.
- A chunk holds its record, part, index, complete text, and `(input field, byte start, byte end)` on UTF-8 boundaries, with `input[field][start..end]` equal to the chunk. For converted documents a range addresses the extracted text or a page, never fictional source offsets; page or region references are added where the extractor supplies them.
- Chunk sizes are partition targets, never input ceilings: output beyond 4 MiB is complete, and every non-whitespace character is represented in some chunk.
- Invalid front matter keeps the text with its diagnostic; an empty document has metadata and zero chunks; an unsupported or encrypted input is a typed failure whose bytes stay readable. A document that shrinks from ten chunks to two leaves two live chunks.
- Text and chunks are searchable through the ordinary query and its declared `search` ([query](query.md)). There is no second document index.

### Declared tags and classifications

- `tag.applied` has identity `(tag, stable target record)` and carries the matching event type, the sorted IDs of every matching rule, inherited time and a direct target parent. Every matching tag and every matching reason is kept.
- Tag rules are data: event type, exact metadata source, JSON-pointer field conditions, the real final-component file extension and MIME type. Removing the last rule that produced a tag removes the tag.
- Tags on events, the operator's [library tags](library.md) and inferred classifications are different records with explicit authorship.
- A classification names its label set, method and version. An unmatched item is classified `unknown`; classified time reports classified, total and unknown seconds. No label ships without its construct declaration ([principles](#principles)).

## Model features

### Current state

Derivations that call a model provider are off (default; open operator question). No provider is constructed or probed at start, no weights are downloaded, and no request is made.

They stay off until the features are redesigned purpose first: search ranking, OCR beyond the local engine, speech transcription with speaker separation, model-written [narratives](higher-layer.md#narratives) and the [narrative corpus](higher-layer.md#the-narrative-corpus), classification, episode labels, and [model suggestions](higher-layer.md#model-suggestions). The settled inputs to that design:

- Cloud processing is acceptable on privacy; cost, latency and capability decide between providers.
- Cost is an operating setting, not a design driver or an activation gate. (ruling 2026-10-11, cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:07bbe1df-c4cd-4c59-b07e-540182d93fdd).
- Local models are first-class providers. (ruling 2026-05-30, cc:34a46275-7b63-44c2-ac47-bdf5388514a7:8b965461-8556-4cef-96c5-2a92532222d1).
- What reaches a provider follows the same [privacy](privacy.md) output rules as every other caller; there is no separate model allowlist.

### Local OCR

Local OCR runs. It calls no provider and spends nothing, so the off switch above does not cover it. (ruling 2026-10-18, cc-queued:01d1e6dc-4672-468a-83b4-1956ce71dea2@2026-10-18T16:54:35Z).

- It is a deterministic-tool derivation of `screen.text` from stored frames, and runs by default when the engine is installed. Its producer version includes the engine and the engine's version.
- `screen.text` is searchable.
- One segment has one canonical `screen.text` reading. Captured accessibility text, including observed empty text, takes precedence; an explicitly selected and validated recorded OCR binding comes next, followed by the configured local engine. Ambiguous requested bindings report unavailable until selected rather than choosing a receipt by arrival time.
- A versioned screen reading distinguishes available text (`Some("")` is observed empty) from typed unavailable status with no text, regions or effect claims. A new unavailable reading replaces stale text under the same segment key. Historical positive readings remain available historical evidence. Requested missing or archived target references are unavailable status, not verified evidence or fabricated parents.

### The contract every model output follows

- **Every model output is a stored derived event** over admitted inputs, with provider, model and revision, prompt, rubric and settings version, exact input hash, output hash, request identity and target parents. Raw requests, responses and receipts are recorded as materials before interpretation where the protocol permits. No model output exists that is not stored this way.
- **Replay reuses recorded effects.** A new model or new settings is a new version, compared before it switches ([versions](#versions)), never a silent re-call.
- **A deterministic baseline ships first** for every model step. A model variant is enabled only after it beats the baseline on its evaluation with zero unsupported claims.
- **Indexes over model outputs** (vector indexes) are rebuildable consumers of stored results; there is no external vector service.
- **Retries are exact.** A partial batch retries only failed or unrecorded inputs; rate limits and cancellation keep retry state; a call that succeeded but was not recorded is uncertain and uses provider idempotency where supported.

| Operation | Stored result and validation |
|---|---|
| Embedding | `text.embedding`: stable input record, provider, model and revision, input and settings hashes, dimensions, complete finite vector; count, dimension and identity checked. Polylogue's session vectors are reused through its events, never bought twice |
| Speech transcription | segments with text, language if known, speaker label with the separation method, bounds relative to the stored audio and their absolute-time quality; the audio itself is always kept; segments beyond the audio are refused |
| OCR by a provider | enriches `screen.text` with regions and frame identity; geometry validated |
| Classification and labels | values from a recorded vocabulary, evidence spans, method and abstention; out-of-vocabulary labels refused; inferred and unchecked until evaluated |
| Narratives and summaries | the [narrative contract](higher-layer.md#narratives) |

### Supplied embeddings

**Embeddings rank; they never filter.** Once embeddings are on, Sinex search may combine a stored similarity as a ranking term over the filtered set (`similar:`); it never removes a row the filters selected.

Supplied `embedding.supplied` receipts cite their target reading as a parent. The deterministic `text.embeddings` derivation validates the receipt's identity fields, BLAKE3 digest encoding, provider/model/revision/settings/dimension space, finite components and exact count, then stores a complete `text.embedding` with the supplied receipt as its parent. This path constructs no provider and rereads no purchased embedding. Source replacement archives the old embedding before recomputation.

The `read.embeddings` consumer builds a pgvector projection and an HNSW index for each supported space and dimension. Original f64 components remain in the stored event. Projection rows state indexed, exact, fallback or unavailable coverage and its reason. Extension type/index limits and precision range choose a fallback; they never truncate the stored vector. Initial projection and rebuild read retained events in bounded pages, including after commit-log pruning, and commit the projection with its position. Tombstoning removes projected copies in the same transaction.

`similar:sinex:ev/<embedding-id>` reuses that exact supplied embedding under the reader's grants and read position. Similarity scores every filtered row, using the best compatible chunk where several are present. Rows without a compatible vector retain unknown rank. The approximate index supplies candidates for optimization, while query membership and exact fallback remain independent of its recall. Model-driven text-to-vector requests remain disabled.

## Performance

Targets are stated as hardware-independent figures (rows read per key, invalidated keys per input, outputs and parents per input, WAL bytes per derived event, peak memory per key, lag per stage) plus absolute rates on the machine that runs them. They are measured as paired runs of the devkit bench ([benchmarks](testing.md#benchmarks)); no test asserts elapsed time.

| Concern | Target |
|---|---|
| Live derived path | at the admission target's source mix ([pipelines](architecture.md#pipeline)) the derived backlog reaches a steady state; provisional updates appear within 2 s of admitted evidence when the runner is caught up; semantic waiting is reported apart from compute lag |
| Fusion | sub-quadratic in dense repeated-identity windows, with candidates grouped by scoped identity; measured at 1k, 10k and 100k observations; no candidate or event limit changes an answer |
| Documents | 4 MiB-plus and 100 MiB inputs are complete with range equality; extraction and chunking measured apart; a resource failure is explicit and retryable, never a successful prefix |
| Long history | full, incremental, reversed and late worlds agree beyond 10,000 windows, 1,024 units, 64 refresh neighbours and a closer a month late; unrelated keys are not recomputed |
| Disabled providers | zero provider setup, probes and calls on every path |
