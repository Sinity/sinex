# Recall

Answers to "what was happening": `recall`, `context`, marks, notes, the one assembly
behind recall, context, the resume card, the daily brief and the machine section, and
how an agent's session receives Sinex's context when it starts. What the products
contain and how they are computed (episodes, the resume card, context packs) is in
[higher layer](higher-layer.md); the query rules every answer follows (read context,
budgets, citations) are in [query](query.md); signals that feed briefs are in
[interfaces](interfaces.md#signals).

```sh
sinexctl recall
sinexctl recall last --cwd /src/tide
sinexctl recall yesterday
sinexctl context --episode sinex:ev/EVENT_ID
sinexctl brief
```

## Recall and context

### Recall

`recall [TIME [±DUR] | DAY | last [--cwd DIR] | mark N | (QUERY) | since last_seen]`
answers what was happening, grouped by session.

| Form | Window |
|---|---|
| no argument | now ±30 min |
| `TIME [±DUR]` | the instant with the given radius |
| `DAY` | one logical day ([clocks](query.md#clocks-and-the-logical-day)) |
| `last` | the most recent recorded shell session; `last --activity` the last activity session |
| `last --cwd DIR` | the latest shell session that ran a command at or below `DIR`, matched by whole path components, with everything recorded during it |
| `mark N` | the moment of the Nth newest mark ([marks](#marks)) |
| `(QUERY)` | each span the query returns |
| `since last_seen` | everything since this consumer's presentation checkpoint ([seen state](web.md#today)), not source progress |

- A shell session spans its recorded commands, so its end is the last command seen, not
  a known end of activity.
- `recall last` with no matching timed shell session says "no matching timed shell
  session", never "no activity".
- A window citation `sinex:window/<from>/<until>` is a stored recall question
  ([citation forms](query.md#citation-forms)).

### Context

`context --since T --until T | --around REF | --episode REF | --project P` returns the
activity around a span, grouped by family (commands, focus, pages, files, git, agent
sessions, source health), each group with its coverage. With an episode or a project it
returns that episode's resume card; with no argument, the top three candidate episodes,
the first with its full card ([the resume card](higher-layer.md)).

### What every answer includes

- point observations, and states already holding when the window starts;
- provisional intervals, marked provisional;
- shell, activity and attention groupings where their producers exist;
- per-source counts and lag;
- every applicable source that was missing or behind: the bindings expected in the
  window, not every registered source.

A source with some events can still have a gap in the window. Absence is not coverage,
and co-occurrence is not support. Each source's share of the digest is independent of
page size, so a busy source never crowds out the others, and the question's filters,
joins and limits apply before grouping.

## Marks

```sh
sinexctl mark 'Investigate the test failure'
sinexctl mark 'Build broke here' --at 2026-09-14T08:01:00Z
sinexctl recall mark 2
```

- `mark [TEXT] [--at TIME]` records `operator.mark`, an authored record, with the
  context the client reads itself at the press: the focused window from the compositor,
  the terminal's foreground command and directory, and the text. It holds no event
  references, because that context is not yet in Sinex when the key is pressed. It
  commits in under 1 s, so it can sit on a hotkey.
- The marked time has operator quality. `--at` backdates it; recall never substitutes
  the recording time for an explicitly backdated mark.
- Links to what the mark is about arrive later as derived `reference.about` events, by
  source record keys for commands and focus and by containing time for sessions
  ([derivations](derivations.md)).
- `recall mark N` orders marks by marked time, newest first, with the ID as a stable
  tie-break, and returns the mark's text and context at one commit position. An absent
  or unavailable mark is reported as such.
- Marks are open to every caller and carry their [computed author](interfaces.md#computed-authorship).

## Notes

- `note REF… TEXT [--revises NOTE] [--tag T]` records a note as an authored record (a
  request material plus an event) whose parents are the cited events.
- A revision is a new note naming the one it revises; the earlier text stays readable. A
  note is withdrawn by its author's archive act.
- Notes are records about events, never a second model of the substrate.

## One assembly

Recall, context, the resume card, the daily brief and the machine section are
presentations of one deterministic compiler over query results, stored as
`context.pack` products ([context packs](higher-layer.md)). They differ only in their
query set and template.

- Each takes one selected read context and one presentation budget in the
  destination's encoded bytes, cuts whole lines with an omission count, and names
  missing or behind sources.
- Every line carries a [citation](query.md#citation-forms).
- The stored product is complete; a presentation is a budgeted view of it and never
  truncates what is stored.

## The daily brief

`brief [today | yesterday | DATE] [--budget BYTES]` (MCP `sinex_brief`) renders the
stored daily `context.pack` for one logical day. Its sections, each with its
drill-down:

1. data quality;
2. activity composition;
3. projects and episodes;
4. outcomes and open work;
5. rest and rhythm;
6. machine incidents;
7. deviations and other findings;
8. unresolved gaps.

- One read context governs every section.
- The current day is labelled partial. A historical day is read either as known then
  or recomputed from current readings, and says which.
- A failed rebuild leaves the previous brief, labelled with its day, revision and
  reason.
- A model-written narrative is a separate attributed `narrative.period`, shown as such
  and never replacing the templated facts; model calls are off for now
  ([model-calling](derivations.md)).
- Building, delivering and reading a brief are separate facts. A scheduled delivery is
  a signal rule firing `at` a local time ([signals](interfaces.md#signals)).

## Agent arrival

Agents rarely ask for history, so context is delivered at session start rather than
waiting to be requested. Ruling, 2026-10-11: "agents rarely look up histoyr but there's
also plans ot have skills and / or AGENTS.md setup … but hooks also probably"
(cc:56c7f026-fd50-4480-b484-72f1299a534c:848fbe0c-20ce-41a5-8475-48322d93a787).

### One brief, composed by Polylogue

- Polylogue's session-start hook (start, resume, compaction, a pasted session ID) is
  the only thing that injects text into a harness session; Sinex installs no injecting
  hook, so an agent receives one brief, not two.
- Polylogue asks Sinex for the **machine section** over the local socket with a
  **100 ms deadline**. Sinex's share is at most 25% of the brief's budget (a setting).
- When Sinex cannot answer, Polylogue writes "machine context: could not read
  (<reason>)", never nothing. Polylogue records what it delivered; Sinex records that it
  served the section, to which session, at which position.
- Without Polylogue, `brief --session-start --cwd DIR` prints the same section for a hook
  the operator wires.

### The machine section

Content, pending before past, every line cited:

1. what is still running or unresolved for this directory's project: a long command or
   job not finished, the last failing check there with no later pass, a failed or
   stalled source;
2. the resume card's trail for the directory's current episode, and recent machine
   events (commands in the directory, files changed, git activity);
3. quiet or behind sources;
4. a pointer to more (`sinexctl context`, `sinexctl recall last`).

- A command with no recorded end says **end not recorded**.
- Directories match by whole path components. Project lines use
  [project attribution](higher-layer.md), so a worktree and its main checkout share
  project lines and keep their own commands.
- The same output rules apply as on every surface; times are in the logical day's zone.

### Prepared, so the request is a read

- The section is a stored product per working directory seen in the last 7 days,
  refreshed as events arrive.
- `brief --session-start --cwd DIR --budget N` returns it cut to the budget, with a cut
  marker and the names of the signal rules that supplied lines.
- A directory not yet prepared is answered "not yet computed" when computing would miss
  the deadline; a stale section returns its reference and age. The request never
  rebuilds synchronously and never calls a model.

### Agents tune it

- Each enabled passive signal rule scoped to the session's project adds its current
  matches as lines within the budget.
- An agent adds a line kind by writing such a rule and drops one with `signal disable`;
  both are attributed and undoable.
- Agents judge a line's value from later annotations, using the rule names Polylogue
  records with each delivery.

## Laws

- **one-assembly**: recall, context, the brief and the machine section over one fixture
  read one context and produce the same cited lines where they overlap.
- **machine-section-deadline**: the machine section answers within the deadline from the
  prepared product; a stale or unprepared directory returns its reason within the
  deadline; `/src/tide` never shows lines of `/src/tidepool`.
- **every-line-cited**: every line of recall, context, the brief and the machine section
  carries a citation that resolves.
- **budget-cuts-whole-lines**: a presentation budget cuts whole lines with an omission
  count and leaves the stored product unchanged.
- **recall-last-honest**: with no matching timed shell session, `recall last` says so and
  never reports "no activity".
- **missing-sources-named**: a window with an expected binding that was missing or behind
  names that binding in the answer, even when the binding has some events in the window.
- **mark-order**: `recall mark N` orders by marked time with a stable tie-break, and a
  backdated mark keeps its marked time.
- **note-revision-keeps-text**: revising a note leaves the earlier note readable, and the
  revision names it.
- **brief-one-context**: every section of one brief reads the same read context, and a
  failed rebuild leaves the previous brief labelled with its day, revision and reason.
