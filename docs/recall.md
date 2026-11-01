# Recall

Answers to "what was happening": selections, the one `recall` verb, recipes and the packs compiled from them, strata, lanes, notes, and how an agent's session receives Sinex's context when it starts. What the products contain and how they are computed (presence, agency, episodes, the resume card) is in [higher layer](higher-layer.md); the query rules every answer follows (read context, budgets, citations) are in [query](query.md); coverage is in [sources](sources.md#completeness); the views that render recall are in [web](web.md).

```sh
sinexctl recall                      # where was I: the top three resume candidates
sinexctl recall 11:00                # the moment pack around 11:00
sinexctl recall yesterday            # the day pack
sinexctl recall here --cwd /src/tide # the here pack for a directory
sinexctl recall last --cwd /src/tide # the latest shell session there
sinexctl note "check bench disk" --when here
```

Decided target (2026-11-01). The current build answers `recall` with a time-ordered list of every event in the window, and keeps `context`, `brief` and `mark` as separate verbs with their own MCP tools. The decided design below replaces them: one verb, strata instead of a row list, recipes compiled into stored packs, and one `note` kind. The laws at the end are its acceptance.

## Purpose

Recall turns stored readings into a record the operator and his agents can stand in: at any instant, day or piece of work they see what was happening, at the grain they want, with every statement traceable to bytes and every unknown shown as unknown. The tests are "what was I doing", "where was I", "what happened while I was away", answered in one or two steps and never invented.

The record reaches the operator in ordinary work (an agent's session start, a hotkey, a notification, the browser's home page), not through a daily ritual. Three rules shape it:

- **The hierarchy answers, not rows.** The first answer to any time question is the derived layers at the right zoom (episodes and sessions, then states and acts, then incidents); raw rows are one step down, never the first screen.
- **One assembly, one coverage source.** Every surface compiles from the same recipes over the same read context, and takes its coverage from the [coverage read model](sources.md#completeness), so two answers about one day cannot disagree.
- **Unknown is not empty.** A section whose sources were not observing says so; it never reads "nothing to report".

## Selections

A **selection** is what every question here is about: a subject plus a read context (commit position, `now`, zone, day start, grants). The subjects are closed:

| Subject | Form | Resolves to |
|---|---|---|
| moment | `now`, `TIME [±DUR]`, `note N`, `sinex:at/…` | an instant with a radius (default ±15 min) |
| window | `FROM..UNTIL`, `sinex:window/…` | a half-open interval |
| period | `today`, `yesterday`, `DATE`, `week`, `month`, `YYYY-Www`, `since last_seen`, `since woke` | logical days ([the logical day](query.md#clocks-and-the-logical-day)), or the wake view |
| episode | `episode REF`, a task ID, a pull request, a branch | one `work.episode` and its member spans |
| project | `project P` | the attributed project's episodes and spans |
| here | `here [--cwd DIR]` | the working directory's project, its current episode and the directory's own acts |
| last | `last [--cwd DIR]`, `last --activity` | the most recent shell session, or activity session |
| query | `(QUERY)` | each span or instant the query returns |

- A selection is citable as `sinex:sel/<hash>` of its canonical form, so a pack, a link in a note and a web URL name the same question; `sinex:window/<from>/<until>` remains the window form. A selection is not stored by itself; the pack compiled from it is.
- `last` keeps the shell meaning: a shell session spans its recorded commands, so its end is the last command seen. `last --cwd DIR` matches by whole path components. With no matching timed shell session it says "no matching timed shell session", never "no activity".
- `since last_seen` reads from this consumer's [seen mark](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/command-surface/README.md), not from source progress.

## The `recall` verb

`recall [SUBJECT] [--recipe NAME] [--budget N] [--rows LANE] [--as-known-then]` is the one verb for time questions. Its subject chooses the default recipe:

| Subject | Default recipe | Prepared |
|---|---|---|
| none | `resume`: the top three candidate episodes, the first in full | yes |
| `now`, `TIME`, `note N` | `moment` | `now` only |
| `today`, `yesterday`, `DATE` | `day` | the current day |
| `week`, `month`, `FROM..UNTIL`, `since …` | `period`: one line per day, then episodes across the span, then figures | no |
| `episode REF`, task, pull request, branch | `resume` for that episode | open and recently suspended episodes |
| `project P` | `project`: episodes, time by agency, open loops | no |
| `here [--cwd]` | `here`: the machine section of an agent's brief | each directory seen in the last 7 days |
| `last [--cwd]` | `moment` over the shell session's span | no |
| `(QUERY)` | `moment` per returned span | no |
| `compare A B` | `compare`: two selections lane by lane | no |

- `--rows LANE` descends to the rows of one lane, paged with the ordinary cursor; the flat row list is an explicit choice, never the default.
- `--as-known-then` compiles at the commit position at the end of the selection's span and is labelled `historical_preview` ([historical previews](#historical-previews)).
- `--budget N` is in tokens (`budget_tokens` over MCP), the presentation budget of the [answer contract](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/command-surface/README.md).
- The current `context` and `brief` verbs become subjects of `recall` (`context --episode X` is `recall episode X`; `brief yesterday` is `recall yesterday`; `brief --session-start --cwd D` is `recall here --cwd D`), and `mark` becomes `note`. One verb per concept follows the rule against commands that are subsets of another ([one entry point](interfaces.md#principles)).

Text renderings start with a header naming the selection in plain words and the logical-day setting, then the coverage header, then sections. Every line ends with a short citation handle (`[e:4f2a]`, the full form with `--cite full`), and `@N` names lines of the last answer, so `recall @3` expands line 3.

## Recipes and packs

A **recipe** is an ordered list of sections, each a named query over the derived layers with a render rule and a status rule. A **pack** is a recipe compiled for one selection under a budget: every line cited, every section's status stated, and what was left out counted with expand handles. Sinex stores a pack as the `context.pack` kind; the resume card, the moment overlay and the here card are boxed renderings of packs, and "card" names only such a rendering. The concept is the same as Polylogue's pack ([vocabulary](vocabulary.md)).

The shipped recipes are `resume`, `moment`, `day`, `period`, `here`, `project`, `compare` and `today`, plus the evidence recipes `study` and `experiment` ([personal evidence](evidence.md)). Recipes are data; the operator and agents add their own.

- **Identity.** A pack's key is the recipe and its version, the canonical selection, settings, evidence identity and compiler version. Compiling the same evidence again returns the same pack.
- **Lifecycle.** Packs are derived events and archive with their inputs. A pack for the current day is provisional until the day closes.
- **Prepared packs**, compiled as evidence arrives so that a request is a read: the current day, every open or recently suspended episode's resume pack, a here pack per working directory seen in the last 7 days, and the moment pack for `now`. Each recipe declares the kinds and scopes its sections read; an admitted event of those kinds in the pack's span marks it dirty, and compilation is debounced (2 s for the current day and `now`, immediate on episode suspension). Compilation reads stored derived events and figures, never raw ambient events. Everything else compiles on request and is stored when it is cited or delivered; an ordinary interactive read is not stored.
- **Every line** has a citation, a stratum and an expand handle (a narrower selection or a lane query), so a reader at a small budget descends without restating the question. An expansion reads at the original answer's read context.
- **Every section** has a status: `complete`, `partial(reason)`, `not_estimable(reason)`, `not_observed(sources)` or `empty`. A section whose evidence is missing never renders "nothing to report", and a not-estimable figure says why.
- **The coverage header** is the coverage facet for the pack's span, restricted to the sources its sections read and the sources expected in that span. An export-only source appears only when the span is newer than its newest export ("Takeout covers to 31 Oct"), never as a daily "not observed".
- **Budgets cut whole lines.** A presentation budget orders pending before past (open loops, running work, stalls, then the trail newest first), cuts whole lines with an omission count, and never changes the stored pack. An agent's `sinex_recall` with no arguments answers in at most 1,200 tokens by default.

### The day pack

The day pack's sections, each with its drill-down: data quality (coverage from the read model, expected sources only); activity composition with unallocated and unknown time; projects and episodes; outcomes and open work; rest and rhythm; machine incidents; "Worth a look", with the selection rule named on each line ([descriptions](higher-layer.md#descriptions-not-insights)); unresolved gaps. Time at the machine comes from presence, or is unknown; it is never reported as zero over a span with presence evidence. A historical day is read either as known then or recomputed from current readings, and says which; a failed rebuild leaves the previous pack, labelled with its day, revision and reason. A scheduled delivery of the day pack is a signal rule firing `at` a local time.

### Historical previews

`--as-known-then` (or an explicit `as of`) reads its historical position through the ordinary query compiler and returns a labelled `historical_preview`: an on-request, unmaterialized presentation, because historical inputs can already be archived and a new live product cannot cite archived parents. The response carries the exact read position, evaluation clock, time-setting version, current grants, source account and raw event citations. Current visibility rules and permanent removal still apply; a historical position grants no additional access. A day pack can show what Sinex knew at the end of that day beside what it knows now, with the replays that changed it.

## Strata

Each registered kind declares one **stratum**, checked at registration (a kind without one is refused):

| Stratum | Meaning | Examples | Where it shows |
|---|---|---|---|
| `act` | someone did something at an instant: the operator, an agent, or a person writing to him | command, edit, prompt, commit, push, page visit, mail sent or received, chat message, copy, note | the trail; act lanes; counted per agency |
| `state` | something held over an interval | focus, presence, agency span, playback, location, rest, unit running, episode, session | "what held"; interval lanes |
| `incident` | a machine or service event worth a person's attention by a declared rule | journal at warning or above, unit failure, check failure, capture stall, crash | the incidents lane |
| `ambient` | continuous observation that is context, not an act | journal below warning, bus traffic, input events, audio and screen segments, environment readings | behind expansion; counted, not listed |
| `measure` | a derived figure | `metric.figure`, `health.figure`, `rhythm.deviation` | figure sections and charts; never rows of a time span |
| `presentation` | an answer, pack, delivery or saved query | `context.pack`, `study.result`, signal deliveries, saved queries | its own object views; never activity, counts or parents of a recall |

A kind may declare a promotion rule: journal entries become `incident` at warning priority or above; a bus message becomes an `act` only through a typed derivation such as `notification.shown`. This one declaration replaces namespace heuristics and recall membership exceptions. Outside producers declare their kinds' strata in their kind registration.

Recall reads a span as strata: states holding at its start and inside it (from the span index), acts grouped by lane and agency, incidents, ambient counts per source, then the measure and presentation objects that cite the span, as links. No source can crowd out another, because no stratum is one time-ordered list, and nothing is cut: each lane carries its complete count and continues by its expand handle.

## Lanes

A **lane** is a timeline row defined by a query: a named query plus a render (`interval`, `point` or `count`, defaulted by the unit's stratum) and a grouping key (application for focus, project for episodes). The timeline element and its series row are common [`web-shell`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/web-shell/README.md)'s. A lane is an ordinary saved query with one more field; saving one is `query save NAME QUERY --lane RENDER`.

The shipped Day lanes, in order: rest; presence and agency (one strip, coloured by agency, hatched where unknown); work episodes; focus; commands (with exit status); pages; communication; media; places (when location capture exists); agents (delegated work by session, from Polylogue's kinds); incidents; notes. A lane whose sources were not observing draws the coverage hatch, never an empty band. In primary mode the AI lanes come from the Polylogue kinds already in Sinex, so drawing them reads no message text and calls no Polylogue.

## Notes

A **note** is an authored statement anchored in time or on events. It is the one kind for moment notes, notes about events, library notes and self-reports, which differ only in anchor and body:

| Field | Values |
|---|---|
| `at` | an instant (default now; `--at` backdates it, and the noted time keeps operator quality) |
| `about` | citations of events, items or other targets |
| `body` | text, and/or a typed value of a declared **instrument** (`mood: 1..5`, `doing: free text`) |
| `client_context` | what the noting client read itself at the press: the focused window, the terminal's foreground command and directory with its git identity. Never event references, because that context is not yet in Sinex when the key is pressed |
| `prompt` | the signal delivery that asked, as parent, when there was one; the response latency follows |
| `trigger` | optional: a standing query that resurfaces the note (a **tripwire**: `--when here` returns it at the next command in that repository) |
| `revises` | the note it revises; the earlier text stays readable |
| author, `via` | computed by the daemon |

- `note ["TEXT"] [--at TIME] [--about REF…] [--value INSTRUMENT=V] [--when QUERY|here] [--revises NOTE]`. It commits in under 1 s, so it sits on a hotkey; a client may supply native command and focus record keys.
- Links from an `at`-anchored note to what it was about arrive later as derived `reference.about` events, by source record keys and containing time.
- `recall note N` orders notes by noted time, newest first, with the ID as a stable tie-break.
- A note is withdrawn by its author's archive act. A tripwire fires once per period its trigger holds, under the signals evaluator's laws. Notes are records about the substrate, never a second model of it, and they obey the [authored-record rules](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/authored/README.md).
- Instruments are declared data (`instruments [list | show NAME]`). Prompts for them are signal rules; none is sent until the operator decides which may be (default; open operator question). Personal evidence's instruments are in [personal evidence](evidence.md#self-reports-and-intake).

## Agent arrival

Agents rarely ask for history, so context is delivered at session start rather than waiting to be requested. Ruling, 2026-10-11: "agents rarely look up histoyr but there's also plans ot have skills and / or AGENTS.md setup … but hooks also probably" (cc:56c7f026-fd50-4480-b484-72f1299a534c:848fbe0c-20ce-41a5-8475-48322d93a787).

### One brief, composed by Polylogue

- Polylogue's session-start hook (start, resume, compaction, a pasted session ID) is the only thing that injects text into a harness session; Sinex installs no injecting hook, so an agent receives one brief, not two.
- Polylogue asks Sinex for the **here pack** (the machine section) over the local socket with a **100 ms deadline**. Sinex's share is at most 25% of the brief's budget (a setting).
- When Sinex cannot answer, Polylogue writes "machine context: could not read (<reason>)", never nothing. Polylogue records what it delivered; Sinex records that it served the pack, to which session, at which position.
- Without Polylogue, `recall here --cwd DIR` prints the same pack for a hook the operator wires.

### The here pack

Pending before past, every line cited:

1. what is still running or unresolved for this directory's project: a long command or job not finished, the last failing check there with no later pass, a stalled source;
2. the current episode's trail (last operator act, last edit location) and recent machine events in the directory;
3. sources not observing;
4. a pointer to more (`recall`, `recall last`).

- A command with no recorded end says **end not recorded**.
- Directories match by whole path components. Project lines use [project attribution](higher-layer.md#project-attribution), so a worktree and its main checkout share project lines and keep their own commands.
- The pack is prepared per working directory seen in the last 7 days. A directory not yet prepared is answered "not yet computed" when computing would miss the deadline; a stale pack returns its reference and age. The request never rebuilds synchronously and never calls a model.
- Each enabled passive signal rule scoped to the session's project adds its current matches as lines within the budget; an agent adds a line kind by writing such a rule and drops one by disabling it, both attributed and undoable.

### Presence for agents

`recall now` includes the operator's presence and agency now (present; absent since T with the evidence kind; unknown with the reason) and his current episode, so agents batch questions while he is away and do not interrupt work in another project. It is read-only: no rule may start or resume an agent from it.

## Laws

- **rec.strata**: recall over any span returns states, acts, incidents, ambient counts and linked presentations as separate strata; no stratum is one cross-source time-ordered list. *(query test)*
- **rec.no-crowding**: adding 10⁶ ambient events to a span changes only the ambient counts; every act and state line is unchanged. *(property)*
- **rec.no-bookkeeping-rows**: no `measure` or `presentation` event appears as an activity row, a source count or a parent of a recall pack. *(query test)*
- **rec.stratum-declared**: registering a kind without a stratum is refused. *(schema test)*
- **rec.complete-lanes**: every lane reports its complete count, and the rows a presentation omits are reachable through its expand handle at the same read context. *(property)*
- **rec.subject-closed**: every subject form parses to one canonical selection, and equal selections share one `sinex:sel/<hash>`. *(parser property)*
- **recall-last-honest**: with no matching timed shell session, `recall last` says so and never reports "no activity".
- **one-assembly**: the day, moment, here and resume packs over one fixture and read context produce identical lines where their sections overlap, and every section of one pack reads the same read context.
- **every-line-cited**: every line of every pack has a resolving citation and a stratum.
- **card.section-status**: a section whose input sources were not observing has status `not_observed(sources)` or `partial`, never `empty` and never "nothing to report"; a not-estimable figure says why.
- **card.unknown-not-zero**: a figure over a span with presence evidence never reports zero time at the machine; over a span without coverage it reports unknown.
- **card.expected-coverage-only**: the coverage header lists only sources expected in the span plus export sources whose newest export ends before it; a never-staged export source does not appear per day.
- **card.surfaces-agree**: for any source and day, recall's coverage, the day pack's data quality and the Sources grid report the same state.
- **card.prepared-read**: a prepared pack is answered from storage within its budget; a missing or stale one returns its reference and age, never a synchronous compile on the hook path.
- **machine-section-deadline**: the here pack answers within Polylogue's 100 ms deadline from the prepared product; a stale or unprepared directory returns its reason within the deadline; `/src/tide` never shows lines of `/src/tidepool`.
- **card.no-invented-next**: the resume pack's Next section contains only recorded next steps; with none, it says unknown.
- **budget-cuts-whole-lines**: a presentation budget cuts whole lines, reports the omission count, orders pending before past, and leaves the stored pack unchanged.
- **card.known-then**: `--as-known-then` over a day reproduces the pack compiled at that day's end; a replay since then shows in the current rendering and in the difference note.
- **note.client-context-only**: a note written at an instant stores only what the client read, never event references; `reference.about` links appear after the evidence is admitted and survive replay by key.
- **note.backdate-kept**: `recall note N` orders by noted time with a stable tie-break, and a backdated note keeps its time.
- **note-revision-keeps-text**: revising a note leaves the earlier note readable, and the revision names it.
- **note.prompt-parent**: an answer to a prompt has the delivery as parent and records its latency; an unanswered prompt is a delivery with no answer, never an imputed value.
- **note.tripwire-once**: a tripwire note fires once per period its trigger holds, under the signals evaluator's laws.
- **agent.handles-consistent**: expanding a handle returns rows at the original answer's read context, even after new admissions. *(MCP test)*

Stored with the scope `sx.`. No rule or delivery starts or resumes an agent from presence or recall state: that is the shared signals law `trigger-never-starts-agent`.
