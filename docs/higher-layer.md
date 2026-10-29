# The higher layer

The higher layer answers the question an operator or an agent has on returning to work: what was I doing, on which project, what changed, what failed, what is still open, and which records establish that? The answer connects commands, windows, pages, files, git, tasks and Polylogue sessions, and every claim in it resolves to stored evidence. This page owns the products that answer it: presence, agency, activity, attention, project attribution, work episodes and the resume card, figures, answers, findings, forecasts, experiments, context packs and narratives, and the remaining derived products. Every product here is a derivation and follows [the derivation model](derivations.md) (keys, versions, time, the common payload, the lifecycle). How recall, context and the brief assemble these products for a caller is in [recall](recall.md); the synthetic world that exercises them end to end is in [testing](testing.md#the-synthetic-world).

## Purpose

These products carry the purposes Lynchpin served as a stopgap, rebuilt on primary data. Ruling, 2026-08-16: "it is literally a stopgap for sinex, but … it is crystallized intelligence regarding how higher layers of sinex should funciton" (cc:ec60fadc-f448-407d-baab-ce3741265a67:3f5eec78-a742-429f-b9e8-6078dbb6b172).

- **Purposes are ported; formats are not.** Lynchpin's NDJSON products, DuckDB snapshots, graph nodes and hard-coded scores are evidence of intent. Sinex never reads them as input and rebuilds each purpose on the original source data. Ruling, 2026-07-07: "Not copied 1-to-1 (since, agin, lynchpin was just a stopgap) - but sinex should >>= lynchpin in basically all kinds of ways" (cc:cce07895-09a1-4d90-9313-3017a899291d:2cd02fc1-4978-4b92-8707-3bae0794d7ad).
- **Results reach the operator in ordinary work**: agent session start, resumption, the brief, Today and signals. A product that needs a new daily ritual to be useful does not count.
- **No prescriptions.** No streaks, productivity scores, nudges, or actions taken on the operator's behalf from a narrative. The products describe; they do not grade.
- **Operator time and agent time are separate measures** everywhere. Agent work runs beside operator work, not instead of it.

## Presence

Presence comes from physical input first, because agents and passive emitters keep windows, titles and notifications changing while the operator is away, so window focus is not evidence that the operator is there.

**Evidence.** Physical input on the non-virtual evdev layer; human prose prompts (Polylogue messages written by the human, excluding dispatch prompts written by orchestrating agents); commands typed in interactive shells; the operator's journal entries, notes and task edits; lock, unlock, suspend and display-off. A uinput event counts as operator input only when paired with its physical source. ActivityWatch AFK and compositor idle are **claims** to reconcile, never facts, and are never rewritten.

**Algorithm**, per scope (host and seat), profile `presence/v1`:

1. Physical events at most 30 s apart form an input bout. Pointer motion counts for presence, not for typing figures.
2. The operator is present from bout start to bout end, extended through a quiet allowance of up to 300 s only when the same foreground window holds on both sides. Display, playback and agent output never extend presence.
3. Absent where lock, suspend or display-off evidence exists, or after the no-input threshold (default 180 s; 120 s is kept in the profile for sensitivity).
4. **Unknown** where no physical-input coverage exists. In that era the fallback reconciles the AFK claim with prompts and commands, labelled `method=claim_reconciled`.

- **Missing capture is not inactivity.** A watcher outage or an unreadable source is a typed gap ([gaps](sources.md#gaps)), and presence over it is unknown.
- `presence.interval` carries `method`, support (bouts, acts), the reconciled claim and contradicting observations. Corrections that Lynchpin applied to archived data (sleep overlap, stuck window, empty app) are labelled rules in the profile, not edits to source records.
- When older keylog history is admitted, presence over that era is recomputed through the ordinary path; there is no special backfill.

## Agency

Each elementary segment of the boundary sweep ([attention](#attention)) gets `agency`, from typed evidence:

| Value | Evidence |
|---|---|
| `operator` | present, and the operator's own acts fall in the segment |
| `supervising` | present, the focused window belongs to a running agent session (exact window-to-session join, [attribution](#project-attribution)), and no operator act points elsewhere |
| `delegated` | an agent is working (tool calls, an open declared job) and the operator is absent or elsewhere |
| `automated` | system work with no agent session |
| `passive` | playback or reading with no input past the quiet allowance |
| `unknown` | no coverage |
| `mixed` | incompatible exact evidence |

`agency` describes the operator's channel; delegated work runs concurrently and is listed beside it.

## Activity windows and sessions

- **Activity evidence is operator-agency evidence only.** Commands, prompts and visits support extent only when their agency is `operator`; focus, tab and clipboard events support context, not activity.
- **`activity.window`** is the local group of operator evidence in a recorded scope, closed after a gap of at least 300 s from the latest supported end, or at known absence or suspend. Hours partition the calculation, with continuation flags instead of fictitious closes. Quiet detection never adds the gap to a duration. A trailing window is provisional.
- **`activity.session`** is a presence envelope. It joins windows across gaps shorter than the break threshold (profile default 15 min) and closes on explicit absence: lock, suspend or a rest interval. Breaks of 2 to 15 min inside it are `short_break` members and never count as focus. It carries `operator_present_s`, short breaks, seconds by agency, and `delegated_overlap_s`; multi-source counts are context counts, not a measure of activity.
- A session's identity is its first stable window; its owning day is the logical day of that start; it may cross any number of hours and days.
- **Episode boundaries come from semantic keys ([episodes](#work-episodes)), never from session gaps**, because fixed time-outs cap at about 70% precision on task boundaries.
- `shell.session` (commands in one terminal, partitioned by host and the native shell or terminal session identity where captured), `activity.session` and `work.episode` are three different things. `recall last` keeps the shell-session meaning; the cross-source selector is explicit ([recall](recall.md)).

## Attention

`activity.span` is the elementary segment: non-overlapping within one scope and partition, cut at every focus, presence, input, display, playback and interruption boundary. It carries scope, period, start and end, the optional containing window, the independent signal states (presence, focus, input, display, playback; each may be unknown), `agency` and its evidence, app, title and window references, input counts, observed and supported durations, profile version, and provisional and continuation flags. Unsupported time is never covered by an envelope.

Attention products count only spans whose agency is `operator` or `supervising`, and report supervising time separately; otherwise agent title churn would produce loops and fragmentation. Lynchpin's parameters are carried as the named profile `attention/lynchpin-v1`, recorded in every output. Parameters qualify a detection; they never discard valid spans.

| Product | Behaviour under `attention/lynchpin-v1` |
|---|---|
| `attention.app_session` | same app and logical day; focused spans ≥ 10 s; 120 s grouping gap; interruptions up to 30 s absorbed and counted; ≥ 60 s observed support; wall bounds and observed union stored separately |
| `attention.deep_work` | a configured detection: compatible project, else configured mode; 600 s gap; 300 s interruption allowance; ≥ 30 min productive support and ≥ 85% of wall extent; numerator and denominator stored |
| `attention.hour_profile` | active, absent and unknown seconds and predominant context per logical day and UTC hour; the < 30 s span filter is exposed with the seconds it omits |
| `attention.loop` | strictly alternating (app, title) contexts, gaps ≤ 180 s, ≥ 4 spans and ≥ 8 min; ordered supporting spans and gaps stored |
| `attention.fragmentation_day` | groups by project, else mode, else app, up to a 5 min gap; `1 − longest / total` under the explicit name of its gap-inclusive denominator; switches, observed and wall seconds |
| `attention.project_day` | time per attributed project, observed and allocated coverage, entropy in bits, Gini, top project; unallocated time reported, never counted as a project |
| `attention.sustained_block` | supported active intervals joined across gaps ≤ 10 min, lasting ≥ 25 min |
| input rhythm and daily composition | typing bursts from original key instants (inter-key gap < 2,000 ms); counts and intervals; supported, context and missing fractions; a day with fewer than 200 key presses is not estimable, with that reason |

Deep work and fragmentation are descriptive detections with their numerators and denominators, not validated scores. "Mode" lookup tables do not ship as labels without their construct declaration ([principles](derivations.md#principles)).

## Project attribution

Attribution is an ordered ladder of exact keys, highest first:

1. **Explicit objective link**: a declared job or task identifier recorded in the agent session, a commit trailer, a branch name or a pull request. Which identifier kinds exist is deployment data.
2. **Agent-session repository**: Polylogue's commit links for commits; for a session, its cwd resolved through captured git identity.
3. **Path inside a recorded root**: a cwd, editor file or `file.changed` path whose git common-dir or root commit was captured at the time. Capture records the git common-dir and root commit with every cwd ([sources](sources.md)). Otherwise an effective-dated root table, held as deployment data, maps paths to projects. Paths match by component, never by string prefix or basename: `/work/app` never owns `/work/apple`. Root moves and aliases are effective-dated facts, never a live filesystem lookup.
4. **Window to terminal to session**: a window and terminal identity adapter maps a focused window to a terminal, and the agent session's terminal identity recorded by the session-start hook (parent PID and the terminal's own identifier) completes an exact map from a focused window to cwd and session. Each compositor and terminal is one adapter implementation; Hyprland window addresses and Kitty window IDs are the first. AI terminal windows inherit a project only through this join.
5. **Browser**: a forge or pull-request URL of a registered repository; a task or job page whose URL form is declared as data.

The exact attribution registry is deployment data under
`project_attribution.projects`: each entry has `project` and optional
`repositories`, `common_dirs`, `root_commits`, and `roots`. A root entry has
`path`, required `from`, and optional exclusive `until`. These are captured
identities and effective-dated facts; derivation never opens a repository.
Structured session `objectives` contain `kind` (`task` or `pull_request`),
`reference` and `repository`. At the highest present tier, competing exact
references remain unresolved. Work acts retain optional native terminal and git
identity, and checks retain `revision`, `check_name` and `check_passed`.
A passing check resolves an earlier failure only for the same repository,
revision and check name. Missing fields keep the failure open.

**Inferred tiers** are qualified proposals that never displace an exact owner:

- normalised title tokens matched against the project registry;
- `inferred_flanked`: a span with no key, flanked within one activity session by the same exact project on both sides with gaps within the quiet allowance;
- `inferred_overlap`: work-event overlap within ±30 s, used only where the window join of rung 4 was not captured.

No numeric confidence floor admits an inferred tier. Each inferred method carries its name, its evidence and its leave-one-signal-out precision (hide an exact key on spans that have one and predict it from the rest). An inferred tier is shown as a visible proposal only when that precision on held-out exact cases meets the profile threshold (proposed 0.9); otherwise the time stays unallocated.

**Conserved operator time.** `activity.project_span` carries interval, scope, activity-span references, project shares as exact rationals, the unallocated share, `ladder_rung` and reason (`exact`, `inferred_*`, `conflicting`) per share, conflicting links, the window-join evidence, the profile and supported seconds.

- For every eligible operator interval, shares plus unallocated equal one.
- One exact uncontradicted owner receives the time. Conflicting exact owners leave it unallocated unless a recorded allocation rule supplies shares; equal division is never invented.
- Cross-device overlap first forms one operator-time union; within a device, source records and the fused records built from them never both count. Three observers of ten minutes are ten operator minutes.
- Operator time, shell runtime, machine work and agent time are separate measures; a background agent's runtime never multiplies operator time. Supervising time is shown beside operator project time, not counted in it, so either total can be read.
- AI commit authorship comes only from trustworthy identities (commit trailers, Polylogue's commit links), never from a commit author string alone.

## Work episodes

### Episodes anchored on objectives

Anchors are structured objectives only, in order: an explicit task reference named in a structured field (a commit trailer, a branch name, a pull request, a session's task link recorded by Polylogue); a pull request; a branch in a repository. No anchor is recognised from command names or prose.

1. Each attributed span, act, commit, check and agent session joins the anchors its exact links name.
2. A span with only a project joins that project's open episode within its activity session only if exactly one is open; otherwise it becomes an unanchored project segment, never a guess between objectives.
3. **An episode persists across days** until its anchor closes (task closed, pull request merged, branch merged or deleted) or until there has been no activity for the dormancy period (profile default 14 days), when it becomes `dormant`, never `closed`.
4. Conflicting exact anchors or a new objective split an episode; late evidence linking two segments to one anchor merges them, keeping old and new membership.
5. A commit-only gap or a shared title never defines human work.

`work.episode` carries the anchor kind and reference; project; scope, supported start and end; `state` (`open`, `dormant`, or `closed` with its closing evidence); `operator_s` and `agent_s`; member references; suspension and resumption references; the last acts as references; open items (a failing check with no later pass on the same commit, unanswered agent questions or permission waits, unchecked todos, open child tasks, uncommitted changes when captured); the coverage snapshot; and split and merge membership links.

**Labels are extractive and name their source**: the task title, pull-request title, branch name, the first human prompt's derived title, or the top commit subjects. An abstractive label written by a model is a [model-calling](derivations.md#model-calling) step.

**Outcomes are distinct facts**: recorded work, commit, push, pull request opened, pull request merged, task closed, check passed on a named commit, deployed revision, accepted behaviour. A closed task is not proof of verification; a nearby passing test does not certify another revision; an agent message saying "done" is an authored claim, not acceptance. Contradictions and reopened work stay visible, and every "current" item carries its observation time and coverage.

### Interruptions and resumptions

- **Suspension**: the operator leaves the episode's context (a focus or project change to another episode, or absence); its time is the last operator act in the episode.
- **Resumption**: the next operator act in the same episode, possibly on another day.
- **Resumption lag**: from the first return of focus to the episode's context until the first productive act there (an edit, a commit, a typed command, a prompt to that session).
- **Source** of the interruption: `external` (a notification, message or alert shown at most 30 s before the switch, with the switch going to its sender's app); `agent` (the switch goes to a window whose agent session just finished or asked for permission or input, as Polylogue reports); `self`; `absence`.
- Excursions under 30 s that return to the same context are absorbed and counted, not split.
- `work.suspension` and `work.resumption` carry the episode, times, source, time away, intervening episodes and resumption lag; their parents are the boundary acts. Together they form the day's interruption profile and the outcome measure for the resume card.

### The resume card

The resume card is an activity trail: a stored [`context.pack`](#context-packs-and-narratives) for one episode, with these sections in order:

1. **Where**: the episode label and its source, anchor, project, state; when last worked on and for how long, operator and agent time apart.
2. **Goal**: the objective as recorded, cited; unknown if none.
3. **Trail**: the last operator and agent acts in time order (edits with file and line where captured, commands with exit status, prompts and the agent's last answer heading, commits, pages), with the last-edit location first, because it is the resumption cue with measured support.
4. **Open**: failing checks with no later pass on the same commit, unanswered agent questions, unchecked todos, a closed task without verification, uncommitted changes.
5. **Since you left**: what agents, CI or reviewers did after the suspension.
6. **Next**: only recorded next steps (todo, handoff, journal, task notes); unknown otherwise. The card never writes an intention for the operator.
7. **Restore**: commands that reopen the context: `polylogue continue <session>`, the worktree path, the pull-request URL, and the windows to refocus through the window identity adapter when still open.
8. **Coverage and caveats**: sources missing in the period, inferred attributions, contradictions.

Asked with no argument, the answer is the top three candidate episodes, ranked by most recent operator act, then open failures and waits, then interruptions from an agent or an external source; one line each, with the full card for the first. Asked with a project, task, session, episode or time, it returns that episode's card.

Polylogue supplies work as an atomic `polylogue.work_snapshot` (kind version 4, reducer version 2), keyed by logical session and reducer version. WorkActs reads only this complete set of typed human message metadata, calls and turns. Tool-call entry IDs identify individual source definitions independently of reused native or display call references. Ambiguous outcomes remain unknown, with partial coverage, and never become a negative failure result. The highest explicit reducer controls a session; older retained reducers stay citable, while an unsupported newer reducer prevents fallback. Each act cites the exact snapshot as its sole parent. Replacing a snapshot archives its previous derived chain before recomputation; an empty replacement retires omitted calls, turns and episodes. The adapter does not combine old per-entry work records with the snapshot.

`work.coverage` records complete, partial or unavailable interpretation, with eligible, timed and untimed entry counts and reason codes. Snapshot positions are commit positions, never wall times. Entries without a recorded start remain readable in the snapshot and contribute an explicit unknown-time count. Unsupported reducers produce unavailable coverage and no fallback to an older reducer's acts. Source time quality, precision and bounds stay explicit. A period's start does not become an action instant; selected sparse source copies keep coverage partial. A retired snapshot contains no session metadata or entries and withdraws the preceding chain with complete zero-entry retirement coverage.

Cards are recomputed on suspension and on new evidence. The session-start hook reads a prepared card; a stale or missing card returns the episode's reference and age, never a synchronous rebuild or a model call (deadline and request shape: [agent arrival](recall.md#agent-arrival)). Polylogue's session-start brief embeds the card by reference with its budget share; Sinex owns presence, agency, attribution, episodes and the machine-side trail, and Polylogue owns its own half ([Polylogue agents](https://github.com/Sinity/polylogue/blob/master/docs/agents.md)).

## Figures

- `metric.figure` and `health.figure`: a metric declares its input kinds, its measure (event count, interval-union seconds, ratio, typed numeric or map sum), units, dimensions and observing sources.
- Hourly figures hold period and day, metric and dimension identity, a nullable value, numerator and denominator, counted and supporting rows, and exact parents. **Daily figures read hourly figures only.** Ruling, 2026-10-11: "yes things are supposed to be hierharhical like that to scale sensibly" (cc:e3e37f29-2490-47ae-89ef-fd40b6242e9a:d716993a-f5d6-4422-aa5e-97d91e430d93).
- Counts are attributed once, at original starts; durations clip the interval union into every overlapping hour. Daily ratios sum numerators and denominators; averages derive from sums and known counts, never means of hourly means. Distinct sources per day come from dimension identities. Overlap reads include every interval that began before the period.
- Coverage per cell is `observed`, `partial`, `recorded_unknown_coverage`, or `missing` with a reason. Sparse positive-event exports never turn silent days into observed zeros.
- Biological health figures and component status are separate kinds.

## Answers and findings

- **Every analysis answer is admitted before it is returned.** `recipe.answer` holds recipe and method versions, normalized typed parameters and settings, the logical-day setting, the period, the event and visibility positions read, the coverage evidence and its hash (including run identities, because a run can end without advancing the commit position), typed estimates or typed not-estimable reasons, denominators and diagnostics, findings and caveats, and its figure or request parents. It is keyed `query:<hash>` ([identity](derivations.md#identity-of-derived-records)).
- Reusing an answer requires the same normalized question, method, grant-sensitive selection and evidence snapshot. Changed evidence marks it dirty, and an exact historical answer stays readable. A computation racing a change either produces a correctly bound historical answer or leaves the key dirty.
- **A finding** declares its evidence class (authorship class, epistemic role, claim status), period, unit, denominator, method and settings, estimability, estimate and uncertainty, caveats, and supporting and contradicting references. Association is never presented as causation.
- **Methods** (correlation, trend, change-point with false-discovery control, effective sample size and refusals) are pure code in `sinex-analysis`; their results are stored events.
- **`rhythm.deviation`** is a stored event per day and measure, computed from a prior-only window (default 28 calendar days with at least 14 observed days, in settings). Too little evidence is not estimable, never a zero deviation. Retrospective baselines are not stored.
- **`pattern.match`** finds configured sequences: kinds, ordered predicates, maximum gap and scope. It stores the template version, ordered parents and actual gaps, labelled as co-occurrence. Build, fix and test cycles, AI work followed by file overlap and a commit, and deep work followed by a commit are templates. Every valid match is stored; a top N is presentation.
- **Relationships are typed reference events and joins**, not a stored graph: mention, about, same resource, explicit task or commit closure, membership; each endpoint is checked at admission. Time, project and containment relations are ordinary joins. Corroboration lists its sources, never a weighted score.
- Agent annotations keep author, session, target, rubric and check state; an unchecked claim is never promoted into a measured fact.

## Forecasts and experiments

- **`forecast.issued`** records the question, resolution rule, horizon, probability or distribution, information cutoff, author and method. Citations of the information it was based on must be recorded before the cutoff.
- **`forecast.resolved`** records the outcome (`true`, `false` or `unknown`), the live evidence that resolves it, and when the outcome happened; without that time, it stays unknown.
- **Scoring is a stored `recipe.answer`**: the Brier score over known outcomes and calibration in ten bins. Two conflicting known resolutions of one forecast make the score not estimable until one is archived; an archived forecast leaves the score.
- **An experiment** is a set of authored records: protocol, assignment, exposure and outcomes. A causal estimand is reported only when assignment was recorded; otherwise results are associations. Planning and running benchmarks belong to the devkit; Sinex analyses their manifests.

## Context packs and narratives

### Context packs

The resume card, the agent machine section, context packs, the daily brief, daily, weekly and monthly narratives and project retrospectives are all built by the one deterministic assembly ([the one assembly](recall.md#one-assembly)) from named queries and stored findings, at one read snapshot with a fixed evaluation instant and settings. Each result is a stored product:

- **`context.pack`** and **`narrative.period`** hold the specification and its version; selectors (project, objective, episode, period); the evaluation instant; settings; the event and visibility positions and coverage identity; ordered sections with typed status and reason, findings, supporting and contradicting citations, and denominators; and complete item counts. The key is the specification, selectors, settings, evidence identity and assembly version.
- The stored product is complete. Budgeted renderings (in bytes or tokens, whole items only, with omission counts and continuation) are presentations ([interfaces](interfaces.md)); they never replace or truncate the stored product, and no fixed day, project or item count truncates it.
- **Resume and context packs are task-scoped**, one episode each, never one global memory; their section order is the [resume card's](#the-resume-card). Day narratives are lists of episode cards plus the presence and agency composition.
- Sections: data quality first (daily and weekly); activity composition with unallocated and unknown time; projects, objectives and last actions; outcomes and open work; rest, rhythm and deviations when their evidence exists; machine incidents; notable patterns; caveats and counterevidence. A missing source has its section status and reason; a day without evidence reports coverage and the absence of supported findings, never a story about inactivity.

### Narratives

Narratives are a first-class product: rich prose over the operator's recent past, written by a model. Ruling, 2026-04-17: "need great prompt, for one thing, to actually induce creation of rich narratives, multifaceted, interesting, insightful, useful" (cc:5472844a-19e2-40fd-8536-c471e3f9f020:767e5798-c84d-4db8-813b-7353ad0b9600).

- **`narrative.period`** (day, week, month, project, thread) is model-written prose over the stored pack and findings, with per-claim citations and slot-bound numbers: every number in the prose is bound to a slot in the pack, and every claim cites its evidence. A narrative with an unsupported number, a nonexistent citation or a lost material caveat is refused at admission.
- The deterministic assembly supplies the evidence and the fallback: template prose from stored facts.
- Model narratives are judged on their own purpose (usefulness, insight, coverage, zero unsupported claims), not on whether they shorten resumption; the resume card leads for resumption.
- Model calls are off ([model calling](derivations.md#model-calling)), so the template fallback is what runs; the contract, prompt design and evaluation are specified so that model narratives can be enabled once they beat it.

### The narrative corpus

- The corpus is a set of `narrative.entry` events per period, project and thread that agents and the operator extend and revise over stored evidence. Each entry has an author (computed by the daemon), citations, and the entry or entries it revises; a revision is a new entry naming its predecessor, never an edit.
- It differs from the assembly: the assembly is deterministic and keyed; the corpus is authored and accumulates.
- It is read in the [reader web](web.md) and through the query language. Agents write entries through the ordinary authored-write path (`record`, [callers](interfaces.md#callers)); outside agents may also write through the ingress as outside derivations ([producers](producers.md)).
- Writing entries with a model is a model-calling feature and is off; the record contract and the reader exist without it.

## Rest and the wake view

- `rest.interval` comes from native wearable and phone exports, with measured or inferred boundaries and the instrument tier (band, phone, or inferred from input absence). A night without samples is `unknown`, never unslept. Source formats are in [source families](source-families.md).
- `day.wake` is derived from `rest.interval` ends: one event per wake, with the rest interval as its parent. It offers a wake-to-wake view beside the logical day ([time](derivations.md#time)).

## Other products

Each is a derivation with its inputs, output kind, key and time rule.

| Product | Inputs → output | Rule |
|---|---|---|
| Media sessions | plays, focus intervals and playback state, each from its own source records → `media.session` | time on a video site in focus versus background listening, so time spent on a site shows among a day's proportions |
| Screen grounding | `screen.text`, accessibility text, `activity.span` → `screen.grounding` | joins recognised text to the attention span it was on screen during |
| Plan versus actual | authored plans (todos, calendar, task estimates) and episodes → `plan.comparison` | descriptive; no score |
| Consumption episodes | plays, visits, reading → `consumption.episode` | grouped by resource and session |
| Communication threads | mail, chat → `thread.membership`, `thread.span` | identity from provider or account IDs or actual reply links, never subject text alone |
| Mark links | `operator.mark` and nearby records → `reference.about` | delayed linking by stable keys and containing time |
| Paste attribution | clipboard and later text → `reference.pasted_from` | exact content identity |
| Machine episodes and incidents | machine metrics, journal, unit intervals → `machine.episode`, `machine.incident` | thresholds from configuration; signatures as declared templates |
| Component reports | typed component observations → `component.report` | requires an actual observation feed and an expected-component set; silence and run rows never imply a healthy status |
| Resource mentions | text fields → `resource.mention` | every URL, path, tool and email mention with exact field and range; an email address is not a person |

### Entities

Exact mention records (`resource.mention`) are built. Merging identities beyond exact rules stays an authored or inferred claim. An entity chain (extraction, resolution, relations, enrichment) is not running; it returns, when needed, as a compare-then-apply operation over labelled evidence, with no live outputs before its evaluation is accepted.

### Self-reports

- Self-reports are authored records: a request material plus an event per answer (mood, energy, what I am doing, a rating of a card), with the computed author and, when there was one, the prompt that elicited it as a parent.
- Elicitation prompts are deliveries of declared signal rules ([signals](interfaces.md#signals)), so any elicitation design is configuration. No prompts are sent until the operator designs them; spontaneous self-reports and an optional monthly rating of resume cards are accepted (default; open operator question).

### Model suggestions

`model.suggestion` events (a proposed link, label or note, with its evidence) are written by a model-calling derivation and are off with the rest ([model calling](derivations.md#model-calling)). A suggestion is never applied without an authored act.

## What Lynchpin's purposes become

Sinex's coverage of Lynchpin is judged by purpose, not by module. The purposes and the products that serve them:

| Purpose | Sinex products |
|---|---|
| day panel and rollups | [figures](#figures), [context packs](#context-packs), Today ([web](web.md)) |
| cross-source correlations (weather, mood, substances, sleep and productivity), load index, life phase, readiness forecasts | [findings](#answers-and-findings), `rhythm.deviation`, [forecasts](#forecasts-and-experiments) |
| sleep and rest | [rest](#rest-and-the-wake-view) |
| activity, input and terminal | [presence](#presence), [agency](#agency), [activity](#activity-windows-and-sessions), [attention](#attention) |
| projects, code and work: velocity, campaign and current state | [attribution](#project-attribution), [episodes](#work-episodes), [the resume card](#the-resume-card) |
| machine incidents and quota advice | [other products](#other-products) |
| evidence graph and presentation: references, corroboration, patterns, packs, narratives | [answers and findings](#answers-and-findings), [context packs and narratives](#context-packs-and-narratives) |

Lynchpin's MCP tools and actions map onto Sinex's ordinary surfaces: status, the analysis catalog, the query language, findings, recipes and the brief ([interfaces](interfaces.md)). Nothing needs a separate operations tool for materializing or pruning, because every product is a maintained derivation. Lynchpin's lessons kept: title normalisation and heartbeat re-merging belong in source parsers; agency is modelled and used; the attention parameters are a named profile.

## Performance

Targets follow [the derivation performance rules](derivations.md#performance).

| Concern | Target |
|---|---|
| Resume and context | a prepared card is read within the hook deadline ([agent arrival](recall.md#agent-arrival)); an on-demand card is warm p95 < 2 s for a 30-day project; the first evidence page arrives in < 1 s |
| Figures | daily figures read hourly figures, verified by rows read; 30-, 365- and 730-day histories measured with selection, computation, admission and rendering apart |

## Laws

- **conservation**: at every boundary, project shares plus unallocated equal one for every operator interval; hourly and daily figures sum to the same supported seconds; three observers of ten minutes are ten minutes.
- **agency-from-evidence**: agent-generated focus, titles, commits and clipboard changes with no operator input yield `delegated` or `automated`, never `operator`, and activity sessions do not extend over them.
- **unknown-is-not-zero**: outages and time before coverage are `unknown` or `missing` in presence, figures and packs, never zero.
- **path-components**: `/work/app` never owns `/work/apple`, and a renamed root attributes history by its effective date.
- **exact-over-inferred**: an inferred tier never displaces an exact owner, and conflicting exact owners stay unallocated.
- **episodes-persist**: an episode resumed after five days is one episode; dormancy never closes it; two tasks in one repository are two episodes.
- **stored-hierarchy**: every answer, finding, pack, card and narrative returned by any surface is a stored event whose citation resolves, and daily figures read only hourly figures.
- **narrative-grounding**: a narrative with an unbound number, a nonexistent citation or a dropped caveat is refused.
- **complete-products**: sessions beyond 10,000 windows, traces beyond 100 levels, and packs over all projects and days are complete.
