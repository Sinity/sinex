# Web

Today, the view Sinex opens with, and the reader web, where the operator and agents
query, browse and trace the substrate, with the keys and the counterparts that tie it to
Polylogue's web. The tokens, the page shell, shared keys, the
palette and copy are the common
[`web-shell`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/web-shell/README.md);
the operations every view calls are in [interfaces](interfaces.md), and the answers they
render in [query](query.md) and [recall](recall.md).

## Technology

`sinexd` renders HTML with `maud` from the same typed answers that feed the CLI and MCP,
so a page and the command behind it cannot disagree. Pages are ordinary links and GET
forms; writes are POSTs of operation-table requests. The shared TypeScript shell from
`web-shell` handles keys, the palette, copy, SSE fragment swaps, one event connection per
browser, and the timeline element, which draws lanes in the client from typed series
rows that Sinex bins server-side (at most one mark per pixel); live updates arrive as server-rendered fragments over SSE with the commit
position as the event ID, so the browser never assembles a view from JSON; the timeline
element is the one exception, drawing typed series rows (decided target). The web is
the one graphical surface: there is no native application, because an installed web
app reaches the desktop's notification and shortcut services while keeping URLs as
citations and one shell shared with Polylogue; desktop integration goes through the
desktop's own services. The reader uses the API only and holds no database of its own.

The web interface is built as a designed product, not a placeholder. (ruling 2026-10-27, cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3c3dd59f-4560-4fee-95d4-5c43b6c94ca5).

## Access

The reader listens on the configured address with the configured authentication
(default loopback with a peer-uid check), set through the NixOS module, the config file
or environment variables ([configuration](operations.md#configuration)). Reaching it
from a phone or another device is that configuration, not a separate feature.

## Visual baselines

Views are built as drawn in their designs, improved visually without inventing anything
incongruent with Sinex. (ruling 2026-05-04, cc:9afa1f68-65b4-4edd-8276-36fb059dc130:2bbd67ef-65f3-4acf-bdf0-64d7d661c676).

| Baseline | Binds | Where |
|---|---|---|
| Sinex MK2 (2026-05-24) | the overall visual language and layout of the reader | the operator's Claude Design project, `claude.ai-design:6496eecd-8f37-4a77-bec1-7571d1b75519` |
| the reading-surface boards | Day, Trace, Sources, Inputs, Import and Writing | copied here: [day](web/day.html), [trace](web/trace.html), [sources](web/sources.html), [inputs](web/inputs.html), [import](web/import.html), [writing](web/writing.html) |
| the marks design | notes at an instant, citations and recall around a note | its rendered outputs are text; the behaviour is in [recall](recall.md#notes) |
| the machine-brief design | the here pack and the day pack as rendered | the behaviour is in [recall](recall.md#recipes-and-packs) |

The boards are drawn on an invented world (the user Iris on the host `kestrel`)
and bind layout and what each view shows; where a board's wording or reference
forms differ from these pages, the pages govern. The Backed view (Polylogue on
Sinex) and Signals have no copied board: their drawn versions show behaviour the
design no longer has, so they follow the status contract in
[interfaces](interfaces.md#polylogue-on-sinex) and the nearest board's patterns.
A view with no board follows the nearest baseline's patterns and the shell's tokens.

## Today

One view model serves bare `sinexctl`, the `today` recipe over MCP (`sinex_recall`) and HTTP, and the reader's home page (`/`). It can be installed as a web app and used as the browser's home tab, which the deployment configures.

```
Sun 1 Nov · logical day from 04:00 · so far 4 h 10 min          [since woke 07:52]   [◀ yesterday]
Since you were last here (Sat 23:40): away 7 h 52 min · agents 2 h 15 min in 3 sessions · 1 waits on you
Not observed: phone since Thu 08:21 (out of contact)                                 [sources →]

RESUME                                                          OPEN LOOPS
1 tide-17 "archive and restore" · open · last you 23:38          ✗ cargo test failed 23:31 on 9f1c…, no later pass
   last edit src/archive.rs:88   [restore ▸]                    ? agent session asked for permission 01:42, unanswered
2 PR #41 "coverage grid" · checks ✓ on head · review open        ✉ thread "quarterly report": you were addressed, no reply (2 d)
3 tide-12 · dormant 3 d · last you Thu 18:02                     ⏰ note "check bench disk" waits for: next command in /src/tide

TODAY ── lanes: rest · you (operator ▇ supervising ▅ unknown ░) · episodes · agents · incidents · coverage

SIGNALS (2 unseen)               AWAITING YOU                       WORTH A LOOK
capture: Atuin stalled 2 d       2 duplicate pairs (command family) check failed, no later pass (rule: open-failure)
note returned: "check bench"     1 library comparison               typing time below prior range (rule: rhythm-outside-range)
```

**Panes**, each independent: when a pane's dependency is missing, it shows its partial data with its own reason and the others still render.

| Pane | Shows |
|---|---|
| since you were last here | time away since the operator's seen mark, agent time apart, what waits on him |
| not observed | sources not `complete` today, with their states ([completeness](sources.md#completeness)) |
| resume | the top three candidate episodes, one line each, the first opening its resume pack with **Restore**, which runs the deployment's configured opener for the worktree, the agent session and still-open windows |
| open loops | unfinished things gathered from episodes, failing checks with no later pass, waits, unanswered threads where the operator was addressed, tripwire notes; each with its evidence and a dismiss that is itself a recorded note. A recall section, not a queue to empty |
| today | the day's lanes so far, with the coverage strip |
| signals | passive notices not yet seen, from both products in primary mode, with an attention line (interruptions today, by whom, opened, dismissed at once) |
| awaiting you | Sinex's review queue (`query:review`): duplicate pairs, library comparisons, judging proposals |
| worth a look | lines chosen by declared selection rules, each naming its rule ([descriptions](higher-layer.md#descriptions-not-insights)); `query:resurface` items |

- Today-so-far, a completed logical day, an as-of historical view and a current
  re-reading of history are distinct views and are labelled as such.
- The logical day is shown first; the wake view (`since woke`) is offered beside it
  ([clocks](query.md#clocks-and-the-logical-day)).
- **Seen marks.** "Since you were last here" reads the consumer's seen mark (common
  `command-surface`'s type, written only when the target was displayed, under
  `web-shell`'s display rule). Building, polling, an agent's read, a preview, a hidden
  tab and a failed render do not acknowledge it; an explicit `since` never moves it.
- Counts are neutral. Defer and skip are explicit. A reduced-demand mode asks less of
  the operator without changing any task or judgment state. No copy assigns guilt.
- Decided target (2026-11-01): the panes above replace the current resume and away,
  findings and awaiting-the-operator panes.

## The reader

Every view has a data contract: the operation it calls and what it must show. Every page is the URL of a reference or a selection (`/<type>/<id>`, `/day/2026-10-31`, `/at/<time>`), so a link in a note, a notification action or an agent's answer opens exactly that view. Rows marked decided target are not built.

| View | Contract |
|---|---|
| Day (`/day/DATE`) | the logical day's lanes ([lanes](recall.md#lanes)); "what happened" in plain sentences, each with its sources; "Worth a look" with rule names; the coverage strip; a selection brush that opens a window pack beside the lanes; an "as first read / now" toggle that shows what a replay changed; previous and next day. A click anywhere opens the moment pack at that instant |
| Timeline (`/timeline?from…&until…`, decided target) | one zoomable element over the lane tiles: year (one column per day, coloured by operator time per agency), month and week (day strips with episode bars), day (the Day lanes), episode (its trail beside its span), act (the object view: reading, three clocks, citations) and bytes (the stored range). Each level is reached by following parent links, so Trace is the bottom zoom level, not a separate destination |
| Trace | from a figure down through hourly and daily figures, the fused record where the family is fused (or the source record itself), a member's reading with its three clocks and citation, to the stored byte range; what a replay changed |
| Moment (overlay on any page, `/at/TIME`; decided target) | the moment pack: what held, what you did (operator acts by lane), what agents did, incidents, notes near it, not observed; the radius widens in steps (±5 min, ±15, ±1 h, the activity session) |
| Episodes (`/episodes`, `/episode/REF`) | episodes by state and project with operator and agent time apart; the resume card with its eight sections and Restore; the suspension and resumption history ([higher layer](higher-layer.md)) |
| Period and Compare (`/period/…`, `/compare?left=…&right=…`; decided target) | day rows with lane sparklines; figures with denominators; two selections' lanes aligned on relative time with differences in plain words |
| Notes (`/notes`; decided target) | notes by time and author with anchors, values and triggers; instruments; revisions |
| Sources (`/sources`) | one row per source (grouped by host), one cell per day in the coverage vocabulary, the state with its reason; derivation lag, failures, stored-unparsed materials, disk runway; missing is never drawn as zero |
| Inputs (`/inputs/ID`) | `sources NAME` per-input accounting: records read, new, the same as another export, revised, removed at source, failures with the first unreadable form ([sources relation](interfaces.md#the-sources-relation)) |
| Stage (`/stage`) | a drop zone and the list of stage operations, each opening to its inventory with per-product accounting, what paces it, and the stored-but-not-interpreted section with candidate parsers ([inventory](materials.md#inventory-and-progress)) |
| Material (`/materials/ID`; decided target) | manifest, part sequence, a record strip (anchors with their readings, live or archived, coloured by run), a byte view of any record range (escapes never executed), the runs that read it, and replay with compare |
| Hosts (`/hosts/NAME`; decided target) | contact, clock offset, spool and inputs with transfer classes ([hosts](hosts.md#surfaces)) |
| Backed | Polylogue on Sinex, from both sides ([status](interfaces.md#polylogue-on-sinex)) |
| Inbox, Why, Signals, Attention (`/notices`, `/signals`, `/attention`) | the common `signals` views: notices by interrupted, held and passive with `why`; rules with author, channel, last firing and usefulness, and a rule editor with a live backtest strip; a 24-hour strip of presence, deep work and quiet hours with every interrupting delivery drawn on it |
| Writing | material supplied as text, as drawn on the [writing board](web/writing.html) |
| Search and object | the query box with completion and `read_as`; any object's full view ([read](query.md#read-trace-related)) |
| Firehose | `follow` over all events or a saved scope at full detail, as server fragments over SSE with a resumable position |
| Narratives | `narrative.period` products and the accumulating narrative corpus, with their citations |
| Library | grid, item, reader, judge, collection and duplicates ([library](library.md#surfaces)) |
| Evidence | figures, studies, experiments, forecasts, money and places ([personal evidence](evidence.md#surfaces)) |

- A **coverage strip** under every timeline (Day, Trace, recall) shows each source's coverage for the visible span, so absence is visible where the eye already is.
- In primary mode, Day and Timeline gain the AI lanes (agent sessions by live interval, prompts, notices) from Polylogue's kinds already in Sinex; **Add as lane** pins any query with times; a selection's second tab shows the AI work in that span through Polylogue; and `g o` goes to the matching place in Polylogue ([counterparts](#keys-and-counterparts)).

### Keys and counterparts

Keys follow the shared table in `web-shell` (the same key means the same act in both products; a focused card rebinds keys visibly as a card layer): Today `g h`, Timeline `g t`, Day `g d`, Episodes `g e`, Sources `g s`, notices `g n`, the counterpart in the other product `g o`; `[` `]` previous and next day or period; `-` `=` zoom, `0` fits the selection; `G` jumps to now; `j` `k` and `Enter` move and descend, `Backspace` ascends; `r` recalls the moment under the cursor; `y` and `Y` copy the focused line's citation and the selection's URL; `a` writes a note at the cursor's instant or on the focused line; `c` compares; `/` opens the query box.

**Counterparts** (decided target): each view maps to the matching place in Polylogue or names why none exists (a library item has none). A Day or Timeline selection maps to Polylogue's Timeline over the same span; an event of a `polylogue.*` kind to the Polylogue object it names; Signals to Signals. Every reference rendered in a page is a link to its owner's view when the owner is configured, and plain text with "not configured" when not; a standalone Sinex renders nothing of Polylogue and calls nothing of it.

**Desktop integration:** two deployment-bound hotkeys, note (a one-line prompt that writes a note with client context) and recall now (a notification showing the moment pack's first lines, with open and note actions), both through the desktop's own services.

The firehose is a query into the event stream, which is why it lives in Sinex. (ruling 2026-07-30, journal 2026-07-30 23:51).

- The reader writes only what the CLI can write, through the same operations, with the
  same [computed author](interfaces.md#computed-authorship) (`via: web`).
- Captured text renders as text, never as HTML; captured terminal escapes render inert.
- Fields a rule withholds or masks are absent from the page as from every output
  ([output parity](privacy.md#output-parity)). Pages shown to agents through a browser
  carry the data notice.
- Capabilities that agents would otherwise build as one-off HTML reports belong in these
  views.

## Laws

- **today-one-view-model**: bare `sinexctl`, the `today` recipe over MCP and HTTP, and
  the reader's home page render the same panes from the same answer.
- **panes-degrade-alone**: with one pane's dependency missing, that pane shows its
  partial data and reason, and every other pane renders complete.
- **seen-state**: polling, previewing, agent reads, hidden tabs and failed renders leave
  the seen mark unchanged; acknowledging an exact view changes only presentation state.
- **reader-escapes**: captured HTML and terminal escapes render as text.
- **reader-writes-through-operations**: every write the reader makes is an operation-table
  request that the CLI can also make, recorded with `via: web`.
- **missing-never-zero**: a missing cell in Sources or Day is drawn as missing with its
  reason, never as zero.
- **web.zoom-parent-closure**: at every timeline zoom, each item links to its parent at
  the next level, and the children's supported seconds sum to the parent's. *(property
  on the tiles read model; decided target)*
Laws with the `joint.` scope are the shared joint suite that both products run; they are stored under `joint.`, not `sx.`.

- **joint.counterpart-total**: every view returns a counterpart URL in Polylogue or a
  stated reason, and every counterpart URL resolves in the other product's test stack.
  *(decided target)*
- **joint.reference-links**: every rendered reference is a link to its owner's view when
  the owner is configured, and plain text with "not configured" when not. *(decided
  target)*
- **joint.ai-lanes-without-text**: drawing the default AI lanes reads no message text
  and makes no call to Polylogue. *(decided target)*
- **joint.standalone-silent**: a standalone Sinex renders no link, lane, panel or count
  of Polylogue and makes no network call to it. *(decided target)*
- **firehose-resumes**: reconnecting the firehose with its last position delivers every
  later event in order, including lifecycle changes, with no gap.
