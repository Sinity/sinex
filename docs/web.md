# Web

Today, the view Sinex opens with, and the reader web, where the operator and agents
query, browse and trace the substrate. The tokens, the page shell, shared keys, the
palette and copy are the common
[`web-shell`](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/web-shell.md);
the operations every view calls are in [interfaces](interfaces.md), and the answers they
render in [query](query.md) and [recall](recall.md).

## Technology

`sinexd` renders HTML with `maud` from the same typed answers that feed the CLI and MCP,
so a page and the command behind it cannot disagree. Pages are ordinary links and GET
forms; writes are POSTs of operation-table requests. The shared TypeScript shell from
`web-shell` handles keys, the palette, copy, SSE fragment swaps and the timeline's drag
and zoom; live updates arrive as server-rendered fragments over SSE with the commit
position as the event ID, so the browser never assembles a view from JSON. The web is
the one graphical surface: there is no native application, because an installed web
app reaches the desktop's notification and shortcut services while keeping URLs as
citations and one shell shared with Polylogue; desktop integration goes through the
desktop's own services. The reader uses the API only and holds no database of its own.

The web interface is built as a designed product, not a placeholder. Ruling,
2026-10-27: "wasn't it actually designed? what happened to that? why it's so basic?"
(cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3c3dd59f-4560-4fee-95d4-5c43b6c94ca5).

## Access

The reader listens on the configured address with the configured authentication
(default loopback with a peer-uid check), set through the NixOS module, the config file
or environment variables ([configuration](operations.md#configuration)). Reaching it
from a phone or another device is that configuration, not a separate feature.

## Visual baselines

Views are built as drawn in their designs, improved visually without inventing anything
incongruent with Sinex. Ruling, 2026-05-04: "does it impeltn the claude design
faithrfully?" (cc:9afa1f68-65b4-4edd-8276-36fb059dc130:2bbd67ef-65f3-4acf-bdf0-64d7d661c676).

| Baseline | Binds | Where |
|---|---|---|
| Sinex MK2 (2026-05-24) | the overall visual language and layout of the reader | the operator's Claude Design project, `claude.ai-design:6496eecd-8f37-4a77-bec1-7571d1b75519` |
| the reading-surface boards | Day, Trace, Sources, Inputs, Import and Writing | copied here: [day](web/day.html), [trace](web/trace.html), [sources](web/sources.html), [inputs](web/inputs.html), [import](web/import.html), [writing](web/writing.html) |
| the marks design | marks, citations and recall around a mark | its rendered outputs are text; the behaviour is in [recall](recall.md#marks) |
| the machine-brief design | the machine section and the brief as rendered | the behaviour is in [recall](recall.md#the-daily-brief) |

The boards are drawn on an invented world (the user Iris on the host `kestrel`)
and bind layout and what each view shows; where a board's wording or reference
forms differ from these pages, the pages govern. The Backed view (Polylogue on
Sinex) and Signals have no copied board: their drawn versions show behaviour the
design no longer has, so they follow the status contract in
[interfaces](interfaces.md#polylogue-on-sinex) and the nearest board's patterns.
A view with no board follows the nearest baseline's patterns and the shell's tokens.

## Today

One view model serves bare `sinexctl`, a `today` operation over MCP and HTTP, and the
reader's home page.

**Panes**, each independent: when a pane's dependency is missing, it shows its partial
data with its own reason and the others still render.

| Pane | Shows |
|---|---|
| resume and away | the top three candidate episodes, time away since the last operator act, and what arrived `since last_seen` |
| timeline | the logical day so far |
| coverage and health | sources observed, partial, missing (with reason) or not yet read; derivation lag; failures |
| findings | the day's findings |
| signals | passive deliveries not yet seen |
| awaiting the operator | judgments only the operator gives: `duplicates`, library comparisons |

- Today-so-far, a completed logical day, an as-of historical view and a current
  re-reading of history are distinct views and are labelled as such.
- The logical day is shown first; the wake view (`since woke`) is offered beside it
  ([clocks](query.md#clocks-and-the-logical-day)).
- **Seen state.** "Since last seen" is kept per explicit consumer and tied to the
  selected position that was successfully displayed. Building, polling, an agent's read,
  a preview and a failed render do not acknowledge it; an explicit `since` never moves
  the checkpoint. The acknowledgement records presentation state only.
- Counts are neutral. Defer and skip are explicit. A reduced-demand mode asks less of
  the operator without changing any task or judgment state. No copy assigns guilt.

## The reader

Every view has a data contract: the operation it calls and what it must show.

| View | Contract |
|---|---|
| Day | a logical day with lanes (away time, focused window, commands, journal, the derived work session and episodes); "what happened" in plain sentences, each with its sources; "Worth a look" showing `pattern.match` results as co-occurrence, never cause; a click anywhere opens recall at that moment; a note when the day as first read differs from the day after a replay |
| Trace | from a figure down through hourly and daily figures, the fused record, a member's reading with its three clocks and citation, to the stored byte range; what a replay changed |
| Sources | one row per source, one cell per day: observed, partial, missing with reason, or not yet; derivation lag, failures, disk; missing is never drawn as zero |
| Inputs | `sources NAME` per-input accounting ([sources relation](interfaces.md#the-sources-relation)) |
| Import | `stage list`: what holds an import back at each moment; no estimate while the rate varies |
| Backed | Polylogue on Sinex, from both sides ([status](interfaces.md#polylogue-on-sinex)) |
| Signals | rules, their authors, their deliveries, and why each fired |
| Writing | material supplied as text, as drawn on the [writing board](web/writing.html) |
| Search and object | the query box with completion and `read_as`; any object's full view ([read](query.md#read-trace-related)) |
| Firehose | `follow` over all events or a saved scope (coding-agent activity, files, outputs) at full detail, as server fragments over SSE with a resumable position |
| Timeline | a scrubbable, zoomable day and week timeline whose selection opens recall |
| Narratives | `narrative.period` products and the accumulating narrative corpus, with their citations |
| Episodes | the resume card and episode history ([higher layer](higher-layer.md)) |

The firehose is a query into the event stream, which is why it lives in Sinex. Ruling,
2026-07-30: "this is obviously actual good usecase of sinex to prioritize" (journal
2026-07-30 23:51).

- The reader writes only what the CLI can write, through the same operations, with the
  same [computed author](interfaces.md#computed-authorship) (`via: web`).
- Captured text renders as text, never as HTML; captured terminal escapes render inert.
- Fields a rule withholds or masks are absent from the page as from every output
  ([output parity](privacy.md#output-parity)). Pages shown to agents through a browser
  carry the data notice.
- Capabilities that agents would otherwise build as one-off HTML reports belong in these
  views.

## Laws

- **today-one-view-model**: bare `sinexctl`, the `today` operation over MCP and HTTP, and
  the reader's home page render the same panes from the same answer.
- **panes-degrade-alone**: with one pane's dependency missing, that pane shows its
  partial data and reason, and every other pane renders complete.
- **seen-state**: polling, previewing, agent reads and failed renders leave the
  checkpoint unchanged; acknowledging an exact view changes only presentation state.
- **reader-escapes**: captured HTML and terminal escapes render as text.
- **reader-writes-through-operations**: every write the reader makes is an operation-table
  request that the CLI can also make, recorded with `via: web`.
- **missing-never-zero**: a missing cell in Sources or Day is drawn as missing with its
  reason, never as zero.
- **firehose-resumes**: reconnecting the firehose with its last position delivers every
  later event in order, including lifecycle changes, with no gap.
