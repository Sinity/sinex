# Data Taxonomy & Derivation Catalog

Settled reference (2026-07-03 Fable session, operator-flagged as load-bearing).
This is the map derivation work navigates by: what kinds of events exist, how
they join, and which cross-source composites pay. Work items live in beads
(`bd list -l composite`); this doc is the classification they cite.

## 1. Organizing dimensions (the space is a product, not a list)

A **source** is a point in:
`capture mode {continuous-stream, scheduled-snapshot, one-shot-import,
external-producer, operator-invoked}` × `input shape {file-tail, sqlite,
journald, dbus, socket, dirwalk, static-file, API-sync, embedded}` ×
`ts_orig quality (RealtimeCapture → … → StagedAt)` × `privacy tier` ×
`volume class (journald ≫ fs ≫ browser ≫ everything else)`.

An **event kind** is a point in `temporal character × semantic role ×
content-bearing?` — the first two do most of the work.

Temporal character (decides how it joins):

- **Point** — command.executed, page.visited, notification.
- **Interval** — session, focus span, sleep. Mostly *derived*, rarely captured.
- **State observation** — "world was X at t" (system snapshot, device state).
- **Delta/transition** — file.modified, workspace.switched; implies a state
  machine.

Semantic role (decides what derivations it feeds):

| Role | Answers | Live coverage (2026-07-03) |
|---|---|---|
| Activity (operator did) | attention, effort | strong: terminal, fs, wm, browser visits, clipboard |
| Content (read/wrote/saw) | topics, knowledge | weak: documents live, screen-OCR partial; browser content, email bodies absent |
| State (machine/world was) | context | good: system.monitor, journald, udev, systemd |
| Communication (dyadic) | people, relationships | near-zero live — email/messaging dark |
| Physiological | body | zero live (sleep export dark) |
| Intentional (operator-authored) | goals, commitments | tasks, records, instructions exist; predictions missing |
| Reflexive (system about itself) | trust, health | over-complete → the telemetry-lane split |

**Rule of thumb the registry violates when unguarded:** a kind earns existence
by naming the derivation it feeds. 159 payloads vs ~20 contracts is what
happens otherwise. Enumerate the combinatorial space beyond current consumers
**as data** (catalog/ledger), not as payload structs.

The one glaring semantic hole is the **people dimension**: with comms dark,
nothing dyadic exists to join on.

## 2. The formal core of cross-source composition

Every composite below is `(time-join ⊕ | entity-join ⋈ | both) + aggregation +
an interpretation`. Time-joins run on overlapping/adjacent `ts_orig` intervals
(interval lifting is L2's job); entity-joins run on resolved entity ids.
**A derivation that needs a third ad-hoc key is usually a missing entity kind.**

- **L2 — mechanical, per-source** (exists or trivial): canonicalization,
  sessionization, rollups, interval lifting from transitions (VPN-on span,
  on-battery span, load-high span).
- **L3 — cross-source composites** (the payoff; see catalog below).
- **L4 — knowledge**: person resolution (email ⋈ git-author ⋈ handle), typed
  relations replacing co-occurrence@0.5, project/topic entities — all LLM-worthy
  and lane-gated; promotion only via judgment.
- **L5 — packs**: recall, briefs, context packs = renderings over L2–L4 with
  honest gaps. No new semantics.

## 3. L3 composite catalog

| Composite | Recipe | Why it matters |
|---|---|---|
| `attention.stream` | wm focus spans ⊕ browser visits ⊕ terminal bursts ⊕ fs activity → one interleaved attention timeline | THE recall substrate; all inputs live; strictly better than any single source |
| `project.attribution` | command cwd ⋈ git repo paths ⋈ fs paths ⋈ window titles ⋈ browser context | honest time-per-project; entity derivable mechanically |
| `work.episode` | session ⊕ attention.stream ⊕ git commits ⊕ agent sessions | the unit of resumption; "what was I doing and did it land" |
| `interruption.event` | notification ⊕ focus-change within Δt | cheap, demoable; both inputs live |
| `screen.grounding` | screen-OCR text ⊕ active-window interval | aligns "what was on screen" to attention; media lane exists |
| `machine.context` overlay | state intervals ⊕ anything | "that build was slow because swap storm" |
| `routine.baseline` / `anomaly.event` | per-hour-of-week aggregates over attention.stream → deviation events | typed data products, **not behavioral narratives** (system computes, operator interprets) |
| `plan.vs.actual` | calendar intervals ⊕ attention.stream; predictions ⋈ resolutions; instructions ⋈ expectations | the intentional loop closed with evidence |
| `change.episode` | agent session ⊕ xtask history ⊕ git ⊕ CI | "this change: intent, toil, verification, outcome" — the self-hosting derivation |
| `consumption.episode` | visit+dwell ⊕ (crawled content ⋈ topic) ⊕ raindrop save ⊕ later reference | "read → saved → used" chains; needs browser content |
| `comm.thread` / `relationship.activity` | email ⊕ messaging ⊕ notifications ⋈ person | the social layer; blocked on comms deployment |

**Priority sequence (value-per-effort, given what is live):**
`attention.stream → project.attribution → work.episode`, with
`interruption.event` as the cheap side-demo — that quartet makes the tower's
value undeniable with zero new capture and zero LLM. Then email deployment
(opens people), browser content (opens topics), GitHub + nix-generations (small
sources, outsized devwork leverage).

## 3.1. L2 interval-lift status

The shared interval-lift lane emits `derived.interval-lift/state.interval`.
Its payload is intentionally generic: `state_kind`, optional `subject_id`,
`start_time`, `end_time`, parent transition event types, duration, and bounded
string attributes. Source-specific rules belong in the interval-lift automaton,
not in one-off span emitters.

Live first rule:

| Rule | Input transitions | Output `state_kind` | Consumer |
|---|---|---|---|
| Hyprland focus | `wm.hyprland/window.focused` N and N+1 | `desktop.focus` | `attention.stream`, `screen.grounding`, `machine.context` |

This keeps capture as point/transition evidence and makes intervals a derived
mechanism with parent refs to the exact opening and closing observations.

## 4. Design rules for extending the taxonomy (review-enforceable)

1. A new kind must name its **consumer derivation**, temporal character,
   occurrence key, and ts_orig quality — or it is vocabulary debt.
2. Content-bearing events reference materials/CAS; never embed bulk (the 10MB
   NATS cap is the tripwire).
3. Prefer state-transition capture + derived state intervals over polling
   snapshots.
4. Interval lifting is L2's job — capture stays points/transitions; sources do
   not synthesize spans.
5. Every L3+ composite is an automaton with a `semantics_version` and lane
   testability; confidence values carry evidence provenance or are absent.
6. Volume-class the kind up front; nothing joins the wildcard fan-out without a
   volume budget (the telemetry-lane lesson).
7. Cross-source joins only through the two keys (time, entity); a third ad-hoc
   key means a missing entity kind.
