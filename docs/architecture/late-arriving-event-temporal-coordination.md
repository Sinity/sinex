# Late-Arriving Event Temporal Coordination

This document closes the design question tracked in `#325`: what should sinex
do when higher-level derived output depends on sources that arrive with
different latencies?

The answer is not one universal mechanism. The runtime already has three
different shapes, and they should stay distinct:

1. eager raw capture for source-truth events,
2. bounded waiting for genuine window semantics,
3. scope invalidation and recomputation for late correction.

## Decision

Sinex should not introduce a general-purpose "provisional derived event" model.

Instead:

- ingestors emit raw material-provenance events eagerly;
- `TransducerNode` stays eager and 1:1;
- `WindowedNode` is the bounded-wait mechanism when window closure is part of
  the domain truth;
- `ScopeReconcilerNode` plus invalidation/replacement is the correction
  mechanism for late-arriving multi-source synthesis.

This keeps provenance honest and query semantics legible:

- a raw event is either present or not;
- a synthetic event is either the current live interpretation or an archived
  superseded interpretation;
- correction happens through replay/invalidation and replacement records, not by
  mutating rows or by inventing a permanent "tentative" status for derived
  events.

## Why Not General Provisional Derived Events

Sinex already has a transport-level provisional/confirmed pattern:

- nodes publish provisional events to NATS;
- `sinex-ingestd` persists them;
- downstream nodes consume confirmations.

That is a pipeline-delivery mechanism. It is not a semantic model for
late-arriving evidence.

Adding a second notion of "provisional" at the derived-event layer would widen
the system in the wrong place:

- queries would need to explain tentative vs final semantics;
- replacement policy would become implicit instead of explicit;
- more nodes would need lifecycle logic they do not actually need;
- the common case of simple, trustworthy 1:1 synthesis would become slower and
  more confusing.

Sinex already has a better correction mechanism: archive, invalidate, recompute,
and record replacement relations.

## The Three Valid Strategies

### 1. Eager Capture For Source Truth

Raw ingestors should not wait for sibling sources to become "more complete."

If the external world reported a filesystem change, a browser visit, a window
focus, or a shell command, sinex should capture that observation immediately as
material provenance. Waiting at the ingest boundary would:

- add latency to the capture plane,
- blur source truth with later interpretation,
- and make replay/backfill semantics harder to reason about.

Late coordination belongs downstream in derived nodes.

### 2. Bounded Waiting For Real Windows

Use `WindowedNode` when the domain is actually about a bounded interval or
closure rule:

- inactivity gap,
- hour/day boundary,
- max-duration bucket,
- max-event-count bucket.

This is appropriate because the output is defined by a completed window, not by
an attempt to "wait and see if more context arrives someday."

Current examples already fit this model:

- `analytics-automaton` closes an activity window on gap, max duration, or max
  event count;
- `session-detector` closes a session when the final `activity.window.summary`
  says the gap boundary has been reached;
- hourly and daily summarizers close on clock boundaries.

### 3. Scope Recompute For Late Correction

Use `ScopeReconcilerNode` when a higher-level interpretation may need to change
after more evidence for the same logical scope arrives later.

The expected pattern is:

1. derive a stable `scope_key` for the logical object being maintained;
2. emit live outputs for that scope as evidence arrives;
3. attach `equivalence_key` when outputs occupy replaceable slots within the
   scope;
4. when replay/backfill/archive changes the scope's working set, recompute the
   scope from persisted inputs;
5. archive stale outputs and record replacement relations.

This gives correction semantics without mutating `core.events` and without
pretending the original output never existed.

## Concrete Cases

### Terminal Command

Problem: terminal-related evidence can arrive at different times. A shell
command may show up from one surface before richer session or activity context
arrives from another.

Decision:

- keep `command.canonical` as an eager `TransducerNode`;
- do not teach it to wait for desktop/browser/session context;
- do not widen `command.canonical` into a generic late-reconciled object.

Reason:

`command.canonical` is currently a faithful 1:1 normalization of one command
event. That output remains useful and honest even when richer context arrives
later.

If future work wants a higher-level object such as "command in activity/session
context," that should be a downstream derived node with its own scope and
replacement semantics, not a mutation of the canonical command layer.

### Desktop Focus

Problem: `window.focused` may arrive before or after other activity signals for
the same real-world period.

Decision:

- capture each `window.focused` event eagerly;
- use windowed activity/session logic for bounded aggregation;
- only use scope recomputation if a future node maintains a replaceable
  per-scope summary of desktop context.

Reason:

The raw focus event is itself a source-truth observation. The derived question
is not "was the focus event provisional?" but "what higher-level activity window
or context summary should this focus contribute to?"

### Browser Visit

Problem: `page.visited` may lag or lead sibling activity signals.

Decision:

- capture `page.visited` eagerly as material provenance;
- let activity windows incorporate it using their existing bounded rules;
- reserve scope recomputation for future cross-source correlated objects, not
  for the raw visit event itself.

Reason:

A browser visit is a real source observation, not a tentative guess. What may
need correction is the later interpretation of that visit inside a broader
session or task scope.

### Session Boundary

Problem: session boundaries necessarily depend on waiting long enough to know a
gap has occurred.

Decision:

- keep this as a `WindowedNode` problem.

Reason:

A session boundary is not a late-correction problem. It is a bounded waiting
problem whose closure condition is part of the domain definition. The current
windowed/session stack is the right shape.

## Authoring Rules

When adding a new derived node:

- choose `TransducerNode` only if the output remains truthful when produced from
  one trigger immediately;
- choose `WindowedNode` only if a bounded completion rule is part of the output
  semantics;
- choose `ScopeReconcilerNode` when the node maintains a logical object whose
  current best interpretation may need replacement as the scope's working set
  changes.

Do not:

- invent a generic derived-event `provisional/final` state machine;
- delay raw capture waiting for other sources;
- use a transducer as an implicit mini-window;
- overload windowed nodes with indefinite "maybe more evidence later"
  semantics.

## Query And Replay Semantics

This decision preserves the existing event model:

- `core.events` stays append-only;
- replay/archive/invalidation produces new outputs rather than mutating old
  rows;
- replacement relations and archived rows explain correction history;
- temporal policy remains explicit on the derived row (`inherit_parent`,
  `latest_input`, `window_boundary`, `declared_effective`).

The important distinction is:

- bounded waiting changes when an output is first emitted;
- scope recomputation changes which output is currently live for a scope;
- neither requires a new "tentative" event kind.

## Immediate Implication

No generic runtime feature needs to be added before canonicalization expands.

The current guidance is:

- keep present eager transducers simple;
- use the existing windowed model for real bounded intervals;
- when a concrete cross-source late-correction node is introduced, build it as a
  scope reconciler with explicit `scope_key` and `equivalence_key` discipline.
