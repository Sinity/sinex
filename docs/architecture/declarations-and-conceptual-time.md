# Declarations And Conceptual Time

Passive capture is not the only legitimate input. Some facts become available
because the user states them, because an operator records an intentional gap, or
because a later interpretation assigns the time a fact is about. Those inputs
need provenance and typed semantics instead of becoming notes, missing data, or
provenance-free truth rows.

This record defines three primitives:

| Primitive | Purpose |
| --- | --- |
| Declaration | A user, system, or policy assertion admitted as an event. |
| Intentional omission | An explicit record that capture did not occur by policy or choice. |
| Conceptual time | The time a fact is about, distinct from observation, ingestion, and persistence time. |

## Provenance Rule

Every declaration-like event still obeys XOR provenance.

Material-provenance declarations preserve the input that carried the assertion:
a CLI command, UI form submission, voice transcript, edited note span, imported
file row, or policy-control record is first registered as source material. The
declaration event anchors to that material.

Derived-provenance declarations are only valid when they are derived from
existing events. Inferred assertions from notes, OCR, model output, or parser
workbenches must normally enter as proposals until accepted through the
proposal/judgment/finalizer boundary.

There is no `manual_truth` table and no provenance-free escape hatch.

## Event Shapes

Candidate payloads:

```rust
pub struct DeclarationRecordedPayload {
    pub declaration_kind: DeclarationKind,
    pub subject: SubjectRef,
    pub assertion: serde_json::Value,
    pub conceptual_time: Option<ConceptualTimeRef>,
    pub applies_during: Option<TimeRange>,
    pub actor: ActorRef,
    pub authority: DeclarationAuthority,
    pub confidence: Option<f32>,
}

pub struct IntentionalOmissionPayload {
    pub omission_kind: OmissionKind,
    pub scope: CaptureScope,
    pub time_range: TimeRange,
    pub reason: OmissionReason,
    pub policy_ref: Option<String>,
    pub visibility: OmissionVisibility,
}

pub struct ConceptualTimeAssignedPayload {
    pub target: SubjectRef,
    pub conceptual_time: TimeRange,
    pub basis: ConceptualTimeBasis,
    pub confidence: Option<f32>,
}
```

Declarations are facts in the event stream. Conceptual-time assignments are
assertions about another subject. Intentional omissions are audit records about
absence and must be visibility-scoped.

## Indexes

Indexes make declarations queryable without replacing events as the source of
truth:

```sql
create table core.declaration_index (
  event_id uuid primary key references core.events(id) on delete cascade,
  declaration_kind text not null,
  subject_kind text not null,
  subject_id text not null,
  actor text not null,
  authority text not null,
  applies_during tstzrange,
  conceptual_time tstzrange,
  assertion_hash text not null
);

create table core.intentional_omissions (
  id uuid primary key,
  event_id uuid references core.events(id) on delete set null,
  scope jsonb not null,
  time_range tstzrange not null,
  omission_kind text not null,
  visibility text not null,
  policy_ref text,
  reason text,
  created_at timestamptz not null default now()
);

create table core.conceptual_time_assertions (
  id uuid primary key,
  target_kind text not null,
  target_id text not null,
  conceptual_time tstzrange not null,
  basis text not null,
  event_id uuid references core.events(id),
  confidence double precision,
  created_at timestamptz not null default now()
);
```

These indexes should be rebuildable from `core.events` where visibility policy
allows it. Omission records can intentionally keep only policy metadata when
private-mode denial is stronger than event-level auditability.

## Conceptual Time

Sinex already tracks several clocks:

| Clock | Meaning |
| --- | --- |
| `ts_orig` | When the observed source says something happened. |
| `ts_coided` | When Sinex first observed/coined the event id. |
| `ts_persisted` | When the row reached storage. |
| conceptual time | What time the assertion is about. |

Example: on 2026-05-17 the user records "I started vitamin D in March 2024".
The declaration event is observed on 2026-05-17, but its conceptual time is
March 2024. Queries about capture latency should use `ts_coided`; queries about
health chronology may include conceptual time, clearly labeled as declared
rather than sensed.

Conceptual-time assertions must carry a basis:

| Basis | Meaning |
| --- | --- |
| `user_declared` | The user directly supplied the time. |
| `source_metadata` | The source carried a timestamp or date field. |
| `text_inferred` | A note or document implied the time; this should be proposal-first unless accepted. |
| `policy_assigned` | A deterministic importer assigned a known period. |

## Intentional Omission Boundaries

Not every absence belongs in `core.events`.

| Case | Representation |
| --- | --- |
| Ordinary source disabled intentionally | Material-provenance `intentional.omission` event plus `core.intentional_omissions` index. |
| Parser skipped a known range by policy | Event or operation record with source-material anchor to the policy/control input. |
| Private mode suppresses sensitive live capture | Deniable state may stay outside `core.events`; store only coarse operation metadata if policy permits. |
| Private mode with explicit audit requested | Visibility-scoped omission record with no captured content. |

Private mode is a runtime suppression control, not evidence that something
happened. If private-mode policy says the operator wants deniability, Sinex must
not create detailed omission events that reveal the suppressed activity class.

## Gap Explanations

Gap explanation code consumes omissions as one evidence source:

```text
source continuity gap
  -> matching intentional omission? explain as intentional
  -> matching private-mode interval? explain as private-mode-attributed
  -> matching source health outage? explain as unavailable
  -> otherwise unexplained
```

The explanation must report the visibility class. A private-mode-attributed gap
can say "capture was intentionally suppressed" without exposing a reason,
source payload, or target activity.

## Notes And Candidate Declarations

Notes and documents are source material. They can produce candidate
declarations, but text decomposition does not make them canonical by default.

1. Register the note/document as source material.
2. Parse spans and emit extracted facts as proposals or low-authority
   declarations, depending on source policy.
3. Accepted proposals finalize into declaration events with provenance to the
   proposal and judgment.
4. Domain reducers consume admitted declaration events, not raw notes.

This keeps living documents, notes, and knowledge extraction from silently
becoming a second authority surface.

## Fixtures

The first implementation should include these fixtures:

| Fixture | Shape | Expected trace |
| --- | --- | --- |
| Task declaration | `sinexctl declare task --title "Pay tax" --conceptual-time 2026-04-01` | source material for CLI input -> `declaration.recorded` -> task reducer projection. |
| Conceptual-time assignment | Assign a 2021-03 conceptual month to a material-backed note span. | note material -> proposal or declaration -> `conceptual_time_assertions`. |
| Intentional omission | `sinexctl declare omission --source shell.atuin --from ... --until ... --reason disabled-intentionally` | command material -> `intentional.omission` -> gap explanation cites omission. |

These fixtures are deliberately non-LLM. Model-assisted extraction can reuse the
same path once model effects and proposals are available.
