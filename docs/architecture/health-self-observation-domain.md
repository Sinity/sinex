# Health Self-Observation Domain

Medication, substance, symptom, mood, energy, and effect logs are sensitive
event-native records. They should be typed observations and interventions, not
loose markdown notes or knowledge-graph nodes.

This record defines the v1 health/self-observation domain model. It is data
modeling only. It does not provide medical advice, dosing guidance, diagnosis,
or treatment recommendations.

## Event Families

V1 event families:

| Event | Meaning |
| --- | --- |
| `health.substance.intake_recorded` | A medication, supplement, caffeine, alcohol, or other substance intake occurred or was declared. |
| `health.symptom.observed` | User reports or imported source records a symptom. |
| `health.effect.reported` | User reports a perceived effect, adverse reaction, or subjective outcome. |
| `health.mood.observed` | Mood state observation. |
| `health.energy.observed` | Energy/focus/sleepiness observation. |

Avoid `health.intervention.proposed` as a domain event unless it is explicitly
part of the proposal/judgment/finalizer substrate. A proposed intervention is a
candidate assertion, not a canonical health lifecycle event.

## Payloads

```rust
pub struct SubstanceIntakePayload {
    pub occurrence_id: Option<Uuid>,
    pub substance_name: String,
    pub normalized_substance_id: Option<String>,
    pub dose_value: Option<Decimal>,
    pub dose_unit: Option<String>,
    pub route: Option<String>,
    pub form: Option<String>,
    pub taken_at: Timestamp,
    pub timing_quality: TimingEvidenceClass,
    pub time_uncertainty: Option<Duration>,
    pub confidence: Option<Confidence>,
    pub notes_redacted: Option<String>,
}

pub struct EffectReportedPayload {
    pub related_intake_id: Option<Uuid>,
    pub effect_kind: String,
    pub valence: Option<EffectValence>,
    pub intensity: Option<Decimal>,
    pub observed_at: Timestamp,
    pub timing_quality: TimingEvidenceClass,
    pub time_uncertainty: Option<Duration>,
    pub confidence: Option<Confidence>,
    pub notes_redacted: Option<String>,
}

pub struct SymptomObservedPayload {
    pub symptom_kind: String,
    pub intensity: Option<Decimal>,
    pub observed_at: Timestamp,
    pub duration: Option<Duration>,
    pub timing_quality: TimingEvidenceClass,
    pub confidence: Option<Confidence>,
    pub notes_redacted: Option<String>,
}
```

Units/routes/forms should start as validated strings plus normalization tables,
not a prematurely closed enum. Unknown or approximate values are valid when
marked with timing/quantity quality.

## Uncertainty

Do not fake precision.

| Situation | Representation |
| --- | --- |
| "around 9" | timestamp rounded or midpoint plus `time_uncertainty`. |
| "morning" | conceptual/applies-during range with coarse timing quality. |
| "maybe 100mg" | dose value plus low confidence or approximate quantity flag. |
| "some coffee" | no numeric dose; normalized substance can still be recorded. |
| imported exact timestamp | intrinsic timing quality from source. |

Observation and intake events may also use conceptual-time assertions when the
user records an event later than it happened.

## Provenance

| Input mode | Canonical path |
| --- | --- |
| Raw log parser | Register raw log as source material. Clear structured entries can emit material-provenance events; ambiguous entries become proposals/workbench candidates. |
| Structured CLI/UI declaration | Register declaration input as source material; emit material-provenance health event. |
| Imported health app/export | Register export as source material; emit material-provenance events anchored to rows/records. |
| LLM extraction from prose | Record model effect when applicable; emit proposal, not direct canonical health event. |
| Accepted proposal | Finalizer emits canonical event with provenance to proposal and judgment, or records a new user-authored correction as material. |

Subjective effects can link to intake records, but the link is evidence, not a
causal claim. Correlation workflows must stay exploratory unless a separate
judgment/finalizer records an accepted conclusion.

## Privacy Policy

This domain is high sensitivity by default.

| Surface | Policy |
| --- | --- |
| Raw source material | Encrypt, quarantine, or otherwise protect by default; avoid plaintext broad export. |
| Event payload notes | Store redacted/structured notes only by default. |
| Substance names | Treat as sensitive even when normalized. |
| Export | Require explicit scoped export with redaction options. |
| Delete/redact | Must be available before broad ingestion from private logs. |
| Context packs/search | Include caveats and privacy tier; avoid accidental broad inclusion. |

No medical recommendations should be generated from this substrate without a
separate explicitly reviewed product decision. Exploratory correlation output
must label uncertainty and avoid treatment advice.

## Reducer And Queries

The first query surfaces are not "current state" in the task sense; they are
time-series observations and linked episodes:

| Projection | Meaning |
| --- | --- |
| intake timeline | Intake events by substance and time. |
| effect timeline | Effects/symptoms/mood/energy observations over time. |
| episode grouping | Optional bounded windows linking observations around an intake. |
| source coverage | Whether relevant logs/apps were active during a queried period. |

If a future health hypothesis reducer exists, it should use the shared domain
reducer contract and remain distinct from raw intake/effect observations.

## First Slice

The first implementation slice should be explicit structured declarations, not
ambiguous raw-log parsing.

Fixture:

```text
sinexctl declare health intake \
  --substance "caffeine" \
  --dose 100 \
  --unit mg \
  --taken-at 2026-05-17T08:30:00+02:00

sinexctl declare health effect \
  --effect "focused" \
  --intensity 0.7 \
  --observed-at 2026-05-17T09:10:00+02:00
```

Expected behavior:

1. CLI input is registered as source material.
2. Material-provenance health events are emitted.
3. Freeform notes are redacted or omitted by default.
4. Query output can show intake/effect timeline with timing quality.
5. No advice or recommendation is generated.

Raw-log parsing can follow once review/proposal handling for ambiguous entries
is wired.

## Boundaries

- Do not parse ambiguous raw-log entries directly into canonical events.
- Do not make KG own health domain semantics.
- Do not store raw sensitive notes in event payloads by default.
- Do not infer causality from temporal proximity.
- Do not build reminders, scheduling, or recommendation behavior in v1.
