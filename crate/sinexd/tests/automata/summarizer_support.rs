use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use sinexd::runtime::automaton::{AutomatonContext, DerivedOutput};
use sinexd::runtime::{AutomatonLogicError, Windowed};

pub fn summary_context<P>(ts_orig: Timestamp) -> AutomatonContext
where
    P: EventPayload,
{
    let event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id: event_id,
        source: P::SOURCE,
        event_type: P::EVENT_TYPE,
        ts_orig: Some(ts_orig),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

pub async fn process_windowed_input<W>(
    summarizer: &mut W,
    state: &mut W::State,
    payload: W::Input,
    context: &AutomatonContext,
) -> Result<Option<DerivedOutput<W::Output>>, AutomatonLogicError>
where
    W: Windowed,
{
    summarizer.accumulate(state, payload, context).await?;
    if summarizer.window_complete(state) {
        summarizer.emit(state, context).await
    } else {
        Ok(None)
    }
}

// sinex-2ged: hourly/daily now bucket on the operator-local CIVIL hour/day, so
// these expected-boundary helpers delegate to the same civil functions the
// automata use. (Names kept `utc_*` to avoid churning call sites; at a
// whole-hour tz offset the civil and UTC *hour* boundaries coincide, but the
// *day* boundary shifts, which is exactly what the daily summaries must honour.)
pub fn utc_hour_start(timestamp: Timestamp) -> Timestamp {
    sinexd::automata::civil::floor_to_civil_hour(timestamp)
}

pub fn utc_day_start(timestamp: Timestamp) -> Timestamp {
    sinexd::automata::civil::floor_to_civil_day(timestamp)
}
