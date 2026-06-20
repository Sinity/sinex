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

pub fn utc_hour_start(timestamp: Timestamp) -> Timestamp {
    Timestamp::from(
        timestamp
            .inner()
            .replace_minute(0)
            .expect("valid UTC hour minute")
            .replace_second(0)
            .expect("valid UTC hour second")
            .replace_nanosecond(0)
            .expect("valid UTC hour nanosecond"),
    )
}

pub fn utc_day_start(timestamp: Timestamp) -> Timestamp {
    Timestamp::from(
        timestamp
            .inner()
            .replace_hour(0)
            .expect("valid UTC day hour")
            .replace_minute(0)
            .expect("valid UTC day minute")
            .replace_second(0)
            .expect("valid UTC day second")
            .replace_nanosecond(0)
            .expect("valid UTC day nanosecond"),
    )
}
