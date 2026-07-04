use super::{
    confirmed_events_max_age, confirmed_events_max_bytes, diagnostic_stream_max_age,
    diagnostic_stream_max_bytes,
};
use crate::event_engine::jetstream_consumer::settings::{
    JETSTREAM_BOOTSTRAP_MAX_BYTES, REFLECTION_CONFIRMED_MAX_BYTES,
    REFLECTION_DIAGNOSTIC_MAX_BYTES,
};
use sinex_primitives::nats::JetStreamEventLane;
use tokio::time::Duration;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn activity_stream_caps_keep_full_delivery_budget() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        confirmed_events_max_bytes(JetStreamEventLane::Activity),
        JETSTREAM_BOOTSTRAP_MAX_BYTES
    );
    assert_eq!(
        diagnostic_stream_max_bytes(JetStreamEventLane::Activity),
        JETSTREAM_BOOTSTRAP_MAX_BYTES
    );
    assert_eq!(
        confirmed_events_max_age(JetStreamEventLane::Activity),
        Duration::from_hours(72)
    );
    assert_eq!(
        diagnostic_stream_max_age(JetStreamEventLane::Activity),
        Duration::from_hours(72)
    );
    Ok(())
}

#[sinex_test]
async fn reflection_stream_caps_do_not_reserve_activity_budget() -> xtask::sandbox::TestResult<()>
{
    assert_eq!(
        confirmed_events_max_bytes(JetStreamEventLane::Reflection),
        REFLECTION_CONFIRMED_MAX_BYTES
    );
    assert_eq!(
        diagnostic_stream_max_bytes(JetStreamEventLane::Reflection),
        REFLECTION_DIAGNOSTIC_MAX_BYTES
    );
    assert_eq!(
        confirmed_events_max_age(JetStreamEventLane::Reflection),
        Duration::from_hours(24)
    );
    assert_eq!(
        diagnostic_stream_max_age(JetStreamEventLane::Reflection),
        Duration::from_hours(24)
    );
    Ok(())
}
