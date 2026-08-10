//! Shared helper for reading the raw-events `JetStream` consumer backlog.
//!
//! Both the publish-side backpressure gate ([`crate::runtime::nats_publisher`])
//! and the source-scan-loop pacing controller ([`crate::runtime::pacing`])
//! need to know how far behind the event-engine's raw consumer is. They must
//! consult the SAME underlying signal (`num_pending` on the raw-events
//! stream's event-engine consumer) rather than maintain two independent
//! notions of "backlog" that could disagree — see sinex-2n9.

use async_nats::jetstream::context::ConsumerInfoErrorKind;
use sinex_primitives::environment::SinexEnvironment;

use crate::runtime::RuntimeResult;

/// Resolve the event-engine's raw-events consumer name, honoring the same
/// override env var the event engine itself uses (`SINEX_EVENT_ENGINE_CONSUMER_NAME`).
#[must_use]
pub fn event_engine_raw_consumer_name(env: &SinexEnvironment) -> String {
    std::env::var("SINEX_EVENT_ENGINE_CONSUMER_NAME")
        .unwrap_or_else(|_| format!("event-engine-{}", env.name()))
}

/// Current pending-message depth on the raw-events stream's event-engine
/// consumer.
///
/// Returns `Ok(None)` when the consumer does not exist yet (fresh/dev
/// deployments, or event_engine not started) — callers should treat that as
/// "no pressure signal available", not as zero pressure. Returns `Err` for
/// genuine NATS/network failures (including a missing raw-events *stream*,
/// which is a harder failure than a missing consumer).
pub async fn raw_events_consumer_pending(
    js: &async_nats::jetstream::Context,
    env: &SinexEnvironment,
    namespace: Option<&str>,
) -> RuntimeResult<Option<async_nats::jetstream::consumer::Info>> {
    let stream_name = env.nats_stream_name_with_namespace(namespace, "SINEX_RAW_EVENTS");
    let consumer_name = event_engine_raw_consumer_name(env);

    let stream = js.get_stream(&stream_name).await.map_err(|error| {
        sinex_primitives::SinexError::network("Failed to inspect raw-events stream")
            .with_std_error(&error)
    })?;

    match stream.consumer_info(&consumer_name).await {
        Ok(info) => Ok(Some(info)),
        Err(error) if error.kind() == ConsumerInfoErrorKind::NotFound => Ok(None),
        Err(error) => Err(sinex_primitives::SinexError::network(
            "Failed to inspect event-engine raw consumer",
        )
        .with_std_error(&error)),
    }
}
