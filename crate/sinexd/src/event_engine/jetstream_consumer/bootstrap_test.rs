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

/// sinex-bor: `verify_externally_managed_streams_present` must fail loud when
/// the topology's streams don't exist yet — this is the safety property that
/// replaces the old "skip bootstrap and hope" behavior for externally-managed
/// deployments (a stale/incomplete Nix topology must not let sinexd start
/// serving silently).
#[sinex_test]
async fn verify_externally_managed_streams_reports_every_missing_stream(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let js = nats.jetstream_with_client(nats_client.clone());
    let env = sinex_primitives::environment();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = crate::event_engine::JetStreamTopology::new(
        &env,
        ctx.pipeline_namespace().stream("SINEX_BOR_VERIFY_MISSING"),
        ctx.pipeline_namespace().consumer_name("bor-verify-missing"),
        Some(&namespace),
    );

    let validator = crate::event_engine::validator::IngestEventValidator::new(false);
    let consumer = crate::event_engine::JetStreamConsumer::new(
        nats_client.clone(),
        ctx.pool.clone(),
        std::sync::Arc::new(tokio::sync::RwLock::new(validator)),
        topology.clone(),
    );

    // None of this topology's streams exist yet — every one must be named in
    // the resulting error, not just the first.
    let err = consumer
        .verify_externally_managed_streams_present()
        .await
        .expect_err("verification must fail loud when required streams are missing");
    let message = err.to_string();
    for name in [
        topology.events_stream.to_string(),
        topology.confirmed_events_stream.to_string(),
        topology.dlq_stream.to_string(),
        topology.processing_failures_stream.to_string(),
        topology.invalidation_stream.to_string(),
    ] {
        assert!(
            message.contains(&name),
            "missing-stream error should name {name}; got: {message}"
        );
    }

    // Once every required stream exists, verification passes.
    for (name, subject) in [
        (
            topology.events_stream.to_string(),
            topology.events_subject.to_string(),
        ),
        (
            topology.confirmed_events_stream.to_string(),
            topology.confirmed_events_subject.to_string(),
        ),
        (
            topology.dlq_stream.to_string(),
            topology.dlq_subject.to_string(),
        ),
        (
            topology.processing_failures_stream.to_string(),
            topology.processing_failures_subject.to_string(),
        ),
        (
            topology.invalidation_stream.to_string(),
            topology.invalidation_subject.to_string(),
        ),
    ] {
        js.create_stream(async_nats::jetstream::stream::Config {
            name,
            subjects: vec![subject],
            ..Default::default()
        })
        .await?;
    }

    consumer
        .verify_externally_managed_streams_present()
        .await
        .expect("verification must pass once every required stream exists");
    Ok(())
}
