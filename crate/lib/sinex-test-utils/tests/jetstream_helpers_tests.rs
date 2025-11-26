use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use color_eyre::{eyre, Result};
use serde_json::json;
use sinex_test_utils::{EphemeralNats, TestSatellitePublisher};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_nats_helpers_can_create_streams_and_wait() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let env = sinex_core::environment();
    let subject_prefix = env.nats_subject("tests.sample");
    let wildcard_subject = format!("{subject_prefix}.>");
    let stream = env.nats_stream_name("TEST_SAMPLE_STREAM");

    let (_stream_name, _consumer) = nats
        .ensure_stream_with_consumer(
            &stream,
            &[wildcard_subject.as_str()],
            ConsumerConfig {
                durable_name: Some("helper".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let client = nats.connect().await?;
    let target_subject = format!("{subject_prefix}.foo");
    tokio::try_join!(
        async {
            nats.wait_for_subject_messages(&target_subject, 1, Duration::from_secs(2))
                .await?;
            Ok::<(), color_eyre::Report>(())
        },
        async {
            sleep(Duration::from_millis(25)).await;
            client
                .publish(target_subject.clone(), b"payload".to_vec().into())
                .await?;
            Ok::<(), color_eyre::Report>(())
        },
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_satellite_publisher_emits_events_and_confirmations() -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let env = sinex_core::environment();
    let events_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    let events_subject = env.nats_subject("events.raw.>");
    nats.create_stream(&events_stream, &[events_subject.as_str()])
        .await?;
    let confirmations_stream = format!("{}_CONFIRMATIONS", events_stream);
    let confirmations_subject = env.nats_subject("events.confirmations.>");
    nats.create_stream(&confirmations_stream, &[confirmations_subject.as_str()])
        .await?;

    let begin_stream = env.nats_stream_name("SOURCE_MATERIAL_BEGIN");
    nats.create_stream(
        &begin_stream,
        &[env.nats_subject("source_material.begin").as_str()],
    )
    .await?;
    let slice_stream = env.nats_stream_name("SOURCE_MATERIAL_SLICES");
    nats.create_stream(
        &slice_stream,
        &[env.nats_subject("source_material.slices.>").as_str()],
    )
    .await?;
    let end_stream = env.nats_stream_name("SOURCE_MATERIAL_END");
    nats.create_stream(
        &end_stream,
        &[env.nats_subject("source_material.end").as_str()],
    )
    .await?;

    let publisher = TestSatellitePublisher::new(nats.connect().await?, "test.source");
    let event_id = publisher
        .publish_event("test.event", json!({"hello": "world"}))
        .await?;

    // Ensure the raw event landed on JetStream.
    let mut events_stream_handle = publisher
        .jetstream()
        .clone()
        .get_stream(&events_stream)
        .await?;
    let events_info = events_stream_handle.info().await?;
    assert!(
        events_info.state.messages >= 1,
        "expected at least one raw event message"
    );

    // Publish a manual confirmation and ensure the helper observes it.
    let confirmation_subject = format!("{}.{}", env.nats_subject("events.confirmations"), event_id);
    let js = publisher.jetstream().clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        let payload = serde_json::to_vec(&json!({ "status": "ok" })).unwrap();
        let _ = async {
            let ack = js.publish(confirmation_subject, payload.into()).await?;
            ack.await.map_err(|err| eyre::Report::new(err))?;
            Ok::<(), eyre::Report>(())
        }
        .await;
    });

    publisher
        .wait_confirmation(&event_id, Duration::from_secs(2))
        .await?;

    // Publish a material stream and ensure begin/end messages exist.
    publisher
        .publish_material_stream([b"chunk-a".as_slice(), b"chunk-b".as_slice()])
        .await?;
    let mut begin_stream = publisher
        .jetstream()
        .clone()
        .get_stream(&env.nats_stream_name("SOURCE_MATERIAL_BEGIN"))
        .await?;
    assert!(
        begin_stream.info().await?.state.messages >= 1,
        "expected begin stream message"
    );
    let mut slice_stream = publisher
        .jetstream()
        .clone()
        .get_stream(&env.nats_stream_name("SOURCE_MATERIAL_SLICES"))
        .await?;
    assert!(
        slice_stream.info().await?.state.messages >= 2,
        "expected at least two slice messages"
    );
    let mut end_stream = publisher
        .jetstream()
        .clone()
        .get_stream(&env.nats_stream_name("SOURCE_MATERIAL_END"))
        .await?;
    assert!(
        end_stream.info().await?.state.messages >= 1,
        "expected material end message"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_nats_with_chaos_refuses_connections() {
    let nats = EphemeralNats::start()
        .await
        .expect("nats should start")
        .with_chaos(std::time::Duration::ZERO, 1.0);
    let err = nats
        .connect()
        .await
        .expect_err("chaos failure_rate=1 should force connect error");
    assert!(
        err.to_string()
            .to_lowercase()
            .contains("simulated connection failure"),
        "unexpected error: {err}"
    );
}
