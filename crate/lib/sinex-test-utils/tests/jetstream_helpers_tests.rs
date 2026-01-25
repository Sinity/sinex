use std::time::Duration;

use async_nats::jetstream::{self, consumer::pull::Config as ConsumerConfig, consumer::AckPolicy};
use color_eyre::eyre;
use serde_json::json;
use sinex_test_utils::{sinex_test, timing_utils::Timeouts, EphemeralNats, TestNodePublisher};
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;

#[sinex_test]
async fn ephemeral_nats_helpers_can_create_streams_and_wait() -> sinex_test_utils::TestResult<()> {
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
            nats.wait_for_subject_messages(
                &target_subject,
                1,
                Duration::from_secs(Timeouts::SHORT),
            )
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

#[sinex_test]
async fn test_node_publisher_emits_events_and_confirmations() -> TestResult<()> {
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

    let publisher = TestNodePublisher::new(nats.connect().await?, "test.source");
    let event_id = publisher
        .publish("test.event", json!({"hello": "world"}))
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
        .wait_confirmation(&event_id, Duration::from_secs(Timeouts::SHORT))
        .await?;

    // Publish a material stream and ensure begin/end messages exist.
    publisher
        .publish_material_stream_via_acquisition_manager([
            b"chunk-a".as_slice(),
            b"chunk-b".as_slice(),
        ])
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

#[sinex_test]
async fn ephemeral_nats_with_chaos_refuses_connections() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn jetstream_redelivery_increments_delivery_count() -> TestResult<()> {
    let nats = EphemeralNats::start().await?;
    let env = sinex_core::environment();
    let subject_prefix = env.nats_subject("tests.redelivery");
    let stream = env.nats_stream_name("TEST_REDELIVERY_STREAM");

    let mut config = ConsumerConfig::default();
    config.ack_policy = AckPolicy::Explicit;
    config.ack_wait = Duration::from_millis(200);
    config.max_deliver = 3;

    let subject = format!("{subject_prefix}.>");
    let (_stream_name, consumer) = nats
        .ensure_stream_with_consumer(&stream, &[subject.as_str()], config)
        .await?;

    let client = nats.connect().await?;
    client
        .publish(format!("{subject_prefix}.one"), b"payload".to_vec().into())
        .await?;

    let mut messages = consumer
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(Timeouts::SHORT))
        .messages()
        .await?;
    let first = timeout(Duration::from_secs(Timeouts::SHORT), messages.next())
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for first delivery"))?
        .ok_or_else(|| eyre::eyre!("no message delivered"))?;
    let first = first.map_err(|err| eyre::eyre!(err.to_string()))?;
    let first_info = first.info().map_err(|err| eyre::eyre!(err.to_string()))?;
    first
        .ack_with(jetstream::AckKind::Nak(Some(Duration::from_millis(50))))
        .await
        .map_err(|err| eyre::eyre!(err.to_string()))?;

    let mut messages = consumer
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(Timeouts::SHORT))
        .messages()
        .await?;
    let second = timeout(Duration::from_secs(Timeouts::SHORT), messages.next())
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for redelivery"))?
        .ok_or_else(|| eyre::eyre!("no redelivered message"))?;
    let second = second.map_err(|err| eyre::eyre!(err.to_string()))?;
    let second_info = second.info().map_err(|err| eyre::eyre!(err.to_string()))?;
    assert!(
        second_info.delivered > first_info.delivered,
        "expected redelivery count to increase ({} -> {})",
        first_info.delivered,
        second_info.delivered
    );
    second
        .ack()
        .await
        .map_err(|err| eyre::eyre!(err.to_string()))?;

    nats.assert_log_does_not_contain(&["[ERR]", "[FTL]"], 200)?;
    Ok(())
}
