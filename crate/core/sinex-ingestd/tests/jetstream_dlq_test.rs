//! JetStream Dead Letter Queue integration tests

use async_nats::jetstream;
use serde_json::json;
use sinex_core::types::{error::SinexError, Ulid};
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::{Timeouts, WaitHelpers};
use sinex_test_utils::{sinex_test, EventOverrides, TestContext, TestNodePublisher, TestResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

async fn wait_for_consumer(js: &jetstream::Context, base_stream: &str) -> TestResult<()> {
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let base_stream = base_stream.to_string();
            async move {
                let mut stream = js
                    .get_stream(&base_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok(info.state.consumer_count > 0)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;
    Ok(())
}

#[sinex_test]
async fn test_dlq_cases_table() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_shared_nats().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(true);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();

    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![ctx.pipeline_namespace().subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let dlq_stream = format!("{base_stream}_DLQ");
    js.get_or_create_stream(jetstream::stream::Config {
        name: dlq_stream.clone(),
        subjects: vec![ctx.pipeline_namespace().subject("events.dlq.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(
        &env,
        base_stream.clone(),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let consumer = JetStreamConsumer::with_ack_wait(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
        Duration::from_secs(1),
    )
    .with_batch_fetch_config(10, Duration::from_millis(200));
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    wait_for_consumer(&js, &base_stream).await?;

    let publisher =
        TestNodePublisher::with_namespace(nats_client.clone(), "test", Some(namespace.clone()));

    let mut expected_messages = 0u64;
    let wait_for_dlq = |expected_messages: u64| {
        let js = js.clone();
        let dlq_stream = dlq_stream.clone();
        async move {
            WaitHelpers::wait_for_condition(
                || {
                    let js = js.clone();
                    let dlq_stream = dlq_stream.clone();
                    async move {
                        let mut stream = js
                            .get_stream(&dlq_stream)
                            .await
                            .map_err(|e| SinexError::network(e.to_string()))?;
                        let info = stream
                            .info()
                            .await
                            .map_err(|e| SinexError::network(e.to_string()))?;
                        Ok(info.state.messages >= expected_messages)
                    }
                },
                Timeouts::STANDARD,
            )
            .await
        }
    };

    publisher
        .publish_event_with_overrides(
            "test.invalid",
            json!({"data": "test"}),
            EventOverrides {
                ts_orig: Some("invalid-timestamp-format".to_string()),
                ..Default::default()
            },
        )
        .await?;
    expected_messages += 1;
    wait_for_dlq(expected_messages).await?;

    publisher
        .publish_raw_event_bytes("test.malformed", b"{\"id\": \"not-closed\"", None)
        .await?;
    expected_messages += 1;
    wait_for_dlq(expected_messages).await?;

    let incomplete_payload = json!({
        "id": Ulid::new().to_string(),
        "source": "test"
    });
    publisher
        .publish_raw_event_bytes(
            "test.missing_fields",
            serde_json::to_vec(&incomplete_payload)?,
            None,
        )
        .await?;
    expected_messages += 1;
    wait_for_dlq(expected_messages).await?;

    Ok(())
}
