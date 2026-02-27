use async_nats::jetstream;
use serde_json::json;
use sinex_db::repositories::schema_management::{NewEventSchema, SchemaManagementRepository};
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_primitives::{
    domain::{EventSource, EventType},
    error::SinexError,
    Ulid,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

async fn wait_for_dlq_count(js: &jetstream::Context, stream: &str, count: u64) -> TestResult<()> {
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let stream = stream.to_string();
            async move {
                let mut s = js
                    .get_stream(&stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok::<bool, SinexError>(
                    s.info()
                        .await
                        .map_err(|e| SinexError::network(e.to_string()))?
                        .state
                        .messages
                        >= count,
                )
            }
        },
        Timeouts::STANDARD,
    )
    .await?;
    Ok(())
}

#[sinex_test]
async fn test_schema_violation_routes_to_dlq() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let js = async_nats::jetstream::new(nats_client.clone());
    let pool = ctx.pool.clone();
    let env = ctx.env();

    // 1. Register Strict Schema
    let repo = SchemaManagementRepository::new(&pool);
    let schema_content = json!({
        "type": "object",
        "properties": {
            "required_field": { "type": "string" }
        },
        "required": ["required_field"],
        "additionalProperties": false
    });

    repo.register_schema(NewEventSchema {
        source: EventSource::new("test"),
        event_type: EventType::new("test.schema_violation"),
        schema_version: "1.0.0".to_string(),
        schema_content,
    })
    .await?;

    // 2. Setup Consumer (Ingestd) with Strict Validator
    let mut validator = EventValidator::new_strict(true);
    let loaded = validator.reload_schemas(&pool).await?;
    println!("Loaded schemas: {}", loaded);

    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS_DLQ_TEST");
    let dlq_stream = format!("{}_DLQ", base_stream);

    // Subjects must be namespaced
    let events_subject = ctx.pipeline_namespace().subject("events.raw.>");
    let dlq_subject = ctx.pipeline_namespace().subject("events.dlq.>");

    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![events_subject.clone()],
        ..Default::default()
    })
    .await?;

    js.get_or_create_stream(jetstream::stream::Config {
        name: dlq_stream.clone(),
        subjects: vec![dlq_subject.clone()],
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(
        env,
        base_stream.clone(),
        ctx.pipeline_namespace().consumer_name("ingestd_test"),
        Some(ctx.pipeline_namespace().prefix()),
    );

    let consumer = JetStreamConsumer::with_ack_wait(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
        Duration::from_secs(1),
    )
    .with_batch_fetch_config(10, Duration::from_millis(100));

    let _handle = tokio::spawn(async move { consumer.run().await });

    // 3. Publish Invalid Event
    let event_id = Ulid::new();
    let payload = json!({ "wrong_field": "value" });

    let event = json!({
        "id": event_id.to_string(),
        "source": "test",
        "event_type": "test.schema_violation",
        "payload": payload,
        "ts_orig": sinex_primitives::temporal::now(),
        "host": "test-host",
        "node_version": "test"
    });

    let publish_subject = ctx
        .pipeline_namespace()
        .subject("events.raw.test.test_schema_violation");
    nats_client
        .publish(publish_subject, serde_json::to_vec(&event)?.into())
        .await?;

    // 4. Wait for DLQ
    wait_for_dlq_count(&js, &dlq_stream, 1).await?;

    Ok(())
}
