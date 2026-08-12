use super::DLQ_REQUEUE_GENERATION_HEADER;
use crate::{
    event_engine::{JetStreamConsumer, validator::IngestEventValidator},
    runtime::{DlqRetryConfig, DlqRetryHandler},
};
use async_nats::jetstream;
use futures::StreamExt;
use serde_json::json;
use sinex_primitives::{SinexError, Uuid, nats::JetStreamTopology};
use std::sync::Arc;
use tokio::{sync::RwLock, time::Duration};
use xtask::sandbox::prelude::*;

fn ack_error(error: impl std::fmt::Display) -> SinexError {
    SinexError::processing(format!("JetStream ACK failed: {error}"))
}

async fn bootstrapped_consumer(
    ctx: &TestContext,
) -> TestResult<(JetStreamConsumer, JetStreamTopology, jetstream::Context)> {
    let env = ctx.env().clone();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        format!("dlq-route-test-{}", Uuid::now_v7()),
        None,
    );
    let nats_client = ctx.nats_client();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
        topology.clone(),
    );
    consumer.bootstrap_streams().await?;
    Ok((consumer, topology, jetstream::new(nats_client)))
}

/// Two multi-event intents sharing a leading child must remain two recoverable
/// DLQ records. This drives the production JetStream route, including its
/// `Nats-Msg-Id` dupeWindow behavior, rather than merely testing a helper.
#[sinex_test]
async fn dlq_route_keeps_multi_child_intents_with_same_leading_event_recoverable(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let (consumer, topology, js) = bootstrapped_consumer(&ctx).await?;
    let raw_stream = js.get_stream(&topology.events_stream).await?;
    let raw_consumer_name = format!("dlq-multi-child-{}", Uuid::now_v7());
    let raw_consumer = raw_stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(raw_consumer_name.clone()),
            durable_name: Some(raw_consumer_name),
            filter_subject: topology.events_subject.to_string(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;
    let mut messages = raw_consumer.messages().await?;

    // intent A = [X, Y]
    let intent_a = json!({
        "events": [
            {"id": "00000000-0000-7000-8000-000000000001"},
            {"id": "00000000-0000-7000-8000-000000000002"},
        ]
    });
    // intent B = [X, Z] -- same leading event id X, different second sibling
    let intent_b = json!({
        "events": [
            {"id": "00000000-0000-7000-8000-000000000001"},
            {"id": "00000000-0000-7000-8000-000000000003"},
        ]
    });

    for (msg_id, intent) in [("multi-child-a", intent_a), ("multi-child-b", intent_b)] {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", msg_id);
        js.publish_with_headers(
            topology.events_subject.clone(),
            headers,
            serde_json::to_vec(&intent)?.into(),
        )
        .await?
        .await?;
    }

    for _ in 0..2 {
        let message = tokio::time::timeout(Duration::from_secs(2), messages.next())
            .await?
            .expect("raw consumer must receive the published intent")?;
        let durable_failure_id = consumer
            .route_to_dlq(
                &message,
                "adversarial multi-child admission failure".to_string(),
            )
            .await?;
        let evidence = sqlx::query!(
            "SELECT failed_event_id, error_category FROM sinex_schemas.dlq_events WHERE dlq_id = $1",
            durable_failure_id,
        )
        .fetch_one(ctx.pool())
        .await?;
        assert_eq!(
            evidence.failed_event_id,
            Uuid::parse_str("00000000-0000-7000-8000-000000000001")?
        );
        assert_eq!(evidence.error_category, "permanent");
        message.ack().await.map_err(ack_error)?;
    }

    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state
        .clone();
    assert_eq!(
        dlq_state.messages, 2,
        "both [X, Y] and [X, Z] must survive the DLQ stream dupeWindow"
    );
    Ok(())
}

/// A requeued DLQ entry is removed only after its raw republish confirms. If
/// that raw message fails admission again within the DLQ dupeWindow, the
/// refailed entry must get a new Msg-Id and remain recoverable.
#[sinex_test]
async fn dlq_requeue_then_refail_survives_the_dlq_dupe_window(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let (consumer, topology, js) = bootstrapped_consumer(&ctx).await?;
    let raw_stream = js.get_stream(&topology.events_stream).await?;
    let raw_consumer_name = format!("dlq-refail-{}", Uuid::now_v7());
    let raw_consumer = raw_stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: Some(raw_consumer_name.clone()),
            durable_name: Some(raw_consumer_name),
            filter_subject: topology.events_subject.to_string(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;
    let mut messages = raw_consumer.messages().await?;
    let payload = json!({
        "events": [{"id": "00000000-0000-7000-8000-000000000011"}]
    });
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "requeue-refail-original");
    js.publish_with_headers(
        topology.events_subject.clone(),
        headers,
        serde_json::to_vec(&payload)?.into(),
    )
    .await?
    .await?;

    let first_raw = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .expect("raw consumer must receive the original intent")?;
    consumer
        .route_to_dlq(&first_raw, "initial admission failure".to_string())
        .await?;
    first_raw
        .ack()
        .await
        .map_err(ack_error)?;

    let initial_dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state
        .clone();
    let handler = DlqRetryHandler::new(ctx.nats_client(), ctx.env().clone(), DlqRetryConfig::default());
    handler
        .retry_sequence_range(initial_dlq_state.first_sequence, initial_dlq_state.first_sequence)
        .await?;

    let refailed_raw = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .expect("DLQ retry must republish the original raw intent")?;
    consumer
        .route_to_dlq(&refailed_raw, "refailed admission within dupeWindow".to_string())
        .await?;
    refailed_raw
        .ack()
        .await
        .map_err(ack_error)?;

    let mut dlq_stream = js.get_stream(&topology.dlq_stream).await?;
    let state = dlq_stream.info().await?.state.clone();
    assert_eq!(
        state.messages, 1,
        "the refailed message must remain in the DLQ after the requeued entry is removed"
    );
    let entry = dlq_stream.direct_get(state.first_sequence).await?;
    assert_eq!(
        entry
            .headers
            .get(DLQ_REQUEUE_GENERATION_HEADER)
            .map(|value| value.as_str()),
        Some("1"),
        "the refailed DLQ entry must preserve the generation that makes its Msg-Id fresh"
    );
    Ok(())
}
