use super::{DLQ_REQUEUE_GENERATION_HEADER, dlq_intent_identity};
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

#[test]
fn malformed_multi_child_intents_keep_full_child_identity() {
    let first = json!({
        "events": [
            {"id": "same-leading-child"},
            {"payload": {"kind": "recoverable-a"}}
        ]
    });
    let second = json!({
        "events": [
            {"id": "same-leading-child"},
            {"payload": {"kind": "recoverable-b"}}
        ]
    });

    let first_identity = dlq_intent_identity(&first).expect("multi-child intent identity");
    let second_identity = dlq_intent_identity(&second).expect("multi-child intent identity");
    assert_ne!(
        first_identity, second_identity,
        "malformed child payloads must not collapse to the leading child id"
    );
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

    let mut durable_failure_ids = Vec::new();
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
        durable_failure_ids.push(durable_failure_id);
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

    // Explicitly remove the bounded NATS copy to model max_age expiry without
    // making the test wait. The Postgres witnesses must remain queryable.
    let mut dlq_stream = js.get_stream(&topology.dlq_stream).await?;
    dlq_stream.purge().await?;
    for durable_failure_id in durable_failure_ids {
        let evidence = sqlx::query!(
            "SELECT dlq_id FROM sinex_schemas.dlq_events WHERE dlq_id = $1",
            durable_failure_id,
        )
        .fetch_optional(ctx.pool())
        .await?;
        assert_eq!(
            evidence.map(|row| row.dlq_id),
            Some(durable_failure_id),
            "Postgres DLQ evidence must outlive bounded NATS retention"
        );
    }
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
    let raw_bytes = serde_json::to_vec(&payload)?;
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "requeue-refail-original");
    js.publish_with_headers(
        topology.events_subject.clone(),
        headers,
        raw_bytes.clone().into(),
    )
    .await?
    .await?;

    let first_raw = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .expect("raw consumer must receive the original intent")?;
    consumer
        .route_to_dlq(&first_raw, "initial admission failure".to_string())
        .await?;
    first_raw.ack().await.map_err(ack_error)?;

    let initial_dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state
        .clone();
    let handler = DlqRetryHandler::new(
        ctx.nats_client(),
        ctx.env().clone(),
        DlqRetryConfig::default(),
    );
    handler
        .retry_sequence_range(
            initial_dlq_state.first_sequence,
            initial_dlq_state.first_sequence,
        )
        .await?;

    let refailed_raw = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .expect("DLQ retry must republish the original raw intent")?;
    assert_eq!(refailed_raw.payload.as_ref(), raw_bytes.as_slice());
    consumer
        .route_to_dlq(
            &refailed_raw,
            "refailed admission within dupeWindow".to_string(),
        )
        .await?;
    refailed_raw.ack().await.map_err(ack_error)?;

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

#[sinex_test]
async fn dlq_route_marks_redacted_and_parse_failure_entries_non_requeueable(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let (consumer, topology, js) = bootstrapped_consumer(&ctx).await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "dlq-route-raw-fidelity-secret",
            "DLQ raw-fidelity test rule",
            "literal",
            "DLQ_ROUTE_SECRET",
            false,
            "redact",
            Some("<REDACTED>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("dlq-route-raw-fidelity-secret", None, None, None, 0)
        .await?;
    let consumer = consumer.with_policy_engine().await?;

    let raw_stream = js.get_stream(&topology.events_stream).await?;
    let raw_consumer_name = format!("dlq-raw-fidelity-{}", Uuid::now_v7());
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

    let redacted_raw =
        br#"{"event_id":"00000000-0000-7000-8000-000000000012","secret":"DLQ_ROUTE_SECRET"}"#;
    let malformed_raw = b"not-json-dlq-raw";
    for (msg_id, raw) in [
        ("dlq-raw-fidelity-redacted", redacted_raw.as_slice()),
        ("dlq-raw-fidelity-malformed", malformed_raw.as_slice()),
    ] {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", msg_id);
        js.publish_with_headers(
            topology.events_subject.clone(),
            headers,
            raw.to_vec().into(),
        )
        .await?
        .await?;
    }

    let first_raw = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .expect("raw consumer must receive the redacted payload")?;
    consumer
        .route_to_dlq(&first_raw, "redacted admission failure".to_string())
        .await?;
    first_raw.ack().await.map_err(ack_error)?;

    let second_raw = tokio::time::timeout(Duration::from_secs(2), messages.next())
        .await?
        .expect("raw consumer must receive the malformed payload")?;
    consumer
        .route_to_dlq(&second_raw, "parse failure".to_string())
        .await?;
    second_raw.ack().await.map_err(ack_error)?;

    let mut dlq_stream = js.get_stream(&topology.dlq_stream).await?;
    let state = dlq_stream.info().await?.state.clone();
    assert_eq!(state.messages, 2);
    let first_entry = dlq_stream.direct_get(state.first_sequence).await?;
    let first_envelope: serde_json::Value = serde_json::from_slice(&first_entry.payload)?;
    assert_eq!(first_envelope["requeueable"], false);
    assert!(first_envelope["raw_bytes_base64"].is_null());
    assert!(
        first_envelope["requeue_blocked_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("privacy policy"))
    );
    let second_entry = dlq_stream
        .direct_get(state.first_sequence.saturating_add(1))
        .await?;
    let second_envelope: serde_json::Value = serde_json::from_slice(&second_entry.payload)?;
    assert_eq!(second_envelope["requeueable"], false);
    assert!(second_envelope["raw_bytes_base64"].is_null());
    assert!(
        second_envelope["requeue_blocked_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("failed JSON parsing"))
    );

    let handler = DlqRetryHandler::new(
        ctx.nats_client(),
        ctx.env().clone(),
        DlqRetryConfig::default(),
    );
    let result = handler
        .retry_sequence_range(state.first_sequence, state.last_sequence)
        .await?;
    assert_eq!(result.retried, 0);
    assert_eq!(result.permanently_failed, 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), messages.next())
            .await
            .is_err()
    );
    Ok(())
}
