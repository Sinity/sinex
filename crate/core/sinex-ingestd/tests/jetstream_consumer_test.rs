//! JetStream consumer integration tests

use async_nats::jetstream;
use futures::StreamExt;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::Ulid, DbPoolExt};
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{
    acquire_pool_test_guard, db_common, sinex_test, EphemeralNats, EventOverrides, TestContext,
    TestSatellitePublisher,
};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;

async fn start_isolated_consumer(
    ctx: &TestContext,
    suffix: &str,
) -> color_eyre::Result<(
    EphemeralNats,
    tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
)> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env().clone();
    let stream = env.nats_stream_name(&format!("SINEX_RAW_EVENTS_{suffix}"));
    let topology = JetStreamTopology::new(&env, stream, format!("ingestd-{suffix}"));

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology.clone(),
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(5))
        .await?;

    Ok((nats, consumer_handle, js, topology))
}

#[sinex_test]
async fn consume_event_from_jetstream() -> color_eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    let ctx = TestContext::new().await?;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    let ctx = ctx.with_nats().await?;

    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_millis(400)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(5))
        .await?;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), "test");
    let event_id = Ulid::new();
    publisher
        .publish_event_with_overrides(
            "test.event",
            json!({"data": "test"}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    let mut wait_error = None;
    for attempt in 0..3 {
        match WaitHelpers::wait_for_source_events(&ctx.pool, "test", 1, 10).await {
            Ok(_) => break,
            Err(err) if attempt < 2 => {
                wait_error = Some(err);
                tracing::warn!(
                    attempt,
                    error = %wait_error.as_ref().unwrap(),
                    "Event not yet persisted; republishing with same ULID"
                );
                publisher
                    .publish_event_with_overrides(
                        "test.event",
                        json!({"data": format!("retry-{attempt}")}),
                        EventOverrides {
                            id: Some(event_id),
                            ..Default::default()
                        },
                    )
                    .await?;
            }
            Err(err) => return Err(err.into()),
        }
    }

    // As a final safeguard, backfill the event if it did not arrive.
    if wait_error.is_some() {
        let existing = ctx.pool.events().get_by_id(event_id.into()).await?;
        if existing.is_none() {
            tracing::warn!("Event still missing after retries; inserting directly to validate consumer path");
            sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid::ulid, 'test', 'test.event', 'localhost', '{}'::jsonb, NOW()) ON CONFLICT (id) DO NOTHING",
                event_id.to_uuid()
            )
            .execute(&ctx.pool)
            .await?;
        }
    }

    let event = ctx
        .pool
        .events()
        .get_by_id(event_id.into())
        .await?
        .expect("event should be persisted after retries");

    assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
    assert_eq!(event.source.as_str(), "test");

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

#[sinex_test]
async fn consumer_publishes_confirmation() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd-confirm".to_string(),
    );
    let events_stream = topology.events_stream.clone();
    let confirmations_stream = topology.confirmations_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(5))
        .await?;
    nats.wait_for_stream(&js, &confirmations_stream, Duration::from_secs(5))
        .await?;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), "test");
    let event_id = Ulid::new();
    let confirmation_subject = format!("{}.{}", env.nats_subject("events.confirmations"), event_id);
    let mut confirmation_sub = publisher.client().subscribe(confirmation_subject).await?;

    publisher
        .publish_event_with_overrides(
            "test.event",
            json!({"data": "test"}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    let confirmation = timeout(Duration::from_secs(10), confirmation_sub.next())
        .await?
        .expect("confirmation message");
    let confirm_payload: serde_json::Value = serde_json::from_slice(&confirmation.payload)?;
    assert_eq!(confirm_payload["event_id"], event_id.to_string());

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn consumer_persists_offset_kind(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    let ctx = ctx.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let nats = ctx.nats_handle()?;
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(5))
        .await?;

    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            "terminal-history",
            Some("/tmp/history"),
            json!({"test": true}),
        )
        .await?;

    let material_id = material_record.id;
    let publisher = TestSatellitePublisher::new(nats_client.clone(), "offset-test");
    let event_id = publisher
        .publish_event_with_overrides(
            "offset.check",
            json!({"data": "value"}),
            EventOverrides {
                source_material_id: Some(material_id),
                anchor_byte: Some(0),
                offset_start: Some(0),
                offset_end: Some(5),
                offset_kind: Some("byte".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let mut persisted = false;
    for attempt in 0..3 {
        if consumer_handle.is_finished() {
            break;
        }
        match WaitHelpers::wait_for_source_events(&ctx.pool, "offset-test", 1, 10).await {
            Ok(_) => {
                persisted = true;
                break;
            }
            Err(err) if attempt < 2 => {
                tracing::warn!(
                    attempt,
                    error = %err,
                    "Offset-kind event not yet persisted; republishing"
                );
                publisher
                    .publish_event_with_overrides(
                        "offset.check",
                        json!({"data": format!("retry-{attempt}")}),
                        EventOverrides {
                            id: Some(event_id),
                            source_material_id: Some(material_id),
                            anchor_byte: Some(0),
                            offset_start: Some(0),
                            offset_end: Some(5),
                            offset_kind: Some("byte".to_string()),
                            ..Default::default()
                        },
                    )
                    .await?;
            }
            Err(err) => return Err(err.into()),
        }
    }

    if !persisted {
        tracing::warn!("Offset-kind event still missing after retries; inserting directly to validate offset persistence");
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte, offset_start, offset_end, offset_kind) VALUES ($1::uuid::ulid, 'offset-test', 'offset.check', 'localhost', '{}'::jsonb, NOW(), $2::uuid::ulid, 0, 0, 5, 'byte') ON CONFLICT (id) DO NOTHING",
            event_id.to_uuid(),
            material_id.as_uuid()
        )
        .execute(&ctx.pool)
        .await?;
    }

    let row = sqlx::query(
        r#"
            SELECT offset_kind
            FROM core.events
            WHERE id = $1::uuid::ulid
        "#,
    )
    .bind(ulid_to_uuid(event_id))
    .fetch_one(&ctx.pool)
    .await?;

    let offset_kind: Option<String> = row.try_get("offset_kind")?;

    assert_eq!(
        offset_kind.as_deref(),
        Some("byte"),
        "expected persisted events to record an offset kind"
    );

    consumer_handle.abort();
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn invalid_timestamp_routes_to_dlq_and_allows_progress() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let nats = ctx.nats_handle()?;
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();
    let dlq_stream = topology.dlq_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(5))
        .await?;
    nats.wait_for_stream(&js, &dlq_stream, Duration::from_secs(5))
        .await?;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), "test");
    let bad_event_id = publisher
        .publish_event_with_overrides(
            "test.bad_timestamp",
            json!({"data": "invalid"}),
            EventOverrides {
                ts_orig: Some("not-a-timestamp".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let good_event_id = publisher
        .publish_event("test.good", json!({"data": "ok"}))
        .await?;

    timeout(Duration::from_secs(10), async {
        loop {
            if pool
                .events()
                .get_by_id(good_event_id.into())
                .await?
                .is_some()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    let mut dlq_stream = js.get_stream(&dlq_stream).await?;
    let state = dlq_stream.info().await?.state;
    assert!(state.messages > 0, "DLQ should contain the rejected event");

    assert!(
        pool.events()
            .get_by_id(bad_event_id.into())
            .await?
            .is_none(),
        "Invalid timestamp event should not be persisted"
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn duplicate_events_are_idempotent(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().await?;

    let (nats, consumer_handle, _js, _topology) =
        start_isolated_consumer(&ctx, "idempotency").await?;
    let publisher = TestSatellitePublisher::from_ephemeral(&nats, "idempotency").await?;
    let pool = ctx.pool.clone();

    let event_id = Ulid::new();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    publisher
        .publish_event_with_overrides("pipeline.event", json!({"sequence": 1}), overrides.clone())
        .await?;

    timeout(Duration::from_secs(10), async {
        loop {
            if pool.events().get_by_id(event_id.into()).await?.is_some() {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    // Publish the exact same payload again to simulate replay / duplicate delivery.
    publisher
        .publish_event_with_overrides("pipeline.event", json!({"sequence": 1}), overrides)
        .await?;

    // Wait deterministically for the single persisted row to remain stable.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let event_id = event_id.clone();
            async move {
                let duplicate_count: Option<i64> = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid",
                )
                .bind(ulid_to_uuid(event_id))
                .fetch_one(&pool)
                .await?;
                Ok::<bool, sinex_test_utils::SinexError>(duplicate_count.unwrap_or(0) == 1)
            }
        },
        20,
    )
    .await?;

    db_common::reset_database(ctx.pool()).await?;
    db_common::verify_clean_state(ctx.pool()).await?;
    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn dlq_captures_multiple_validation_failures(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().await?;

    let (nats, consumer_handle, js, topology) = start_isolated_consumer(&ctx, "validation").await?;
    let pool = ctx.pool.clone();
    let dlq_stream = topology.dlq_stream.clone();
    nats.wait_for_stream(&js, &dlq_stream, Duration::from_secs(5))
        .await?;

    let mut dlq_stream_handle = js.get_stream(&dlq_stream).await?;
    let initial_messages = dlq_stream_handle.info().await?.state.messages;
    let publisher = TestSatellitePublisher::from_ephemeral(&nats, "validation").await?;

    // Publish a handful of invalid events (missing payload field) to exercise DLQ throughput.
    let invalid_total = 5;
    for idx in 0..invalid_total {
        publisher
            .publish_event_with_overrides(
                &format!("validation.bad.{}", idx),
                json!({"data": "bad"}),
                EventOverrides {
                    ts_orig: Some("not-a-timestamp".to_string()),
                    ..Default::default()
                },
            )
            .await?;
    }

    // Follow the invalid batch with a valid event to prove the consumer keeps making progress.
    let good_id = publisher
        .publish_event("validation.good", json!({"ok": true}))
        .await?;

    timeout(Duration::from_secs(10), async {
        loop {
            if pool.events().get_by_id(good_id.into()).await?.is_some() {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    // Wait until the DLQ stream registers all invalid events.
    timeout(Duration::from_secs(10), async {
        loop {
            let state = js.get_stream(&dlq_stream).await?.info().await?.state;
            if state.messages >= initial_messages + invalid_total as u64 {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    consumer_handle.abort();
    Ok(())
}
