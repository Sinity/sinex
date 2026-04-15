//! Integration tests for the SSE event subscription stream.
//!
//! Tests cover:
//! - Bus bookkeeping (register/unregister)
//! - Filter-based event delivery via the `SubscriptionBus`
//! - Gap detection for slow consumers
//! - HTTP-level auth rejection on the SSE endpoint

use serde_json::json;
use sinex_gateway::sse_bus::{MAX_ACTIVE_SUBSCRIPTIONS, SseMessage, SubscriptionBus};
use sinex_primitives::query::{PayloadFilter, SubscriptionFilter};
use sinex_primitives::temporal;
use sinex_primitives::{EventSource, EventType, Uuid as CoreUuid};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use xtask::sandbox::sinex_test;

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

/// Insert a test event directly into the database, bypassing the ingestion pipeline.
/// Returns the `UUIDv7` of the inserted event.
///
/// Uses material provenance (`source_material_id` set, `source_event_ids` NULL)
/// to satisfy the `events_check` constraint that enforces XOR provenance.
/// Creates a source material record first to satisfy the FK constraint.
async fn insert_test_event(
    pool: &sqlx::PgPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
) -> color_eyre::Result<CoreUuid> {
    let id = CoreUuid::now_v7();
    let material_id = CoreUuid::now_v7();
    // Unique identifier for the source material (must be unique per constraint).
    let source_identifier = format!("test-{material_id}");

    // Insert source material to satisfy FK.
    sqlx::query(
        r"INSERT INTO raw.source_material_registry
           (id, material_kind, source_identifier, status, timing_info_type)
           VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime')",
    )
    .bind(material_id)
    .bind(&source_identifier)
    .execute(pool)
    .await?;

    // Insert event with material provenance.
    // anchor_byte is required by the domain conversion layer for material provenance.
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_material_id,
            anchor_byte
        ) VALUES (
            $1::uuid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::uuid,
            $8
        )
        "#,
        id,
        source,
        event_type,
        host,
        payload,
        *temporal::now(),
        material_id,
        0i64,
    )
    .execute(pool)
    .await?;
    Ok(id)
}

/// Publish a fake confirmation message to NATS (mimics what ingestd does after persisting).
async fn publish_confirmation(
    nats: &async_nats::Client,
    env_name: &str,
    event_id: &CoreUuid,
) -> color_eyre::Result<()> {
    let id_str = event_id.to_string();
    let subject = format!("{env_name}.events.confirmations.{id_str}");
    let payload = serde_json::to_vec(&json!({
        "event_id": id_str,
        "persisted": true,
        "ts_ingest": sinex_primitives::Timestamp::now().format_rfc3339(),
    }))?;
    nats.publish(subject, payload.into()).await?;
    nats.flush().await?;
    Ok(())
}

/// Spawn the bus run loop with a readiness signal, and wait until it's subscribed to NATS.
/// Returns the shutdown sender and join handle.
async fn spawn_bus_ready(
    bus: &Arc<SubscriptionBus>,
    nats: async_nats::Client,
    pool: sqlx::PgPool,
    env: sinex_primitives::environment::SinexEnvironment,
) -> color_eyre::Result<(watch::Sender<bool>, tokio::task::JoinHandle<()>)> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let ready = Arc::new(Notify::new());
    let ready_clone = Arc::clone(&ready);
    let bus_clone = Arc::clone(bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run_with_ready(nats, pool, env, shutdown_rx, Some(ready_clone))
            .await;
    });
    // Wait until the bus has subscribed to NATS before returning.
    tokio::time::timeout(Duration::from_secs(5), ready.notified()).await?;
    Ok((shutdown_tx, bus_task))
}

/// Receive the next `SseMessage` from the channel with a timeout.
async fn recv_timeout(
    rx: &mut tokio::sync::mpsc::Receiver<SseMessage>,
    timeout: Duration,
) -> Option<SseMessage> {
    tokio::time::timeout(timeout, rx.recv())
        .await
        .ok()
        .flatten()
}

async fn drain_until_idle<F>(
    rx: &mut tokio::sync::mpsc::Receiver<SseMessage>,
    idle_timeout: Duration,
    mut on_message: F,
) where
    F: FnMut(SseMessage),
{
    while let Some(message) = recv_timeout(rx, idle_timeout).await {
        on_message(message);
    }
}

async fn assert_task_stays_running(
    task: &tokio::task::JoinHandle<()>,
    window: Duration,
    context: &str,
) -> color_eyre::Result<()> {
    let finished = tokio::time::timeout(window, async {
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    assert!(!finished, "{context}");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Tests: Bus bookkeeping
// ─────────────────────────────────────────────────────────────────────

#[sinex_test]
async fn register_and_unregister_updates_count() -> TestResult<()> {
    let bus = SubscriptionBus::new();
    assert_eq!(bus.active_count(), 0);

    let (id1, _rx1) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");
    assert_eq!(bus.active_count(), 1);

    let (id2, _rx2) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");
    assert_eq!(bus.active_count(), 2);

    bus.unregister(id1);
    assert_eq!(bus.active_count(), 1);

    // Unregistering the same ID again is a no-op.
    bus.unregister(id1);
    assert_eq!(bus.active_count(), 1);

    bus.unregister(id2);
    assert_eq!(bus.active_count(), 0);

    Ok(())
}

#[sinex_test]
async fn drop_receiver_cleans_up_on_next_flush() -> TestResult<()> {
    let bus = SubscriptionBus::new();
    let (id, rx) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");
    assert_eq!(bus.active_count(), 1);

    // Drop the receiver — the bus won't notice until the next send attempt.
    drop(rx);

    // Manual unregister still works.
    bus.unregister(id);
    assert_eq!(bus.active_count(), 0);

    Ok(())
}

#[sinex_test]
async fn register_enforces_active_subscription_limit() -> TestResult<()> {
    let bus = SubscriptionBus::new();
    let mut subscriptions = Vec::new();

    for _ in 0..MAX_ACTIVE_SUBSCRIPTIONS {
        subscriptions.push(
            bus.register(SubscriptionFilter::default(), None)
                .expect("subscriptions below the hard cap should register"),
        );
    }

    assert_eq!(bus.active_count(), MAX_ACTIVE_SUBSCRIPTIONS);
    assert!(
        bus.register(SubscriptionFilter::default(), None).is_none(),
        "register should reject subscriptions beyond the configured cap"
    );

    let (sub_id, _rx) = subscriptions
        .pop()
        .expect("at least one subscription should exist");
    bus.unregister(sub_id);

    assert!(
        bus.register(SubscriptionFilter::default(), None).is_some(),
        "freeing a slot should allow a new subscription"
    );

    Ok(())
}

#[sinex_test]
async fn bus_retries_initial_subscribe_failures_until_shutdown(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    ctx.nats_handle()?.shutdown().await?;

    let bus = Arc::new(SubscriptionBus::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_task = tokio::spawn({
        let bus = Arc::clone(&bus);
        async move {
            bus.run_with_ready(nats, pool, env, shutdown_rx, None).await;
        }
    });

    assert_task_stays_running(
        &bus_task,
        Duration::from_millis(250),
        "initial subscribe failure should keep the bus retrying instead of exiting",
    )
    .await?;

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), bus_task).await??;
    Ok(())
}

#[sinex_test]
async fn bus_retries_when_subscription_closes_after_startup(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();

    let bus = Arc::new(SubscriptionBus::new());
    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool, env).await?;

    ctx.nats_handle()?.shutdown().await?;

    assert_task_stays_running(
        &bus_task,
        Duration::from_secs(1),
        "closing the live NATS subscription should keep the bus in reconnect mode instead of exiting",
    )
    .await?;

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), bus_task).await??;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Tests: Bus data flow (NATS + DB)
// ─────────────────────────────────────────────────────────────────────
//
// These tests run SubscriptionBus against the real confirmation subject, which
// is environment-scoped but not per-test namespaced. Use dedicated NATS here so
// concurrent shared-NATS tests cannot inject unrelated confirmations.

#[sinex_test]
async fn empty_filter_receives_all_events(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    // Insert two events with different sources.
    let id1 =
        insert_test_event(&pool, "source-a", "type.one", "localhost", json!({"k": 1})).await?;
    let id2 =
        insert_test_event(&pool, "source-b", "type.two", "localhost", json!({"k": 2})).await?;

    // Create bus, register with empty filter (matches everything).
    let bus = Arc::new(SubscriptionBus::new());
    let (_, mut rx) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");

    // Spawn bus and wait for NATS subscription to be active.
    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    // Publish confirmations (bus is guaranteed to be subscribed now).
    let nats_for_pub = ctx.nats_client();
    publish_confirmation(&nats_for_pub, &env_name, &id1).await?;
    publish_confirmation(&nats_for_pub, &env_name, &id2).await?;

    // Receive events (allow time for batch window + DB fetch).
    let mut received_ids = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while received_ids.len() < 2 && tokio::time::Instant::now() < deadline {
        if let Some(msg) = recv_timeout(&mut rx, Duration::from_millis(200)).await
            && let SseMessage::Event { event, .. } = msg
            && let Some(id) = &event.id
        {
            received_ids.push(*id.as_uuid());
        }
    }

    assert!(
        received_ids.contains(&id1),
        "Expected to receive event id1={id1}, got: {received_ids:?}"
    );
    assert!(
        received_ids.contains(&id2),
        "Expected to receive event id2={id2}, got: {received_ids:?}"
    );

    // Shutdown.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn source_filter_delivers_matching_only(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    // Insert events: one matching source, one not.
    let id_match = insert_test_event(
        &pool,
        "wanted-source",
        "file.created",
        "host-a",
        json!({"match": true}),
    )
    .await?;
    let _id_other = insert_test_event(
        &pool,
        "other-source",
        "file.deleted",
        "host-b",
        json!({"match": false}),
    )
    .await?;

    // Register subscription with source filter.
    let bus = Arc::new(SubscriptionBus::new());
    let filter = SubscriptionFilter {
        sources: vec![EventSource::from_static("wanted-source")],
        ..Default::default()
    };
    let (_, mut rx) = bus
        .register(filter, None)
        .expect("test subscription should register");

    // Spawn bus and wait for NATS subscription.
    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    // Publish confirmations for both events.
    let nats_pub = ctx.nats_client();
    publish_confirmation(&nats_pub, &env_name, &id_match).await?;
    publish_confirmation(&nats_pub, &env_name, &_id_other).await?;

    // Wait for delivery.
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                received.push(event.source.as_str().to_string());
            }
            Some(_) => {} // heartbeat or gap
            None => {
                if !received.is_empty() {
                    drain_until_idle(&mut rx, Duration::from_millis(100), |msg| {
                        if let SseMessage::Event { event, .. } = msg {
                            received.push(event.source.as_str().to_string());
                        }
                    })
                    .await;
                    break;
                }
            }
        }
    }

    assert!(
        received.iter().all(|s| s == "wanted-source"),
        "Only wanted-source events should be delivered, got: {received:?}"
    );
    assert!(
        !received.is_empty(),
        "Should have received at least one event"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn event_type_filter_works(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    let _id_file = insert_test_event(
        &pool,
        "fs",
        "file.created",
        "localhost",
        json!({"path": "/a"}),
    )
    .await?;
    let id_shell = insert_test_event(
        &pool,
        "term",
        "shell.command",
        "localhost",
        json!({"cmd": "ls"}),
    )
    .await?;

    let bus = Arc::new(SubscriptionBus::new());
    let filter = SubscriptionFilter {
        event_types: vec![EventType::from_static("shell.command")],
        ..Default::default()
    };
    let (_, mut rx) = bus
        .register(filter, None)
        .expect("test subscription should register");

    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    let nats_pub = ctx.nats_client();
    publish_confirmation(&nats_pub, &env_name, &_id_file).await?;
    publish_confirmation(&nats_pub, &env_name, &id_shell).await?;

    let mut received_types = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                received_types.push(event.event_type.as_str().to_string());
            }
            Some(_) => {}
            None if !received_types.is_empty() => {
                drain_until_idle(&mut rx, Duration::from_millis(100), |msg| {
                    if let SseMessage::Event { event, .. } = msg {
                        received_types.push(event.event_type.as_str().to_string());
                    }
                })
                .await;
                break;
            }
            None => {}
        }
    }

    assert_eq!(
        received_types,
        vec!["shell.command"],
        "Only shell.command events should be delivered"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn payload_text_search_filter(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    let id_needle = insert_test_event(
        &pool,
        "app",
        "log.entry",
        "localhost",
        json!({"message": "connection refused from 10.0.0.1"}),
    )
    .await?;
    let id_hay = insert_test_event(
        &pool,
        "app",
        "log.entry",
        "localhost",
        json!({"message": "heartbeat ok"}),
    )
    .await?;

    let bus = Arc::new(SubscriptionBus::new());
    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::TextSearch {
            text: "connection refused".to_string(),
        }),
        ..Default::default()
    };
    let (_, mut rx) = bus
        .register(filter, None)
        .expect("test subscription should register");

    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    let nats_pub = ctx.nats_client();
    publish_confirmation(&nats_pub, &env_name, &id_needle).await?;
    publish_confirmation(&nats_pub, &env_name, &id_hay).await?;

    let mut received_ids = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                if let Some(id) = &event.id {
                    received_ids.push(*id.as_uuid());
                }
            }
            Some(_) => {}
            None if !received_ids.is_empty() => {
                drain_until_idle(&mut rx, Duration::from_millis(100), |msg| {
                    if let SseMessage::Event { event, .. } = msg
                        && let Some(id) = &event.id
                    {
                        received_ids.push(*id.as_uuid());
                    }
                })
                .await;
                break;
            }
            None => {}
        }
    }

    assert_eq!(received_ids.len(), 1, "Only one event should match");
    assert_eq!(
        received_ids[0], id_needle,
        "The matching event should be the one with 'connection refused'"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn combined_source_and_type_filter(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    // 3 events: only one matches both source AND type.
    let id1 = insert_test_event(&pool, "fs", "file.created", "h", json!({"f": "a"})).await?;
    let id2 = insert_test_event(&pool, "fs", "file.deleted", "h", json!({"f": "b"})).await?;
    let id3 = insert_test_event(&pool, "term", "file.created", "h", json!({"f": "c"})).await?;

    let bus = Arc::new(SubscriptionBus::new());
    let filter = SubscriptionFilter {
        sources: vec![EventSource::from_static("fs")],
        event_types: vec![EventType::from_static("file.created")],
        ..Default::default()
    };
    let (_, mut rx) = bus
        .register(filter, None)
        .expect("test subscription should register");

    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    let nats_pub = ctx.nats_client();
    for id in [&id1, &id2, &id3] {
        publish_confirmation(&nats_pub, &env_name, id).await?;
    }

    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                if let Some(id) = &event.id {
                    received.push(*id.as_uuid());
                }
            }
            Some(_) => {}
            None if !received.is_empty() => {
                drain_until_idle(&mut rx, Duration::from_millis(100), |msg| {
                    if let SseMessage::Event { event, .. } = msg
                        && let Some(id) = &event.id
                    {
                        received.push(*id.as_uuid());
                    }
                })
                .await;
                break;
            }
            None => {}
        }
    }

    assert_eq!(received.len(), 1, "Only one event matches both filters");
    assert_eq!(received[0], id1, "Only fs + file.created should match");

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn slow_consumer_gap_arrives_before_resumed_event(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    let bus = Arc::new(SubscriptionBus::with_channel_capacity(2));
    let (_, mut rx) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");
    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    let nats_pub = ctx.nats_client();
    // Hit the bus's immediate flush threshold in a single wave so the initial
    // overflow is deterministic and the later publish becomes the recovery trigger.
    for i in 0..32 {
        let id = insert_test_event(
            &pool,
            "gap-source",
            "gap.event",
            "localhost",
            json!({ "seq": i }),
        )
        .await?;
        publish_confirmation(&nats_pub, &env_name, &id).await?;
    }

    for _ in 0..2 {
        match recv_timeout(&mut rx, Duration::from_secs(2)).await {
            Some(SseMessage::Event { .. }) => {}
            other => panic!("expected buffered event before resumption, got {other:?}"),
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut gap_seen = None;
    let mut resumed_seen = false;
    let mut recovery_ids = HashSet::new();
    let mut recovery_seq = 0u32;
    while tokio::time::Instant::now() < deadline {
        let recovery_id = insert_test_event(
            &pool,
            "gap-source",
            "gap.event",
            "localhost",
            json!({ "seq": format!("recovery-{recovery_seq}") }),
        )
        .await?;
        recovery_seq = recovery_seq.saturating_add(1);
        recovery_ids.insert(recovery_id);
        publish_confirmation(&nats_pub, &env_name, &recovery_id).await?;

        match recv_timeout(&mut rx, Duration::from_millis(250)).await {
            Some(SseMessage::Gap {
                from_seq,
                to_seq,
                dropped,
            }) => {
                gap_seen = Some((from_seq, to_seq, dropped));
            }
            Some(SseMessage::Event { event, .. }) => {
                if event
                    .id
                    .as_ref()
                    .map(|id| *id.as_uuid())
                    .is_some_and(|event_id| recovery_ids.contains(&event_id))
                    && gap_seen.is_some()
                {
                    resumed_seen = true;
                    break;
                }
            }
            Some(SseMessage::Heartbeat { .. }) | None => {}
        }
    }

    let (from_seq, to_seq, dropped) = gap_seen
        .ok_or_else(|| color_eyre::eyre::eyre!("expected a gap marker after saturation"))?;
    assert!(to_seq >= from_seq, "gap range should be ordered");
    assert!(dropped > 0, "gap marker should report dropped events");
    assert!(
        resumed_seen,
        "resumed event should eventually be delivered after gap"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn multiple_subscribers_get_independent_delivery(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    let id_fs = insert_test_event(&pool, "fs", "file.created", "h", json!({"x": 1})).await?;
    let id_term = insert_test_event(&pool, "term", "shell.command", "h", json!({"x": 2})).await?;

    let bus = Arc::new(SubscriptionBus::new());

    // Subscriber A: only fs events.
    let filter_a = SubscriptionFilter {
        sources: vec![EventSource::from_static("fs")],
        ..Default::default()
    };
    let (_, mut rx_a) = bus
        .register(filter_a, None)
        .expect("test subscription should register");

    // Subscriber B: only term events.
    let filter_b = SubscriptionFilter {
        sources: vec![EventSource::from_static("term")],
        ..Default::default()
    };
    let (_, mut rx_b) = bus
        .register(filter_b, None)
        .expect("test subscription should register");

    assert_eq!(bus.active_count(), 2);

    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    let nats_pub = ctx.nats_client();
    publish_confirmation(&nats_pub, &env_name, &id_fs).await?;
    publish_confirmation(&nats_pub, &env_name, &id_term).await?;

    // Subscriber A should only get the fs event.
    let mut a_sources = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx_a, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                a_sources.push(event.source.as_str().to_string());
            }
            Some(_) => {}
            None if !a_sources.is_empty() => break,
            None => {}
        }
    }

    // Subscriber B should only get the term event.
    let mut b_sources = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx_b, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                b_sources.push(event.source.as_str().to_string());
            }
            Some(_) => {}
            None if !b_sources.is_empty() => break,
            None => {}
        }
    }

    assert_eq!(a_sources, vec!["fs"], "Sub A should only get fs events");
    assert_eq!(b_sources, vec!["term"], "Sub B should only get term events");

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}

#[sinex_test]
async fn multiple_subscribers_keep_independent_sequence_numbers(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    let first_id =
        insert_test_event(&pool, "shared", "shared.event", "h", json!({"seq": 1})).await?;
    let second_id =
        insert_test_event(&pool, "shared", "shared.event", "h", json!({"seq": 2})).await?;

    let bus = Arc::new(SubscriptionBus::new());
    let (_, mut rx_a) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");
    let (_, mut rx_b) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");
    let (shutdown_tx, bus_task) = spawn_bus_ready(&bus, nats, pool.clone(), env.clone()).await?;

    let nats_pub = ctx.nats_client();
    publish_confirmation(&nats_pub, &env_name, &first_id).await?;
    publish_confirmation(&nats_pub, &env_name, &second_id).await?;

    let mut seen_a = Vec::new();
    let mut seen_b = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline && (seen_a.len() < 2 || seen_b.len() < 2) {
        if seen_a.len() < 2
            && let Some(SseMessage::Event { seq, event }) =
                recv_timeout(&mut rx_a, Duration::from_millis(200)).await
            && let Some(id) = event.id.as_ref().map(|value| *value.as_uuid())
        {
            seen_a.push((seq, id));
        }

        if seen_b.len() < 2
            && let Some(SseMessage::Event { seq, event }) =
                recv_timeout(&mut rx_b, Duration::from_millis(200)).await
            && let Some(id) = event.id.as_ref().map(|value| *value.as_uuid())
        {
            seen_b.push((seq, id));
        }
    }

    assert_eq!(
        seen_a.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
        vec![1, 2],
        "subscriber A should observe a local 1,2 sequence regardless of DB fetch order"
    );
    assert_eq!(
        seen_b.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
        vec![1, 2],
        "subscriber B should observe a local 1,2 sequence regardless of DB fetch order"
    );
    assert_eq!(
        seen_a.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        seen_b.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        "both subscribers should observe the same event ordering"
    );
    assert_eq!(
        seen_a
            .iter()
            .map(|(_, id)| *id)
            .collect::<std::collections::BTreeSet<_>>(),
        [first_id, second_id].into_iter().collect(),
        "subscriber A should receive the full event set"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}
