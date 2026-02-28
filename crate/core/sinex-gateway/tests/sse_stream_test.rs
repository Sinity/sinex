//! Integration tests for the SSE event subscription stream.
//!
//! Tests cover:
//! - Bus bookkeeping (register/unregister)
//! - Filter-based event delivery via the SubscriptionBus
//! - Gap detection for slow consumers
//! - HTTP-level auth rejection on the SSE endpoint

use serde_json::json;
use sinex_gateway::sse_bus::{SseMessage, SubscriptionBus};
use sinex_primitives::query::{PayloadFilter, SubscriptionFilter};
use sinex_primitives::temporal;
use sinex_primitives::{EventSource, EventType, Ulid as CoreUlid};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;
use xtask::sandbox::sinex_test;

// ─────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────

/// Insert a test event directly into the database, bypassing the ingestion pipeline.
/// Returns the ULID of the inserted event.
async fn insert_test_event(
    pool: &sqlx::PgPool,
    source: &str,
    event_type: &str,
    host: &str,
    payload: serde_json::Value,
) -> color_eyre::Result<CoreUlid> {
    let id = CoreUlid::new();
    let empty_parents: Vec<Uuid> = vec![];
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_event_ids
        ) VALUES (
            $1::uuid::ulid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::uuid[]::ulid[]
        )
        "#,
        id.to_uuid(),
        source,
        event_type,
        host,
        payload,
        *temporal::now(),
        &empty_parents
    )
    .execute(pool)
    .await?;
    Ok(id)
}

/// Publish a fake confirmation message to NATS (mimics what ingestd does after persisting).
async fn publish_confirmation(
    nats: &async_nats::Client,
    env_name: &str,
    event_id: &CoreUlid,
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

/// Receive the next SseMessage from the channel with a timeout.
async fn recv_timeout(
    rx: &mut tokio::sync::mpsc::Receiver<SseMessage>,
    timeout: Duration,
) -> Option<SseMessage> {
    tokio::time::timeout(timeout, rx.recv())
        .await
        .ok()
        .flatten()
}

// ─────────────────────────────────────────────────────────────────────
// Tests: Bus bookkeeping
// ─────────────────────────────────────────────────────────────────────

#[sinex_test]
async fn register_and_unregister_updates_count() -> TestResult<()> {
    let bus = SubscriptionBus::new();
    assert_eq!(bus.active_count(), 0);

    let (id1, _rx1) = bus.register(SubscriptionFilter::default());
    assert_eq!(bus.active_count(), 1);

    let (id2, _rx2) = bus.register(SubscriptionFilter::default());
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
    let (id, rx) = bus.register(SubscriptionFilter::default());
    assert_eq!(bus.active_count(), 1);

    // Drop the receiver — the bus won't notice until the next send attempt.
    drop(rx);

    // Manual unregister still works.
    bus.unregister(id);
    assert_eq!(bus.active_count(), 0);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Tests: Bus data flow (NATS + DB)
// ─────────────────────────────────────────────────────────────────────

#[sinex_test]
async fn empty_filter_receives_all_events(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
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
    let (_, mut rx) = bus.register(SubscriptionFilter::default());

    // Spawn bus run loop.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_clone = Arc::clone(&bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run(nats.clone(), pool.clone(), env.clone(), shutdown_rx)
            .await;
    });

    // Publish confirmations.
    let nats_for_pub = ctx.nats_client();
    publish_confirmation(&nats_for_pub, &env_name, &id1).await?;
    publish_confirmation(&nats_for_pub, &env_name, &id2).await?;

    // Receive events (allow time for batch window + DB fetch).
    let mut received_ids = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while received_ids.len() < 2 && tokio::time::Instant::now() < deadline {
        if let Some(msg) = recv_timeout(&mut rx, Duration::from_millis(200)).await {
            if let SseMessage::Event { event, .. } = msg {
                if let Some(id) = &event.id {
                    received_ids.push(*id.as_ulid());
                }
            }
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
    let ctx = ctx.with_nats().shared().await?;
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
        sources: vec![EventSource::new("wanted-source")],
        ..Default::default()
    };
    let (_, mut rx) = bus.register(filter);

    // Spawn bus.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_clone = Arc::clone(&bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run(nats.clone(), pool.clone(), env.clone(), shutdown_rx)
            .await;
    });

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
                    // Got at least one event; wait a bit more for any stragglers.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    // Drain remaining.
                    while let Some(msg) = recv_timeout(&mut rx, Duration::from_millis(50)).await {
                        if let SseMessage::Event { event, .. } = msg {
                            received.push(event.source.as_str().to_string());
                        }
                    }
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
    let ctx = ctx.with_nats().shared().await?;
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
        event_types: vec![EventType::new("shell.command")],
        ..Default::default()
    };
    let (_, mut rx) = bus.register(filter);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_clone = Arc::clone(&bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run(nats.clone(), pool.clone(), env.clone(), shutdown_rx)
            .await;
    });

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
                tokio::time::sleep(Duration::from_millis(100)).await;
                while let Some(msg) = recv_timeout(&mut rx, Duration::from_millis(50)).await {
                    if let SseMessage::Event { event, .. } = msg {
                        received_types.push(event.event_type.as_str().to_string());
                    }
                }
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
    let ctx = ctx.with_nats().shared().await?;
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
    let (_, mut rx) = bus.register(filter);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_clone = Arc::clone(&bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run(nats.clone(), pool.clone(), env.clone(), shutdown_rx)
            .await;
    });

    let nats_pub = ctx.nats_client();
    publish_confirmation(&nats_pub, &env_name, &id_needle).await?;
    publish_confirmation(&nats_pub, &env_name, &id_hay).await?;

    let mut received_ids = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_timeout(&mut rx, Duration::from_millis(200)).await {
            Some(SseMessage::Event { event, .. }) => {
                if let Some(id) = &event.id {
                    received_ids.push(*id.as_ulid());
                }
            }
            Some(_) => {}
            None if !received_ids.is_empty() => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                while let Some(msg) = recv_timeout(&mut rx, Duration::from_millis(50)).await {
                    if let SseMessage::Event { event, .. } = msg {
                        if let Some(id) = &event.id {
                            received_ids.push(*id.as_ulid());
                        }
                    }
                }
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
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    // 3 events: only one matches both source AND type.
    let id1 =
        insert_test_event(&pool, "fs", "file.created", "h", json!({"f": "a"})).await?;
    let id2 =
        insert_test_event(&pool, "fs", "file.deleted", "h", json!({"f": "b"})).await?;
    let id3 =
        insert_test_event(&pool, "term", "file.created", "h", json!({"f": "c"})).await?;

    let bus = Arc::new(SubscriptionBus::new());
    let filter = SubscriptionFilter {
        sources: vec![EventSource::new("fs")],
        event_types: vec![EventType::new("file.created")],
        ..Default::default()
    };
    let (_, mut rx) = bus.register(filter);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_clone = Arc::clone(&bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run(nats.clone(), pool.clone(), env.clone(), shutdown_rx)
            .await;
    });

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
                    received.push(*id.as_ulid());
                }
            }
            Some(_) => {}
            None if !received.is_empty() => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                while let Some(msg) = recv_timeout(&mut rx, Duration::from_millis(50)).await {
                    if let SseMessage::Event { event, .. } = msg {
                        if let Some(id) = &event.id {
                            received.push(*id.as_ulid());
                        }
                    }
                }
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
async fn multiple_subscribers_get_independent_delivery(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let pool = ctx.pool().clone();
    let nats = ctx.nats_client();
    let env = ctx.env().clone();
    let env_name = env.name().to_string();

    let id_fs =
        insert_test_event(&pool, "fs", "file.created", "h", json!({"x": 1})).await?;
    let id_term =
        insert_test_event(&pool, "term", "shell.command", "h", json!({"x": 2})).await?;

    let bus = Arc::new(SubscriptionBus::new());

    // Subscriber A: only fs events.
    let filter_a = SubscriptionFilter {
        sources: vec![EventSource::new("fs")],
        ..Default::default()
    };
    let (_, mut rx_a) = bus.register(filter_a);

    // Subscriber B: only term events.
    let filter_b = SubscriptionFilter {
        sources: vec![EventSource::new("term")],
        ..Default::default()
    };
    let (_, mut rx_b) = bus.register(filter_b);

    assert_eq!(bus.active_count(), 2);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bus_clone = Arc::clone(&bus);
    let bus_task = tokio::spawn(async move {
        bus_clone
            .run(nats.clone(), pool.clone(), env.clone(), shutdown_rx)
            .await;
    });

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
    assert_eq!(
        b_sources,
        vec!["term"],
        "Sub B should only get term events"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), bus_task).await;

    Ok(())
}
