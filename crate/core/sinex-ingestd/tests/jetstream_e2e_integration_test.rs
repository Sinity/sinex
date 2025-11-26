//! End-to-end JetStream integration test
//!
//! This test demonstrates the complete JetStream event flow as specified in docs/way.md:
//! 1. Satellite publishes provisional event to JetStream (events.raw)
//! 2. ingestd consumes event, persists to database
//! 3. ingestd publishes confirmation (events.confirmations)
//! 4. Automaton consumes confirmed event
//!
//! This validates Phase 1 (Event Backbone) and Phase 2 (Confirmation-Aware Consumption).

use async_nats::{jetstream, Client};
use serde_json::json;
use sinex_core::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_satellite_sdk::{
    AutomatonEventHandler, JetStreamEventConsumer, JetStreamEventConsumerConfig, NatsPublisher,
    ProcessingModel,
};
use sinex_test_utils::{sinex_test, EphemeralNats, TestContext};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

#[ignore = "requires full ingestd pipeline"]
#[sinex_test]
async fn test_jetstream_e2e_event_flow() -> color_eyre::Result<()> {
    info!("🚀 Starting E2E JetStream test");

    // Setup test infrastructure
    let ctx = TestContext::new().await?.with_nats().await?;
    let nats = EphemeralNats::start().await?;
    let nats_client: Client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let env = ctx.env();

    // Create JetStream context
    let js = jetstream::new(nats_client.clone());

    // Bootstrap events_raw stream
    let events_raw_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: events_raw_stream.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    // Bootstrap events_confirmations stream
    let confirmations_stream = format!("{events_raw_stream}_CONFIRMATIONS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: confirmations_stream.clone(),
        subjects: vec![env.nats_subject("events.confirmations.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    info!("✅ JetStream streams created");

    // Step 1: Start ingestd consumer
    let validator = EventValidator::new(false); // Validation disabled for test
    let topology = JetStreamTopology::new(&env, events_raw_stream.clone(), "ingestd".to_string());
    let ingestd_consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let ingestd_handle = tokio::spawn(async move { ingestd_consumer.run().await });

    // Wait for ingestd to initialize
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("✅ ingestd JetStream consumer started");

    // Step 2: Start automaton consumer
    let automaton_handler = Arc::new(AutomatonEventHandler::new());
    let automaton_config = JetStreamEventConsumerConfig {
        processing_model: ProcessingModel::StatelessWorker,
        batch_size: 100,
        confirmation_timeout: Duration::from_secs(30),
        consumer_name: "test-automaton".to_string(),
        enable_provisional_processing: false,
    };
    let automaton_consumer = JetStreamEventConsumer::new(
        nats_client.clone(),
        env.clone(),
        automaton_config,
        automaton_handler.clone(),
        None,
    );
    let automaton_handle = tokio::spawn(async move { automaton_consumer.run().await });

    // Wait for automaton to initialize
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("✅ Automaton JetStream consumer started");

    // Step 3: Publish event using NatsPublisher (satellite simulation)
    let publisher = NatsPublisher::new(nats_client.clone());

    let test_event = ctx
        .create_test_event(
            "test-satellite",
            "test.event",
            json!({
                "message": "E2E JetStream test event",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }),
        )
        .await?;

    let event_id = test_event
        .id
        .as_ref()
        .expect("Event should have ID")
        .clone();

    // Publish to JetStream using the SDK publisher
    publisher
        .publish(&test_event)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("Publish failed: {}", e))?;
    info!(event_id = %event_id, "✅ Event published to JetStream via NatsPublisher");

    // Step 4: Wait for event to flow through the pipeline
    // Event flow: JetStream → ingestd → DB → confirmation → automaton
    let mut event_persisted = false;
    let mut confirmation_received = false;

    for attempt in 0..30 {
        // Check if event persisted to database
        if !event_persisted {
            if let Some(event_from_db) = pool.events().get_by_id(event_id.clone()).await? {
                info!(
                    attempt,
                    event_id = %event_id,
                    "✅ Event persisted to database"
                );
                assert_eq!(event_from_db.source.as_str(), "test-satellite");
                assert_eq!(event_from_db.event_type.as_str(), "test.event");
                event_persisted = true;
            }
        }

        // Check if automaton received confirmation
        if !confirmation_received {
            let processed_ids = automaton_handler.processed_event_ids().await;
            if processed_ids.contains(&event_id.as_ulid()) {
                info!(
                    attempt,
                    event_id = %event_id,
                    "✅ Automaton received confirmed event"
                );
                confirmation_received = true;
            }
        }

        // Both conditions met
        if event_persisted && confirmation_received {
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify the complete flow succeeded
    assert!(
        event_persisted,
        "Event should be persisted to database by ingestd"
    );
    assert!(
        confirmation_received,
        "Automaton should receive confirmed event"
    );

    // Verify automaton processed exactly one event
    assert_eq!(
        automaton_handler.processed_count().await,
        1,
        "Automaton should have processed exactly one event"
    );

    info!("🎉 E2E JetStream test PASSED");
    info!("   ✓ Satellite → JetStream (events.raw)");
    info!("   ✓ ingestd → Database persistence");
    info!("   ✓ ingestd → JetStream (events.confirmations)");
    info!("   ✓ Automaton → Confirmed event consumption");

    // Cleanup
    drop(ingestd_handle);
    drop(automaton_handle);

    Ok(())
}

#[ignore = "requires full ingestd pipeline"]
#[sinex_test]
async fn test_jetstream_idempotency() -> color_eyre::Result<()> {
    info!("🚀 Starting JetStream idempotency test");

    let ctx = TestContext::new().await?.with_nats().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let env = ctx.env();

    // Create JetStream context and bootstrap streams
    let js = jetstream::new(nats_client.clone());
    let events_raw_stream = env.nats_stream_name("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: events_raw_stream.clone(),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    // Start ingestd
    let validator = EventValidator::new(false);
    let topology = JetStreamTopology::new(&env, events_raw_stream.clone(), "ingestd".to_string());
    let ingestd_consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let _ingestd_handle = tokio::spawn(async move { ingestd_consumer.run().await });
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish same event 3 times with same event_id (idempotency header)
    let publisher = NatsPublisher::new(nats_client.clone());
    let test_event = ctx
        .create_test_event(
            "idempotency-test",
            "test.duplicate",
            json!({"test": "idempotency"}),
        )
        .await?;

    let event_id = test_event
        .id
        .as_ref()
        .expect("Event should have ID")
        .clone();

    // Publish 3 times
    for i in 1..=3 {
        publisher
            .publish(&test_event)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Publish failed: {}", e))?;
        info!(iteration = i, event_id = %event_id, "Published event");
    }

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify only ONE event in database
    let events = pool
        .events()
        .get_by_source(
            &sinex_core::EventSource::new("idempotency-test".to_string()),
            sinex_core::types::Pagination::new(Some(100), None),
        )
        .await?;

    assert_eq!(
        events.len(),
        1,
        "Should have exactly 1 event despite 3 publishes (idempotency)"
    );
    assert_eq!(
        events[0].id.as_ref().expect("Event should have ID"),
        &event_id
    );

    info!("🎉 Idempotency test PASSED - 3 publishes → 1 database row");

    Ok(())
}
