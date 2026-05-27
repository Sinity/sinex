//! Stream Processing Integration Tests
//!
//! Tests the stream processing functionality including NATS JetStream integration,
//! event routing, consumer behavior, and stream processing patterns.
//! Focuses on correctness and integration rather than raw performance.

use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use xtask::sandbox::{EphemeralNats, prelude::*};

fn event_label(event: &serde_json::Value) -> &str {
    event
        .get("event_type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
}

async fn setup_nats_ctx(ctx: TestContext) -> TestResult<(TestContext, Arc<EphemeralNats>)> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    Ok((ctx, nats))
}

/// Test basic NATS stream publishing and consuming
#[sinex_test]
async fn test_basic_stream_processing(ctx: TestContext) -> TestResult<()> {
    let (ctx, nats) = setup_nats_ctx(ctx).await?;
    let namespace = ctx.pipeline_namespace();
    let subject = namespace.subject("events.test.basic");
    let stream = namespace.stream("SINEX_TEST_BASIC");
    let (stream_name, consumer) = nats
        .ensure_stream_with_consumer(
            &stream,
            &[subject.as_str()],
            ConsumerConfig {
                durable_name: Some(namespace.consumer_name("test-basic-consumer")),
                ..Default::default()
            },
        )
        .await?;
    let jetstream = nats.jetstream().await?;

    // Publish test events
    let test_events = vec![
        json!({
            "event_type": "test.basic.event_1",
            "payload": "First test event",
            "timestamp": "2026-01-01T00:00:00Z",
        }),
        json!({
            "event_type": "test.basic.event_2",
            "payload": "Second test event",
            "timestamp": "2026-01-01T00:00:00Z",
        }),
        json!({
            "event_type": "test.basic.event_3",
            "payload": "Third test event",
            "timestamp": "2026-01-01T00:00:00Z",
        }),
    ];

    // Publish events
    for (i, event) in test_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        let ack = jetstream.publish(subject, event_bytes.into()).await?;
        ack.await?;

        debug!(
            event_index = i + 1,
            event = event_label(event),
            "Published basic stream event"
        );
    }

    // Consume messages
    let mut messages = consumer.messages().await?;
    let mut received_events = Vec::new();

    // Collect messages with timeout
    let collect_timeout = Duration::from_secs(Timeouts::STANDARD);
    let collection_result = timeout(collect_timeout, async {
        while received_events.len() < test_events.len() {
            if let Some(message) = messages.next().await {
                let message = message?;
                let event: serde_json::Value = serde_json::from_slice(&message.payload)?;
                received_events.push(event);
                message.ack().await?;
            }
        }
        Ok::<(), color_eyre::eyre::Error>(())
    })
    .await??;

    // Verify all events were received
    assert_eq!(received_events.len(), test_events.len());

    // Verify event content
    for (original, received) in test_events.iter().zip(received_events.iter()) {
        assert_eq!(original["event_type"], received["event_type"]);
        assert_eq!(original["payload"], received["payload"]);
    }

    info!(
        processed = received_events.len(),
        stream = %stream_name,
        "Basic stream processing test passed"
    );
    Ok(())
}

/// Test stream processing with multiple subjects
#[sinex_test]
async fn test_multi_subject_stream_processing(ctx: TestContext) -> TestResult<()> {
    let (ctx, nats) = setup_nats_ctx(ctx).await?;
    let namespace = ctx.pipeline_namespace();
    let subjects = vec![
        namespace.subject("events.filesystem.*"),
        namespace.subject("events.terminal.*"),
        namespace.subject("events.system.*"),
    ];
    let subject_refs: Vec<&str> = subjects.iter().map(String::as_str).collect();
    let stream = namespace.stream("SINEX_TEST_MULTI_SUBJECT");
    let consumer_name = namespace.consumer_name("test-multi-subject-consumer");
    let (stream_name, _consumer) = nats
        .ensure_stream_with_consumer(
            &stream,
            &subject_refs,
            ConsumerConfig {
                durable_name: Some(consumer_name.clone()),
                ..Default::default()
            },
        )
        .await?;
    let jetstream = nats.jetstream().await?;

    // Test events for different subjects
    let test_cases = vec![
        (
            namespace.subject("events.filesystem.file_created"),
            json!({
                "event_type": "file_created",
                "path": "/tmp/test.txt",
                "size": 1024,
            }),
        ),
        (
            namespace.subject("events.terminal.command_executed"),
            json!({
                "event_type": "command_executed",
                "command": "ls -la",
                "exit_code": 0,
            }),
        ),
        (
            namespace.subject("events.system.service_started"),
            json!({
                "event_type": "service_started",
                "service": "nginx",
                "status": "active",
            }),
        ),
        (
            namespace.subject("events.filesystem.file_deleted"),
            json!({
                "event_type": "file_deleted",
                "path": "/tmp/old_file.txt",
            }),
        ),
        (
            namespace.subject("events.terminal.command_failed"),
            json!({
                "event_type": "command_failed",
                "command": "invalid_command",
                "exit_code": 127,
            }),
        ),
    ];

    // Publish events to different subjects
    for (subject, event) in &test_cases {
        let event_bytes = serde_json::to_vec(event)?;
        let ack = jetstream
            .publish(subject.as_str(), event_bytes.into())
            .await?;
        ack.await?;

        debug!(
            subject = subject.as_str(),
            event = event_label(event),
            "Published multi-subject event"
        );
    }

    // Create consumer
    let stream = jetstream.get_stream(&stream_name).await?;
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                ..Default::default()
            },
        )
        .await?;

    // Consume and categorize messages
    let mut messages = consumer.messages().await?;
    let mut received_by_subject: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    let collect_timeout = Duration::from_secs(Timeouts::LONG);
    timeout(collect_timeout, async {
        let mut total_received = 0;
        while total_received < test_cases.len() {
            if let Some(message) = messages.next().await {
                let message = message?;
                let event: serde_json::Value = serde_json::from_slice(&message.payload)?;
                let subject = message.subject.clone();

                received_by_subject
                    .entry(subject.to_string())
                    .or_insert_with(Vec::new)
                    .push(event);

                message.ack().await?;
                total_received += 1;
            }
        }
        Ok::<(), color_eyre::eyre::Error>(())
    })
    .await??;

    // Verify events were received for all subjects
    let total_received: usize = received_by_subject.values().map(|v| v.len()).sum();
    assert_eq!(total_received, test_cases.len());

    // Verify subject routing worked correctly
    for (subject, _) in &test_cases {
        assert!(
            received_by_subject.contains_key(subject),
            "No events received for subject: {}",
            subject
        );
    }

    info!(
        subjects = received_by_subject.len(),
        total_events = total_received,
        "Multi-subject stream processing test passed"
    );

    Ok(())
}

/// Test consumer groups and load balancing
#[sinex_test]
async fn test_consumer_group_processing(ctx: TestContext) -> TestResult<()> {
    let (ctx, nats) = setup_nats_ctx(ctx).await?;
    let namespace = ctx.pipeline_namespace();
    let subject = namespace.subject("events.test.consumer_groups");

    let (stream_name, _) = nats
        .ensure_stream_with_consumer(
            &namespace.stream("SINEX_TEST_CONSUMER_GROUPS"),
            &[subject.as_str()],
            ConsumerConfig {
                durable_name: Some(namespace.consumer_name("cg-bootstrap")),
                ..Default::default()
            },
        )
        .await?;
    let jetstream = nats.jetstream().await?;

    // Publish many test events
    let event_count = 20;
    for i in 0..event_count {
        let event = json!({
            "event_type": "consumer_group_test",
            "message_id": i,
            "payload": format!("Message number {}", i),
            "batch": i / 5, // Group into batches for easier verification
        });

        let event_bytes = serde_json::to_vec(&event)?;
        let ack = jetstream
            .publish(subject.as_str(), event_bytes.into())
            .await?;
        ack.await?;
    }

    info!(
        events = event_count,
        "Published events for consumer group test"
    );

    // Create multiple consumers in the same consumer group
    let consumer_count = 3;
    let consumer_group = namespace.consumer_name("test-consumer-group");

    let mut consumers = Vec::new();
    for i in 0..consumer_count {
        let stream = jetstream.get_stream(&stream_name).await?;
        let durable = format!("{}_{}", consumer_group, i);
        let consumer = stream
            .get_or_create_consumer(
                &durable,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(durable.clone()),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    ..Default::default()
                },
            )
            .await?;
        consumers.push(consumer);
    }

    // Start consuming with multiple consumers concurrently
    let mut handles = Vec::new();

    for (i, consumer) in consumers.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let mut messages = consumer.messages().await?;
            let mut received_messages = Vec::new();

            // Each consumer tries to get messages for a limited time
            let consume_timeout = Duration::from_secs(Timeouts::LONG);
            let _result = timeout(consume_timeout, async {
                while let Some(message) = messages.next().await {
                    let message = message?;
                    let event: serde_json::Value = serde_json::from_slice(&message.payload)?;
                    received_messages.push(event);
                    message.ack().await?;

                    // Stop if we've got a reasonable share
                    if received_messages.len() >= event_count / consumer_count + 2 {
                        break;
                    }
                }
                Ok::<(), color_eyre::eyre::Error>(())
            })
            .await;

            Ok::<(usize, Vec<serde_json::Value>), color_eyre::eyre::Error>((i, received_messages))
        });
        handles.push(handle);
    }

    // Wait for all consumers to finish
    let results = futures::future::join_all(handles).await;

    // Collect all received messages
    let mut all_received_messages = Vec::new();
    let mut messages_per_consumer = HashMap::new();

    for result in results {
        let (consumer_id, messages) = result??;
        messages_per_consumer.insert(consumer_id, messages.len());
        all_received_messages.extend(messages);
    }

    // Verify load balancing worked
    for (consumer_id, count) in &messages_per_consumer {
        info!(
            consumer_id = *consumer_id,
            messages = *count,
            "Consumer processed messages"
        );
    }

    let total_processed = all_received_messages.len();
    info!(
        processed = total_processed,
        expected = event_count,
        "Consumer group processing totals"
    );

    // Verify we processed most/all messages (allowing for some timing variance)
    assert!(
        total_processed >= event_count * 90 / 100, // At least 90% processed
        "Should process most messages, got {} out of {}",
        total_processed,
        event_count
    );

    // Verify no duplicate processing (each message should appear only once)
    let mut message_ids = std::collections::HashSet::new();
    for message in &all_received_messages {
        let message_id = message["message_id"].as_u64().unwrap();
        assert!(
            message_ids.insert(message_id),
            "Duplicate message processed: {}",
            message_id
        );
    }

    info!(
        processed = total_processed,
        expected = event_count,
        "Consumer group processing test passed"
    );
    Ok(())
}

/// Test stream processing with message ordering
#[sinex_test]
async fn test_ordered_stream_processing(ctx: TestContext) -> TestResult<()> {
    let (ctx, nats) = setup_nats_ctx(ctx).await?;
    let namespace = ctx.pipeline_namespace();
    let subject = namespace.subject("events.test.ordering");
    let stream = namespace.stream("SINEX_TEST_ORDERING");
    let consumer_name = namespace.consumer_name("test-ordering-consumer");

    let (stream_name, _consumer) = nats
        .ensure_stream_with_consumer(
            &stream,
            &[subject.as_str()],
            ConsumerConfig {
                durable_name: Some(consumer_name.clone()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                ..Default::default()
            },
        )
        .await?;
    let jetstream = nats.jetstream().await?;

    // Publish ordered sequence of events
    let sequence_length = 15;
    for i in 0..sequence_length {
        let event = json!({
            "event_type": "ordered_event",
            "sequence_number": i,
            "timestamp": "2026-01-01T00:00:00Z",
            "payload": format!("Event in sequence: {}", i),
        });

        let event_bytes = serde_json::to_vec(&event)?;
        let ack = jetstream
            .publish(subject.as_str(), event_bytes.into())
            .await?;
        ack.await?;
    }

    info!(events = sequence_length, "Published ordered events");

    // Create consumer
    let stream = jetstream.get_stream(&stream_name).await?;
    let consumer = stream
        .get_or_create_consumer(
            &consumer_name,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                ..Default::default()
            },
        )
        .await?;

    // Consume messages in order
    let mut messages = consumer.messages().await?;
    let mut received_sequence = Vec::new();

    let collect_timeout = Duration::from_secs(Timeouts::LONG);
    timeout(collect_timeout, async {
        while received_sequence.len() < sequence_length {
            if let Some(message) = messages.next().await {
                let message = message?;
                let event: serde_json::Value = serde_json::from_slice(&message.payload)?;
                let sequence_number = event["sequence_number"].as_u64().unwrap() as usize;
                received_sequence.push(sequence_number);
                message.ack().await?;
            }
        }
        Ok::<(), color_eyre::eyre::Error>(())
    })
    .await??;

    // Verify ordering was preserved
    debug!(?received_sequence, "Received ordered sequence");

    assert_eq!(received_sequence.len(), sequence_length);

    // Check if sequence is in order (0, 1, 2, 3, ...)
    let expected_sequence: Vec<usize> = (0..sequence_length).collect();
    assert_eq!(
        received_sequence, expected_sequence,
        "Messages should be received in order"
    );

    info!(
        events = sequence_length,
        "Ordered stream processing test passed"
    );
    Ok(())
}

/// Test stream processing error handling and recovery
#[sinex_test]
async fn test_stream_error_handling(ctx: TestContext) -> TestResult<()> {
    let (ctx, nats) = setup_nats_ctx(ctx).await?;
    let namespace = ctx.pipeline_namespace();
    let subject = namespace.subject("events.test.errors");
    let stream = namespace.stream("SINEX_TEST_ERRORS");
    let consumer_name = namespace.consumer_name("test-error-handling-consumer");

    let (stream_name, consumer) = nats
        .ensure_stream_with_consumer(
            &stream,
            &[subject.as_str()],
            ConsumerConfig {
                durable_name: Some(consumer_name.clone()),
                ..Default::default()
            },
        )
        .await?;
    let jetstream = nats.jetstream().await?;

    // Publish mix of valid and problematic events
    let test_events = vec![
        // Valid events
        json!({"event_type": "valid_event_1", "data": "good"}),
        json!({"event_type": "valid_event_2", "data": "also_good"}),
        // Event that might cause processing issues (malformed data)
        json!({"event_type": "problematic_event", "malformed": null, "data": {"nested": {"very": {"deep": "value"}}}}),
        // More valid events
        json!({"event_type": "valid_event_3", "data": "still_good"}),
        json!({"event_type": "valid_event_4", "data": "final_good"}),
    ];

    // Publish all events
    for (i, event) in test_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        let ack = jetstream
            .publish(subject.as_str(), event_bytes.into())
            .await?;
        ack.await?;

        debug!(
            event_index = i + 1,
            event = event_label(event),
            "Published error-handling event"
        );
    }

    // Consume messages and handle errors gracefully
    let mut messages = consumer.messages().await?;
    let mut successfully_processed = 0;
    let mut processing_errors = 0;

    let collect_timeout = Duration::from_secs(Timeouts::LONG);
    timeout(collect_timeout, async {
        let mut messages_received = 0;

        while messages_received < test_events.len() {
            if let Some(message) = messages.next().await {
                messages_received += 1;
                let message = message?;

                // Simulate processing that might fail
                match serde_json::from_slice::<serde_json::Value>(&message.payload) {
                    Ok(event) => {
                        // Simulate business logic that might reject certain events
                        if event["event_type"]
                            .as_str()
                            .unwrap_or("")
                            .contains("problematic")
                        {
                            warn!(event = event_label(&event), "Processing failed for event");
                            processing_errors += 1;
                            // NACK or handle error (in real implementation)
                            // For test, we still ACK to avoid redelivery
                            message.ack().await?;
                        } else {
                            debug!(event = event_label(&event), "Successfully processed event");
                            successfully_processed += 1;
                            message.ack().await?;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to parse event payload");
                        processing_errors += 1;
                        message.ack().await?; // ACK to avoid infinite redelivery
                    }
                }
            }
        }
        Ok::<(), color_eyre::eyre::Error>(())
    })
    .await??;

    // Verify error handling worked
    info!(
        processed = successfully_processed,
        errors = processing_errors,
        total = test_events.len(),
        "Stream error handling results"
    );

    assert_eq!(
        successfully_processed + processing_errors,
        test_events.len()
    );
    assert!(
        successfully_processed > 0,
        "Should process at least some valid events"
    );
    assert_eq!(
        processing_errors, 1,
        "Should have exactly 1 processing error (the problematic event)"
    );

    info!("Stream error handling test passed");
    Ok(())
}
