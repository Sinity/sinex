//! Stream Processing Integration Tests
//!
//! Tests the stream processing functionality including NATS JetStream integration,
//! event routing, consumer behavior, and stream processing patterns.
//! Focuses on correctness and integration rather than raw performance.

use color_eyre::eyre::Result;
use async_nats::jetstream::{consumer::PullConsumer, stream::Config as StreamConfig, Context};
use futures::StreamExt;
use serde_json::json;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

/// Test basic NATS stream publishing and consuming
#[sinex_test]
async fn test_basic_stream_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create NATS connection
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let stream_name = "sinex-test-basic";
    let subject = "events.test.basic";

    // Create stream
    let stream_config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        ..Default::default()
    };
    
    let _stream = jetstream
        .get_or_create_stream(stream_config)
        .await?;

    // Publish test events
    let test_events = vec![
        json!({
            "event_type": "test.basic.event_1",
            "payload": "First test event",
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }),
        json!({
            "event_type": "test.basic.event_2", 
            "payload": "Second test event",
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }),
        json!({
            "event_type": "test.basic.event_3",
            "payload": "Third test event",
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }),
    ];

    // Publish events
    for (i, event) in test_events.iter().enumerate() {
        let event_bytes = serde_json::to_vec(event)?;
        let ack = jetstream
            .publish(subject, event_bytes.into())
            .await?;
        ack.await?;
        
        println!("Published event {}: {:?}", i + 1, event["event_type"]);
    }

    // Create consumer
    let consumer = jetstream
        .create_consumer_on_stream(
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("test-basic-consumer".to_string()),
                ..Default::default()
            },
            stream_name,
        )
        .await?;

    // Consume messages
    let mut messages = consumer.messages().await?;
    let mut received_events = Vec::new();

    // Collect messages with timeout
    let collect_timeout = Duration::from_secs(5);
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
    }).await??;

    // Verify all events were received
    assert_eq!(received_events.len(), test_events.len());
    
    // Verify event content
    for (original, received) in test_events.iter().zip(received_events.iter()) {
        assert_eq!(original["event_type"], received["event_type"]);
        assert_eq!(original["payload"], received["payload"]);
    }

    println!("✅ Basic stream processing test passed - processed {} events", received_events.len());
    Ok(())
}

/// Test stream processing with multiple subjects
#[sinex_test]
async fn test_multi_subject_stream_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let stream_name = "sinex-test-multi-subject";
    
    // Create stream with multiple subjects
    let subjects = vec![
        "events.filesystem.*".to_string(),
        "events.terminal.*".to_string(),
        "events.system.*".to_string(),
    ];
    
    let stream_config = StreamConfig {
        name: stream_name.to_string(),
        subjects: subjects.clone(),
        ..Default::default()
    };
    
    let _stream = jetstream
        .get_or_create_stream(stream_config)
        .await?;

    // Test events for different subjects
    let test_cases = vec![
        ("events.filesystem.file_created", json!({
            "event_type": "file_created",
            "path": "/tmp/test.txt",
            "size": 1024,
        })),
        ("events.terminal.command_executed", json!({
            "event_type": "command_executed", 
            "command": "ls -la",
            "exit_code": 0,
        })),
        ("events.system.service_started", json!({
            "event_type": "service_started",
            "service": "nginx",
            "status": "active",
        })),
        ("events.filesystem.file_deleted", json!({
            "event_type": "file_deleted",
            "path": "/tmp/old_file.txt",
        })),
        ("events.terminal.command_failed", json!({
            "event_type": "command_failed",
            "command": "invalid_command",
            "exit_code": 127,
        })),
    ];

    // Publish events to different subjects
    for (subject, event) in &test_cases {
        let event_bytes = serde_json::to_vec(event)?;
        let ack = jetstream
            .publish(subject, event_bytes.into())
            .await?;
        ack.await?;
        
        println!("Published to {}: {:?}", subject, event["event_type"]);
    }

    // Create consumer
    let consumer = jetstream
        .create_consumer_on_stream(
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("test-multi-subject-consumer".to_string()),
                ..Default::default()
            },
            stream_name,
        )
        .await?;

    // Consume and categorize messages
    let mut messages = consumer.messages().await?;
    let mut received_by_subject: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    let collect_timeout = Duration::from_secs(10);
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
    }).await??;

    // Verify events were received for all subjects
    let total_received: usize = received_by_subject.values().map(|v| v.len()).sum();
    assert_eq!(total_received, test_cases.len());

    // Verify subject routing worked correctly
    for (subject, _) in &test_cases {
        assert!(
            received_by_subject.contains_key(*subject),
            "No events received for subject: {}",
            subject
        );
    }

    println!("✅ Multi-subject stream processing test passed");
    println!("  - Subjects processed: {}", received_by_subject.len()); 
    println!("  - Total events: {}", total_received);
    
    Ok(())
}

/// Test consumer groups and load balancing
#[sinex_test]
async fn test_consumer_group_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let stream_name = "sinex-test-consumer-groups";
    let subject = "events.test.consumer_groups";

    // Create stream
    let stream_config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        ..Default::default()
    };
    
    let _stream = jetstream
        .get_or_create_stream(stream_config)
        .await?;

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
            .publish(subject, event_bytes.into())
            .await?;
        ack.await?;
    }

    println!("Published {} events for consumer group test", event_count);

    // Create multiple consumers in the same consumer group
    let consumer_count = 3;
    let consumer_group = "test-consumer-group";
    
    let mut consumers = Vec::new();
    for i in 0..consumer_count {
        let consumer = jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(format!("{}-{}", consumer_group, i)),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    ..Default::default()
                },
                stream_name,
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
            let consume_timeout = Duration::from_secs(8);
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
            }).await;
            
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
    println!("Consumer group processing results:");
    for (consumer_id, count) in &messages_per_consumer {
        println!("  Consumer {}: {} messages", consumer_id, count);
    }
    
    let total_processed = all_received_messages.len();
    println!("  Total processed: {} / {}", total_processed, event_count);

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

    println!("✅ Consumer group processing test passed");
    Ok(())
}

/// Test stream processing with message ordering
#[sinex_test]
async fn test_ordered_stream_processing(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let stream_name = "sinex-test-ordering";
    let subject = "events.test.ordering";

    // Create stream
    let stream_config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        ..Default::default()
    };
    
    let _stream = jetstream
        .get_or_create_stream(stream_config)
        .await?;

    // Publish ordered sequence of events
    let sequence_length = 15;
    for i in 0..sequence_length {
        let event = json!({
            "event_type": "ordered_event",
            "sequence_number": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": format!("Event in sequence: {}", i),
        });
        
        let event_bytes = serde_json::to_vec(&event)?;
        let ack = jetstream
            .publish(subject, event_bytes.into())
            .await?;
        ack.await?;
        
        // Small delay to ensure ordering
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    println!("Published {} ordered events", sequence_length);

    // Create consumer
    let consumer = jetstream
        .create_consumer_on_stream(
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("test-ordering-consumer".to_string()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                ..Default::default()
            },
            stream_name,
        )
        .await?;

    // Consume messages in order
    let mut messages = consumer.messages().await?;
    let mut received_sequence = Vec::new();

    let collect_timeout = Duration::from_secs(10);
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
    }).await??;

    // Verify ordering was preserved
    println!("Received sequence: {:?}", received_sequence);
    
    assert_eq!(received_sequence.len(), sequence_length);
    
    // Check if sequence is in order (0, 1, 2, 3, ...)
    let expected_sequence: Vec<usize> = (0..sequence_length).collect();
    assert_eq!(
        received_sequence, expected_sequence,
        "Messages should be received in order"
    );

    println!("✅ Ordered stream processing test passed");
    Ok(())
}

/// Test stream processing error handling and recovery
#[sinex_test]
async fn test_stream_error_handling(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let stream_name = "sinex-test-error-handling";
    let subject = "events.test.errors";

    // Create stream
    let stream_config = StreamConfig {
        name: stream_name.to_string(),
        subjects: vec![subject.to_string()],
        ..Default::default()
    };
    
    let _stream = jetstream
        .get_or_create_stream(stream_config)
        .await?;

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
            .publish(subject, event_bytes.into())
            .await?;
        ack.await?;
        
        println!("Published event {}: {:?}", i + 1, event["event_type"]);
    }

    // Create consumer
    let consumer = jetstream
        .create_consumer_on_stream(
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("test-error-handling-consumer".to_string()),
                ..Default::default()
            },
            stream_name,
        )
        .await?;

    // Consume messages and handle errors gracefully
    let mut messages = consumer.messages().await?;
    let mut successfully_processed = 0;
    let mut processing_errors = 0;

    let collect_timeout = Duration::from_secs(8);
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
                        if event["event_type"].as_str().unwrap_or("").contains("problematic") {
                            println!("Processing failed for event: {:?}", event["event_type"]);
                            processing_errors += 1;
                            // NACK or handle error (in real implementation)
                            // For test, we still ACK to avoid redelivery
                            message.ack().await?;
                        } else {
                            println!("Successfully processed: {:?}", event["event_type"]);
                            successfully_processed += 1;
                            message.ack().await?;
                        }
                    }
                    Err(e) => {
                        println!("Failed to parse event: {}", e);
                        processing_errors += 1;
                        message.ack().await?; // ACK to avoid infinite redelivery
                    }
                }
            }
        }
        Ok::<(), color_eyre::eyre::Error>(())
    }).await??;

    // Verify error handling worked
    println!("Processing results:");
    println!("  Successfully processed: {}", successfully_processed);
    println!("  Processing errors: {}", processing_errors);
    println!("  Total messages: {}", test_events.len());

    assert_eq!(successfully_processed + processing_errors, test_events.len());
    assert!(successfully_processed > 0, "Should process at least some valid events");
    assert_eq!(processing_errors, 1, "Should have exactly 1 processing error (the problematic event)");

    println!("✅ Stream error handling test passed");
    Ok(())
}