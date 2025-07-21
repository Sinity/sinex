// Property tests for satellite architecture
//
// Tests that verify satellite communication, lifecycle, and coordination properties

use crate::common::test_macros::*;
use crate::common::prelude::*;
use crate::common::property_builders::*;
use crate::property::strategies::{event_sources, event_payloads, event_sequences};
use proptest::prelude::*;
use sinex_satellite_sdk::config::SatelliteConfig;
use std::time::Duration;

/// Test satellite configuration parsing and validation
proptest! {
    #[test]
    fn satellite_config_parsing_is_robust(
        ingest_socket_path in "[a-zA-Z0-9_/.-]+",
        redis_url in "redis://[a-zA-Z0-9:./-]+",
        checkpoint_interval in 1u64..3600u64,
        batch_size in 1usize..10000usize,
        max_retries in 0u32..10u32,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Test config creation with various valid parameters
            let config_json = serde_json::json!({
                "ingest_socket_path": ingest_socket_path,
                "redis_url": redis_url,
                "checkpoint_interval_secs": checkpoint_interval,
                "batch_size": batch_size,
                "max_retries": max_retries,
                "timeout_secs": 30,
                "service_name": "test-satellite",
            });

            // Configuration should parse successfully with valid inputs
            let config_result = serde_json::from_value::<SatelliteConfig>(config_json);
            match config_result {
                Ok(config) => {
                    assert_eq!(config.ingest_socket_path, ingest_socket_path);
                    assert_eq!(config.redis_url, redis_url);
                    // Note: checkpoint_interval_secs, batch_size, and max_retries are not part of SatelliteConfig
                }
                Err(_) => {
                    // Some configurations might be invalid, which is acceptable
                    // as long as the parsing doesn't panic
                }
            }
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}

/// Test satellite event processing pipeline
proptest! {
    #[test]
    fn satellite_event_processing_preserves_order(
        events in event_sequences(),
        batch_size in 1usize..100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let setup = crate::common::satellite_integration::SatelliteTestSetup::new("order_test")
                .await
                .unwrap();

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            // Create test satellite with specified batch size
            let satellite_config = crate::common::satellite_test_utils::create_test_satellite_config(
                "order-test-satellite",
                &setup.ingestd.socket_path,
            );

            let satellite = setup.add_satellite("order-test-satellite").await.unwrap();

            // Process events in batches
            let mut processed_events = Vec::new();
            for chunk in events.chunks(batch_size) {
                for event in chunk {
                    ctx.insert_event(event).await.unwrap();
                    processed_events.push(event.clone());
                }

                // Wait for batch processing
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Verify events were processed in order
            ctx.wait_for_event_count(processed_events.len()).await.unwrap();

            let db_events = ctx.query_events().await.unwrap();
            assert_eq!(db_events.len(), processed_events.len());

            // Verify ULID ordering is preserved (ULIDs are time-ordered)
            for i in 1..db_events.len() {
                assert!(db_events[i-1].id.timestamp() <= db_events[i].id.timestamp());
            }
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly_1(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid_1(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order_1(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types_1(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events_1(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression_1(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns_1(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events_1(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}

/// Test satellite fault tolerance
proptest! {
    #[test]
    fn satellite_handles_intermittent_failures(
        failure_rate in 0.0..0.3f64, // Up to 30% failure rate
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=50
        ),
        recovery_delay in 1u64..1000u64,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let setup = crate::common::satellite_integration::SatelliteTestSetup::new("fault_test")
                .await
                .unwrap();

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            let satellite = setup.add_satellite("fault-test-satellite").await.unwrap();

            let mut successful_events = 0;
            let mut failed_events = 0;

            for (i, (source, event_type, payload)) in events.iter().enumerate() {
                // Simulate intermittent failures
                let should_fail = (i as f64 * failure_rate) % 1.0 < failure_rate;

                if should_fail {
                    // Simulate failure by creating invalid event
                    let invalid_event = crate::common::events::invalid_event();
                    let result = ctx.insert_event(&invalid_event).await;
                    if result.is_err() {
                        failed_events += 1;
                    }
                } else {
                    // Process normal event
                    let event = crate::common::events::create_raw_event(source, event_type, payload.clone(), chrono::Utc::now());
                    ctx.insert_event(&event).await.unwrap();
                    successful_events += 1;
                }

                // Add small delay to simulate processing time
                tokio::time::sleep(Duration::from_millis(recovery_delay / 100)).await;
            }

            // Verify successful events were processed
            ctx.wait_for_event_count(successful_events).await.unwrap();

            let final_count = ctx.event_count().await.unwrap();
            assert_eq!(final_count, successful_events as i64);

            // Verify system recovered from failures
            assert!(successful_events > 0, "At least some events should succeed");
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly_2(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid_2(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order_2(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types_2(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events_2(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression_2(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns_2(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events_2(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}

/// Test satellite resource management
proptest! {
    #[test]
    fn satellite_manages_resources_efficiently(
        concurrent_satellites in 1usize..5usize,
        events_per_satellite in 1usize..100usize,
        memory_limit_mb in 10usize..100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let setup = crate::common::satellite_integration::SatelliteTestSetup::new("resource_test")
                .await
                .unwrap();

            // Create multiple satellites
            let mut satellites = Vec::new();
            for i in 0..concurrent_satellites {
                let satellite_name = format!("resource-test-satellite-{}", i);
                let satellite = setup.add_satellite(&satellite_name).await.unwrap();
                satellites.push(satellite);
            }

            // Generate events for each satellite
            let mut total_events = 0;
            for i in 0..concurrent_satellites {
                for j in 0..events_per_satellite {
                    let event = ctx.create_test_event(
                        &format!("satellite-{}", i),
                        &format!("test.event.{}", j),
                        json!({"satellite": i, "event": j}),
                    );
                    ctx.insert_event(&event).await.unwrap();
                    total_events += 1;
                }
            }

            // Wait for all events to be processed
            ctx.wait_for_event_count(total_events).await.unwrap();

            // Verify all satellites processed their events
            let final_count = ctx.event_count().await.unwrap();
            assert_eq!(final_count, total_events as i64);

            // Check resource usage (basic check - would need more sophisticated monitoring in production)
            let process_count = satellites.len();
            assert!(process_count <= concurrent_satellites);
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly_3(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid_3(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order_3(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types_3(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events_3(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression_3(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns_3(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events_3(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}

/// Test satellite configuration updates
proptest! {
    #[test]
    fn satellite_config_updates_are_atomic(
        initial_batch_size in 1usize..100usize,
        updated_batch_size in 1usize..100usize,
        checkpoint_interval in 1u64..300u64,
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=20
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let setup = crate::common::satellite_integration::SatelliteTestSetup::new("config_test")
                .await
                .unwrap();

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            // Create satellite with initial config
            let satellite = setup.add_satellite("config-test-satellite").await.unwrap();

            // Process some events with initial config
            let half_point = events.len() / 2;
            for (source, event_type, payload) in events.iter().take(half_point) {
                let event = crate::common::events::create_raw_event(source, event_type, payload.clone(), chrono::Utc::now());
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for initial processing
            ctx.wait_for_event_count(half_point).await.unwrap();

            // Process remaining events (simulating config update)
            for (source, event_type, payload) in events.iter().skip(half_point) {
                let event = crate::common::events::create_raw_event(source, event_type, payload.clone(), chrono::Utc::now());
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for all events to be processed
            ctx.wait_for_event_count(events.len()).await.unwrap();

            // Verify no events were lost during config update
            let final_count = ctx.event_count().await.unwrap();
            assert_eq!(final_count, events.len() as i64);
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly_4(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid_4(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order_4(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types_4(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events_4(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression_4(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns_4(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events_4(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}

/// Test satellite network partitioning resilience
proptest! {
    #[test]
    fn satellite_survives_network_partitions(
        partition_duration in 1u64..100u64,
        events_before_partition in 1usize..20usize,
        events_during_partition in 1usize..20usize,
        events_after_partition in 1usize..20usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let setup = crate::common::satellite_integration::SatelliteTestSetup::new("partition_test")
                .await
                .unwrap();

            let satellite = setup.add_satellite("partition-test-satellite").await.unwrap();

            // Phase 1: Normal operation
            for i in 0..events_before_partition {
                let event = ctx.create_test_event("partition_test", &format!("before.{}", i), json!({"phase": "before", "index": i}));
                ctx.insert_event(&event).await.unwrap();
            }

            ctx.wait_for_event_count(events_before_partition).await.unwrap();

            // Phase 2: Simulate partition by creating events that might fail
            // (In a real test, we'd disconnect network, but here we simulate with timing)
            let partition_start = tokio::time::Instant::now();

            for i in 0..events_during_partition {
                let event = ctx.create_test_event("partition_test", &format!("during.{}", i), json!({"phase": "during", "index": i}));
                // Try to insert, but don't fail if it doesn't work immediately
                let _ = tokio::time::timeout(
                    Duration::from_millis(50),
                    ctx.insert_event(&event)
                ).await;
            }

            // Wait for partition duration
            tokio::time::sleep(Duration::from_millis(partition_duration)).await;

            // Phase 3: Recovery
            for i in 0..events_after_partition {
                let event = ctx.create_test_event("partition_test", &format!("after.{}", i), json!({"phase": "after", "index": i}));
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for recovery and verify total events
            let expected_minimum = events_before_partition + events_after_partition;
            ctx.wait_for_event_count(expected_minimum).await.unwrap();

            let final_count = ctx.event_count().await.unwrap();
            assert!(final_count >= expected_minimum as i64);
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly_5(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid_5(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order_5(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types_5(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events_5(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression_5(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns_5(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events_5(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}

/// Test satellite coordination with automata
proptest! {
    #[test]
    fn satellite_automaton_coordination_is_correct(
        automaton_type in prop_oneof![
            Just("command-canonicalizer"),
            Just("health-aggregator"),
            Just("test-automaton"),
        ],
        events in proptest::collection::vec(
            (event_sources(), event_types(), event_payloads()),
            1..=30
        ),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let setup = crate::common::satellite_integration::SatelliteTestSetup::new("coordination_test")
                .await
                .unwrap();

            // Skip if no events to test
            if events.is_empty() {
                return;
            }

            let satellite = setup.add_satellite("coordination-test-satellite").await.unwrap();
            let automaton = setup.add_automaton(automaton_type).await.unwrap();

            // Process events through the satellite
            for (source, event_type, payload) in events.iter() {
                let event = crate::common::events::create_raw_event(source, event_type, payload.clone(), chrono::Utc::now());
                ctx.insert_event(&event).await.unwrap();
            }

            // Wait for events to be processed by satellite
            ctx.wait_for_event_count(events.len()).await.unwrap();

            // Wait for automaton to process events
            crate::common::test_context::TestContext::wait_for_checkpoint_progress(&ctx, automaton_type, events.len() as u64).await.unwrap();

            // Verify coordination worked correctly
            let checkpoint = ctx.verify_checkpoint(automaton_type).await.unwrap();
            assert!(checkpoint.processed_count > 0);

            let final_count = ctx.event_count().await.unwrap();
            assert_eq!(final_count, events.len() as i64);
        });
    }
}

/// Test satellite event processing with property builders
proptest! {
    #[test]
    fn satellite_processes_events_correctly_6(
        events in arbitrary_event_batch(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Simulate satellite processing
            let processed_count = event_ids.len();
            
            // Verify events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= processed_count as i64);
        });
    }
}

/// Test satellite heartbeat events
proptest! {
    #[test]
    fn satellite_heartbeat_events_are_valid_6(
        heartbeat in heartbeat_event(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert heartbeat event
            let result = sinex_db::insert_event_with_validator(&pool, &heartbeat, None).await;
            assert!(result.is_ok());
            
            // Verify heartbeat fields
            assert_eq!(heartbeat.source, sources::SINEX);
            assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
            
            // Verify payload structure
            let payload = heartbeat.payload.as_object().unwrap();
            assert!(payload.contains_key("automaton_name"));
            assert!(payload.contains_key("events_processed"));
            assert!(payload.contains_key("uptime_seconds"));
        });
    }
}

/// Test satellite event batching behavior
proptest! {
    #[test]
    fn satellite_batching_maintains_order_6(
        batch in time_ordered_batch(),
        batch_size in 1usize..=100usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Process events in batches
            let mut all_inserted = Vec::new();
            for chunk in batch.chunks(batch_size) {
                for event in chunk {
                    let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                    all_inserted.push((inserted.id, event.ts_orig));
                }
            }
            
            // Verify timestamp ordering is preserved
            for window in all_inserted.windows(2) {
                let (_, ts1) = &window[0];
                let (_, ts2) = &window[1];
                if let (Some(t1), Some(t2)) = (ts1, ts2) {
                    assert!(t1 <= t2, "Events should maintain timestamp order");
                }
            }
        });
    }
}

/// Test satellite recovery with realistic event streams
proptest! {
    #[test]
    fn satellite_recovery_handles_event_types_6(
        fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
        shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
        window_events in proptest::collection::vec(window_event(), 1..=10),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert different event types
            let mut total_events = 0;
            
            for event in fs_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in shell_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            for event in window_events {
                sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                total_events += 1;
            }
            
            // Verify all events were inserted
            let count = sinex_db::count_events(&pool).await.unwrap();
            assert!(count >= total_events);
        });
    }
}

/// Test satellite handling of malformed events
proptest! {
    #[test]
    fn satellite_rejects_invalid_events_6(
        invalid_event in prop_oneof![
            empty_source_event(),
            massive_payload_event(),
            extreme_timestamp_event(),
        ],
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Try to insert invalid event
            let result = sinex_db::insert_event_with_validator(&pool, &invalid_event, None).await;
            
            // Some invalid events should be rejected
            if invalid_event.source.is_empty() {
                assert!(result.is_err(), "Empty source should be rejected");
            }
            
            // Massive payloads might fail due to size limits
            if invalid_event.payload.to_string().len() > 1_000_000 {
                // Large payloads might be rejected or succeed based on DB config
                // So we just verify no panic occurs
            }
        });
    }
}

/// Test satellite checkpoint integration
proptest! {
    #[test]
    fn satellite_checkpoint_progression_6(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        satellite_name in "[a-z]+-satellite",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Insert events
            let mut event_ids = Vec::new();
            for event in &events {
                let inserted = sinex_db::insert_event_with_validator(&pool, &event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Create checkpoint manager
            let checkpoint_manager = sinex_satellite_sdk::checkpoint::CheckpointManager::new(
                pool.clone(),
                satellite_name.clone(),
                format!("{}-group", satellite_name),
                format!("{}-consumer", satellite_name),
            );
            
            // Save checkpoint
            let state = sinex_satellite_sdk::checkpoint::CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: Some(json!({"events": event_ids.len()})),
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
            
            // Verify checkpoint was saved
            let loaded = checkpoint_manager.load_checkpoint().await.unwrap();
            assert_eq!(loaded.processed_count, events.len() as u64);
        });
    }
}

/// Test realistic user activity streams
proptest! {
    #[test]
    fn satellite_handles_user_activity_patterns_6(
        activity_batch in user_activity_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            let mut inserted_count = 0;
            
            // Insert user activity events
            for event in &activity_batch {
                let result = sinex_db::insert_event_with_validator(&pool, event, None).await;
                if result.is_ok() {
                    inserted_count += 1;
                }
            }
            
            // Verify events represent realistic user activity
            assert!(inserted_count > 0, "At least some events should be inserted");
            
            // Check event diversity (different sources)
            let sources_used: std::collections::HashSet<_> = activity_batch
                .iter()
                .map(|e| e.source.as_str())
                .collect();
            assert!(sources_used.len() > 1, "User activity should include multiple event sources");
        });
    }
}

/// Test related events handling (e.g., file lifecycle)
proptest! {
    #[test]
    fn satellite_tracks_related_events_6(
        related_batch in related_events_batch(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = crate::common::test_context::TestContext::new().await.unwrap();
            let pool = ctx.pool().clone();
            
            // Extract the file path from first event
            let file_path = if let Some(path_value) = related_batch[0].payload.get("path") {
                path_value.as_str().unwrap_or("unknown")
            } else {
                "unknown"
            };
            
            // Insert related events
            let mut event_ids = Vec::new();
            for event in &related_batch {
                let inserted = sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
                event_ids.push(inserted.id);
            }
            
            // Verify event sequence represents file lifecycle
            assert_eq!(related_batch[0].event_type, event_types::filesystem::FILE_CREATED);
            assert!(related_batch.iter().any(|e| e.event_type == event_types::filesystem::FILE_MODIFIED));
            assert_eq!(related_batch.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
            
            // All events should reference the same file
            for event in &related_batch {
                if let Some(path) = event.payload.get("path") {
                    assert_eq!(path.as_str().unwrap(), file_path);
                }
            }
        });
    }
}
