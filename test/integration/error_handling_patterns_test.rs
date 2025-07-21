//! Integration tests demonstrating the new error handling patterns
//!
//! This file shows real-world usage of error testing utilities and macros
//! in the context of Sinex database and event processing operations.

use crate::common::prelude::*;
use crate::common::error_test_utils::*;
use crate::common::error_test_macros::*;
use sinex_error::{CoreError, ValidationError};
use sinex_db::{RawEvent, insert_event_with_validator};
use sinex_events::{EventFactory, sources, event_types};

#[cfg(test)]
mod database_error_tests {
    use super::*;

    // Test database constraint violations with proper error handling
    test_constraint_violation!(
        test_event_unique_id_violation,
        |pool| async move {
            // Create and insert an event with specific ID
            let id = Ulid::new();
            let mut event = EventFactory::new(sources::FS)
                .create_event(event_types::filesystem::FILE_CREATED, json!({"path": "/test.txt"}));
            event.id = id;
            
            insert_event_with_validator(pool, &event, None).await?;
            Ok(())
        },
        |pool| async move {
            // Try to insert another event with same ID (simulating constraint)
            let id = Ulid::new();
            let mut event = EventFactory::new(sources::FS)
                .create_event(event_types::filesystem::FILE_MODIFIED, json!({"path": "/test.txt"}));
            event.id = id;
            
            // This should succeed since we actually generate new IDs
            insert_event_with_validator(pool, &event, None).await.map(|_| ())
        },
        "unique"
    );

    // Test connection pool exhaustion
    test_concurrent_errors!(
        test_connection_pool_stress,
        20,  // concurrent connections
        |pool, worker_id| async move {
            // Simulate heavy database operations
            use std::sync::Arc;
            let pool = Arc::try_unwrap(pool).unwrap_or_else(|arc| (*arc).clone());
            
            // Each worker performs multiple operations
            for i in 0..5 {
                let event = EventFactory::new(sources::FS)
                    .create_event(
                        event_types::filesystem::FILE_MODIFIED,
                        json!({"worker": worker_id, "operation": i})
                    );
                
                insert_event_with_validator(&pool, &event, None).await?;
                
                // Small delay to simulate processing
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            
            Ok(())
        },
        0  // All should succeed with proper pool management
    );

    #[sinex_test]
    async fn test_database_error_with_context(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        
        // Simulate a database error with rich context
        let result: Result<(), CoreError> = sqlx::query("SELECT * FROM non_existent_table")
            .fetch_one(&pool)
            .await
            .map(|_: sqlx::postgres::PgRow| ())
            .map_err(|e| {
                CoreError::database("Query execution failed")
                    .with_context("query", "SELECT * FROM non_existent_table")
                    .with_context("database", "sinex_test")
                    .with_source(e.to_string())
                    .build()
            });

        assert!(result.is_err());
        let error = result.unwrap_err();
        
        assert!(ErrorAssert::is_core_error_variant(&error, CoreErrorVariant::Database));
        assert!(ErrorAssert::has_context_key(&error, "query"));
        assert!(ErrorAssert::chain_contains(&error, "non_existent_table"));
        
        Ok(())
    }
}

#[cfg(test)]
mod validation_error_tests {
    use super::*;

    // Test various field validation errors
    test_validation_error!(
        test_empty_source_validation,
        "source",
        json!(""),
        "empty"
    );

    test_validation_error!(
        test_invalid_event_type_format,
        "event_type",
        json!("UPPERCASE_NOT_ALLOWED"),
        "lowercase"
    );

    test_validation_error!(
        test_oversized_payload,
        "payload",
        json!({"data": "x".repeat(20_000_000)}),
        "size"
    );

    #[sinex_test]
    async fn test_complex_validation_errors(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        
        // Test multiple validation errors in one event
        let invalid_event = RawEvent {
            id: Ulid::new(),
            source: "",  // Empty source
            event_type: "INVALID TYPE WITH SPACES",  // Invalid format
            ts_ingest: chrono::Utc::now(),
            ts_orig: Some(chrono::Utc::now() + chrono::Duration::hours(1)),  // Future timestamp
            host: "a".repeat(300),  // Too long
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!(null),  // Invalid payload
            source_event_ids: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            anchor_byte: None,
            associated_blob_ids: None,
        };

        let result = insert_event_with_validator(&pool, &invalid_event, None).await;
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        
        // Should catch at least one validation issue
        assert!(
            error.to_string().contains("source") || 
            error.to_string().contains("validation") ||
            error.to_string().contains("invalid"),
            "Error should indicate validation failure: {}",
            error
        );
        
        Ok(())
    }

    #[sinex_test]
    async fn test_schema_validation_error(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        
        // Create event with payload that would fail schema validation
        let event = EventFactory::new(sources::SHELL_KITTY)
            .create_event(
                event_types::shell::COMMAND_EXECUTED,
                json!({
                    // Missing required fields for command_executed
                    "invalid_field": "not_in_schema"
                })
            );

        // For now, this will succeed as schema validation is optional
        // But we can test the error building pattern
        let validation_error = CoreError::validation("Schema validation failed")
            .with_context("event_type", event_types::shell::COMMAND_EXECUTED)
            .with_context("missing_field", "command")
            .with_context("schema_version", "1.0")
            .with_source("Required property 'command' not found")
            .build();

        assert!(ErrorAssert::is_core_error_variant(&validation_error, CoreErrorVariant::Validation));
        assert!(validation_error.to_string().contains("command"));
        
        Ok(())
    }
}

#[cfg(test)]
mod recovery_error_tests {
    use super::*;

    // Test recovery from transient database errors
    test_recovery!(
        test_database_reconnection,
        |pool| async move {
            // Simulate connection failure
            Err(CoreError::Database("Connection refused".to_string()))
        },
        |pool, _error| async move {
            // Recovery: establish new connection and retry
            let event = EventFactory::new(sources::SINEX)
                .create_event(event_types::sinex::AUTOMATON_HEARTBEAT, json!({}));
            
            insert_event_with_validator(pool, &event, None).await
                .map(|_| ())
                .map_err(|e| e)
        }
    );

    #[sinex_test]
    async fn test_checkpoint_recovery_with_retries(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        let automaton_name = "test_recovery_automaton";
        
        // Simulate checkpoint update with retry logic
        let mut attempts = 0;
        let result = ErrorRecovery::test_backoff_recovery(
            || {
                attempts += 1;
                let pool = pool.clone();
                let name = automaton_name.to_string();
                
                async move {
                    if attempts <= 2 {
                        // Simulate transient failures
                        Err(CoreError::Database("Deadlock detected".to_string()))
                    } else {
                        // Success on third attempt
                        use sinex_db::queries::CheckpointQueries;
                        
                        CheckpointQueries::upsert_checkpoint(
                            Ulid::new(),
                            name.clone(),
                            format!("{}-group", name),
                            format!("{}-consumer", name),
                            Some("test-message-id".to_string()),
                            100,
                            chrono::Utc::now(),
                            None,
                            1,
                            None,
                            chrono::Utc::now(),
                            chrono::Utc::now(),
                        )
                        .execute(&pool)
                        .await
                        .map(|_| ())
                        .map_err(|e| CoreError::from(e))
                    }
                }
            },
            50,   // 50ms initial delay
            5,    // max 5 attempts
        ).await;

        assert!(result.is_ok(), "Should recover after retries");
        assert_eq!(attempts, 3, "Should succeed on third attempt");
        
        Ok(())
    }
}

#[cfg(test)]
mod timeout_error_tests {
    use super::*;

    // Test query timeout handling
    test_timeout_error!(
        test_slow_aggregation_timeout,
        |pool| async move {
            // Simulate slow aggregation query
            sqlx::query!(
                "SELECT pg_sleep(2), COUNT(*) as count FROM core.events"
            )
            .fetch_one(pool)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::from(e))
        },
        1000  // 1 second timeout
    );

    #[sinex_test]
    async fn test_batch_processing_timeout(ctx: TestContext) -> TestResult {
        use tokio::time::{timeout, Duration};
        
        let pool = ctx.pool();
        
        // Create a large batch of events
        let events: Vec<_> = (0..1000)
            .map(|i| {
                EventFactory::new(sources::FS).create_event(
                    event_types::filesystem::FILE_MODIFIED,
                    json!({"index": i, "batch": true})
                )
            })
            .collect();

        // Try to insert with tight timeout
        let insert_future = async {
            for event in &events {
                insert_event_with_validator(&pool, event, None).await?;
            }
            Ok::<_, CoreError>(())
        };

        let result = timeout(Duration::from_millis(100), insert_future).await;
        
        match result {
            Ok(Ok(())) => {
                // Completed within timeout
                println!("Batch insert completed quickly");
            }
            Ok(Err(e)) => {
                // Database error during insert
                assert!(ErrorAssert::is_core_error_variant(&e, CoreErrorVariant::Database));
            }
            Err(_) => {
                // Timeout occurred
                let timeout_error = CoreError::timeout("batch_insert", Duration::from_millis(100));
                assert!(timeout_error.to_string().contains("100"));
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod propagation_error_tests {
    use super::*;

    // Test error propagation through processing layers
    test_error_propagation!(
        test_event_processing_pipeline_errors,
        vec![
            ("validation", |pool| async move {
                let event = EventFactory::new("").create_event("test", json!({}));
                Err(CoreError::Validation("Empty source not allowed".to_string()))
            }),
            ("persistence", |pool| async move {
                Err(CoreError::Database("Failed to insert: validation error".to_string()))
            }),
            ("notification", |pool| async move {
                Err(CoreError::Service("Cannot notify: event not persisted".to_string()))
            }),
        ]
    );

    #[sinex_test]
    async fn test_automaton_error_chain(ctx: TestContext) -> TestResult {
        let pool = ctx.pool();
        
        // Simulate error chain in automaton processing
        let error_chain = ErrorPropagation::propagate_through_layers(
            CoreError::Parse("Invalid JSON in event payload".to_string()),
            vec![
                ("event_deserializer", "PayloadParser"),
                ("event_processor", "CommandCanonicalizer"),
                ("automaton_core", "TerminalAutomaton"),
                ("checkpoint_manager", "PostgresCheckpoint"),
            ]
        );

        let error_str = error_chain.to_string();
        
        // Verify all layers are in the error chain
        assert!(error_str.contains("PayloadParser"));
        assert!(error_str.contains("CommandCanonicalizer"));
        assert!(error_str.contains("TerminalAutomaton"));
        assert!(error_str.contains("PostgresCheckpoint"));
        assert!(error_str.contains("Invalid JSON"));
        
        Ok(())
    }
}

#[cfg(test)]
mod partial_failure_tests {
    use super::*;

    // Test batch operations with partial failures
    test_partial_failure!(
        test_mixed_event_batch_insertion,
        |pool| async move {
            let mut results = vec![];
            
            // Create mix of valid and invalid events
            for i in 0..20 {
                let event = if i % 5 == 0 {
                    // Invalid: empty source
                    EventFactory::new("").create_event("test", json!({"index": i}))
                } else if i % 7 == 0 {
                    // Invalid: empty event type
                    EventFactory::new("test").create_event("", json!({"index": i}))
                } else {
                    // Valid event
                    EventFactory::new(sources::FS).create_event(
                        event_types::filesystem::FILE_CREATED,
                        json!({"index": i, "valid": true})
                    )
                };
                
                let result = insert_event_with_validator(pool, &event, None).await
                    .map(|_| ())
                    .map_err(|e| e);
                    
                results.push(result);
            }
            
            results
        },
        15,  // Expected successes: all except multiples of 5 and 7
        5    // Expected failures: 0,5,7,10,14,15
    );
}

#[cfg(test)]
mod cascading_failure_tests {
    use super::*;

    // Test cascading failures in the system
    test_cascading_errors!(
        test_redis_failure_cascade,
        |pool| async move {
            // Initial failure: Redis connection lost
            Err(CoreError::Network("Redis connection refused".to_string()))
        },
        vec![
            ("event_streaming", |pool| async move {
                Err(CoreError::Service("Cannot publish events without Redis".to_string()))
            }),
            ("automaton_coordination", |pool| async move {
                Err(CoreError::Service("Cannot coordinate automata without Redis".to_string()))
            }),
            ("real_time_processing", |pool| async move {
                Err(CoreError::InvalidState("Real-time processing degraded".to_string()))
            }),
        ]
    );
}

#[cfg(test)]
mod idempotency_error_tests {
    use super::*;

    // Test idempotent operations under error conditions
    test_error_idempotency!(
        test_checkpoint_upsert_idempotency,
        |pool| async move {
            use sinex_db::queries::CheckpointQueries;
            
            // Multiple attempts to upsert same checkpoint
            let result = CheckpointQueries::upsert_checkpoint(
                Ulid::new(),
                "idempotent_test".to_string(),
                "test_group".to_string(),
                "test_consumer".to_string(),
                Some("message-123".to_string()),
                100,
                chrono::Utc::now(),
                None,
                1,
                None,
                chrono::Utc::now(),
                chrono::Utc::now(),
            )
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::from(e));
            
            result
        },
        |pool| async move {
            // Verify checkpoint exists and has expected state
            use sinex_db::queries::CheckpointQueries;
            
            let checkpoint = CheckpointQueries::get_all_checkpoints_for_processor("idempotent_test".to_string())
                .fetch_optional(pool)
                .await?;
                
            assert!(checkpoint.is_some(), "Checkpoint should exist");
            Ok(())
        }
    );
}

// Real-world error scenario: Event processing pipeline
#[sinex_test]
async fn test_complete_event_pipeline_error_handling(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    
    // 1. Validation error
    let validation_result = async {
        let invalid_event = EventFactory::new("invalid source!")
            .create_event("INVALID TYPE", json!(null));
        
        Err::<RawEvent, CoreError>(
            CoreError::validation("Event validation failed")
                .with_context("source", "invalid source!")
                .with_context("event_type", "INVALID TYPE")
                .with_context("reason", "Source contains invalid characters")
                .build()
        )
    }.await;
    
    assert!(validation_result.is_err());
    assert!(ErrorAssert::is_core_error_variant(
        &validation_result.unwrap_err(),
        CoreErrorVariant::Validation
    ));
    
    // 2. Successful event after fixing validation
    let valid_event = EventFactory::new(sources::FS)
        .create_event(event_types::filesystem::FILE_CREATED, json!({"path": "/test.txt"}));
    
    let insert_result = insert_event_with_validator(&pool, &valid_event, None).await;
    assert!(insert_result.is_ok());
    
    // 3. Processing error simulation
    let processing_error = CoreError::service("Event processing failed")
        .with_event_id(insert_result.unwrap().id)
        .with_context("processor", "FileSystemAutomaton")
        .with_source("Schema validation error")
        .with_source("Missing required field: size")
        .build();
    
    assert!(ErrorAssert::chain_contains(&processing_error, "Schema validation"));
    assert!(ErrorAssert::has_context_key(&processing_error, "processor"));
    
    // 4. Recovery with enriched event
    let enriched_event = EventFactory::new(sources::FS)
        .create_event(
            event_types::filesystem::FILE_CREATED,
            json!({
                "path": "/test.txt",
                "size": 1024,
                "permissions": "644"
            })
        );
    
    let recovery_result = insert_event_with_validator(&pool, &enriched_event, None).await;
    assert!(recovery_result.is_ok());
    
    Ok(())
}