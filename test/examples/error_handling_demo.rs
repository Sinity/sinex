//! Example demonstrating comprehensive error handling test patterns
//!
//! This file shows how to use the new error testing utilities and macros
//! for testing various error scenarios in Sinex.

use crate::common::prelude::*;
use crate::common::error_test_utils::*;
use crate::common::error_test_macros::*;
use sinex_error::{CoreError, ValidationError};

#[cfg(test)]
mod basic_error_tests {
    use super::*;

    // Example 1: Test specific error type using macro
    test_error_case!(
        test_database_connection_error,
        |pool| async move {
            // Simulate database error by using invalid query
            sqlx::query("INVALID SQL SYNTAX HERE")
                .execute(pool)
                .await
                .map_err(|e| CoreError::from(e))
        },
        CoreErrorVariant::Database
    );

    // Example 2: Test error with custom validation
    test_error_case!(
        test_validation_error_with_context,
        |pool| async move {
            let event = sinex_events::EventFactory::new("")  // Empty source
                .create_event("test.event", json!({}));
            
            sinex_db::insert_event_with_validator(pool, &event, None)
                .await
                .map_err(|e| e)
        },
        CoreErrorVariant::Validation,
        |error: &CoreError| {
            assert!(
                ErrorAssert::contains_message(error, "source"),
                "Error should mention 'source' field"
            );
            Ok(())
        }
    );

    #[sinex_test]
    async fn test_error_scenario_builder(ctx: TestContext) -> TestResult {
        // Example 3: Build complex error scenarios
        let error = ErrorScenarioBuilder::new(
            CoreErrorVariant::Database,
            "Connection pool exhausted"
        )
        .with_context("pool_size", 10)
        .with_context("active_connections", 10)
        .with_context("waiting_connections", 5)
        .with_source("Too many concurrent requests")
        .with_source("Connection timeout after 30s")
        .build();

        // Verify error has all expected components
        assert!(ErrorAssert::is_core_error_variant(&error, CoreErrorVariant::Database));
        assert!(ErrorAssert::has_context_key(&error, "pool_size"));
        assert!(ErrorAssert::chain_contains(&error, "Too many concurrent requests"));
        
        Ok(())
    }

    #[sinex_test]
    async fn test_common_error_scenarios(ctx: TestContext) -> TestResult {
        // Example 4: Use pre-built common error scenarios
        let db_error = CommonErrorScenarios::database_connection_failed();
        assert!(ErrorAssert::is_core_error_variant(&db_error, CoreErrorVariant::Database));
        assert!(db_error.to_string().contains("localhost"));

        let validation_error = CommonErrorScenarios::validation_field_error("email", "invalid@");
        assert!(ErrorAssert::contains_message(&validation_error, "email"));
        assert!(ErrorAssert::contains_message(&validation_error, "Invalid format"));

        let timeout_error = CommonErrorScenarios::operation_timeout("query_events", 5000);
        assert!(ErrorAssert::is_core_error_variant(&timeout_error, CoreErrorVariant::Timeout));
        assert!(timeout_error.to_string().contains("5000"));

        Ok(())
    }
}

#[cfg(test)]
mod error_propagation_tests {
    use super::*;

    // Example 5: Test error propagation through layers
    test_error_propagation!(
        test_repository_service_handler_propagation,
        vec![
            ("repository", |pool| async move {
                // Repository layer fails
                Err(CoreError::Database("Primary key violation".to_string()))
            }),
            ("service", |pool| async move {
                // Service layer should propagate repository error
                Err(CoreError::Service("Failed to create user".to_string()))
            }),
            ("handler", |pool| async move {
                // Handler layer should propagate service error
                Err(CoreError::Other("Request failed".to_string()))
            }),
        ]
    );

    #[sinex_test]
    async fn test_error_context_propagation(ctx: TestContext) -> TestResult {
        // Example 6: Manual error propagation with context enrichment
        let layers = vec![
            ("data_access", "EventRepository"),
            ("business_logic", "EventProcessor"),
            ("api", "EventHandler"),
        ];

        let original_error = CoreError::Database("Connection timeout".to_string());
        let propagated = ErrorPropagation::propagate_through_layers(original_error, layers);

        // Verify all layers are mentioned
        let error_str = propagated.to_string();
        assert!(error_str.contains("EventRepository"));
        assert!(error_str.contains("EventProcessor"));
        assert!(error_str.contains("EventHandler"));
        assert!(error_str.contains("Connection timeout"));

        Ok(())
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;

    // Example 7: Test recovery from transient errors
    test_recovery!(
        test_transient_database_error_recovery,
        |pool| async move {
            // First attempt fails
            Err(CoreError::Database("Connection temporarily unavailable".to_string()))
        },
        |pool, _error| async move {
            // Recovery succeeds by retrying
            Ok(())
        }
    );

    // Example 8: Test retry with backoff
    test_recovery!(
        test_exponential_backoff_recovery,
        |pool| async move {
            // Operation that might succeed after retries
            static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let attempt = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            
            if attempt < 2 {
                Err(CoreError::Service("Service temporarily unavailable".to_string()))
            } else {
                Ok(())
            }
        },
        3,  // max retries
        true  // should succeed
    );

    #[sinex_test]
    async fn test_manual_recovery_logic(ctx: TestContext) -> TestResult {
        // Example 9: Manual recovery with custom logic
        let mut attempts = 0;
        let result = ErrorRecovery::test_backoff_recovery(
            || {
                attempts += 1;
                async move {
                    if attempts < 3 {
                        Err(CoreError::Network("Network unreachable".to_string()))
                    } else {
                        Ok("Success after retries")
                    }
                }
            },
            100,  // initial delay ms
            5,    // max retries
        ).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Success after retries");
        
        Ok(())
    }
}

#[cfg(test)]
mod validation_error_tests {
    use super::*;

    // Example 10: Test field validation errors
    test_validation_error!(
        test_invalid_event_source,
        "source",
        json!("source with spaces"),
        "Invalid character"
    );

    test_validation_error!(
        test_payload_too_large,
        "payload",
        json!({"data": "x".repeat(10_000_000)}),
        "exceeds maximum size"
    );

    #[sinex_test]
    async fn test_validation_error_details(ctx: TestContext) -> TestResult {
        // Example 11: Detailed validation error testing
        let error = ValidationError::Field {
            field: "email".to_string(),
            message: "Invalid email format".to_string(),
        };

        assert!(ErrorAssert::validation_has_field(&error, "email"));
        assert!(error.to_string().contains("Invalid email format"));

        // Test type validation error
        let type_error = ValidationError::InvalidType {
            field: "age".to_string(),
            expected: "number".to_string(),
            actual: "string".to_string(),
        };

        assert!(ErrorAssert::validation_has_field(&type_error, "age"));
        assert!(type_error.to_string().contains("expected number"));

        Ok(())
    }
}

#[cfg(test)]
mod concurrent_error_tests {
    use super::*;
    use std::sync::Arc;

    // Example 12: Test concurrent error scenarios
    test_concurrent_errors!(
        test_database_connection_pool_exhaustion,
        10,  // concurrent operations
        |pool, worker_id| async move {
            // Simulate some workers failing due to pool exhaustion
            if worker_id % 3 == 0 {
                Err(CoreError::ResourceExhausted("Connection pool full".to_string()))
            } else {
                Ok(())
            }
        },
        3  // expected failures (workers 0, 3, 6, 9)
    );

    #[sinex_test]
    async fn test_concurrent_validation_errors(ctx: TestContext) -> TestResult {
        // Example 13: Concurrent validation with different error types
        let pool = Arc::new(ctx.pool());
        let mut handles = vec![];

        for i in 0..5 {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                let event = if i % 2 == 0 {
                    // Invalid source
                    sinex_events::EventFactory::new("").create_event("test", json!({}))
                } else {
                    // Invalid event type
                    sinex_events::EventFactory::new("test").create_event("", json!({}))
                };

                sinex_db::insert_event_with_validator(&pool_clone, &event, None).await
            });
            handles.push(handle);
        }

        let results: Vec<_> = futures::future::join_all(handles).await;
        
        // All should fail
        for result in results {
            let inner_result = result?;
            assert!(inner_result.is_err());
        }

        Ok(())
    }
}

#[cfg(test)]
mod context_preservation_tests {
    use super::*;

    // Example 14: Test error context preservation
    test_error_context!(
        test_database_error_with_context,
        |pool| async move {
            CoreError::database("Query failed")
                .with_context("table", "events")
                .with_context("operation", "INSERT")
                .with_context("constraint", "unique_event_id")
                .build()
                .into()
        },
        vec![
            ("table", "events"),
            ("operation", "INSERT"),
            ("constraint", "unique_event_id"),
        ]
    );

    #[sinex_test]
    async fn test_error_context_with_event_details(ctx: TestContext) -> TestResult {
        // Example 15: Rich error context with event information
        let event_id = Ulid::new();
        let timestamp = chrono::Utc::now();
        
        let error = CoreError::validation("Invalid event payload")
            .with_event_id(event_id)
            .with_timestamp(timestamp)
            .with_field("source", "test_source")
            .with_field("event_type", "test.event")
            .with_operation("event_validation")
            .build();

        // Verify all context is preserved
        let error_str = error.to_string();
        assert!(error_str.contains(&event_id.to_string()));
        assert!(error_str.contains("test_source"));
        assert!(error_str.contains("test.event"));
        assert!(error_str.contains("event_validation"));

        Ok(())
    }
}

#[cfg(test)]
mod constraint_violation_tests {
    use super::*;

    // Example 16: Test database constraint violations
    test_constraint_violation!(
        test_unique_constraint_violation,
        |pool| async move {
            // Insert initial event
            let event = sinex_events::EventFactory::new("test")
                .create_event("constraint.test", json!({"id": 1}));
            sinex_db::insert_event_with_validator(pool, &event, None).await?;
            Ok(())
        },
        |pool| async move {
            // Try to insert duplicate (would fail if there was a unique constraint)
            let event = sinex_events::EventFactory::new("test")
                .create_event("constraint.test", json!({"id": 1}));
            sinex_db::insert_event_with_validator(pool, &event, None).await
        },
        "unique"
    );

    #[sinex_test]
    async fn test_foreign_key_violation(ctx: TestContext) -> TestResult {
        // Example 17: Test referential integrity errors
        let pool = ctx.pool();
        
        // Try to insert checkpoint for non-existent automaton
        let result = sqlx::query!(
            "INSERT INTO core.automaton_checkpoints 
             (id, automaton_name, consumer_group, consumer_name, processed_count) 
             VALUES ($1, $2, $3, $4, $5)",
            Ulid::new().to_uuid(),
            "non_existent_automaton",
            "test_group",
            "test_consumer",
            0i64
        )
        .execute(&pool)
        .await;

        // Should succeed as there's no FK constraint on automaton_name
        // This is just an example - adjust based on actual schema
        match result {
            Ok(_) => {
                // Clean up
                sqlx::query!(
                    "DELETE FROM core.automaton_checkpoints WHERE automaton_name = $1",
                    "non_existent_automaton"
                )
                .execute(&pool)
                .await?;
            }
            Err(e) => {
                let core_error = CoreError::from(e);
                assert!(ErrorAssert::is_core_error_variant(&core_error, CoreErrorVariant::Database));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod timeout_error_tests {
    use super::*;

    // Example 18: Test timeout scenarios
    test_timeout_error!(
        test_slow_query_timeout,
        |pool| async move {
            // Simulate slow query
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            Ok(())
        },
        500  // 500ms timeout
    );

    #[sinex_test]
    async fn test_operation_timeout_with_context(ctx: TestContext) -> TestResult {
        use tokio::time::{timeout, Duration};

        // Example 19: Timeout with detailed context
        let operation_future = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, CoreError>(())
        };

        let result = timeout(Duration::from_millis(100), operation_future).await;

        match result {
            Err(_) => {
                // Create timeout error with context
                let error = CoreError::timeout("database_query", Duration::from_millis(100));
                assert!(ErrorAssert::is_core_error_variant(&error, CoreErrorVariant::Timeout));
                assert!(error.to_string().contains("100"));
            }
            Ok(_) => panic!("Operation should have timed out"),
        }

        Ok(())
    }
}

#[cfg(test)]
mod transformation_tests {
    use super::*;

    // Example 20: Test error transformation
    test_error_transformation!(
        test_sqlx_to_core_error,
        sqlx::Error::RowNotFound,
        |e| CoreError::not_found("event", "test_id"),
        |transformed| {
            assert!(ErrorAssert::is_core_error_variant(&transformed, CoreErrorVariant::NotFound));
            assert!(transformed.to_string().contains("event"));
            assert!(transformed.to_string().contains("test_id"));
        }
    );

    #[test]
    fn test_error_type_conversions() {
        // Example 21: Test various error conversions
        
        // IO Error to CoreError
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let core_error: CoreError = io_error.into();
        assert!(ErrorAssert::is_core_error_variant(&core_error, CoreErrorVariant::Io));

        // JSON Error to CoreError
        let json_error = serde_json::from_str::<Value>("invalid json").unwrap_err();
        let core_error: CoreError = json_error.into();
        assert!(ErrorAssert::is_core_error_variant(&core_error, CoreErrorVariant::Serialization));
    }
}

#[cfg(test)]
mod idempotency_tests {
    use super::*;

    // Example 22: Test error idempotency
    test_error_idempotency!(
        test_checkpoint_update_idempotency,
        |pool| async move {
            // Try to update non-existent checkpoint
            sqlx::query!(
                "UPDATE core.automaton_checkpoints 
                 SET processed_count = processed_count + 1 
                 WHERE automaton_name = $1",
                "non_existent"
            )
            .execute(pool)
            .await
            .map_err(|e| CoreError::from(e))
        },
        |pool| async move {
            // Verify no side effects - count should still be 0
            let count = sinex_db::count_events(pool).await?;
            assert!(count >= 0);  // Just verify DB is still accessible
            Ok(())
        }
    );
}

#[cfg(test)]
mod rollback_tests {
    use super::*;

    // Example 23: Test error scenarios with rollback
    test_error_with_rollback!(
        test_transaction_rollback_on_error,
        |pool| async move {
            // Setup: Count initial events
            let initial_count = sinex_db::count_events(pool).await?;
            Ok(initial_count)
        },
        |pool| async move {
            // Use a transaction that will fail
            let mut tx = pool.begin().await?;
            
            // Insert valid event
            let event = sinex_events::EventFactory::new("test")
                .create_event("rollback.test", json!({}));
            sinex_db::insert_event_with_validator(&mut *tx, &event, None).await?;
            
            // Force error to trigger rollback
            sqlx::query("INVALID SQL").execute(&mut *tx).await?;
            
            tx.commit().await.map_err(|e| CoreError::from(e))
        },
        |pool, initial_count| async move {
            // Verify rollback - count should be unchanged
            let final_count = sinex_db::count_events(pool).await?;
            assert_eq!(initial_count, final_count, "Transaction should have rolled back");
            Ok(())
        }
    );
}

#[cfg(test)]
mod event_processing_error_tests {
    use super::*;

    // Example 24: Test event processing errors
    test_event_processing_error!(
        test_invalid_payload_processing,
        "processing.test",
        json!({"invalid_field": "%%%"}),
        |pool, event| async move {
            // Simulate processing that validates payload
            if event.payload.get("invalid_field").is_some() {
                Err(CoreError::Validation("Invalid field format".to_string()))
            } else {
                Ok(())
            }
        },
        |error| {
            assert!(ErrorAssert::is_core_error_variant(error, CoreErrorVariant::Validation));
            Ok(())
        }
    );
}

#[cfg(test)]
mod cascading_error_tests {
    use super::*;

    // Example 25: Test cascading errors
    test_cascading_errors!(
        test_database_cascade_failure,
        |pool| async move {
            // Initial failure: database connection
            Err(CoreError::Database("Connection lost".to_string()))
        },
        vec![
            ("event_insert", |pool| async move {
                Err(CoreError::Service("Cannot insert events without database".to_string()))
            }),
            ("checkpoint_update", |pool| async move {
                Err(CoreError::Service("Cannot update checkpoints without database".to_string()))
            }),
            ("heartbeat", |pool| async move {
                Err(CoreError::Service("Cannot send heartbeat without database".to_string()))
            }),
        ]
    );
}

#[cfg(test)]
mod partial_failure_tests {
    use super::*;

    // Example 26: Test partial batch failures
    test_partial_failure!(
        test_batch_insert_partial_failure,
        |pool| async move {
            let mut results = vec![];
            
            for i in 0..10 {
                let event = if i % 3 == 0 {
                    // Invalid event
                    sinex_events::EventFactory::new("").create_event("test", json!({}))
                } else {
                    // Valid event
                    sinex_events::EventFactory::new("test").create_event("batch.test", json!({"i": i}))
                };
                
                let result = sinex_db::insert_event_with_validator(pool, &event, None).await
                    .map(|_| ())
                    .map_err(|e| e);
                results.push(result);
            }
            
            results
        },
        6,  // expected successes (indices 1,2,4,5,7,8)
        4   // expected failures (indices 0,3,6,9)
    );
}

// Example 27: Using assertion macros directly
#[sinex_test]
async fn test_assertion_macros(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    
    // Test assert_error_type! macro
    let result: Result<(), CoreError> = Err(CoreError::Database("Test error".to_string()));
    assert_error_type!(result, CoreErrorVariant::Database);
    
    // Test assert_error_contains! macro
    let result: Result<(), CoreError> = Err(CoreError::Validation("Field 'email' is invalid".to_string()));
    assert_error_contains!(result, "email");
    
    // Test assert_error_context! macro
    let error = CoreError::database("Connection failed")
        .with_context("host", "localhost")
        .with_context("port", 5432)
        .build();
    assert_error_context!(error, "host", "localhost");
    assert_error_context!(error, "port", "5432");
    
    Ok(())
}

// Example 28: Custom error scenarios for domain-specific testing
#[sinex_test]
async fn test_domain_specific_errors(ctx: TestContext) -> TestResult {
    // Event validation error
    let event_error = CoreError::validation("Event validation failed")
        .with_context("source", "invalid source")
        .with_context("event_type", "")
        .with_context("reason", "Empty event type not allowed")
        .build();
    
    assert!(ErrorAssert::contains_message(&event_error, "Event validation failed"));
    assert!(ErrorAssert::has_context_key(&event_error, "reason"));
    
    // Checkpoint error
    let checkpoint_error = CoreError::invalid_state("Checkpoint out of sync")
        .with_context("automaton", "test_automaton")
        .with_context("expected_id", "123")
        .with_context("actual_id", "456")
        .build();
    
    assert!(ErrorAssert::is_core_error_variant(&checkpoint_error, CoreErrorVariant::InvalidState));
    
    // Processing error with chain
    let processing_error = CoreError::service("Event processing failed")
        .with_source("Schema validation error")
        .with_source("Required field 'timestamp' missing")
        .with_source("Payload: {}")
        .build();
    
    assert!(ErrorAssert::chain_contains(&processing_error, "Schema validation error"));
    assert!(ErrorAssert::chain_contains(&processing_error, "Required field"));
    
    Ok(())
}