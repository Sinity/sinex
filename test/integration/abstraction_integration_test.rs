//! Integration test demonstrating the usage of all new abstractions
//!
//! This test file showcases how to use ValidationChain, ErrorContext, ChannelSenderExt,
//! ConfigExtractor, and enhanced test infrastructure together in a realistic test scenario.

use crate::common::prelude::*;

/// Comprehensive test demonstrating all new abstractions working together
#[sinex_test]
async fn test_comprehensive_abstraction_integration(ctx: TestContext) -> TestResult {
    println!("🚀 Starting comprehensive abstraction integration test");

    // 1. Configuration Testing with ConfigExtractor and ValidationChain
    let test_config = test_configs::valid_database_config();

    // Validate configuration using ConfigValidator with ValidationChain integration
    let config_validator = config_validation::validate_complete_config();
    assert_config_valid(&test_config, config_validator, "integration_test_config")?;

    // Extract configuration using ConfigExtractor
    let db_config = assert_config_extraction(
        config_extraction::extract_database_config(&test_config),
        "database section"
    )?;

    println!("✓ Configuration validation and extraction completed");

    // 2. Enhanced Assertions with ErrorContext
    let test_event = RawEventBuilder::new(
        "integration_test",
        "comprehensive.test",
        json!({
            "test_phase": "abstraction_integration",
            "abstractions": ["ValidationChain", "ErrorContext", "ChannelSenderExt", "ConfigExtractor"],
            "db_url": db_config.url,
        })
    ).build();

    // Insert event with enhanced error context
    let event_id = assert_event_inserted_with_context(
        ctx.pool(),
        &test_event,
        "comprehensive_integration_test"
    ).await?;

    println!("✓ Event inserted with ID: {}", event_id);

    // 3. ValidationChain Testing with Event Validation
    let validation_result = assert_with_validation(test_event.source.clone(), "event_source")
        .not_empty()
        .min_length(5)
        .custom(|s| s.contains("integration"), "must contain 'integration'");

    assert_validation_passes(validation_result)?;

    // Test JSON payload validation using ValidationChain
    let payload_validation = assert_with_validation(test_event.payload.clone(), "event_payload")
        .has_field("test_phase")
        .field_type("abstractions", JsonType::Array)
        .max_depth(5);

    assert_validation_passes(payload_validation)?;

    println!("✓ ValidationChain assertions completed");

    // 4. Channel Testing with ChannelSenderExt and Enhanced Assertions
    let test_channel_setup = TestChannelSetup::new(10);

    // Test basic channel operations
    let test_messages = vec![
        "abstraction_test_1",
        "abstraction_test_2",
        "abstraction_test_3"
    ];

    // Run comprehensive channel test scenario
    channel_scenarios::run_comprehensive_channel_test(
        "abstraction_integration_channels",
        test_messages.clone(),
        10
    ).await?;

    // Test channel timeout behavior with enhanced assertions
    assert_channel_send_timeout(
        &test_channel_setup.sender,
        "timeout_test".to_string(),
        Duration::from_millis(100),
        false // Should not timeout with this buffer size
    ).await?;

    println!("✓ Channel operations testing completed");

    // 5. Multi-Assertion Batch Testing (MultiValidator pattern)
    let mut assertion_batch = TestAssertionBatch::new("abstraction_integration_batch");

    assertion_batch
        .assert_that(|| {
            assert_eq_with_context(&event_id.to_string().len(), &26, "ULID length check")
        }, "ULID format validation")
        .assert_that(|| {
            assert_with_context(
                test_event.payload["abstractions"].is_array(),
                "Payload should contain abstractions array",
                "payload structure check"
            )
        }, "payload structure validation")
        .assert_validation(
            ValidationChain::validate(db_config.pool_size, "pool_size")
                .min(1)
                .max(100),
            "database pool size validation"
        );

    assertion_batch.execute()?;

    println!("✓ Multi-assertion batch completed");

    // 6. Database State Validation with Enhanced Context
    let event_count = assert_database_state(
        ctx.pool(),
        async {
            sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events WHERE source = $1", "integration_test")
                .fetch_one(ctx.pool())
                .await
        },
        "count integration test events"
    ).await?;

    assert_with_context(
        event_count.unwrap_or(0) >= 1,
        "Should have at least one integration test event",
        "database state verification"
    )?;

    println!("✓ Database state validation completed");

    // 7. Error Context Demonstration
    let complex_operation_result = assert_completes_within(
        async {
            // Simulate complex operation with multiple steps
            tokio::time::sleep(Duration::from_millis(10)).await;

            // Use ValidationChain to validate complex data
            let data_validation = ValidationChain::validate(
                json!({
                    "config": db_config,
                    "event_id": event_id.to_string(),
                    "test_results": "all_passed"
                }),
                "integration_test_results"
            )
            .has_field("config")
            .has_field("event_id")
            .has_field("test_results");

            if !data_validation.is_valid() {
                let errors: Vec<String> = data_validation.errors()
                    .iter()
                    .map(|e| e.to_string())
                    .collect();

                return Err(Box::new(
                    CoreError::validation("Integration test data validation failed")
                        .with_context("validation_errors", errors.join("; "))
                        .with_event_id(event_id)
                        .build()
                ) as Box<dyn std::error::Error>);
            }

            Ok("Complex operation completed successfully")
        },
        Duration::from_secs(1),
        "complex_integration_operation"
    ).await?;

    println!("✓ Complex operation completed: {}", complex_operation_result);

    // 8. Final Verification using Event Equivalence
    let retrieved_event = sinex_db::events_correct::get_event_by_id(ctx.pool(), event_id).await
        .map_err(|e| {
            CoreError::database("Failed to retrieve inserted event")
                .with_event_id(event_id)
                .with_source(e)
                .build()
        })?;

    assert_events_equivalent(&retrieved_event, &test_event)?;

    println!("✅ Comprehensive abstraction integration test completed successfully!");
    println!("🎯 All abstractions working together harmoniously:");
    println!("   • ValidationChain: ✅ Fluent validation with error accumulation");
    println!("   • ErrorContext: ✅ Rich error context with chaining");
    println!("   • ChannelSenderExt: ✅ Enhanced channel operations");
    println!("   • ConfigExtractor: ✅ Type-safe configuration access");
    println!("   • Enhanced Assertions: ✅ Context-aware test failures");

    Ok(())
}

/// Test demonstrating ValidationChain usage in different scenarios
#[sinex_test]
async fn test_validation_chain_scenarios(ctx: TestContext) -> TestResult {
    println!("🧪 Testing ValidationChain in various scenarios");

    // String validation scenarios
    let valid_string_chain = ValidationChain::validate("test_value_123".to_string(), "test_string")
        .not_empty()
        .min_length(5)
        .max_length(50)
        .custom(|s| s.contains("test"), "must contain 'test'");

    assert_validation_passes(valid_string_chain)?;

    let invalid_string_chain = ValidationChain::validate("".to_string(), "empty_string")
        .not_empty()
        .min_length(1);

    assert_validation_fails(invalid_string_chain, "cannot be empty")?;

    // Numeric validation scenarios
    let valid_number_chain = ValidationChain::validate(42i64, "test_number")
        .min(1)
        .max(100)
        .range(10..50);

    assert_validation_passes(valid_number_chain)?;

    // JSON validation scenarios
    let test_json = json!({
        "required_field": "present",
        "number_field": 123,
        "nested": {
            "deep_field": "value"
        }
    });

    let json_validation = ValidationChain::validate(test_json, "test_json")
        .has_field("required_field")
        .field_type("number_field", JsonType::Number)
        .max_depth(3)
        .max_size(1000);

    assert_validation_passes(json_validation)?;

    println!("✅ ValidationChain scenarios completed");
    Ok(())
}

/// Test demonstrating ErrorContext usage for enhanced error reporting
#[sinex_test]
async fn test_error_context_scenarios(ctx: TestContext) -> TestResult {
    println!("🔍 Testing ErrorContext for enhanced error reporting");

    // Simulate a database operation that fails with rich context
    let failing_operation = async {
        // Create an event with invalid data to trigger failure
        let invalid_event = RawEvent {
            id: Ulid::new(),
            source: "".to_string(), // Invalid: empty source
            event_type: "test.error_context".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test_host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({"test": true}),
        };

        // Try to insert invalid event (should fail)
        match queries::insert_event(ctx.pool(), &invalid_event).await {
            Ok(_) => {
                // This should not happen
                Err(Box::new(
                    CoreError::validation("Expected insertion to fail but it succeeded")
                        .with_event_id(invalid_event.id)
                        .build()
                ) as Box<dyn std::error::Error>)
            }
            Err(e) => {
                // Transform the error with rich context
                let enhanced_error = CoreError::database("Event insertion failed as expected")
                    .with_event_id(invalid_event.id)
                    .with_context("source", &invalid_event.source)
                    .with_context("event_type", &invalid_event.event_type)
                    .with_operation("test_error_context_scenarios")
                    .with_source(e)
                    .build();

                println!("✓ Rich error context created: {}", enhanced_error);
                Ok("Error handling test completed")
            }
        }
    };

    let result = failing_operation.await?;
    println!("✅ ErrorContext scenarios completed: {}", result);
    Ok(())
}

/// Test demonstrating channel abstractions in realistic scenarios
#[sinex_test]
async fn test_channel_abstraction_scenarios(_ctx: TestContext) -> TestResult {
    println!("📡 Testing channel abstractions in realistic scenarios");

    // Event streaming scenario
    let event_channel = TestChannelSetup::new(100);
    let test_events = vec![
        events::fs_event("/test/file1.txt"),
        events::term_event("ls -la"),
        events::clip_event("test clipboard content"),
        events::window_event("Test Window"),
    ];

    // Test event streaming with monitoring
    channel_monitoring::test_channel_monitoring(
        &event_channel.sender,
        &event_channel.monitor,
        test_events.clone(),
    ).await?;

    // Test backpressure scenario
    channel_scenarios::run_backpressure_test_scenario(
        "event_stream_backpressure",
        test_events,
    ).await?;

    // Test performance scenario
    let (perf_tx, perf_rx) = tokio::sync::mpsc::channel::<String>(1000);
    let performance_report = channel_performance::measure_channel_throughput(
        perf_tx,
        perf_rx,
        10000,
        "performance_test_item".to_string(),
    ).await.map_err(|e| {
        CoreError::other("Performance measurement failed")
            .with_source(e)
            .build()
    })?;

    performance_report.print_summary();

    // Validate performance meets expectations
    assert_with_context(
        performance_report.send_rate > 1000.0,
        "Channel should achieve at least 1000 ops/sec",
        &format!("actual rate: {:.2}", performance_report.send_rate)
    )?;

    println!("✅ Channel abstraction scenarios completed");
    Ok(())
}

/// Test demonstrating configuration abstractions in realistic scenarios
#[sinex_test]
async fn test_config_abstraction_scenarios(_ctx: TestContext) -> TestResult {
    println!("⚙️ Testing configuration abstractions in realistic scenarios");

    // Test all configuration validation scenarios
    for scenario in config_scenarios::all_validation_scenarios() {
        let validator = config_validation::validate_complete_config();
        let result = validator(&scenario.config);

        match (result.is_ok(), scenario.should_validate) {
            (true, true) => {
                println!("✓ {} passed validation as expected", scenario.name);

                // For valid configs, test extraction
                if scenario.name.contains("valid") {
                    let _db_config = assert_config_extraction(
                        config_extraction::extract_database_config(&scenario.config),
                        &format!("{} database extraction", scenario.name)
                    )?;

                    let _collector_config = assert_config_extraction(
                        config_extraction::extract_collector_config(&scenario.config),
                        &format!("{} collector extraction", scenario.name)
                    )?;
                }
            }
            (false, false) => {
                if let Some(ref expected_substring) = scenario.expected_error_substring {
                    let error_msg = result.unwrap_err().to_string();
                    assert_with_context(
                        error_msg.contains(expected_substring),
                        "Error should contain expected substring",
                        &format!("expected: '{}', actual: '{}'", expected_substring, error_msg)
                    )?;
                }
                println!("✓ {} failed validation as expected", scenario.name);
            }
            (true, false) => {
                return Err(Box::new(
                    CoreError::validation("Configuration scenario validation mismatch")
                        .with_context("scenario", &scenario.name)
                        .with_context("expected", "should fail")
                        .with_context("actual", "passed")
                        .build()
                ));
            }
            (false, true) => {
                return Err(Box::new(
                    CoreError::validation("Configuration scenario validation mismatch")
                        .with_context("scenario", &scenario.name)
                        .with_context("expected", "should pass")
                        .with_context("actual", "failed")
                        .with_context("error", result.unwrap_err().to_string())
                        .build()
                ));
            }
        }
    }

    // Test configuration factory
    let randomized_config = TestConfigFactory::create_randomized_config(12345);
    let validator = config_validation::validate_complete_config();
    assert_config_valid(&randomized_config, validator, "randomized_factory_config")?;

    println!("✅ Configuration abstraction scenarios completed");
    Ok(())
}
