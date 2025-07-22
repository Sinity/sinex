// Harmonized Test Macros - All work together seamlessly
//
// These macros are designed to compose well with each other and the unified TestContext.
// Each serves a distinct purpose, and they all follow consistent patterns.

/// Main test setup macro with automatic database management
#[macro_export]
macro_rules! sinex_test {
    (async fn $test_name:ident(ctx: TestContext) -> TestResult $body:block) => {
        #[tokio::test]
        async fn $test_name() -> $crate::common::TestResult {
            use $crate::common::prelude::*;
            
            let ctx = TestContext::new().await?;
            let result = (async move $body).await;
            result
        }
    };
    
    // Version with custom config
    (async fn $test_name:ident(ctx: TestContext, config: $config:expr) -> TestResult $body:block) => {
        #[tokio::test]
        async fn $test_name() -> $crate::common::TestResult {
            use $crate::common::prelude::*;
            
            let ctx = TestContext::with_config($config).await?;
            let result = (async move $body).await;
            result
        }
    };
    
    // Version with verbose logging
    (async fn $test_name:ident(ctx: TestContext, verbose) -> TestResult $body:block) => {
        sinex_test! {
            async fn $test_name(ctx: TestContext, config: {
                let mut config = TestConfig::default();
                config.verbose = true;
                config.test_name = stringify!($test_name).to_string();
                config
            }) -> TestResult $body
        }
    };
}

/// Assert two events are equivalent (ignoring generated fields)
#[macro_export]
macro_rules! assert_event_eq {
    ($actual:expr, $expected:expr) => {
        {
            let actual = &$actual;
            let expected = &$expected;
            
            pretty_assertions::assert_eq!(actual.source, expected.source, "Event sources differ");
            pretty_assertions::assert_eq!(actual.event_type, expected.event_type, "Event types differ");  
            pretty_assertions::assert_eq!(actual.payload, expected.payload, "Event payloads differ");
            pretty_assertions::assert_eq!(actual.host, expected.host, "Event hosts differ");
        }
    };
}

/// Assert events match pattern (flexible matching)
#[macro_export]
macro_rules! assert_events_match {
    ($events:expr, [ $( { source: $source:expr, event_type: $event_type:expr $(, payload: $payload:expr)? } ),* ]) => {
        {
            let events = &$events;
            let expected_patterns = vec![
                $( ($source, $event_type $(, $payload)?) ),*
            ];
            
            assert_eq!(events.len(), expected_patterns.len(), 
                "Event count mismatch: expected {}, got {}", 
                expected_patterns.len(), events.len()
            );
            
            for (i, event) in events.iter().enumerate() {
                let pattern = &expected_patterns[i];
                assert_eq!(event.source, pattern.0, "Event {} source mismatch", i);
                assert_eq!(event.event_type, pattern.1, "Event {} type mismatch", i);
                
                // Optional payload matching
                $(
                    if let Some(expected_payload) = pattern.2 {
                        assert_eq!(event.payload, expected_payload, "Event {} payload mismatch", i);
                    }
                )*
            }
        }
    };
}

/// Assert error contains specific text with context
#[macro_export]
macro_rules! assert_error_contains {
    ($result:expr, $text:expr) => {
        match $result {
            Ok(val) => panic!(
                "Expected error containing '{}', but got Ok({:?})", 
                $text, val
            ),
            Err(err) => {
                let err_string = err.to_string();
                assert!(
                    err_string.contains($text),
                    "Error '{}' does not contain '{}'",
                    err_string, $text
                );
            }
        }
    };
    
    ($result:expr, $text:expr, $context:expr) => {
        match $result {
            Ok(val) => panic!(
                "{}: Expected error containing '{}', but got Ok({:?})", 
                $context, $text, val
            ),
            Err(err) => {
                let err_string = err.to_string();
                assert!(
                    err_string.contains($text),
                    "{}: Error '{}' does not contain '{}'",
                    $context, err_string, $text
                );
            }
        }
    };
}

/// Assert error is of specific type
#[macro_export]
macro_rules! assert_error_type {
    ($result:expr, $error_type:ty) => {
        match $result {
            Ok(val) => panic!(
                "Expected error of type {}, but got Ok({:?})", 
                stringify!($error_type), val
            ),
            Err(err) => {
                assert!(
                    err.downcast_ref::<$error_type>().is_some(),
                    "Error is not of type {}: {:?}",
                    stringify!($error_type), err
                );
            }
        }
    };
}

/// Eventually assert - wait for condition with timeout
#[macro_export]
macro_rules! eventually {
    ($condition:expr) => {
        eventually!($condition, 3)
    };
    
    ($condition:expr, $timeout_secs:expr) => {
        {
            use std::time::{Duration, Instant};
            let start = Instant::now();
            let timeout = Duration::from_secs($timeout_secs);
            
            loop {
                if $condition {
                    break;
                }
                
                if start.elapsed() > timeout {
                    panic!("Condition not met within {} seconds", $timeout_secs);
                }
                
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
    
    ($ctx:expr, $condition:expr) => {
        $ctx.wait_for_condition(|| async { Ok($condition) }).await?;
    };
}

/// Eventually with custom message
#[macro_export]
macro_rules! eventually_with_message {
    ($condition:expr, $timeout_secs:expr, $message:expr) => {
        {
            use std::time::{Duration, Instant};
            let start = Instant::now();
            let timeout = Duration::from_secs($timeout_secs);
            
            loop {
                if $condition {
                    break;
                }
                
                if start.elapsed() > timeout {
                    panic!("{} (timeout: {} seconds)", $message, $timeout_secs);
                }
                
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };
}

/// Parameterized test with multiple test cases  
#[macro_export]
macro_rules! parameterized_test {
    (async fn $test_name:ident(ctx: TestContext, $param:ident: $param_type:ty) -> TestResult $body:block, cases: $cases:expr) => {
        #[tokio::test]
        async fn $test_name() -> $crate::common::TestResult {
            use $crate::common::prelude::*;
            
            let ctx = TestContext::new().await?;
            let cases: Vec<(&str, $param_type)> = $cases;
            
            for (case_name, $param) in cases {
                println!("Testing case: {}", case_name);
                
                let result: TestResult = (async $body).await;
                result.with_context(|| format!("Test case '{}' failed", case_name))?;
            }
            
            Ok(())
        }
    };
}

/// Property-based test integration
#[macro_export]
macro_rules! property_test {
    (async fn $test_name:ident(ctx: TestContext, $input:ident: $input_type:ty) -> TestResult $body:block, strategy: $strategy:expr, cases: $cases:expr) => {
        #[tokio::test]
        async fn $test_name() -> $crate::common::TestResult {
            use $crate::common::prelude::*;
            use proptest::prelude::*;
            
            let ctx = TestContext::new().await?;
            let strategy = $strategy;
            
            let mut runner = proptest::test_runner::TestRunner::deterministic();
            
            for _ in 0..$cases {
                let $input = strategy.new_tree(&mut runner)?.current();
                
                let result: TestResult = (async $body).await;
                result.with_context(|| format!("Property test failed with input: {:?}", $input))?;
            }
            
            Ok(())
        }
    };
}

/// Concurrent test execution with proper error handling
#[macro_export] 
macro_rules! concurrent_test {
    ($ctx:expr, $task_count:expr, |$task_id:ident| $body:block) => {
        {
            use std::sync::Arc;
            use tokio::task::JoinSet;
            
            let ctx = Arc::new($ctx);
            let mut join_set = JoinSet::new();
            
            for $task_id in 0..$task_count {
                let ctx_clone = ctx.clone();
                join_set.spawn(async move {
                    let ctx = ctx_clone;
                    let result: TestResult = (async $body).await;
                    result
                });
            }
            
            let mut results = Vec::new();
            let mut errors = Vec::new();
            
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(value)) => results.push(value),
                    Ok(Err(e)) => errors.push(e),
                    Err(join_err) => errors.push(join_err.into()),
                }
            }
            
            if !errors.is_empty() {
                return Err(anyhow::anyhow!(
                    "Concurrent test had {} failures: {:?}", 
                    errors.len(), errors
                ));
            }
            
            results
        }
    };
}

/// Time measurement for performance tests
#[macro_export]
macro_rules! measure_time {
    ($operation:expr) => {
        {
            let start = std::time::Instant::now();
            let result = $operation;
            let duration = start.elapsed();
            (result, duration)
        }
    };
    
    (async $operation:expr) => {
        {
            let start = std::time::Instant::now();
            let result = $operation.await;
            let duration = start.elapsed();
            (result, duration)
        }
    };
}

/// Assert performance within limits
#[macro_export] 
macro_rules! assert_performance {
    ($operation:expr, max_duration: $max:expr) => {
        {
            let (result, duration) = measure_time!($operation);
            assert!(
                duration <= $max,
                "Operation took {:?}, expected <= {:?}",
                duration, $max
            );
            result
        }
    };
    
    (async $operation:expr, max_duration: $max:expr) => {
        {
            let (result, duration) = measure_time!(async $operation);
            assert!(
                duration <= $max,
                "Operation took {:?}, expected <= {:?}",
                duration, $max
            );
            result
        }
    };
}

/// Test with setup and cleanup
#[macro_export]
macro_rules! test_with_setup {
    ($ctx:expr, setup: $setup:block, test: $test:block, cleanup: $cleanup:block) => {
        {
            // Setup phase
            let setup_result = (async $setup).await?;
            
            // Test phase (always runs)
            let test_result = (async $test).await;
            
            // Cleanup phase (always runs, even if test failed)
            let cleanup_result = (async $cleanup).await;
            
            // Check cleanup succeeded
            cleanup_result?;
            
            // Return test result
            test_result
        }
    };
}

/// Comprehensive event creation and verification
#[macro_export]
macro_rules! test_event_flow {
    ($ctx:expr, $builder:expr, $verifier:expr) => {
        {
            // Create and insert event
            let event = $builder.insert().await?;
            
            // Verify event was created correctly  
            $ctx.assert_event_exists(event.id).await?;
            
            // Run custom verification
            $verifier(&$ctx, &event).await?;
            
            event
        }
    };
}

/// Error validation testing with context
#[macro_export]
macro_rules! test_validation_error {
    ($operation:expr, expected_error: $error_pattern:expr) => {
        {
            let result = $operation;
            assert_error_contains!(result, $error_pattern);
        }
    };
    
    ($operation:expr, expected_error: $error_pattern:expr, context: $context:expr) => {
        {
            let result = $operation;
            assert_error_contains!(result, $error_pattern, $context);
        }
    };
}

/// Test scenario patterns - useful for complex multi-step tests
#[macro_export]
macro_rules! test_scenario {
    ($ctx:expr, $( $step_name:ident: $step:block ),+ $(,)?) => {
        {
            $(
                if $ctx.config().verbose {
                    println!("Step: {}", stringify!($step_name));
                }
                
                let step_result: TestResult = (async $step).await;
                step_result.with_context(|| format!("Step {} failed", stringify!($step_name)))?;
            )+
        }
    };
}

/// Batch operations with verification
#[macro_export]
macro_rules! test_batch_operation {
    ($ctx:expr, count: $count:expr, operation: |$index:ident| $op:block, verify: |$results:ident| $verify:block) => {
        {
            let mut $results = Vec::new();
            
            // Execute batch
            for $index in 0..$count {
                let result = (async $op).await?;
                $results.push(result);
            }
            
            // Verify batch
            (async $verify).await?;
            
            $results
        }
    };
}

/// Redis stream testing patterns
#[macro_export]
macro_rules! test_redis_stream {
    ($ctx:expr, stream: $stream_key:expr, group: $group:expr, consumer: $consumer:expr, $body:block) => {
        {
            use redis::{AsyncCommands, cmd};
            
            let mut redis = $ctx.redis().await?;
            
            // Setup stream and group
            let _: Result<(), redis::RedisError> = cmd("XGROUP")
                .arg("CREATE")
                .arg($stream_key)
                .arg($group)
                .arg("0")
                .arg("MKSTREAM")
                .query_async(&mut redis)
                .await;
            
            // Test body
            let result = (async $body).await;
            
            // Cleanup
            let _: Result<i64, redis::RedisError> = redis.del($stream_key).await;
            
            result
        }
    };
}

/// Schema validation testing
#[macro_export]
macro_rules! test_schema_validation {
    ($ctx:expr, valid: $valid_payload:expr, invalid: $invalid_payload:expr, schema: $schema:expr) => {
        {
            // Test valid payload
            let valid_result = $ctx.event()
                .source("test")
                .type_("schema.test")
                .payload($valid_payload)
                .build();
            assert!(valid_result.is_ok(), "Valid payload should pass: {:?}", valid_result.err());
            
            // Test invalid payload  
            let invalid_result = $ctx.event()
                .source("test")
                .type_("schema.test")
                .payload($invalid_payload)
                .build();
            // Note: Schema validation happens at insert time, not build time
            
            (valid_result?, invalid_result)
        }
    };
}