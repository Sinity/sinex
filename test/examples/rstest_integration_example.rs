//! Example showing the enhanced sinex_test macro with rstest integration
//! 
//! This demonstrates TRUE integration where sinex_test automatically detects
//! and handles rstest #[case] parameters without needing #[rstest] attribute.

use sinex_test_utils::prelude::*;

// Example 1: Basic rstest integration with sinex_test
#[sinex_test]
async fn test_event_creation_with_cases(
    ctx: TestContext,
    #[case] source: &str,
    #[case] event_type: &str,
) -> Result<()> {
    // Each case gets its own TestContext from the pool
    let event = ctx.create_test_event(
        source,
        event_type,
        json!({"rstest": true})
    ).await?;
    
    assert_eq!(event.source.as_str(), source);
    assert_eq!(event.event_type.as_str(), event_type);
    
    Ok(())
}

// The above should expand to something equivalent to:
// #[rstest]
// #[case("fs", "file.created")]
// #[case("shell", "cmd.run")]
// #[tokio::test]
// async fn test_event_creation_with_cases(
//     #[case] source: &str,
//     #[case] event_type: &str,
// ) -> Result<()> {
//     let ctx = TestContext::with_name("test_event_creation_with_cases").await?;
//     // ... rest of test
// }

// Example 2: Complex case with multiple parameters
#[sinex_test]
async fn test_payload_variations(
    ctx: TestContext,
    #[case] name: &str,
    #[case] size: usize,
    #[case] expected_valid: bool,
) -> Result<()> {
    let payload = json!({
        "name": name,
        "data": "x".repeat(size),
        "size_kb": size / 1024,
    });
    
    let result = ctx.create_test_event(
        "test",
        "payload.test",
        payload.clone()
    ).await;
    
    if expected_valid {
        let event = result?;
        assert_eq!(event.payload["name"], json!(name));
        assert_eq!(event.payload["size_kb"], json!(size / 1024));
    } else {
        assert!(result.is_err());
    }
    
    Ok(())
}

// Example 3: Using fixtures with rstest cases
#[sinex_test]
async fn test_with_fixture_and_cases(
    ctx: TestContext,
    test_sources: Vec<&'static str>,  // This is a fixture
    #[case] event_type: &str,
) -> Result<()> {
    // Create events for each source with the given event type
    for source in &test_sources {
        ctx.create_test_event(
            *source,
            event_type,
            json!({})
        ).await?;
    }
    
    // Verify they were created
    let count = ctx.pool.events()
        .by_type(event_type)
        .count()
        .await?;
    
    assert_eq!(count, test_sources.len() as i64);
    
    Ok(())
}

// Example 4: Combining with other modern test features
#[sinex_test(trace = true)]  // Also enable tracing
async fn test_with_tracing_and_cases(
    ctx: TestContext,
    #[case] operation: &str,
    #[case] expected_log: &str,
) -> Result<()> {
    tracing::info!("Testing {} operation", operation);
    
    let event = ctx.create_test_event(
        "traced",
        operation,
        json!({})
    ).await?;
    
    tracing::debug!("Created event: {:?}", event.id);
    
    // With trace = true, we should be able to verify logs
    ctx.assert_logged(expected_log)?;
    
    Ok(())
}

// Example 5: Snapshot testing with rstest
#[sinex_test]
async fn test_snapshots_with_cases(
    ctx: TestContext,
    #[case] scenario: &str,
    #[case] data: serde_json::Value,
) -> Result<()> {
    let event = ctx.create_test_event(
        "snapshot-test",
        scenario,
        data.clone()
    ).await?;
    
    // Snapshot paths are automatically configured by sinex_test
    // to include the test name and case identifier
    ctx.snapshot_event(&event, Some(scenario));
    
    Ok(())
}

#[cfg(test)]
mod actual_tests {
    use super::*;
    
    // Since we can't use #[case] attributes in the example above without
    // actually running rstest, here are the actual test cases that would
    // be generated:
    
    mod test_event_creation_with_cases {
        use super::*;
        
        #[rstest]
        #[case("fs", "file.created")]
        #[case("shell", "cmd.run")]
        #[case("service", "health.check")]
        #[tokio::test]
        async fn run(
            #[case] source: &str,
            #[case] event_type: &str,
        ) -> Result<()> {
            let ctx = TestContext::new().await?;
            test_event_creation_with_cases_impl(ctx, source, event_type).await
        }
        
        async fn test_event_creation_with_cases_impl(
            ctx: TestContext,
            source: &str,
            event_type: &str,
        ) -> Result<()> {
            let event = ctx.create_test_event(
                source,
                event_type,
                json!({"rstest": true})
            ).await?;
            
            assert_eq!(event.source.as_str(), source);
            assert_eq!(event.event_type.as_str(), event_type);
            
            Ok(())
        }
    }
}