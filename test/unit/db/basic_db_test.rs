use crate::common::prelude::*;

#[sinex_test]
async fn test_basic_event_insertion(ctx: TestContext) -> TestResult {
    // Create a simple test event using enhanced event builder
    let event = EventBuilder::filesystem()
        .path("/test/simple_file.txt")
        .created()
        .size(1024)
        .build();
    
    // Insert using enhanced assertion with error context
    let event_id = assert_event_inserted_with_context(
        ctx.pool(), 
        &event, 
        "basic_event_insertion_test"
    ).await?;
    
    // Retrieve the inserted event
    let inserted_event = queries::get_event_by_id(ctx.pool(), event_id).await
        .map_err(|e| {
            CoreError::database("Failed to retrieve inserted event")
                .with_event_id(event_id)
                .with_context("test_name", "basic_event_insertion")
                .with_source(e)
                .build()
        })?;
    
    // Verify using enhanced event equivalence assertion
    assert_events_equivalent(&inserted_event, &event)?;
    
    // Use ValidationChain to validate the event structure
    let event_validation = assert_with_validation(inserted_event.clone(), "inserted_event")
        .has_valid_source()
        .has_valid_event_type()
        .payload_is_object();
    
    assert_validation_passes(event_validation)?;
    
    // Validate specific payload fields using ValidationChain
    let path_validation = assert_with_validation(
        inserted_event.payload["path"].as_str().unwrap_or("").to_string(),
        "event_path"
    )
    .not_empty()
    .custom(|path| path.starts_with("/test/"), "should be in test directory");
    
    assert_validation_passes(path_validation)?;
    
    Ok(())
}

#[sinex_test]
async fn test_event_validation_creation(_ctx: TestContext) -> TestResult {
    // Test that EventValidator can be created
    let _validator = sinex_db::validation::EventValidator::new();
    // If this compiles and runs, the basic validation infrastructure works
    Ok(())
}

#[sinex_test]
async fn test_database_connection(ctx: TestContext) -> TestResult {
    // Simple test to verify database connectivity
    let result: i32 = sqlx::query_scalar("SELECT 1 as test_value")
        .fetch_one(ctx.pool())
        .await?;
    
    // Verify we can execute queries
    assert!(result == 1);
    
    Ok(())
}