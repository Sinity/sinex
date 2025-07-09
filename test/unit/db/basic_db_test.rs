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
    let event_id =
        assert_event_inserted_with_context(ctx.pool(), &event, "basic_event_insertion_test")
            .await?;

    // Retrieve the inserted event
    let inserted_event = crate::common::get_event_by_id(ctx.pool(), event_id)
        .await
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
        inserted_event.payload["path"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        "event_path",
    )
    .not_empty()
    .custom(
        |path| path.starts_with("/test/"),
        "should be in test directory",
    );

    assert_validation_passes(path_validation)?;

    Ok(())
}

#[sinex_test]
async fn test_event_validation_creation(_ctx: TestContext) -> TestResult {
    // Test that EventValidator can be created and used with ValidationChain
    let validator = sinex_db::validation::EventValidator::new();

    // Create test events for validation
    let valid_event = EventBuilder::terminal()
        .command("echo test")
        .success()
        .build();

    let invalid_event = RawEvent {
        id: Ulid::new(),
        source: "".to_string(), // Invalid: empty source
        event_type: "test.invalid".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test_host".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: json!({}),
    };

    // Use ValidationChain to test the events
    let valid_result = validator.validate(&valid_event);
    let invalid_result = validator.validate(&invalid_event);

    // Use enhanced assertions with context
    assert_with_context(
        valid_result.is_ok(),
        "Valid event should pass validation",
        "event_validator_creation_test",
    )?;

    assert_with_context(
        invalid_result.is_err(),
        "Invalid event should fail validation",
        "event_validator_creation_test",
    )?;

    Ok(())
}

#[sinex_test]
async fn test_database_connection(ctx: TestContext) -> TestResult {
    // Test database connectivity with enhanced error context
    let result: i32 = assert_database_state(
        ctx.pool(),
        async {
            sqlx::query_scalar!("SELECT 1 as test_value")
                .fetch_one(ctx.pool())
                .await
                .map(|opt| opt.unwrap_or(0))
        },
        "basic database connectivity test",
    )
    .await?;

    // Use ValidationChain to validate the result
    let result_validation =
        assert_with_validation(result, "db_test_result").custom(|&val| val == 1, "should equal 1");

    assert_validation_passes(result_validation)?;

    Ok(())
}

/// Test demonstrating multi-assertion batch pattern
#[sinex_test]
async fn test_multi_assertion_batch(ctx: TestContext) -> TestResult {
    // Create multiple test events
    let events = vec![
        EventBuilder::filesystem()
            .path("/test/file1.txt")
            .created()
            .build(),
        EventBuilder::terminal().command("ls").success().build(),
        EventBuilder::clipboard().text("test clipboard").build(),
    ];

    let mut assertion_batch = TestAssertionBatch::new("multi_event_insertion_test");

    // Insert all events and collect results
    let mut event_ids = Vec::new();
    for (i, event) in events.iter().enumerate() {
        let event_id =
            assert_event_inserted_with_context(ctx.pool(), event, &format!("multi_event_{}", i))
                .await?;
        event_ids.push(event_id);
    }

    // Use batch assertions to validate all events
    for (i, (event, event_id)) in events.iter().zip(event_ids.iter()).enumerate() {
        assertion_batch.assert_that(
            || {
                assert_with_context(
                    event_id.to_string().len() == 26,
                    "ULID should be 26 characters",
                    &format!("event_{}_ulid_check", i),
                )
            },
            &format!("event {} ULID validation", i),
        );

        assertion_batch.assert_validation(
            ValidationChain::validate(event.source.clone(), &format!("event_{}_source", i))
                .not_empty(),
            &format!("event {} source validation", i),
        );
    }

    // Execute all batched assertions
    assertion_batch.execute()?;

    Ok(())
}
