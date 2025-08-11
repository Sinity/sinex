use sinex_core::db::pool::DbPool;
use sinex_test_utils::test_context::TestContext;
use sqlx::Row;

#[tokio::test]
async fn test_validation_cache_uses_payload_hash() -> anyhow::Result<()> {
    let ctx = TestContext::new().await?;
    let pool = &ctx.pool;

    // Create a test schema
    let schema_id = sqlx::query_scalar!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (id, schema_name, schema_version, schema_content)
        VALUES (gen_ulid(), 'test_validation_cache', '1.0.0', 
                '{"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}')
        RETURNING id
        "#
    )
    .fetch_one(pool.inner())
    .await?;

    // Create a test event with valid payload
    let test_payload = r#"{"message": "test validation cache"}"#;
    let event_id = sqlx::query_scalar!(
        r#"
        INSERT INTO core.events (
            id, event_type, source, host, payload, payload_schema_id, source_event_ids
        ) VALUES (
            gen_ulid(), 'test.validation_cache', 'test', 'test-host', $1::jsonb, $2,
            ARRAY[gen_ulid()]::ULID[]  -- Use internal provenance for XOR constraint
        )
        RETURNING id
        "#,
        serde_json::from_str::<serde_json::Value>(test_payload)?,
        schema_id
    )
    .fetch_one(pool.inner())
    .await?;

    // Call the validation function
    let validation_result = sqlx::query!(
        "SELECT * FROM sinex_schemas.validate_event_payload($1)",
        event_id
    )
    .fetch_one(pool.inner())
    .await?;

    assert!(validation_result.is_valid.unwrap_or(false), "Event should be valid");

    // Verify cache entry was created with payload_hash
    let cache_entry = sqlx::query!(
        "SELECT payload_hash, schema_id, is_valid FROM sinex_schemas.validation_cache WHERE schema_id = $1",
        schema_id
    )
    .fetch_one(pool.inner())
    .await?;

    // Verify payload hash is correct length (SHA256 = 64 hex characters)
    assert_eq!(cache_entry.payload_hash.len(), 64, "Payload hash should be 64 characters");

    // Verify the hash matches what we expect
    let expected_hash = sqlx::query_scalar!(
        "SELECT encode(digest($1, 'sha256'), 'hex')",
        test_payload
    )
    .fetch_one(pool.inner())
    .await?
    .unwrap();

    assert_eq!(cache_entry.payload_hash, expected_hash, "Payload hash should match expected value");

    // Verify validation result is cached
    assert!(cache_entry.is_valid.unwrap_or(false), "Cached result should be valid");

    // Call validation again to test cache hit
    let cache_count_before = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.validation_cache WHERE schema_id = $1",
        schema_id
    )
    .fetch_one(pool.inner())
    .await?
    .unwrap_or(0);

    sqlx::query!(
        "SELECT * FROM sinex_schemas.validate_event_payload($1)",
        event_id
    )
    .fetch_one(pool.inner())
    .await?;

    let cache_count_after = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.validation_cache WHERE schema_id = $1",
        schema_id
    )
    .fetch_one(pool.inner())
    .await?
    .unwrap_or(0);

    assert_eq!(cache_count_before, cache_count_after, "Cache should not create duplicate entries");

    Ok(())
}

#[tokio::test]
async fn test_validation_cache_with_invalid_payload() -> anyhow::Result<()> {
    let ctx = TestContext::new().await?;
    let pool = &ctx.pool;

    // Create a test schema
    let schema_id = sqlx::query_scalar!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (id, schema_name, schema_version, schema_content)
        VALUES (gen_ulid(), 'test_validation_cache_invalid', '1.0.0',
                '{"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}')
        RETURNING id
        "#
    )
    .fetch_one(pool.inner())
    .await?;

    // Create a test event with invalid payload (missing required field)
    let test_payload = r#"{"wrong_field": "test"}"#;
    let event_id = sqlx::query_scalar!(
        r#"
        INSERT INTO core.events (
            id, event_type, source, host, payload, payload_schema_id, source_event_ids
        ) VALUES (
            gen_ulid(), 'test.validation_cache_invalid', 'test', 'test-host', $1::jsonb, $2,
            ARRAY[gen_ulid()]::ULID[]  -- Use internal provenance for XOR constraint
        )
        RETURNING id
        "#,
        serde_json::from_str::<serde_json::Value>(test_payload)?,
        schema_id
    )
    .fetch_one(pool.inner())
    .await?;

    // Call the validation function
    let validation_result = sqlx::query!(
        "SELECT * FROM sinex_schemas.validate_event_payload($1)",
        event_id
    )
    .fetch_one(pool.inner())
    .await?;

    assert!(!validation_result.is_valid.unwrap_or(true), "Event should be invalid");

    // Verify cache entry was created with correct result
    let cache_entry = sqlx::query!(
        "SELECT is_valid, validation_errors FROM sinex_schemas.validation_cache WHERE schema_id = $1",
        schema_id
    )
    .fetch_one(pool.inner())
    .await?;

    assert!(!cache_entry.is_valid.unwrap_or(true), "Cached result should be invalid");
    assert!(cache_entry.validation_errors.is_some(), "Should have validation errors");

    Ok(())
}