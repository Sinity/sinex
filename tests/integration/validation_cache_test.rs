use sinex_core::db::pool::DbPool;
use sinex_test_utils::{sinex_test, test_context::TestContext};
use sqlx::Row;

#[sinex_test]
async fn test_validation_cache_uses_payload_hash() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await?;
    let pool = &ctx.pool;

    // Create a test schema
    let schema_row = sqlx::query(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (id, schema_name, schema_version, schema_content)
        VALUES (gen_ulid(), 'test_validation_cache', '1.0.0', 
                '{"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}')
        RETURNING id::uuid as id
        "#
    )
    .fetch_one(pool.inner())
    .await?;
    let schema_id: sqlx::types::Uuid = schema_row.get("id");

    // Create a test event with valid payload
    let test_payload = r#"{"message": "test validation cache"}"#;
    let event_row = sqlx::query(
        r#"
        INSERT INTO core.events (
            id, event_type, source, host, payload, payload_schema_id, source_event_ids
        ) VALUES (
            gen_ulid(), 'test.validation_cache', 'test', 'test-host', $1::jsonb, $2::uuid::ulid,
            ARRAY[gen_ulid()]::ULID[]  -- Use internal provenance for XOR constraint
        )
        RETURNING id::uuid as id
        "#
    )
    .bind(serde_json::from_str::<serde_json::Value>(test_payload)?)
    .bind(schema_id)
    .fetch_one(pool.inner())
    .await?;
    let event_id: sqlx::types::Uuid = event_row.get("id");

    // Call the validation function
    let validation_result = sqlx::query(
        "SELECT * FROM sinex_schemas.validate_event_payload($1::uuid::ulid)",
    )
    .bind(event_id)
    .fetch_one(pool.inner())
    .await?;
    let is_valid: Option<bool> = validation_result.get("is_valid");
    assert!(is_valid.unwrap_or(false), "Event should be valid");

    // Verify cache entry was created with payload_hash
    let cache_entry = sqlx::query(
        "SELECT payload_hash, schema_id::uuid as schema_id, is_valid FROM sinex_schemas.validation_cache WHERE schema_id = $1::uuid::ulid",
    )
    .bind(schema_id)
    .fetch_one(pool.inner())
    .await?;

    // Verify payload hash is correct length (SHA256 = 64 hex characters)
    let payload_hash: String = cache_entry.get("payload_hash");
    assert_eq!(payload_hash.len(), 64, "Payload hash should be 64 characters");

    // Verify the hash matches what we expect
    let expected_hash: String = sqlx::query(
        "SELECT encode(digest($1, 'sha256'), 'hex') as hex",
    )
    .bind(test_payload)
    .fetch_one(pool.inner())
    .await?
    .get("hex");

    assert_eq!(payload_hash, expected_hash, "Payload hash should match expected value");

    // Verify validation result is cached
    let cached_valid: Option<bool> = cache_entry.get("is_valid");
    assert!(cached_valid.unwrap_or(false), "Cached result should be valid");

    // Call validation again to test cache hit
    let cache_count_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.validation_cache WHERE schema_id = $1::uuid::ulid",
    )
    .bind(schema_id)
    .fetch_one(pool.inner())
    .await?;

    let _ = sqlx::query(
        "SELECT * FROM sinex_schemas.validate_event_payload($1::uuid::ulid)",
    )
    .bind(event_id)
    .fetch_one(pool.inner())
    .await?;

    let cache_count_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.validation_cache WHERE schema_id = $1::uuid::ulid",
    )
    .bind(schema_id)
    .fetch_one(pool.inner())
    .await?;

    assert_eq!(cache_count_before, cache_count_after, "Cache should not create duplicate entries");

    Ok(())
}

#[sinex_test]
async fn test_validation_cache_with_invalid_payload() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await?;
    let pool = &ctx.pool;

    // Create a test schema
    let schema_row = sqlx::query(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (id, schema_name, schema_version, schema_content)
        VALUES (gen_ulid(), 'test_validation_cache_invalid', '1.0.0',
                '{"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}')
        RETURNING id::uuid as id
        "#
    )
    .fetch_one(pool.inner())
    .await?;
    let schema_id: sqlx::types::Uuid = schema_row.get("id");

    // Create a test event with invalid payload (missing required field)
    let test_payload = r#"{"wrong_field": "test"}"#;
    let event_row = sqlx::query(
        r#"
        INSERT INTO core.events (
            id, event_type, source, host, payload, payload_schema_id, source_event_ids
        ) VALUES (
            gen_ulid(), 'test.validation_cache_invalid', 'test', 'test-host', $1::jsonb, $2::uuid::ulid,
            ARRAY[gen_ulid()]::ULID[]  -- Use internal provenance for XOR constraint
        )
        RETURNING id::uuid as id
        "#
    )
    .bind(serde_json::from_str::<serde_json::Value>(test_payload)?)
    .bind(schema_id)
    .fetch_one(pool.inner())
    .await?;
    let event_id: sqlx::types::Uuid = event_row.get("id");

    // Call the validation function
    let validation_result = sqlx::query(
        "SELECT * FROM sinex_schemas.validate_event_payload($1::uuid::ulid)",
    )
    .bind(event_id)
    .fetch_one(pool.inner())
    .await?;
    let is_valid: Option<bool> = validation_result.get("is_valid");
    assert!(!is_valid.unwrap_or(true), "Event should be invalid");

    // Verify cache entry was created with correct result
    let cache_entry = sqlx::query(
        "SELECT is_valid, validation_errors FROM sinex_schemas.validation_cache WHERE schema_id = $1::uuid::ulid",
    )
    .bind(schema_id)
    .fetch_one(pool.inner())
    .await?;

    let cached_valid: Option<bool> = cache_entry.get("is_valid");
    let validation_errors: Option<serde_json::Value> = cache_entry.get("validation_errors");
    assert!(!cached_valid.unwrap_or(true), "Cached result should be invalid");
    assert!(validation_errors.is_some(), "Should have validation errors");

    Ok(())
}
