use chrono::Utc;
use serde_json::json;
use sinex_core::db::repositories::schema_management::{NewEventSchema, SchemaManagementRepository};
use sinex_core::types::Ulid;
use sinex_test_utils::{sinex_test, TestContext};
use sqlx::Row;

#[sinex_test]
async fn test_validation_cache_uses_payload_hash(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let pool = ctx.pool.clone();

    // Create a test schema
    let schema_id = insert_test_schema(
        &pool,
        "test",
        "test.validation_cache",
        json!({
            "type": "object",
            "properties": { "message": { "type": "string" } },
            "required": ["message"]
        }),
    )
    .await?;

    // Create a test event with valid payload
    let test_payload = r#"{"message": "test validation cache"}"#;
    let event_row = sqlx::query(
        r#"
        INSERT INTO core.events (
            id, event_type, source, host, payload, payload_schema_id, source_event_ids, ts_orig
        ) VALUES (
            gen_ulid(), 'test.validation_cache', 'test', 'test-host', $1::jsonb, $2::uuid::ulid,
            ARRAY[gen_ulid()]::ULID[], $3  -- Use internal provenance for XOR constraint
        )
        RETURNING id::uuid as id
        "#,
    )
    .bind(serde_json::from_str::<serde_json::Value>(test_payload)?)
    .bind(schema_id)
    .bind(Utc::now())
    .fetch_one(&pool)
    .await?;
    let event_id: sqlx::types::Uuid = event_row.get("id");

    let event_ulid: Ulid = event_id.into();
    let repo = SchemaManagementRepository::new(&pool);
    let validation_result = repo.validate_event_payload_by_event_id(&event_ulid).await?;
    assert!(validation_result.is_valid, "Event should be valid");

    // Verify cache entry was created
    let cache_entry = sqlx::query!(
        r#"
        SELECT is_valid, validation_errors as "validation_errors?: serde_json::Value"
        FROM sinex_schemas.validation_cache
        WHERE schema_id = $1::uuid::ulid
          AND event_id = $2::uuid::ulid
        "#,
        schema_id,
        event_id
    )
    .fetch_one(&pool)
    .await?;
    assert!(cache_entry.is_valid, "Cached result should be valid");
    assert!(
        cache_entry.validation_errors.is_none(),
        "Valid event should not cache validation errors"
    );

    // Call validation again to test cache hit
    let cache_count_before = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM sinex_schemas.validation_cache
        WHERE schema_id = $1::uuid::ulid
        "#,
        schema_id
    )
    .fetch_one(&pool)
    .await?;

    let second_result = repo.validate_event_payload_by_event_id(&event_ulid).await?;
    assert!(second_result.is_valid, "Cached result should remain valid");

    let cache_count_after = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM sinex_schemas.validation_cache
        WHERE schema_id = $1::uuid::ulid
        "#,
        schema_id
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(
        cache_count_before, cache_count_after,
        "Cache should not create duplicate entries"
    );

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn test_validation_cache_with_invalid_payload(
    ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool.clone();

    // Create a test schema
    let schema_id = insert_test_schema(
        &pool,
        "test",
        "test.validation_cache_invalid",
        json!({
            "type": "object",
            "properties": { "message": { "type": "string" } },
            "required": ["message"]
        }),
    )
    .await?;

    // Create a test event with invalid payload (missing required field)
    let test_payload = r#"{"wrong_field": "test"}"#;
    let event_row = sqlx::query(
        r#"
        INSERT INTO core.events (
            id, event_type, source, host, payload, payload_schema_id, source_event_ids, ts_orig
        ) VALUES (
            gen_ulid(), 'test.validation_cache_invalid', 'test', 'test-host', $1::jsonb, $2::uuid::ulid,
            ARRAY[gen_ulid()]::ULID[], $3  -- Use internal provenance for XOR constraint
        )
        RETURNING id::uuid as id
        "#
    )
    .bind(serde_json::from_str::<serde_json::Value>(test_payload)?)
    .bind(schema_id)
    .bind(Utc::now())
    .fetch_one(&pool)
    .await?;
    let event_id: sqlx::types::Uuid = event_row.get("id");

    let event_ulid: Ulid = event_id.into();
    let repo = SchemaManagementRepository::new(&pool);
    let validation_result = repo.validate_event_payload_by_event_id(&event_ulid).await?;
    assert!(
        !validation_result.is_valid,
        "Invalid event should fail validation"
    );

    // Verify cache entry was created with correct result
    let cache_entry = sqlx::query!(
        r#"
        SELECT is_valid, validation_errors as "validation_errors?: serde_json::Value"
        FROM sinex_schemas.validation_cache
        WHERE schema_id = $1::uuid::ulid
          AND event_id = $2::uuid::ulid
        "#,
        schema_id,
        event_id
    )
    .fetch_one(&pool)
    .await?;

    assert!(
        !cache_entry.is_valid,
        "Cached result should reflect invalid payload"
    );
    assert!(
        cache_entry.validation_errors.is_some(),
        "Validation errors should be stored"
    );

    Ok(())
}

async fn insert_test_schema(
    pool: &sqlx::PgPool,
    source: &str,
    event_type: &str,
    schema_json: serde_json::Value,
) -> color_eyre::eyre::Result<sqlx::types::Uuid> {
    let schema = NewEventSchema {
        source: source.to_string(),
        event_type: event_type.to_string(),
        schema_version: "1.0.0".to_string(),
        schema_content: schema_json.clone(),
    };
    let content_hash = schema.calculate_content_hash();

    let row = sqlx::query(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (
            source, event_type, schema_version, schema_content, content_hash, is_active
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, true)
        RETURNING id::uuid as id
        "#,
    )
    .bind(source)
    .bind(event_type)
    .bind("1.0.0")
    .bind(schema_json)
    .bind(content_hash)
    .fetch_one(pool)
    .await?;

    Ok(row.get("id"))
}
