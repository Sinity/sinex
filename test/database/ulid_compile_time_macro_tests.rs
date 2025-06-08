use sinex_db::queries_macro_safe::*;
use sinex_ulid::Ulid;
use chrono::Utc;
use std::str::FromStr;

#[sqlx::test]
async fn test_ulid_compile_time_safe_insert_and_fetch(pool: sqlx::PgPool) -> anyhow::Result<()> {
    // Test compile-time safe insert
    let payload = serde_json::json!({
        "test": "compile_time_safe",
        "timestamp": Utc::now(),
        "features": ["type_safety", "compile_time_checking"]
    });
    
    let event = insert_raw_event_safe(
        &pool,
        "test.compile_time",
        "safe_insert_test",
        "test_host",
        payload.clone(),
        Some(Utc::now()),
        Some("2.0.0"),
        None,
    )
    .await?;
    
    // Verify the event was created with proper ULID
    assert!(!event.id.is_nil());
    assert_eq!(event.source, "test.compile_time");
    assert_eq!(event.event_type, "safe_insert_test");
    assert_eq!(event.payload, payload);
    
    // Test compile-time safe fetch by ID
    let fetched = get_event_by_id(&pool, event.id).await?;
    assert!(fetched.is_some());
    
    let fetched_event = fetched.unwrap();
    assert_eq!(fetched_event.id, event.id);
    assert_eq!(fetched_event.source, event.source);
    assert_eq!(fetched_event.payload, event.payload);
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_compile_time_safe_batch_operations(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let mut event_ids = Vec::new();
    
    // Insert multiple events
    for i in 0..5 {
        let event = insert_raw_event_safe(
            &pool,
            "test.batch_compile",
            "batch_test",
            "test_host",
            serde_json::json!({ "index": i }),
            None,
            None,
            None,
        )
        .await?;
        
        event_ids.push(event.id);
    }
    
    // Test batch fetch
    let fetched_events = get_events_by_ids(&pool, &event_ids).await?;
    assert_eq!(fetched_events.len(), 5);
    
    // Verify order preservation (ULIDs are sortable)
    for i in 1..fetched_events.len() {
        assert!(fetched_events[i].id > fetched_events[i-1].id);
    }
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_compile_time_safe_with_schema_id(pool: sqlx::PgPool) -> anyhow::Result<()> {
    // First create a schema
    let schema_id_record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas 
            (event_source, event_type, schema_version, json_schema_definition, description)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id::text as "id!"
        "#,
        "test.schema",
        "test.schema_event", 
        "v1.0",
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"]
        }),
        Some("Test schema for compile-time safe queries")
    )
    .fetch_one(&pool)
    .await?;
    
    let schema_id = Ulid::from_str(&schema_id_record.id)?;
    
    // Insert event with schema reference
    let event = insert_raw_event_safe(
        &pool,
        "test.schema",
        "test.schema_event",
        "test_host",
        serde_json::json!({ "value": "test" }),
        None,
        Some("1.0.0"),
        Some(schema_id),
    )
    .await?;
    
    assert_eq!(event.payload_schema_id, Some(schema_id));
    
    // Verify we can fetch it back
    let fetched = get_event_by_id(&pool, event.id).await?;
    assert!(fetched.is_some());
    assert_eq!(fetched.unwrap().payload_schema_id, Some(schema_id));
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_compile_time_safe_recent_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let source = "test.recent_compile";
    
    // Insert some events
    for i in 0..10 {
        insert_raw_event_safe(
            &pool,
            source,
            "recent_test",
            "test_host",
            serde_json::json!({ "sequence": i }),
            None,
            None,
            None,
        )
        .await?;
    }
    
    // Get recent events with source filter
    let recent_with_filter = get_recent_events(&pool, 5, Some(source)).await?;
    assert_eq!(recent_with_filter.len(), 5);
    
    // Verify they're in descending order (most recent first)
    for i in 1..recent_with_filter.len() {
        assert!(recent_with_filter[i].id < recent_with_filter[i-1].id);
    }
    
    // Get recent events without filter
    let recent_all = get_recent_events(&pool, 100, None).await?;
    assert!(recent_all.len() >= 10); // At least our 10 events
    
    Ok(())
}

#[sqlx::test]
async fn test_ulid_type_conversions_in_queries(pool: sqlx::PgPool) -> anyhow::Result<()> {
    use std::str::FromStr;
    
    let ulid = Ulid::new();
    let uuid = ulid.as_uuid();
    
    // Insert using UUID representation with ULID cast
    sqlx::query!(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
        uuid,
        "test.conversion",
        "conversion_test",
        "test_host",
        serde_json::json!({})
    )
    .execute(&pool)
    .await?;
    
    // Fetch using UUID representation with compile-time macro
    let event = sqlx::query_as!(
        sinex_db::models::RawEvent,
        r#"
        SELECT 
            id::uuid as "id: _",
            source as "source!",
            event_type as "event_type!",
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!",
            ingestor_version,
            payload_schema_id::uuid as "payload_schema_id: _",
            payload as "payload!"
        FROM raw.events
        WHERE id = $1::uuid::ulid
        "#,
        uuid
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(event.id, ulid);
    
    Ok(())
}