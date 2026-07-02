use super::*;

use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_events_table_creation() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let pool = &ctx.pool;

    // Create the events table
    let stmt = Events::create_table_statement();
    let sql = stmt.to_string(PostgresQueryBuilder);

    sqlx::query(&sql)
        .execute(pool)
        .await
        .expect("Events table should be created successfully");

    // Verify the table exists and has the correct structure
    let columns = get_table_columns(pool, "core", "events").await?;

    // Verify essential columns exist
    assert!(columns.contains_key("id"));
    assert!(columns.contains_key("source"));
    assert!(columns.contains_key("event_type"));
    assert!(columns.contains_key("host"));
    assert!(columns.contains_key("payload"));
    assert!(columns.contains_key("ts_orig"));
    assert!(columns.contains_key("ts_coided"));
    assert!(columns.contains_key("source_material_id"));
    assert!(columns.contains_key("source_event_ids"));
    // associated_blob_ids is added in a later migration; table definition may omit it in some contexts

    // Verify primary key
    assert_eq!(columns["id"].data_type, "uuid");
    assert!(columns["id"].is_primary_key);

    // Verify NOT NULL constraints
    assert!(!columns["source"].is_nullable);
    assert!(!columns["event_type"].is_nullable);
    assert!(!columns["host"].is_nullable);
    assert!(!columns["payload"].is_nullable);
    assert!(!columns["ts_orig"].is_nullable);
    assert!(!columns["ts_coided"].is_nullable);

    // Verify nullable columns
    assert!(columns["source_material_id"].is_nullable);
    assert!(columns["source_event_ids"].is_nullable);
    assert!(columns["associated_blob_ids"].is_nullable);
    Ok(())
}

#[sinex_test]
async fn test_blobs_table_creation() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let pool = &ctx.pool;

    let stmt = Blobs::create_table_statement();
    let sql = stmt.to_string(PostgresQueryBuilder);

    sqlx::query(&sql)
        .execute(pool)
        .await
        .expect("Blobs table should be created successfully");

    let columns = get_table_columns(pool, "core", "blobs").await?;

    // Verify essential columns
    assert!(columns.contains_key("id"));
    assert!(columns.contains_key("annex_backend"));
    assert!(columns.contains_key("content_hash"));
    assert!(columns.contains_key("size_bytes"));
    assert!(columns.contains_key("checksum_blake3"));
    assert!(columns.contains_key("original_filename"));
    assert!(columns.contains_key("mime_type"));

    // Verify primary key
    assert_eq!(columns["id"].data_type, "uuid");
    assert!(columns["id"].is_primary_key);
    Ok(())
}

#[sinex_test]
async fn test_source_material_registry_creation() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let pool = &ctx.pool;

    let stmt = SourceMaterialRegistry::create_table_statement();
    let sql = stmt.to_string(PostgresQueryBuilder);

    sqlx::query(&sql)
        .execute(pool)
        .await
        .expect("SourceMaterialRegistry table should be created successfully");

    let columns = get_table_columns(pool, "raw", "source_material_registry").await?;

    assert!(columns.contains_key("id"));
    assert!(columns.contains_key("material_kind"));
    assert!(columns.contains_key("source_identifier"));
    assert!(columns.contains_key("status"));
    assert!(columns.contains_key("timing_info_type"));
    assert!(columns.contains_key("metadata"));

    assert_eq!(columns["id"].data_type, "uuid");
    assert!(columns["id"].is_primary_key);
    Ok(())
}

#[sinex_test]
async fn test_all_record_structs_match_tables() -> color_eyre::eyre::Result<()> {
    let ctx = TestContext::new().await.unwrap();
    let pool = &ctx.pool;

    // Create a few key tables to test Record struct compatibility
    let tables = vec![
        ("core.events", Events::create_table_statement()),
        ("core.blobs", Blobs::create_table_statement()),
        (
            "core.source_material_registry",
            SourceMaterialRegistry::create_table_statement(),
        ),
    ];

    for (table_name, stmt) in tables {
        let sql = stmt.to_string(PostgresQueryBuilder);
        sqlx::query(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|_| panic!("Should create table {table_name}"));
    }

    // Test that we can select into Record structs
    // This will fail at compile time if the structs don't match the tables

    // Insert test data
    let event_id = uuid::Uuid::now_v7();
    let parent_event_id = uuid::Uuid::now_v7();
    sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_event_ids) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid[])",
        event_id,
        "test-source",
        "test-event",
        "test-host",
        serde_json::json!({"test": "data"}),
        *sinex_primitives::temporal::now(),
        &[parent_event_id][..]
    ).execute(pool).await.unwrap();

    // Query and verify the stored fields — proves the schema stores data without corruption,
    // not just that an ID round-trips (which is trivially true after a successful INSERT).
    let row = sqlx::query!(
        r#"SELECT
            source,
            event_type,
            host,
            payload
           FROM core.events WHERE id = $1::uuid"#,
        event_id
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(row.source, "test-source");
    assert_eq!(row.event_type, "test-event");
    assert_eq!(row.host, "test-host");
    assert_eq!(row.payload, serde_json::json!({"test": "data"}));
    Ok(())
}
