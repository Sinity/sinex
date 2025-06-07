use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Connection, PgConnection};
use std::env;

#[tokio::test]
async fn test_all_migrations_run_successfully() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    // Create a fresh test database
    let mut conn = PgConnection::connect(&database_url.replace("/sinex_test", "/postgres"))
        .await
        .expect("Failed to connect to postgres");
    
    // Drop and recreate test database
    sqlx::query("DROP DATABASE IF EXISTS sinex_test")
        .execute(&mut conn)
        .await
        .ok();
    
    sqlx::query("CREATE DATABASE sinex_test")
        .execute(&mut conn)
        .await
        .expect("Failed to create test database");
    
    drop(conn);
    
    // Connect to the test database
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    
    // Verify critical objects exist
    verify_schemas_exist(&pool).await;
    verify_tables_exist(&pool).await;
    verify_extensions_exist(&pool).await;
    verify_functions_exist(&pool).await;
}

async fn verify_schemas_exist(pool: &PgPool) {
    let schemas = vec!["raw", "sinex_schemas", "core"];
    
    for schema in schemas {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)"
        )
        .bind(schema)
        .fetch_one(pool)
        .await
        .unwrap();
        
        assert!(exists, "Schema {} should exist", schema);
    }
}

async fn verify_tables_exist(pool: &PgPool) {
    let tables = vec![
        ("raw", "events"),
        ("sinex_schemas", "event_payload_schemas"),
        ("sinex_schemas", "agent_manifests"),
        ("sinex_schemas", "promotion_queue"),
    ];
    
    for (schema, table) in tables {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
            )"
        )
        .bind(schema)
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap();
        
        assert!(exists, "Table {}.{} should exist", schema, table);
    }
}

async fn verify_extensions_exist(pool: &PgPool) {
    let extensions = vec!["ulid", "timescaledb", "pgvector", "pg_jsonschema"];
    
    for ext in extensions {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = $1)"
        )
        .bind(ext)
        .fetch_one(pool)
        .await
        .unwrap();
        
        assert!(exists, "Extension {} should be installed", ext);
    }
}

async fn verify_functions_exist(pool: &PgPool) {
    // Verify ULID functions
    let ulid_functions = vec!["gen_ulid", "gen_monotonic_ulid"];
    
    for func in ulid_functions {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE p.proname = $1
            )"
        )
        .bind(func)
        .fetch_one(pool)
        .await
        .unwrap();
        
        assert!(exists, "Function {} should exist", func);
    }
    
    // Verify our custom functions
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1 FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'core' AND p.proname = 'set_updated_at_trigger_func_generic'
        )"
    )
    .fetch_one(pool)
    .await
    .unwrap();
    
    assert!(exists, "Function core.set_updated_at_trigger_func_generic should exist");
}

#[tokio::test]
async fn test_raw_events_is_hypertable() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Verify raw.events is a hypertable
    let is_hypertable: bool = sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1 FROM timescaledb_information.hypertables 
            WHERE hypertable_schema = 'raw' AND hypertable_name = 'events'
        )"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(is_hypertable, "raw.events should be a TimescaleDB hypertable");
}

#[tokio::test]
async fn test_ulid_generation() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Test gen_ulid() function
    let ulid1: String = sqlx::query_scalar("SELECT gen_ulid()::text")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    let ulid2: String = sqlx::query_scalar("SELECT gen_ulid()::text")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    // ULIDs should be 26 characters long (Crockford Base32)
    assert_eq!(ulid1.len(), 26, "ULID should be 26 characters");
    assert_eq!(ulid2.len(), 26, "ULID should be 26 characters");
    
    // ULIDs should be different
    assert_ne!(ulid1, ulid2, "Sequential ULIDs should be different");
    
    // Test ULID sorting (lexicographically sortable)
    tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
    let ulid3: String = sqlx::query_scalar("SELECT gen_ulid()::text")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    assert!(ulid3 > ulid1, "Later ULID should sort after earlier ULID");
}

#[tokio::test]
async fn test_ulid_to_timestamp_conversion() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Generate a ULID and extract its timestamp
    let result: (String, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "SELECT gen_ulid()::text as ulid, gen_ulid()::timestamp as ts"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    let now = chrono::Utc::now();
    let diff = now.signed_duration_since(result.1);
    
    // Timestamp should be very close to now (within 1 second)
    assert!(diff.num_seconds().abs() < 1, "ULID timestamp should be close to now");
}

#[tokio::test]
async fn test_migration_rollback() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test_rollback".to_string());
    
    // Create a fresh test database for rollback testing
    let mut conn = PgConnection::connect(&database_url.replace("/sinex_test_rollback", "/postgres"))
        .await
        .expect("Failed to connect to postgres");
    
    sqlx::query("DROP DATABASE IF EXISTS sinex_test_rollback")
        .execute(&mut conn)
        .await
        .ok();
    
    sqlx::query("CREATE DATABASE sinex_test_rollback")
        .execute(&mut conn)
        .await
        .expect("Failed to create test database");
    
    drop(conn);
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    
    // Insert test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)"
    )
    .bind("test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    sqlx::query(
        "INSERT INTO raw.events (source, event_type, host, payload) 
         VALUES ($1, $2, $3, $4::jsonb)"
    )
    .bind("test_source")
    .bind("test_type")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify data exists
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raw.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 1);
    
    // Note: Actual rollback would require down migrations which we haven't implemented
    // This test primarily ensures data can be inserted after migrations
}

#[tokio::test]
async fn test_trigger_functions() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Insert an agent manifest
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)"
    )
    .bind("trigger_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Get initial updated_at
    let initial_updated: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT updated_at FROM sinex_schemas.agent_manifests WHERE agent_name = $1"
    )
    .bind("trigger_test_agent")
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Wait a bit
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    // Update the agent
    sqlx::query(
        "UPDATE sinex_schemas.agent_manifests SET version = $1 WHERE agent_name = $2"
    )
    .bind("1.0.1")
    .bind("trigger_test_agent")
    .execute(&pool)
    .await
    .unwrap();
    
    // Get new updated_at
    let new_updated: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT updated_at FROM sinex_schemas.agent_manifests WHERE agent_name = $1"
    )
    .bind("trigger_test_agent")
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert!(new_updated > initial_updated, "updated_at trigger should update timestamp");
}

#[tokio::test]
async fn test_foreign_key_constraints() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Try to insert a promotion queue item with non-existent event ID
    let result = sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
         VALUES (gen_ulid(), 'non_existent_agent')"
    )
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Should fail due to foreign key constraint on agent_name");
    
    // Insert valid agent first
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)"
    )
    .bind("fk_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Try again with non-existent event
    let result = sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
         VALUES (gen_ulid(), 'fk_test_agent')"
    )
    .execute(&pool)
    .await;
    
    assert!(result.is_err(), "Should fail due to foreign key constraint on raw_event_id");
}

#[tokio::test]
async fn test_indexes_exist() {
    let database_url = env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Check for important indexes
    let indexes = vec![
        ("idx_raw_events_ts_orig_desc", "raw", "events"),
        ("idx_raw_events_source_type_ts_ingest_desc", "raw", "events"),
        ("idx_raw_events_host_ts_ingest_desc", "raw", "events"),
        ("idx_raw_events_payload_gin_path_ops", "raw", "events"),
        ("idx_promo_queue_pending_tasks", "sinex_schemas", "promotion_queue"),
        ("idx_promo_queue_failed_tasks", "sinex_schemas", "promotion_queue"),
    ];
    
    for (index_name, schema, table) in indexes {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = $1 AND tablename = $2 AND indexname = $3
            )"
        )
        .bind(schema)
        .bind(table)
        .bind(index_name)
        .fetch_one(&pool)
        .await
        .unwrap();
        
        assert!(exists, "Index {} on {}.{} should exist", index_name, schema, table);
    }
}