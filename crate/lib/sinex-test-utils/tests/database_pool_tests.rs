use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sinex_core::db::repositories::source_materials::legacy_material_types;
use sinex_core::types::error::SinexError;
use sinex_core::DbPoolExt;
use sinex_core::{Event, EventSource, EventType, HostName, JsonValue, Provenance, SourceMaterial};
use sinex_test_utils::db_common::verify_clean_state;
use sinex_test_utils::{
    acquire_admin_connection, acquire_test_database, check_pool_health, get_pool_stats,
    pool_slot_count, reset_pool, sinex_test,
};
use sqlx::postgres::PgConnection;
use sqlx::Connection;

#[sinex_test]
async fn test_pool_handles_concurrent_acquisition() -> sinex_test_utils::Result<()> {
    reset_pool().await?;

    // Initialize the pool and determine available slots.
    let warm_db = acquire_test_database().await?;
    let slot_count = pool_slot_count().await.max(1);
    drop(warm_db);

    let handles: Vec<_> = (0..slot_count)
        .map(|_| {
            tokio::spawn(async move {
                let db = acquire_test_database().await?;

                verify_clean_state(db.pool()).await?;

                tokio::time::sleep(Duration::from_millis(10)).await;

                Ok::<_, SinexError>(db.name().to_string())
            })
        })
        .collect();

    let mut db_names = Vec::new();
    for handle in handles {
        let name = handle
            .await
            .map_err(|e| SinexError::service(format!("Task failed: {e}")))?
            .map_err(|e| SinexError::database(format!("Database operation failed: {e}")))?;
        db_names.push(name);
    }

    let unique_count = db_names.iter().collect::<HashSet<_>>().len();
    assert_eq!(unique_count, slot_count, "All databases should be unique");

    Ok(())
}

#[sinex_test]
async fn test_database_cleanup_on_drop() -> sinex_test_utils::Result<()> {
    let db_name;

    {
        let db = acquire_test_database().await?;
        let baseline = db.pool().events().count_all().await?;
        db_name = db.name().to_string();

        let repo = db.pool().events();
        let event = Event::<JsonValue>::test_event(
            EventSource::new("test"),
            EventType::new("test.event"),
            serde_json::json!({}),
        )
        .with_host(HostName::new("test-host"));
        repo.insert(event).await?;

        let count = db.pool().events().count_all().await?;
        assert_eq!(count, baseline + 1);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let db2 = acquire_test_database().await?;
    let baseline = db2.pool().events().count_all().await?;

    if db2.name() == db_name {
        let count = db2.pool().events().count_all().await?;
        assert_eq!(count, baseline, "Reused database should be cleaned");
    }

    Ok(())
}

#[sinex_test]
async fn test_advisory_lock_prevents_double_acquisition() -> sinex_test_utils::Result<()> {
    let db1 = acquire_test_database().await?;
    let lock_id1 = db1.lock_id();

    let mut probe_conn = PgConnection::connect(db1.url()).await?;
    let lock_acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(lock_id1)
        .fetch_one(&mut probe_conn)
        .await?;

    assert!(
        !lock_acquired,
        "Should not be able to acquire lock that's already held"
    );

    Ok(())
}

#[sinex_test]
async fn test_database_health_check() -> sinex_test_utils::Result<()> {
    let db = acquire_test_database().await?;
    let baseline = db.pool().events().count_all().await?;

    assert!(db.check_health().await?);

    let stats = db.get_stats().await?;
    assert_eq!(stats.event_count, baseline);

    Ok(())
}

#[sinex_test]
async fn test_pool_statistics() -> sinex_test_utils::Result<()> {
    let initial_stats = get_pool_stats();
    let initial_acquisitions = initial_stats.total_acquisitions;

    {
        let _db = acquire_test_database().await?;
    }

    let after_stats = get_pool_stats();
    assert!(after_stats.total_acquisitions > initial_acquisitions);

    Ok(())
}

#[sinex_test]
async fn test_clean_database_handles_complex_data() -> sinex_test_utils::Result<()> {
    let db = acquire_test_database().await?;

    let repo = db.pool().events();
    let event_to_insert = Event::<JsonValue>::test_event(
        EventSource::new("test"),
        EventType::new("test"),
        serde_json::json!({}),
    )
    .with_host(HostName::new("test"));
    let event = repo.insert(event_to_insert).await?;

    sqlx::query(
        "INSERT INTO core.event_annotations (id, event_id, annotation_type, content, metadata, created_by) \
         VALUES ($1, $2, 'test', 'test-content', '{}'::jsonb, 'test-user')",
    )
    .bind(sinex_core::types::ulid::Ulid::new().to_uuid())
    .bind(event.id.expect("Event must have an ID").to_uuid())
    .execute(db.pool())
    .await?;

    db.force_cleanup().await?;

    let event_count = db.pool().events().count_all().await?;
    let annotation_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.event_annotations")
        .fetch_one(db.pool())
        .await?;

    assert_eq!(event_count, 0);
    assert_eq!(annotation_count, 0);

    Ok(())
}

#[sinex_test]
async fn test_pool_health_report() -> sinex_test_utils::Result<()> {
    let _db = acquire_test_database().await?;

    let health = check_pool_health().await?;
    assert!(health.total_slots > 0);
    assert!(health.healthy_slots > 0);

    Ok(())
}

#[allow(clippy::result_large_err)]
#[cfg_attr(not(feature = "slow-tests"), ignore = "slow fixture")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stress_concurrent_operations() -> sinex_test_utils::Result<()> {
    let mut handles = Vec::new();

    for i in 0..50 {
        let handle = tokio::spawn(async move {
            let db = acquire_test_database().await?;

            let material_record = db
                .pool()
                .source_materials()
                .register_in_flight(
                    legacy_material_types::STREAM,
                    Some(&format!("stress-fixture-{i}")),
                    serde_json::json!({ "test": "stress" }),
                )
                .await?;
            let material_id = sinex_core::Id::<SourceMaterial>::from_ulid(material_record.id);

            let repo = db.pool().events();
            for _ in 0..5 {
                let mut event = Event::<JsonValue>::test_event(
                    EventSource::new(format!("task_{i}")),
                    EventType::new("stress.test"),
                    serde_json::json!({}),
                )
                .with_host(HostName::new("test"));
                event.provenance = Provenance::from_material(material_id, 0, None, None);
                repo.insert(event).await?;
            }

            let repo = db.pool().events();
            let count = repo.count_all().await?;
            assert!(count >= 5);

            Ok::<_, SinexError>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle
            .await
            .map_err(|e| SinexError::service(format!("Task failed: {e}")))?
            .map_err(|e| SinexError::database(format!("Database operation failed: {e}")))?;
    }

    Ok(())
}

#[sinex_test]
async fn test_template_database_exists() -> sinex_test_utils::Result<()> {
    reset_pool().await?;
    let _db = acquire_test_database().await?;

    let mut conn = acquire_admin_connection().await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'sinex_test_template_shared')",
    )
    .fetch_one(&mut conn)
    .await?;

    assert!(exists, "Template database should exist");

    Ok(())
}

#[sinex_test]
async fn test_database_pool_provides_connection() -> sinex_test_utils::Result<()> {
    let db = acquire_test_database().await?;

    let result: i32 = sqlx::query_scalar("SELECT 1").fetch_one(db.pool()).await?;
    assert_eq!(result, 1);

    Ok(())
}

#[sinex_test]
async fn test_concurrent_context_allocation() -> sinex_test_utils::Result<()> {
    let success_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];
    for _ in 0..5 {
        let counter = success_count.clone();
        let handle = tokio::spawn(async move {
            match acquire_test_database().await {
                Ok(db) => {
                    let _: i32 = sqlx::query_scalar("SELECT 1").fetch_one(db.pool()).await?;
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    assert!(success_count.load(Ordering::SeqCst) > 0);

    Ok(())
}

#[sinex_test]
async fn test_basic_pool_functionality() -> sinex_test_utils::Result<()> {
    let db = acquire_test_database().await?;
    let pool = db.pool();

    let result: i32 = sqlx::query_scalar("SELECT 1").fetch_one(pool).await?;
    assert_eq!(result, 1);

    let db1 = acquire_test_database().await?;
    let db2 = acquire_test_database().await?;
    assert_ne!(
        db1.name(),
        db2.name(),
        "Each test should get a unique database"
    );

    Ok(())
}

#[sinex_test]
async fn test_pool_reset_clears_state() -> sinex_test_utils::Result<()> {
    let db = acquire_test_database().await?;
    let baseline = db.pool().events().count_all().await?;
    assert_eq!(baseline, 0);

    let repo = db.pool().events();
    let event = Event::<JsonValue>::test_event(
        EventSource::new("reset"),
        EventType::new("pool.reset"),
        serde_json::json!({}),
    );
    repo.insert(event).await?;

    reset_pool().await?;

    let db = acquire_test_database().await?;
    let count = db.pool().events().count_all().await?;
    assert_eq!(count, 0);

    Ok(())
}
