//! Unit tests for JobManager and Sensor Executors

use sinex_core::types::Ulid;
use sinex_satellite_sdk::{JobManager, JobManagerConfig};
use sinex_test_utils::prelude::*;

/// Test JobManager initialization
#[sinex_test]
async fn job_manager_initialization(ctx: TestContext) -> Result<()> {
    let config = JobManagerConfig {
        poll_interval_ms: 100,
        max_concurrent_jobs: 5,
    };

    let _manager = JobManager::new(ctx.pool.clone(), config);

    // Verify JobManager can be created
    Ok(())
}

/// Test sensor job database operations
#[sinex_test]
async fn sensor_job_crud_operations(ctx: TestContext) -> Result<()> {
    // Create a job using non-macro query interface
    let job_id_str = Ulid::new().to_string();

    sqlx::query(
        r#"
        INSERT INTO raw.sensor_jobs (id, sensor_type, target_uri, config, status, priority)
        VALUES (CAST($1 AS ULID), 'append_stream', '/tmp/test.sock', '{}'::jsonb, 'active', 100)
        "#,
    )
    .bind(&job_id_str)
    .execute(&ctx.pool)
    .await?;

    // Query it back
    let row: (String, String, String) = sqlx::query_as(
        r#"
        SELECT sensor_type, target_uri, status
        FROM raw.sensor_jobs
        WHERE id = CAST($1 AS ULID)
        "#,
    )
    .bind(&job_id_str)
    .fetch_one(&ctx.pool)
    .await?;

    assert_eq!(row.0, "append_stream");
    assert_eq!(row.1, "/tmp/test.sock");
    assert_eq!(row.2, "active");

    // Update status
    sqlx::query(
        r#"
        UPDATE raw.sensor_jobs
        SET status = 'paused'
        WHERE id = CAST($1 AS ULID)
        "#,
    )
    .bind(&job_id_str)
    .execute(&ctx.pool)
    .await?;

    // Verify update
    let row: (String,) =
        sqlx::query_as(r#"SELECT status FROM raw.sensor_jobs WHERE id = CAST($1 AS ULID)"#)
            .bind(&job_id_str)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(row.0, "paused");

    Ok(())
}

/// Test sensor job status transitions
#[sinex_test]
async fn sensor_job_status_transitions(ctx: TestContext) -> Result<()> {
    let job_id_str = Ulid::new().to_string();

    // Create job in active state
    sqlx::query(
        r#"
        INSERT INTO raw.sensor_jobs (id, sensor_type, target_uri, config, status, priority)
        VALUES (CAST($1 AS ULID), 'tree_watch', '/tmp/watch', '{}'::jsonb, 'active', 50)
        "#,
    )
    .bind(&job_id_str)
    .execute(&ctx.pool)
    .await?;

    // Transition: active → paused
    sqlx::query(r#"UPDATE raw.sensor_jobs SET status = 'paused' WHERE id = CAST($1 AS ULID)"#)
        .bind(&job_id_str)
        .execute(&ctx.pool)
        .await?;

    let row: (String,) =
        sqlx::query_as(r#"SELECT status FROM raw.sensor_jobs WHERE id = CAST($1 AS ULID)"#)
            .bind(&job_id_str)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(row.0, "paused");

    // Transition: paused → retired
    sqlx::query(r#"UPDATE raw.sensor_jobs SET status = 'retired' WHERE id = CAST($1 AS ULID)"#)
        .bind(&job_id_str)
        .execute(&ctx.pool)
        .await?;

    let row: (String,) =
        sqlx::query_as(r#"SELECT status FROM raw.sensor_jobs WHERE id = CAST($1 AS ULID)"#)
            .bind(&job_id_str)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(row.0, "retired");

    Ok(())
}

/// Test querying active jobs by priority
#[sinex_test]
async fn query_active_jobs_by_priority(ctx: TestContext) -> Result<()> {
    // Create jobs with different priorities
    for (priority, idx) in [(100, 1), (50, 2), (200, 3)] {
        let job_id_str = Ulid::new().to_string();
        sqlx::query(
            r#"
            INSERT INTO raw.sensor_jobs (id, sensor_type, target_uri, config, status, priority)
            VALUES (CAST($1 AS ULID), $2, $3, '{}'::jsonb, 'active', $4)
            "#,
        )
        .bind(&job_id_str)
        .bind(format!("type_{}", idx))
        .bind(format!("/path/{}", idx))
        .bind(priority)
        .execute(&ctx.pool)
        .await?;
    }

    // Query jobs ordered by priority desc
    let rows: Vec<(i32, String)> = sqlx::query_as(
        r#"
        SELECT priority, sensor_type
        FROM raw.sensor_jobs
        WHERE status = 'active'
        ORDER BY priority DESC
        "#,
    )
    .fetch_all(&ctx.pool)
    .await?;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 200); // Highest priority first
    assert_eq!(rows[1].0, 100);
    assert_eq!(rows[2].0, 50);

    Ok(())
}
