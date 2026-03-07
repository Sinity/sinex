//! `GitOps` sync service integration tests
//!
//! Tests the sync cycle logic, `needs_sync` determination, and sync state updates
//! using the real database with isolated test slots.

use sinex_ingestd::gitops::GitOpsSource;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Uuid, error::SinexError};
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// needs_sync unit tests (pure logic, no DB required)
// ---------------------------------------------------------------------------

/// A source that has never been synced (last_sync_at = None) should always need sync.
#[sinex_test]
async fn test_needs_sync_null_last_sync_at() -> TestResult<()> {
    let source = GitOpsSource {
        id: Uuid::now_v7(),
        repository_url: "https://github.com/org/repo.git".to_string(),
        branch: "main".to_string(),
        path_pattern: "schemas/**/*.json".to_string(),
        sync_enabled: true,
        last_sync_at: None,
        last_sync_commit: None,
        sync_frequency_minutes: 60,
    };

    assert!(
        source.needs_sync(),
        "Source with NULL last_sync_at should always need sync"
    );
    Ok(())
}

/// A source synced very recently (well within its frequency) should NOT need sync.
#[sinex_test]
async fn test_needs_sync_recent_last_sync_at_skips() -> TestResult<()> {
    let source = GitOpsSource {
        id: Uuid::now_v7(),
        repository_url: "https://github.com/org/repo.git".to_string(),
        branch: "main".to_string(),
        path_pattern: "schemas/**/*.json".to_string(),
        sync_enabled: true,
        last_sync_at: Some(Timestamp::now()),
        last_sync_commit: Some("abc123".to_string()),
        sync_frequency_minutes: 60,
    };

    assert!(
        !source.needs_sync(),
        "Source synced just now should NOT need sync (60min interval)"
    );
    Ok(())
}

/// A source whose sync interval has expired should need sync.
#[sinex_test]
async fn test_needs_sync_expired_interval_triggers() -> TestResult<()> {
    // Create a timestamp 120 minutes in the past.
    let two_hours_ago =
        Timestamp::from(time::OffsetDateTime::now_utc() - time::Duration::minutes(120));

    let source = GitOpsSource {
        id: Uuid::now_v7(),
        repository_url: "https://github.com/org/repo.git".to_string(),
        branch: "main".to_string(),
        path_pattern: "schemas/**/*.json".to_string(),
        sync_enabled: true,
        last_sync_at: Some(two_hours_ago),
        last_sync_commit: Some("abc123".to_string()),
        sync_frequency_minutes: 60,
    };

    assert!(
        source.needs_sync(),
        "Source synced 120 minutes ago with 60-minute interval should need sync"
    );
    Ok(())
}

/// Edge case: sync_frequency_minutes = 0 should always need sync when last_sync_at is set.
#[sinex_test]
async fn test_needs_sync_zero_frequency_always_triggers() -> TestResult<()> {
    let source = GitOpsSource {
        id: Uuid::now_v7(),
        repository_url: "https://github.com/org/repo.git".to_string(),
        branch: "main".to_string(),
        path_pattern: "schemas/**/*.json".to_string(),
        sync_enabled: true,
        last_sync_at: Some(Timestamp::now()),
        last_sync_commit: Some("abc123".to_string()),
        sync_frequency_minutes: 0,
    };

    assert!(
        source.needs_sync(),
        "Source with sync_frequency_minutes=0 should always need sync"
    );
    Ok(())
}

/// Edge case: sync_frequency_minutes at boundary. If exactly at the boundary,
/// needs_sync should return true (elapsed >= frequency).
#[sinex_test]
async fn test_needs_sync_boundary_exact() -> TestResult<()> {
    let exactly_30_min_ago =
        Timestamp::from(time::OffsetDateTime::now_utc() - time::Duration::minutes(30));

    let source = GitOpsSource {
        id: Uuid::now_v7(),
        repository_url: "https://github.com/org/repo.git".to_string(),
        branch: "main".to_string(),
        path_pattern: "schemas/**/*.json".to_string(),
        sync_enabled: true,
        last_sync_at: Some(exactly_30_min_ago),
        last_sync_commit: Some("abc123".to_string()),
        sync_frequency_minutes: 30,
    };

    assert!(
        source.needs_sync(),
        "Source at exact boundary (30min elapsed, 30min interval) should need sync"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync cycle with database (requires TestContext)
// ---------------------------------------------------------------------------

/// When there are no enabled GitOps sources in the database, the sync cycle
/// should complete successfully with zero sources synced.
#[sinex_test]
async fn test_sync_cycle_empty_db_zero_sources(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool.clone();

    // Ensure the gitops_schema_sources table is empty for this test slot.
    // (Test databases are isolated, so this should already be the case.)
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.gitops_schema_sources WHERE sync_enabled = true",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to count sources: {e}")))?;

    assert_eq!(
        count, 0,
        "Test database should start with no gitops sources"
    );

    // Create the sync service with a temporary work directory.
    let work_dir = tempfile::tempdir()?;
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service = sinex_ingestd::gitops::GitOpsSyncService::new(
        pool.clone(),
        work_dir.path().to_path_buf(),
        shutdown,
    );

    // Run a single sync cycle.
    let stats = service.run_sync_cycle().await?;

    assert_eq!(stats.sources_checked, 0, "No sources should be checked");
    assert_eq!(stats.sources_synced, 0, "No sources should be synced");
    assert_eq!(stats.sources_skipped, 0, "No sources should be skipped");
    assert!(
        stats.errors.is_empty(),
        "No errors should occur with empty sources"
    );

    Ok(())
}

/// After inserting a source and running a sync cycle, the source's sync state
/// should be updated (or an error recorded for an unreachable repo).
#[sinex_test]
async fn test_sync_state_update_on_unreachable_repo(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool.clone();
    let source_id = Uuid::now_v7();

    // Insert a source pointing to a non-existent repository.
    sqlx::query(
        r"
        INSERT INTO sinex_schemas.gitops_schema_sources
            (id, repository_url, branch, path_pattern, sync_enabled, sync_frequency_minutes)
        VALUES ($1::uuid, $2, $3, $4, true, 0)
        ",
    )
    .bind(source_id)
    .bind("https://does-not-exist.example.com/nonexistent/repo.git")
    .bind("main")
    .bind("schemas/**/*.json")
    .execute(&pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to insert test source: {e}")))?;

    // Run a sync cycle. The clone should fail because the repo is unreachable.
    let work_dir = tempfile::tempdir()?;
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service = sinex_ingestd::gitops::GitOpsSyncService::new(
        pool.clone(),
        work_dir.path().to_path_buf(),
        shutdown,
    );

    let stats = service.run_sync_cycle().await?;

    assert_eq!(stats.sources_checked, 1, "Should check the one source");
    assert_eq!(
        stats.sources_synced, 0,
        "Should NOT successfully sync an unreachable repo"
    );
    assert_eq!(
        stats.errors.len(),
        1,
        "Should record one error for the failed clone"
    );
    assert!(
        stats.errors[0].contains("does-not-exist.example.com"),
        "Error should mention the unreachable URL, got: {}",
        stats.errors[0]
    );

    // Verify last_sync_at was NOT updated (sync failed).
    let row = sqlx::query_as::<_, (Option<Timestamp>, Option<String>)>(
        r"
        SELECT last_sync_at, last_sync_commit
        FROM sinex_schemas.gitops_schema_sources
        WHERE id = $1::uuid
        ",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to query source: {e}")))?;

    assert!(
        row.0.is_none(),
        "last_sync_at should remain NULL after failed sync"
    );
    assert!(
        row.1.is_none(),
        "last_sync_commit should remain NULL after failed sync"
    );

    Ok(())
}

/// Disabled sources (sync_enabled = false) should not be loaded by the sync cycle.
#[sinex_test]
async fn test_sync_cycle_ignores_disabled_sources(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool.clone();
    let source_id = Uuid::now_v7();

    // Insert a disabled source.
    sqlx::query(
        r"
        INSERT INTO sinex_schemas.gitops_schema_sources
            (id, repository_url, branch, path_pattern, sync_enabled, sync_frequency_minutes)
        VALUES ($1::uuid, $2, $3, $4, false, 0)
        ",
    )
    .bind(source_id)
    .bind("https://github.com/org/disabled-repo.git")
    .bind("main")
    .bind("schemas/**/*.json")
    .execute(&pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to insert test source: {e}")))?;

    let work_dir = tempfile::tempdir()?;
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service = sinex_ingestd::gitops::GitOpsSyncService::new(
        pool.clone(),
        work_dir.path().to_path_buf(),
        shutdown,
    );

    let stats = service.run_sync_cycle().await?;

    assert_eq!(
        stats.sources_checked, 0,
        "Disabled source should not be loaded/checked"
    );
    assert_eq!(stats.sources_synced, 0);

    Ok(())
}

/// A source that was recently synced and is within its sync_frequency window
/// should be skipped by the sync cycle.
#[sinex_test]
async fn test_sync_cycle_skips_recently_synced_source(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool.clone();
    let source_id = Uuid::now_v7();

    // Insert a source with a very recent last_sync_at.
    sqlx::query(
        r"
        INSERT INTO sinex_schemas.gitops_schema_sources
            (id, repository_url, branch, path_pattern, sync_enabled,
             sync_frequency_minutes, last_sync_at, last_sync_commit)
        VALUES ($1::uuid, $2, $3, $4, true, 60, NOW(), 'abc123')
        ",
    )
    .bind(source_id)
    .bind("https://github.com/org/recently-synced.git")
    .bind("main")
    .bind("schemas/**/*.json")
    .execute(&pool)
    .await
    .map_err(|e| SinexError::database(format!("Failed to insert test source: {e}")))?;

    let work_dir = tempfile::tempdir()?;
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service = sinex_ingestd::gitops::GitOpsSyncService::new(
        pool.clone(),
        work_dir.path().to_path_buf(),
        shutdown,
    );

    let stats = service.run_sync_cycle().await?;

    assert_eq!(
        stats.sources_checked, 1,
        "Source should be loaded and checked"
    );
    assert_eq!(
        stats.sources_skipped, 1,
        "Recently synced source should be skipped"
    );
    assert_eq!(
        stats.sources_synced, 0,
        "Recently synced source should NOT be synced again"
    );
    assert!(stats.errors.is_empty(), "No errors for skipped source");

    Ok(())
}
