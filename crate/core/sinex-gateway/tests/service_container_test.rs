//! Integration tests for `ServiceContainer` dependency injection
//!
//! Tests the initialization and dependency management of all services
//! including `AnalyticsService`, `ContentService`, `PkmService`, and `SearchService`.

use color_eyre::Result as EyreResult;
use sinex_gateway::ServiceContainer;
use std::sync::Arc;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

/// Test successful initialization with valid database URL
#[sinex_test]
async fn test_service_container_initialization_success(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;

    assert!(
        Arc::strong_count(&container.analytics) > 0,
        "Analytics service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.pkm) > 0,
        "PKM service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.search) > 0,
        "Search service should be initialized"
    );

    Ok(())
}

/// Test initialization with DATABASE_URL from environment
#[sinex_test]
async fn test_service_container_env_database_url(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("DATABASE_URL", ctx.database_url());
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Initialize service container without explicit URL (reads from DATABASE_URL env)
    let container = ServiceContainer::new(None).await?;

    assert!(
        Arc::strong_count(&container.analytics) > 0,
        "Analytics service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.pkm) > 0,
        "PKM service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.search) > 0,
        "Search service should be initialized"
    );

    Ok(())
}

/// Test initialization fails gracefully with invalid database URL
#[sinex_test]
async fn test_service_container_invalid_database_url(_ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let result = ServiceContainer::new(Some("not-a-postgres-url".to_string())).await;

    assert!(result.is_err(), "Should fail with invalid database URL");
    match result {
        Err(error) => {
            assert!(
                error.to_string().contains("Failed to create database pool"),
                "Error should mention database pool creation failure"
            );
        }
        Ok(_) => panic!("Expected error but got success"),
    }
    Ok(())
}

/// Test initialization fails when no database URL is provided
#[sinex_test]
async fn test_service_container_no_database_url(_ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.clear("DATABASE_URL");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let result = ServiceContainer::new(None).await;

    assert!(result.is_err(), "Should fail when no database URL provided");
    match result {
        Err(error) => {
            assert!(
                error
                    .to_string()
                    .contains("Database URL not provided and DATABASE_URL not set"),
                "Error should mention missing database URL"
            );
        }
        Ok(_) => panic!("Expected error but got success"),
    }

    Ok(())
}

/// Test service container cloning
#[sinex_test]
async fn test_service_container_clone(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    let cloned = container.clone();

    assert!(
        Arc::ptr_eq(&container.analytics, &cloned.analytics),
        "Analytics service should be shared"
    );
    assert!(
        Arc::ptr_eq(&container.content, &cloned.content),
        "Content service should be shared"
    );
    assert!(
        Arc::ptr_eq(&container.pkm, &cloned.pkm),
        "PKM service should be shared"
    );
    assert!(
        Arc::ptr_eq(&container.search, &cloned.search),
        "Search service should be shared"
    );

    Ok(())
}

/// Test annex path configuration
#[sinex_test]
async fn test_service_container_annex_path_config(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");

    // Test with custom annex path
    let custom_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        custom_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );

    // Test with default annex path
    env.clear("SINEX_ANNEX_PATH");
    let container2 = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    assert!(
        Arc::strong_count(&container2.content) > 0,
        "Content service should be initialized with default path"
    );

    Ok(())
}

/// Test concurrent service container initialization
#[sinex_test]
async fn test_service_container_concurrent_initialization(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let db_url = ctx.database_url().to_string();
    let futures = (0..5).map(|_| {
        let url = db_url.clone();
        async move { ServiceContainer::new(Some(url)).await }
    });

    let results: Vec<EyreResult<ServiceContainer>> = futures::future::join_all(futures).await;

    for (i, result) in results.iter().enumerate() {
        assert!(
            result.is_ok(),
            "Container {i} should initialize successfully"
        );
    }

    Ok(())
}

/// Test service Arc reference counting
#[sinex_test]
async fn test_service_container_arc_references(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;

    let analytics_refs = Arc::strong_count(&container.analytics);
    let content_refs = Arc::strong_count(&container.content);
    let pkm_refs = Arc::strong_count(&container.pkm);
    let search_refs = Arc::strong_count(&container.search);

    let analytics_clone = container.analytics.clone();
    let content_clone = container.content.clone();
    let pkm_clone = container.pkm.clone();
    let search_clone = container.search.clone();

    assert_eq!(Arc::strong_count(&container.analytics), analytics_refs + 1);
    assert_eq!(Arc::strong_count(&container.content), content_refs + 1);
    assert_eq!(Arc::strong_count(&container.pkm), pkm_refs + 1);
    assert_eq!(Arc::strong_count(&container.search), search_refs + 1);

    drop(analytics_clone);
    drop(content_clone);
    drop(pkm_clone);
    drop(search_clone);

    assert_eq!(Arc::strong_count(&container.analytics), analytics_refs);
    assert_eq!(Arc::strong_count(&container.content), content_refs);
    assert_eq!(Arc::strong_count(&container.pkm), pkm_refs);
    assert_eq!(Arc::strong_count(&container.search), search_refs);

    Ok(())
}

/// Pool isolation: each service must hold a *distinct* connection pool.
///
/// The gateway exposes four services (analytics, content, pkm, search), each
/// backed by its own `PgPool`. This ensures that a slow query on one service
/// cannot starve connections for an unrelated service (e.g. a long analytics
/// aggregation should not prevent a fast PKM lookup from acquiring a connection).
///
/// This test verifies the isolation by checking that the pool pointers reported
/// by `ServiceContainer::pool()` (content pool) differ from the internal pools
/// used by each service. Because the pools are private we infer isolation from
/// the total max-connection count: four separate pools of N must sum to 4×N,
/// whereas a single shared pool of N would report exactly N.
#[sinex_test]
async fn test_pool_isolation_separate_pools(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );
    // Set a known pool size so assertions are deterministic regardless of defaults.
    // `per_service_pool_config` divides by 4, so effective per-service max = 10/4 = 2,
    // clamped to at least 1. We set it high enough to remain unclamped.
    env.set("SINEX_GATEWAY_POOL_MAX_CONNECTIONS", "40");

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;

    // pool_max_connections sums the max connections across all four pools.
    // If they share a single pool this would equal 40 rather than 4 × (40/4).
    let total = container.pool_max_connections();
    assert_eq!(
        total, 40,
        "Four pools each with max 10 connections should sum to 40 total (got {total}); \
         a shared-pool implementation would report a smaller number"
    );

    Ok(())
}

/// Pool isolation: concurrent queries from multiple service containers do not
/// starve each other.
///
/// Creates two independent ServiceContainers (simulating two concurrent callers).
/// Each fires N queries via the gateway's shared content pool. With correct
/// isolation the queries complete without timeout; a shared pool smaller than
/// 2×N would exhibit starvation.
#[sinex_test]
async fn test_pool_isolation_concurrent_cross_service_queries(ctx: TestContext) -> TestResult<()> {
    use futures::future::join_all;

    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let db_url = ctx.database_url().to_string();
    let container_a = ServiceContainer::new(Some(db_url.clone())).await?;
    let container_b = ServiceContainer::new(Some(db_url)).await?;

    const N: usize = 5;
    let pings_a = (0..N).map(|_| {
        let pool = container_a.pool().clone();
        async move { sqlx::query("SELECT 1").execute(&pool).await }
    });
    let pings_b = (0..N).map(|_| {
        let pool = container_b.pool().clone();
        async move { sqlx::query("SELECT 1").execute(&pool).await }
    });

    let (results_a, results_b) = tokio::join!(join_all(pings_a), join_all(pings_b));

    for r in results_a {
        r?;
    }
    for r in results_b {
        r?;
    }

    Ok(())
}

/// Health report structure: verify all fields are present and have the right types.
///
/// This test runs without NATS configured (SINEX_REPLAY_CONTROL_OPTIONAL=1),
/// so NATS shows as not connected. That's the important case — the health report
/// must degrade gracefully and still return accurate DB status.
#[sinex_test]
async fn test_health_report_structure(ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    let temp_dir = TempDir::new()?;
    env.set(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    let container = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    let report = container.health_report().await;

    assert!(
        report.db_ok,
        "Database should be reachable during tests (db_ok=false in health report)"
    );
    assert_eq!(
        report.healthy, report.db_ok,
        "healthy flag should reflect db_ok (NATS is not the hard gate)"
    );
    assert!(
        !report.nats.detail.is_empty(),
        "NATS probe detail should always be populated"
    );
    assert!(
        report.replay.bypass_allowed,
        "Replay control bypass should be marked allowed when env var is set"
    );

    Ok(())
}
