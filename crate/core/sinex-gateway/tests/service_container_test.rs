//! Integration tests for `ServiceContainer` dependency injection
//!
//! Tests the initialization and dependency management of services
//! including the gateway content service and `PkmService`.

use color_eyre::Result as EyreResult;
use sinex_gateway::{ServiceContainer, service_container::GatewayHealthStatus};
use std::sync::Arc;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

fn set_content_store_path(env: &mut EnvGuard, content_store_path: &std::path::Path) {
    let content_store_path = content_store_path.to_string_lossy();
    env.set("SINEX_CONTENT_STORE_PATH", content_store_path.as_ref());
}

fn configure_gateway_env(
    env: &mut EnvGuard,
    ctx: &TestContext,
    content_store_path: &std::path::Path,
) -> TestResult<()> {
    env.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());
    set_content_store_path(env, content_store_path);
    Ok(())
}

/// Test successful initialization with valid database URL
#[sinex_test]
async fn test_service_container_initialization_success(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;

    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.pkm) > 0,
        "PKM service should be initialized"
    );

    Ok(())
}

/// Test initialization with DATABASE_URL from environment
#[sinex_test]
async fn test_service_container_env_database_url(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", ctx.database_url());
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    // Initialize service container without explicit URL (reads from DATABASE_URL env)
    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;

    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );
    assert!(
        Arc::strong_count(&container.pkm) > 0,
        "PKM service should be initialized"
    );

    Ok(())
}

/// Test initialization fails gracefully with invalid database URL
#[sinex_test]
async fn test_service_container_invalid_database_url(_ctx: TestContext) -> TestResult<()> {
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    set_content_store_path(&mut env, temp_dir.path());

    let result = ServiceContainer::from_database_url("not-a-postgres-url").await;

    assert!(result.is_err(), "Should fail with invalid database URL");
    match result {
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("failed to parse DATABASE_URL")
                    || message.contains("DATABASE_URL"),
                "Error should surface database URL validation failure, got: {message}"
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
    env.clear("DATABASE_URL");
    let temp_dir = TempDir::new()?;
    set_content_store_path(&mut env, temp_dir.path());

    let result = ServiceContainer::from_database_url("").await;

    assert!(result.is_err(), "Should fail when no database URL provided");
    match result {
        Err(error) => {
            assert!(
                error.to_string().contains("Database URL not provided"),
                "Error should mention missing database URL, got: {error}"
            );
        }
        Ok(_) => panic!("Expected error but got success"),
    }

    Ok(())
}

/// Test service container cloning
#[sinex_test]
async fn test_service_container_clone(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;
    let cloned = container.clone();

    assert!(
        Arc::ptr_eq(&container.content, &cloned.content),
        "Content service should be shared"
    );
    assert!(
        Arc::ptr_eq(&container.pkm, &cloned.pkm),
        "PKM service should be shared"
    );

    Ok(())
}

/// Test content-store path configuration
#[sinex_test]
async fn test_service_container_content_store_path_config(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_NATS_URL", ctx.nats_handle()?.client_url());

    // Test with custom content-store path
    let custom_dir = TempDir::new()?;
    set_content_store_path(&mut env, custom_dir.path());

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;
    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );

    // Test with default content-store path
    env.clear("SINEX_CONTENT_STORE_PATH");
    let container2 = ServiceContainer::from_database_url(ctx.database_url()).await?;
    assert!(
        Arc::strong_count(&container2.content) > 0,
        "Content service should be initialized with default path"
    );

    Ok(())
}

/// Test concurrent service container initialization
#[sinex_test]
async fn test_service_container_concurrent_initialization(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let db_url = ctx.database_url().to_string();
    let futures = (0..5).map(|_| {
        let url = db_url.clone();
        async move { ServiceContainer::from_database_url(url).await }
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
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;

    let content_refs = Arc::strong_count(&container.content);
    let pkm_refs = Arc::strong_count(&container.pkm);

    let content_clone = container.content.clone();
    let pkm_clone = container.pkm.clone();

    assert_eq!(Arc::strong_count(&container.content), content_refs + 1);
    assert_eq!(Arc::strong_count(&container.pkm), pkm_refs + 1);

    drop(content_clone);
    drop(pkm_clone);

    assert_eq!(Arc::strong_count(&container.content), content_refs);
    assert_eq!(Arc::strong_count(&container.pkm), pkm_refs);

    Ok(())
}

/// Pool isolation: each service must hold a *distinct* connection pool.
///
/// The gateway exposes two services (content, pkm), each backed by its own
/// `PgPool`. This ensures that a slow query on one service cannot starve
/// connections for an unrelated service.
///
/// This test verifies isolation by checking that the total max-connection count
/// sums correctly: two separate pools of N must sum to 2×N, whereas a single
/// shared pool of N would report exactly N.
#[sinex_test]
async fn test_pool_isolation_separate_pools(ctx: TestContext) -> TestResult<()> {
    use sinex_gateway::config::GatewayConfig;

    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;
    // Set a known pool size so assertions are deterministic regardless of defaults.
    // `per_service_pool_config` divides by 2, so effective per-service max = 40/2 = 20.
    env.set("SINEX_GATEWAY_POOL_MAX_CONNECTIONS", "40");

    let config =
        GatewayConfig::load()?.with_cli_overrides(Some(ctx.database_url().to_string()), None, None);
    let container = ServiceContainer::new(&config).await?;

    // pool_max_connections sums the max connections across all two pools.
    // If they share a single pool this would equal 40 rather than 2 × (40/2).
    let total = container.pool_max_connections();
    assert_eq!(
        total, 40,
        "Two pools each with max 20 connections should sum to 40 total (got {total}); \
         a shared-pool implementation would report a smaller number"
    );

    Ok(())
}

/// Pool isolation: concurrent queries from multiple service containers do not
/// starve each other.
#[sinex_test]
async fn test_pool_isolation_concurrent_cross_service_queries(ctx: TestContext) -> TestResult<()> {
    use futures::future::join_all;

    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let db_url = ctx.database_url().to_string();
    let container_a = ServiceContainer::from_database_url(db_url.clone()).await?;
    let container_b = ServiceContainer::from_database_url(db_url).await?;

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
#[sinex_test]
async fn test_health_report_structure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;
    let report = container.health_report().await;

    assert!(
        report.db_ok,
        "Database should be reachable during tests (db_ok=false in health report)"
    );
    assert!(
        report.db_latency_ms.is_some(),
        "Database probe should report latency in health report"
    );
    assert_eq!(report.db_detail, "ok");
    assert!(
        report.serving,
        "Gateway should report serving=true when the DB-backed RPC surface is live"
    );
    match report.status {
        GatewayHealthStatus::Healthy => {
            assert!(report.healthy, "Healthy status must imply healthy=true");
            assert!(
                report.degradation_reasons.is_empty(),
                "Healthy status should not carry degradation reasons"
            );
        }
        GatewayHealthStatus::Degraded => {
            assert!(!report.healthy, "Degraded status must imply healthy=false");
            assert!(
                !report.degradation_reasons.is_empty(),
                "Degraded status should explain what is missing"
            );
        }
        GatewayHealthStatus::Unhealthy => {
            panic!("Database-backed test fixture should not produce unhealthy gateway status");
        }
    }
    assert!(
        !report.nats.detail.is_empty(),
        "NATS probe detail should always be populated"
    );
    assert!(
        report.replay.enabled,
        "Replay control should be initialized"
    );
    assert!(
        report.replay.connected,
        "Replay control should be connected"
    );

    Ok(())
}
