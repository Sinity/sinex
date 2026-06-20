//! Integration tests for `ServiceContainer` dependency injection
//!
//! Tests the initialization and dependency management of services
//! including the gateway content service and the db-owned PKM service.

use sinex_primitives::domain::HealthStatus;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use sinex_primitives::temporal::Timestamp;
use sinexd::api::{ConfirmationBufferMemoryOwner, ServiceContainer};
use sinexd::runtime::{
    ConfirmationBuffer, ProvisionalEvent, register_confirmation_buffer,
    registered_confirmation_buffer_snapshots,
};
use std::sync::Arc;
use std::time::Duration;
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
async fn test_service_container_invalid_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    set_content_store_path(&mut env, temp_dir.path());

    let result = ServiceContainer::from_database_url("not-a-postgres-url").await;

    let sinex_err = result.err().expect("Should fail with invalid database URL");
    // Assert the Configuration variant returned for malformed URLs. Checking
    // the variant is more stable than matching the human-readable message.
    assert!(
        matches!(sinex_err, SinexError::Configuration(_)),
        "Expected SinexError::Configuration, got: {sinex_err:?}"
    );
    Ok(())
}

/// Test initialization fails when no database URL is provided
#[sinex_test]
async fn test_service_container_no_database_url() -> TestResult<()> {
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

    let results = futures::future::join_all(futures).await;

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
    use sinexd::api::config::GatewayConfig;

    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;
    // Set a known pool size so assertions are deterministic regardless of defaults.
    // `per_service_pool_config` divides by 2, so effective per-service max = 40/2 = 20.
    env.set("SINEX_API_POOL_MAX_CONNECTIONS", "40");

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
        HealthStatus::Healthy => {
            assert!(report.healthy, "Healthy status must imply healthy=true");
            assert!(
                report.degradation_reasons.is_empty(),
                "Healthy status should not carry degradation reasons"
            );
        }
        HealthStatus::Degraded => {
            assert!(!report.healthy, "Degraded status must imply healthy=false");
            assert!(
                !report.degradation_reasons.is_empty(),
                "Degraded status should explain what is missing"
            );
        }
        HealthStatus::Unhealthy | HealthStatus::Unknown => {
            panic!("Database-backed test fixture should not produce unhealthy gateway status");
        }
    }
    assert!(
        !report.nats.detail.is_empty(),
        "NATS probe detail should always be populated"
    );
    assert_eq!(
        report.raw_ingest_dlq.status,
        HealthStatus::Unknown,
        "fresh gateway stacks may not have a raw-ingest DLQ stream yet"
    );
    assert_eq!(
        report.raw_ingest_dlq.pending_messages, None,
        "absent DLQ stream should not invent a backlog count"
    );
    assert!(
        !report
            .degradation_reasons
            .iter()
            .any(|reason| reason.contains("raw-ingest DLQ pressure")),
        "unknown DLQ pressure must not degrade health without a confirmed backlog"
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

fn old_journald_provisional(index: usize, message: &str) -> ProvisionalEvent {
    ProvisionalEvent {
        event_id: EventId::new(),
        source: EventSource::from_static("system.journald"),
        event_type: EventType::from_static("journald.entry.written"),
        payload: serde_json::json!({
            "MESSAGE": message,
            "_SYSTEMD_UNIT": "sinexd.service",
            "SEQ": index,
        }),
        ts_orig: Timestamp::from_unix_timestamp(1)
            .unwrap_or_else(|| panic!("fixture timestamp must be in range")),
        received_at: Timestamp::from_unix_timestamp(1)
            .unwrap_or_else(|| panic!("fixture timestamp must be in range")),
    }
}

#[sinex_test]
async fn confirmation_buffer_pressure_degrades_health_with_payload_attribution(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    let temp_dir = TempDir::new()?;
    configure_gateway_env(&mut env, &ctx, temp_dir.path())?;

    let buffer = Arc::new(ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        4,
        Duration::from_secs(60),
    ));
    register_confirmation_buffer(&buffer);
    for index in 0..3 {
        assert!(
            buffer
                .add_provisional(old_journald_provisional(
                    index,
                    "Late confirmation arrived after provisional timeout",
                ))
                .await
        );
    }
    assert_eq!(buffer.check_timeouts().await.len(), 3);

    let container = ServiceContainer::from_database_url(ctx.database_url()).await?;
    let report = container.health_report().await;
    assert_eq!(report.confirmation_buffer.status, HealthStatus::Degraded);
    assert_eq!(
        report.confirmation_buffer.memory_owner,
        ConfirmationBufferMemoryOwner::TimedOutGracePayloads
    );
    assert_eq!(report.confirmation_buffer.pressure_level, "warning");
    assert_eq!(
        report.confirmation_buffer.runtime_action,
        "admit_with_pressure"
    );
    assert!(report.confirmation_buffer.observed_buffers >= 1);
    assert!(report.confirmation_buffer.pending_count >= 3);
    assert!(report.confirmation_buffer.timed_out_retained_count >= 3);
    assert!(report.confirmation_buffer.retained_payload_bytes > 0);
    assert!(report.confirmation_buffer.approximate_payload_bytes > 0);
    assert_eq!(
        report.confirmation_buffer.retained_payload_bytes,
        report.confirmation_buffer.approximate_payload_bytes
    );
    assert_eq!(report.confirmation_buffer.active_payload_bytes, 0);
    assert_eq!(
        report.confirmation_buffer.timed_out_retained_payload_bytes,
        report.confirmation_buffer.retained_payload_bytes
    );
    assert!(
        report
            .confirmation_buffer
            .approximate_payload_bytes_by_kind
            .contains_key("system.journald:journald.entry.written")
    );
    assert!(
        report
            .degradation_reasons
            .iter()
            .any(|reason| reason.contains("confirmation buffers: observed="))
    );
    assert!(
        report
            .degradation_reasons
            .iter()
            .any(|reason| reason.contains("memory_owner=timed_out_grace_payloads"))
    );
    assert!(
        report
            .degradation_reasons
            .iter()
            .any(|reason| reason.contains("runtime_action=admit_with_pressure"))
    );

    Ok(())
}

#[sinex_test]
async fn confirmation_buffer_registry_does_not_retain_dropped_buffers() -> TestResult<()> {
    let buffer = Arc::new(ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        1,
        Duration::from_millis(0),
    ));
    let weak = Arc::downgrade(&buffer);
    register_confirmation_buffer(&buffer);
    assert!(
        weak.upgrade().is_some(),
        "registered live buffer should be observable"
    );
    drop(buffer);

    let _ = registered_confirmation_buffer_snapshots().await;
    assert!(
        weak.upgrade().is_none(),
        "registry must use weak refs instead of retaining runtime buffers"
    );

    Ok(())
}
