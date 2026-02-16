//! Integration tests for `ServiceContainer` dependency injection
//!
//! Tests the initialization and dependency management of all services
//! including `AnalyticsService`, `ContentService`, `PkmService`, and `SearchService`.

use color_eyre::Result as EyreResult;
use sinex_gateway::ServiceContainer;
use std::env;
use std::sync::Arc;
use tempfile::TempDir;
use xtask::sandbox::sinex_test;

fn enable_replay_control_bypass() {
    env::set_var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS", "1");
}

/// Test successful initialization with valid database URL
#[sinex_test]
async fn test_service_container_initialization_success() -> TestResult<()> {
    enable_replay_control_bypass();
    // Use the development database URL from nix environment
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Create temporary directory for annex
    let temp_dir = TempDir::new()?;
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Initialize service container
    let container = ServiceContainer::new(Some(db_url)).await?;

    // Verify all services are initialized
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
async fn test_service_container_env_database_url() -> TestResult<()> {
    enable_replay_control_bypass();
    // Set DATABASE_URL in environment
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    let original_db_url = env::var("DATABASE_URL").ok();
    env::set_var("DATABASE_URL", &db_url);

    // Create temporary directory for annex
    let temp_dir = TempDir::new()?;
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Initialize service container without explicit URL
    let container = ServiceContainer::new(None).await?;

    // Verify all services are initialized
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

    // Restore original DATABASE_URL if it existed
    match original_db_url {
        Some(url) => env::set_var("DATABASE_URL", url),
        None => env::remove_var("DATABASE_URL"),
    }

    Ok(())
}

/// Test initialization fails gracefully with invalid database URL
#[sinex_test]
async fn test_service_container_invalid_database_url() -> TestResult<()> {
    enable_replay_control_bypass();
    // Use an invalid database URL
    let invalid_url = "not-a-postgres-url".to_string();

    // Create temporary directory for annex
    let temp_dir = TempDir::new().unwrap();
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Attempt to initialize service container
    let result = ServiceContainer::new(Some(invalid_url)).await;

    // Should fail with an error
    assert!(result.is_err(), "Should fail with invalid database URL");

    // Check error message without using unwrap_err (which requires Debug)
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
async fn test_service_container_no_database_url() -> TestResult<()> {
    enable_replay_control_bypass();
    // Save and clear DATABASE_URL from environment
    let original_db_url = env::var("DATABASE_URL").ok();
    env::remove_var("DATABASE_URL");

    // Create temporary directory for annex
    let temp_dir = TempDir::new().unwrap();
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Attempt to initialize service container
    let result = ServiceContainer::new(None).await;

    // Should fail with an error about missing database URL
    assert!(result.is_err(), "Should fail when no database URL provided");

    // Check error message without using unwrap_err (which requires Debug)
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

    // Restore original DATABASE_URL if it existed
    if let Some(url) = original_db_url {
        env::set_var("DATABASE_URL", url);
    }
    Ok(())
}

/// Test service container cloning
#[sinex_test]
async fn test_service_container_clone() -> TestResult<()> {
    enable_replay_control_bypass();
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Create temporary directory for annex
    let temp_dir = TempDir::new()?;
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Initialize service container
    let container = ServiceContainer::new(Some(db_url)).await?;

    // Clone the container
    let cloned = container.clone();

    // Verify cloned services are the same (Arc pointers)
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
async fn test_service_container_annex_path_config() -> TestResult<()> {
    enable_replay_control_bypass();
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Test with custom annex path
    let custom_dir = TempDir::new()?;
    let custom_path = custom_dir
        .path()
        .to_str()
        .expect("path should be valid UTF-8");
    env::set_var("SINEX_ANNEX_PATH", custom_path);

    // Initialize service container
    let container = ServiceContainer::new(Some(db_url.clone())).await?;

    // Verify service is initialized
    assert!(
        Arc::strong_count(&container.content) > 0,
        "Content service should be initialized"
    );

    // Test with default annex path
    env::remove_var("SINEX_ANNEX_PATH");
    let container2 = ServiceContainer::new(Some(db_url)).await?;

    // Should use default path /tmp/sinex-annex
    assert!(
        Arc::strong_count(&container2.content) > 0,
        "Content service should be initialized with default path"
    );

    Ok(())
}

/// Test concurrent service container initialization
#[sinex_test]
async fn test_service_container_concurrent_initialization() -> TestResult<()> {
    enable_replay_control_bypass();
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Create temporary directory for annex
    let temp_dir = TempDir::new()?;
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Initialize multiple containers concurrently
    let futures = (0..5).map(|_| {
        let url = db_url.clone();
        async move { ServiceContainer::new(Some(url)).await }
    });

    let results: Vec<EyreResult<ServiceContainer>> = futures::future::join_all(futures).await;

    // All should succeed
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
async fn test_service_container_arc_references() -> TestResult<()> {
    enable_replay_control_bypass();
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    // Create temporary directory for annex
    let temp_dir = TempDir::new()?;
    env::set_var(
        "SINEX_ANNEX_PATH",
        temp_dir
            .path()
            .to_str()
            .expect("path should be valid UTF-8"),
    );

    // Initialize service container
    let container = ServiceContainer::new(Some(db_url)).await?;

    // Get initial reference counts
    let analytics_refs = Arc::strong_count(&container.analytics);
    let content_refs = Arc::strong_count(&container.content);
    let pkm_refs = Arc::strong_count(&container.pkm);
    let search_refs = Arc::strong_count(&container.search);

    // Clone individual services
    let analytics_clone = container.analytics.clone();
    let content_clone = container.content.clone();
    let pkm_clone = container.pkm.clone();
    let search_clone = container.search.clone();

    // Verify reference counts increased
    assert_eq!(Arc::strong_count(&container.analytics), analytics_refs + 1);
    assert_eq!(Arc::strong_count(&container.content), content_refs + 1);
    assert_eq!(Arc::strong_count(&container.pkm), pkm_refs + 1);
    assert_eq!(Arc::strong_count(&container.search), search_refs + 1);

    // Drop clones
    drop(analytics_clone);
    drop(content_clone);
    drop(pkm_clone);
    drop(search_clone);

    // Verify reference counts returned to original
    assert_eq!(Arc::strong_count(&container.analytics), analytics_refs);
    assert_eq!(Arc::strong_count(&container.content), content_refs);
    assert_eq!(Arc::strong_count(&container.pkm), pkm_refs);
    assert_eq!(Arc::strong_count(&container.search), search_refs);

    Ok(())
}
