use anyhow::Result;
use serde_json::json;
use sinex_annex::{
    MultiLocationCoordinator, StorageLocation, StorageHealthMonitor, HealthMonitorConfig,
    AlertSeverity, HealthAlertType
};
use crate::common::prelude::*;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::time::sleep;

#[sinex_test]
async fn test_multi_location_coordinator_creation(ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let coordinator = MultiLocationCoordinator::new(temp_dir.path().to_path_buf());
    
    // Test basic creation
    assert_eq!(coordinator.get_all_status().len(), 0);
    
    Ok(())
}

#[sinex_test]
async fn test_storage_location_management(ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let mut coordinator = MultiLocationCoordinator::new(temp_dir.path().to_path_buf());
    
    // Initialize a git repository in the temp directory
    tokio::process::Command::new("git")
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .await?;
    
    let location = StorageLocation {
        id: "test-location-1".to_string(),
        description: "Test Storage Location".to_string(),
        remote_name: "test-remote".to_string(),
        url: "https://example.com/test-repo.git".to_string(),
        priority: 8,
        max_capacity_gb: Some(100),
        cost: 200,
        enabled: true,
        auto_sync: true,
    };
    
    // Add location
    coordinator.add_location(location.clone()).await?;
    
    // Verify location was added
    let status = coordinator.get_all_status();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].location_id, "test-location-1");
    assert!(!status[0].is_available); // Should start as unavailable
    
    // Remove location
    coordinator.remove_location("test-location-1").await?;
    
    // Verify location was removed
    let status = coordinator.get_all_status();
    assert_eq!(status.len(), 0);
    
    Ok(())
}

#[sinex_test]
async fn test_best_location_selection(ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let mut coordinator = MultiLocationCoordinator::new(temp_dir.path().to_path_buf());
    
    // Initialize git repository
    tokio::process::Command::new("git")
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .await?;
    
    // Add multiple locations with different priorities
    let high_priority_location = StorageLocation {
        id: "high-priority".to_string(),
        description: "High Priority Location".to_string(),
        remote_name: "high-remote".to_string(),
        url: "https://example.com/high-repo.git".to_string(),
        priority: 9,
        max_capacity_gb: Some(200),
        cost: 100,
        enabled: true,
        auto_sync: true,
    };
    
    let low_priority_location = StorageLocation {
        id: "low-priority".to_string(),
        description: "Low Priority Location".to_string(),
        remote_name: "low-remote".to_string(),
        url: "https://example.com/low-repo.git".to_string(),
        priority: 3,
        max_capacity_gb: Some(50),
        cost: 300,
        enabled: true,
        auto_sync: true,
    };
    
    coordinator.add_location(high_priority_location).await?;
    coordinator.add_location(low_priority_location).await?;
    
    // Initially, no location should be considered "best" since they're not available
    let best = coordinator.get_best_location_for_storage();
    assert!(best.is_none());
    
    // Note: In real tests with actual git remotes, we could test location availability
    // and health score calculations, but that requires network access
    
    Ok(())
}

#[sinex_test]
async fn test_health_monitor_creation_and_metrics(ctx: TestContext) -> TestResult {
    let config = HealthMonitorConfig {
        check_interval_seconds: 60,
        disk_warning_threshold: 0.8,
        disk_critical_threshold: 0.95,
        min_replication_factor: 2.0,
        min_healthy_locations: 2,
        alert_retention_hours: 24,
        auto_healing_enabled: true,
    };
    
    let monitor = StorageHealthMonitor::new(config);
    
    // Test initial state
    assert!(monitor.get_current_metrics().is_none());
    assert_eq!(monitor.get_active_alerts().len(), 0);
    assert_eq!(monitor.get_metrics_history().len(), 0);
    
    Ok(())
}

#[sinex_test]
async fn test_health_monitor_with_coordinator(ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let coordinator = MultiLocationCoordinator::new(temp_dir.path().to_path_buf());
    
    let config = HealthMonitorConfig::default();
    let mut monitor = StorageHealthMonitor::new(config);
    monitor.set_coordinator(coordinator);
    
    // Perform health check
    let metrics = monitor.perform_health_check().await?;
    
    // With no locations, metrics should reflect empty system
    assert_eq!(metrics.total_locations, 0);
    assert_eq!(metrics.available_locations, 0);
    assert_eq!(metrics.healthy_locations, 0);
    assert_eq!(metrics.replication_factor, 0.0);
    
    Ok(())
}

#[sinex_test]
async fn test_health_report_generation(ctx: TestContext) -> TestResult {
    let config = HealthMonitorConfig::default();
    let monitor = StorageHealthMonitor::new(config);
    
    let report = monitor.generate_health_report();
    
    // Verify report contains expected sections
    assert!(report.contains("Storage Health Report"));
    assert!(report.contains("No active alerts"));
    
    // Report should be non-empty and formatted
    assert!(report.len() > 100);
    assert!(report.contains("==="));
    
    Ok(())
}

#[sinex_test]
async fn test_multi_location_database_integration(ctx: TestContext) -> TestResult {
    // Apply the multi-location migration
    sqlx::migrate!("../../../migrations").run(ctx.pool()).await?;
    
    // Test database schema was created
    let tables = sqlx::query!(
        "SELECT table_name FROM information_schema.tables 
         WHERE table_schema = 'sinex_schemas' 
         AND table_name LIKE '%location%' OR table_name LIKE '%storage%'"
    )
    .fetch_all(ctx.pool())
    .await?;
    
    let table_names: Vec<&str> = tables.iter().map(|t| t.table_name.as_str()).collect();
    
    assert!(table_names.contains(&"storage_locations"));
    assert!(table_names.contains(&"location_status"));
    assert!(table_names.contains(&"storage_metrics"));
    
    // Test storage health summary function
    let summary = sqlx::query!("SELECT * FROM get_storage_health_summary()")
        .fetch_one(ctx.pool())
        .await?;
    
    assert_eq!(summary.total_locations.unwrap_or(0), 0);
    assert_eq!(summary.available_locations.unwrap_or(0), 0);
    
    Ok(())
}

#[sinex_test]
async fn test_storage_location_database_operations(ctx: TestContext) -> TestResult {
    // Apply migrations
    sqlx::migrate!("../../../migrations").run(ctx.pool()).await?;
    
    // Insert a test storage location
    sqlx::query!(
        "INSERT INTO sinex_schemas.storage_locations 
         (id, description, remote_name, url, priority, max_capacity_gb, cost)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        "test-db-location",
        "Test Database Location",
        "test-db-remote", 
        "https://example.com/test.git",
        7i32,
        500i64,
        150i32
    )
    .execute(ctx.pool())
    .await?;
    
    // Insert location status
    sqlx::query!(
        "INSERT INTO sinex_schemas.location_status 
         (location_id, is_available, health_score, disk_usage_gb, file_count)
         VALUES ($1, $2, $3, $4, $5)",
        "test-db-location",
        true,
        0.85f32,
        125.5f64,
        1250i64
    )
    .execute(ctx.pool())
    .await?;
    
    // Test health summary with data
    let summary = sqlx::query!("SELECT * FROM get_storage_health_summary()")
        .fetch_one(ctx.pool())
        .await?;
    
    assert_eq!(summary.total_locations.unwrap(), 1);
    assert_eq!(summary.available_locations.unwrap(), 1);
    assert_eq!(summary.healthy_locations.unwrap(), 1); // health_score > 0.7
    
    // Test location retrieval
    let location = sqlx::query!(
        "SELECT * FROM sinex_schemas.storage_locations WHERE id = $1",
        "test-db-location"
    )
    .fetch_one(ctx.pool())
    .await?;
    
    assert_eq!(location.id, "test-db-location");
    assert_eq!(location.description, "Test Database Location");
    assert_eq!(location.priority, 7);
    assert!(location.enabled);
    
    Ok(())
}

#[sinex_test]
async fn test_health_alerts_database_operations(ctx: TestContext) -> TestResult {
    // Apply migrations
    sqlx::migrate!("../../../migrations").run(ctx.pool()).await?;
    
    // Insert a test location first
    sqlx::query!(
        "INSERT INTO sinex_schemas.storage_locations 
         (id, description, remote_name, url, priority)
         VALUES ($1, $2, $3, $4, $5)",
        "alert-test-location",
        "Alert Test Location",
        "alert-test-remote",
        "https://example.com/alert-test.git",
        5i32
    )
    .execute(ctx.pool())
    .await?;
    
    // Insert health alerts
    sqlx::query!(
        "INSERT INTO sinex_schemas.health_alerts 
         (alert_type, location_id, message, severity, auto_resolved)
         VALUES ($1, $2, $3, $4, $5)",
        "LocationUnavailable",
        "alert-test-location",
        "Test location is unavailable",
        "Critical",
        false
    )
    .execute(ctx.pool())
    .await?;
    
    sqlx::query!(
        "INSERT INTO sinex_schemas.health_alerts 
         (alert_type, message, severity, auto_resolved, resolved_at)
         VALUES ($1, $2, $3, $4, $5)",
        "ReplicationFactorLow",
        "System replication factor is low",
        "Warning", 
        true,
        Some(chrono::Utc::now())
    )
    .execute(ctx.pool())
    .await?;
    
    // Test querying active alerts
    let active_alerts = sqlx::query!(
        "SELECT * FROM sinex_schemas.health_alerts WHERE auto_resolved = FALSE"
    )
    .fetch_all(ctx.pool())
    .await?;
    
    assert_eq!(active_alerts.len(), 1);
    assert_eq!(active_alerts[0].alert_type, "LocationUnavailable");
    assert_eq!(active_alerts[0].severity, "Critical");
    
    // Test querying resolved alerts
    let resolved_alerts = sqlx::query!(
        "SELECT * FROM sinex_schemas.health_alerts WHERE auto_resolved = TRUE"
    )
    .fetch_all(ctx.pool())
    .await?;
    
    assert_eq!(resolved_alerts.len(), 1);
    assert_eq!(resolved_alerts[0].alert_type, "ReplicationFactorLow");
    
    Ok(())
}

#[sinex_test]
async fn test_sync_errors_tracking(ctx: TestContext) -> TestResult {
    // Apply migrations
    sqlx::migrate!("../../../migrations").run(ctx.pool()).await?;
    
    // Insert test location
    sqlx::query!(
        "INSERT INTO sinex_schemas.storage_locations 
         (id, description, remote_name, url, priority)
         VALUES ($1, $2, $3, $4, $5)",
        "sync-error-location",
        "Sync Error Test Location",
        "sync-error-remote",
        "https://example.com/sync-error.git",
        6i32
    )
    .execute(ctx.pool())
    .await?;
    
    // Insert sync errors
    sqlx::query!(
        "INSERT INTO sinex_schemas.sync_errors 
         (location_id, error_type, message, retry_count)
         VALUES ($1, $2, $3, $4)",
        "sync-error-location",
        "NetworkTimeout",
        "Connection timed out after 30 seconds",
        2i32
    )
    .execute(ctx.pool())
    .await?;
    
    sqlx::query!(
        "INSERT INTO sinex_schemas.sync_errors 
         (location_id, error_type, message, retry_count)
         VALUES ($1, $2, $3, $4)",
        "sync-error-location",
        "AuthenticationFailure", 
        "SSH key authentication failed",
        0i32
    )
    .execute(ctx.pool())
    .await?;
    
    // Query sync errors for location
    let errors = sqlx::query!(
        "SELECT * FROM sinex_schemas.sync_errors 
         WHERE location_id = $1 
         ORDER BY timestamp DESC",
        "sync-error-location"
    )
    .fetch_all(ctx.pool())
    .await?;
    
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].error_type, "AuthenticationFailure"); // Most recent
    assert_eq!(errors[1].error_type, "NetworkTimeout");
    assert_eq!(errors[1].retry_count, 2);
    
    Ok(())
}

#[sinex_test] 
async fn test_storage_metrics_time_series(ctx: TestContext) -> TestResult {
    // Apply migrations
    sqlx::migrate!("../../../migrations").run(ctx.pool()).await?;
    
    // Insert time-series metrics data
    let base_time = chrono::Utc::now();
    
    for i in 0..5 {
        let timestamp = base_time - chrono::Duration::hours(i);
        
        sqlx::query!(
            "INSERT INTO sinex_schemas.storage_metrics 
             (total_locations, available_locations, healthy_locations, 
              total_capacity_gb, used_capacity_gb, replication_factor, avg_health_score, recorded_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            3i32,
            (3 - i / 2) as i32, // Simulate degrading availability
            (2 - i / 3) as i32, // Simulate degrading health
            1000.0f64,
            (500.0 + i as f64 * 50.0), // Simulate increasing usage
            (3.0 - i as f32 * 0.2), // Simulate decreasing replication
            (0.9 - i as f32 * 0.1), // Simulate decreasing health score
            timestamp
        )
        .execute(ctx.pool())
        .await?;
    }
    
    // Query recent metrics
    let recent_metrics = sqlx::query!(
        "SELECT * FROM sinex_schemas.storage_metrics 
         ORDER BY recorded_at DESC 
         LIMIT 3"
    )
    .fetch_all(ctx.pool())
    .await?;
    
    assert_eq!(recent_metrics.len(), 3);
    assert_eq!(recent_metrics[0].total_locations, 3);
    
    // Test time-based querying (TimescaleDB feature)
    let metrics_last_2_hours = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.storage_metrics 
         WHERE recorded_at > NOW() - INTERVAL '2 hours'"
    )
    .fetch_one(ctx.pool())
    .await?;
    
    assert!(metrics_last_2_hours.count.unwrap() >= 2);
    
    Ok(())
}

#[sinex_test]
async fn test_cleanup_functions(ctx: TestContext) -> TestResult {
    // Apply migrations
    sqlx::migrate!("../../../migrations").run(ctx.pool()).await?;
    
    // Insert old resolved alert
    sqlx::query!(
        "INSERT INTO sinex_schemas.health_alerts 
         (alert_type, message, severity, auto_resolved, resolved_at)
         VALUES ($1, $2, $3, $4, $5)",
        "TestAlert",
        "Old resolved alert",
        "Warning",
        true,
        Some(chrono::Utc::now() - chrono::Duration::hours(72)) // 3 days ago
    )
    .execute(ctx.pool())
    .await?;
    
    // Insert recent resolved alert
    sqlx::query!(
        "INSERT INTO sinex_schemas.health_alerts 
         (alert_type, message, severity, auto_resolved, resolved_at)
         VALUES ($1, $2, $3, $4, $5)",
        "TestAlert",
        "Recent resolved alert",
        "Info",
        true,
        Some(chrono::Utc::now() - chrono::Duration::hours(12)) // 12 hours ago
    )
    .execute(ctx.pool())
    .await?;
    
    // Test cleanup function
    let deleted_count = sqlx::query!("SELECT cleanup_old_health_alerts() as deleted")
        .fetch_one(ctx.pool())
        .await?;
    
    assert_eq!(deleted_count.deleted.unwrap(), 1); // Should delete only the old one
    
    // Verify only recent alert remains
    let remaining_alerts = sqlx::query!(
        "SELECT COUNT(*) as count FROM sinex_schemas.health_alerts WHERE auto_resolved = TRUE"
    )
    .fetch_one(ctx.pool())
    .await?;
    
    assert_eq!(remaining_alerts.count.unwrap(), 1);
    
    Ok(())
}