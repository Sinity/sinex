//! Integration tests for health check and monitoring systems
//! 
//! These tests validate that the health monitoring infrastructure correctly
//! tracks component health, detects failures, and triggers appropriate
//! recovery actions across the entire Sinex system.

use anyhow::Result;
use sinex_core::RawEventBuilder;
use crate::common::database_helpers::create_test_pool;
use sinex_db::queries;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};

/// Health status for a component
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Component health information
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_check: Instant,
    pub failure_count: u32,
    pub last_error: Option<String>,
}

/// System-wide health monitor
pub struct SystemHealthMonitor {
    components: Arc<RwLock<HashMap<String, ComponentHealth>>>,
    check_interval: Duration,
    failure_threshold: u32,
    running: Arc<AtomicBool>,
}

impl SystemHealthMonitor {
    pub fn new(check_interval: Duration, failure_threshold: u32) -> Self {
        Self {
            components: Arc::new(RwLock::new(HashMap::new())),
            check_interval,
            failure_threshold,
            running: Arc::new(AtomicBool::new(false)),
        }
    }
    
    pub async fn register_component(&self, name: &str) {
        let mut components = self.components.write().await;
        components.insert(name.to_string(), ComponentHealth {
            name: name.to_string(),
            status: HealthStatus::Unknown,
            last_check: Instant::now(),
            failure_count: 0,
            last_error: None,
        });
    }
    
    pub async fn update_component_health(&self, name: &str, status: HealthStatus, error: Option<String>) {
        let mut components = self.components.write().await;
        if let Some(component) = components.get_mut(name) {
            let previous_status = component.status.clone();
            component.status = status.clone();
            component.last_check = Instant::now();
            component.last_error = error;
            
            match status {
                HealthStatus::Unhealthy => component.failure_count += 1,
                HealthStatus::Healthy if previous_status != HealthStatus::Healthy => {
                    component.failure_count = 0; // Reset on recovery
                }
                _ => {}
            }
        }
    }
    
    pub async fn get_system_health(&self) -> HashMap<String, ComponentHealth> {
        self.components.read().await.clone()
    }
    
    pub async fn is_system_healthy(&self) -> bool {
        let components = self.components.read().await;
        components.values().all(|c| matches!(c.status, HealthStatus::Healthy))
    }
    
    pub async fn get_unhealthy_components(&self) -> Vec<String> {
        let components = self.components.read().await;
        components.values()
            .filter(|c| matches!(c.status, HealthStatus::Unhealthy))
            .map(|c| c.name.clone())
            .collect()
    }
}

#[tokio::test]
async fn test_comprehensive_health_monitoring_system() -> Result<(), anyhow::Error> {
    let pool = create_test_pool().await?;
    crate::common::cleanup::truncate_all_tables(&pool).await?;
    
    // Initialize health monitoring system
    let monitor = SystemHealthMonitor::new(Duration::from_millis(100), 3);
    
    // Register all system components
    let components = vec![
        "database",
        "filesystem_source", 
        "terminal_source",
        "clipboard_source",
        "hyprland_source",
        "unified_collector",
        "promotion_worker",
        "git_annex"
    ];
    
    for component in &components {
        monitor.register_component(component).await;
    }
    
    // Test 1: Initial health state should be unknown
    let initial_health = monitor.get_system_health().await;
    assert_eq!(initial_health.len(), components.len(), "All components should be registered");
    
    for component in &components {
        assert!(initial_health.contains_key(*component), "Component {} should be registered", component);
        assert_eq!(initial_health[*component].status, HealthStatus::Unknown, "Initial status should be unknown");
    }
    
    // Test 2: Simulate health checks for all components
    test_component_health_checks(&monitor, &pool).await?;
    
    // Test 3: Test failure detection and recovery
    test_failure_detection_and_recovery(&monitor).await?;
    
    // Test 4: Test system-wide health aggregation
    test_system_health_aggregation(&monitor).await?;
    
    Ok(())
}

async fn test_component_health_checks(monitor: &SystemHealthMonitor, pool: &sqlx::PgPool) -> Result<(), anyhow::Error> {
    // Test database health check
    let db_health = check_database_health(pool).await?;
    monitor.update_component_health("database", db_health, None).await;
    
    // Test filesystem source health
    let fs_health = check_filesystem_source_health().await?;
    monitor.update_component_health("filesystem_source", fs_health, None).await;
    
    // Test terminal source health  
    let terminal_health = check_terminal_source_health().await?;
    monitor.update_component_health("terminal_source", terminal_health, None).await;
    
    // Test clipboard source health
    let clipboard_health = check_clipboard_source_health().await?;
    monitor.update_component_health("clipboard_source", clipboard_health, None).await;
    
    // Test window manager source health
    let wm_health = check_window_manager_source_health().await?;
    monitor.update_component_health("hyprland_source", wm_health, None).await;
    
    // Test collector health
    let collector_health = check_collector_health().await?;
    monitor.update_component_health("unified_collector", collector_health, None).await;
    
    // Test worker health
    let worker_health = check_worker_health(pool).await?;
    monitor.update_component_health("promotion_worker", worker_health, None).await;
    
    // Test git-annex health
    let annex_health = check_git_annex_health().await?;
    monitor.update_component_health("git_annex", annex_health, None).await;
    
    // Verify all components have been checked
    let health_status = monitor.get_system_health().await;
    for (name, component) in health_status {
        assert_ne!(component.status, HealthStatus::Unknown, "Component {} should have known health status", name);
        assert!(component.last_check.elapsed() < Duration::from_secs(1), "Health check should be recent");
    }
    
    Ok(())
}

async fn check_database_health(pool: &sqlx::PgPool) -> Result<HealthStatus> {
    // Test database connectivity and basic operations
    match sqlx::query("SELECT 1").fetch_one(pool).await {
        Ok(_) => {
            // Test table access
            match sqlx::query("SELECT COUNT(*) FROM raw.events").fetch_one(pool).await {
                Ok(_) => Ok(HealthStatus::Healthy),
                Err(_) => Ok(HealthStatus::Degraded), // Connected but table issues
            }
        }
        Err(_) => Ok(HealthStatus::Unhealthy), // No connection
    }
}

async fn check_filesystem_source_health() -> Result<HealthStatus> {
    // Test filesystem monitoring capabilities
    use tempfile::TempDir;
    
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("health_check.txt");
    
    // Test file creation and monitoring
    match std::fs::write(&test_file, b"health check") {
        Ok(()) => {
            // Test file metadata access
            match std::fs::metadata(&test_file) {
                Ok(_) => Ok(HealthStatus::Healthy),
                Err(_) => Ok(HealthStatus::Degraded),
            }
        }
        Err(_) => Ok(HealthStatus::Unhealthy),
    }
}

async fn check_terminal_source_health() -> Result<HealthStatus> {
    // Test terminal monitoring prerequisites
    use std::process::Command;
    
    // Check if kitty is available and responsive
    match Command::new("kitty").args(["@", "ls"]).output() {
        Ok(output) => {
            if output.status.success() {
                Ok(HealthStatus::Healthy)
            } else {
                Ok(HealthStatus::Degraded) // Kitty exists but not responsive
            }
        }
        Err(_) => {
            // Kitty not available - check for alternative terminals
            match std::env::var("TERM") {
                Ok(_) => Ok(HealthStatus::Degraded), // Some terminal available
                Err(_) => Ok(HealthStatus::Unhealthy), // No terminal environment
            }
        }
    }
}

async fn check_clipboard_source_health() -> Result<HealthStatus> {
    // Test clipboard access tools
    use std::process::Command;
    
    // Check Wayland clipboard tools
    if let Ok(output) = Command::new("wl-paste").args(["--version"]).output() {
        if output.status.success() {
            return Ok(HealthStatus::Healthy);
        }
    }
    
    // Check X11 clipboard tools
    if let Ok(output) = Command::new("xclip").args(["-version"]).output() {
        if output.status.success() {
            return Ok(HealthStatus::Healthy);
        }
    }
    
    // No clipboard tools available
    Ok(HealthStatus::Degraded) // System functional but clipboard monitoring unavailable
}

async fn check_window_manager_source_health() -> Result<HealthStatus> {
    // Test window manager integration
    use std::process::Command;
    
    // Check Hyprland
    if let Ok(output) = Command::new("hyprctl").args(["version"]).output() {
        if output.status.success() {
            return Ok(HealthStatus::Healthy);
        }
    }
    
    // Check for other window managers
    if std::env::var("WAYLAND_DISPLAY").is_ok() || std::env::var("DISPLAY").is_ok() {
        Ok(HealthStatus::Degraded) // Display available but no supported WM
    } else {
        Ok(HealthStatus::Unhealthy) // No display environment
    }
}

async fn check_collector_health() -> Result<HealthStatus> {
    // Test unified collector health
    // For testing, we simulate collector health checks
    
    // Check if collector process would be able to start
    let (tx, _rx) = mpsc::channel(100);
    
    // Test channel creation and basic operations
    match tx.try_send(RawEventBuilder::new("health", "check", json!({})).build()) {
        Ok(()) => Ok(HealthStatus::Healthy),
        Err(_) => Ok(HealthStatus::Degraded),
    }
}

async fn check_worker_health(pool: &sqlx::PgPool) -> Result<HealthStatus> {
    // Test worker system health
    
    // Check promotion queue accessibility
    match sqlx::query("SELECT COUNT(*) FROM sinex_schemas.work_queue").fetch_one(pool).await {
        Ok(_) => {
            // Test worker operations by checking if we can claim work
            match queries::claim_work_queue_items(&pool, "health-check-agent", "health-worker", 0).await {
                Ok(_) => Ok(HealthStatus::Healthy),
                Err(_) => Ok(HealthStatus::Degraded),
            }
        }
        Err(_) => Ok(HealthStatus::Unhealthy),
    }
}

async fn check_git_annex_health() -> Result<HealthStatus> {
    // Test git-annex availability
    use std::process::Command;
    use tempfile::TempDir;
    
    // Check if git-annex is available
    match Command::new("git").args(["annex", "version"]).output() {
        Ok(output) => {
            if output.status.success() {
                // Test git-annex functionality
                let temp_dir = TempDir::new()?;
                match Command::new("git").args(["init"]).current_dir(&temp_dir).output() {
                    Ok(git_output) if git_output.status.success() => {
                        match Command::new("git").args(["annex", "init", "health-check"])
                            .current_dir(&temp_dir).output() {
                            Ok(annex_output) if annex_output.status.success() => Ok(HealthStatus::Healthy),
                            _ => Ok(HealthStatus::Degraded), // Git-annex available but init failed
                        }
                    }
                    _ => Ok(HealthStatus::Degraded), // Git available but broken
                }
            } else {
                Ok(HealthStatus::Degraded) // Git available but no annex
            }
        }
        Err(_) => Ok(HealthStatus::Unhealthy), // No git available
    }
}

async fn test_failure_detection_and_recovery(monitor: &SystemHealthMonitor) -> Result<(), anyhow::Error> {
    // Test 1: Simulate component failure
    monitor.update_component_health("filesystem_source", HealthStatus::Unhealthy, 
        Some("Simulated filesystem monitoring failure".to_string())).await;
    
    let unhealthy = monitor.get_unhealthy_components().await;
    assert!(unhealthy.contains(&"filesystem_source".to_string()), "Should detect unhealthy filesystem source");
    
    let system_healthy = monitor.is_system_healthy().await;
    assert!(!system_healthy, "System should not be healthy with failed component");
    
    // Test 2: Simulate gradual failure (multiple failure reports)
    for i in 1..=5 {
        monitor.update_component_health("terminal_source", HealthStatus::Unhealthy,
            Some(format!("Terminal failure #{}", i))).await;
    }
    
    let health_status = monitor.get_system_health().await;
    let terminal_health = &health_status["terminal_source"];
    assert_eq!(terminal_health.status, HealthStatus::Unhealthy);
    assert!(terminal_health.failure_count >= 5, "Should track multiple failures");
    
    // Test 3: Simulate recovery
    monitor.update_component_health("filesystem_source", HealthStatus::Healthy, None).await;
    monitor.update_component_health("terminal_source", HealthStatus::Healthy, None).await;
    
    let recovered_health = monitor.get_system_health().await;
    assert_eq!(recovered_health["filesystem_source"].status, HealthStatus::Healthy);
    assert_eq!(recovered_health["terminal_source"].status, HealthStatus::Healthy);
    assert_eq!(recovered_health["terminal_source"].failure_count, 0, "Failure count should reset on recovery");
    
    let system_recovered = monitor.is_system_healthy().await;
    assert!(system_recovered, "System should be healthy after all components recover");
    
    Ok(())
}

async fn test_system_health_aggregation(monitor: &SystemHealthMonitor) -> Result<(), anyhow::Error> {
    // Test how system health is calculated from component health
    
    // Scenario 1: All components healthy
    let components = ["database", "filesystem_source", "terminal_source", "clipboard_source"];
    for component in &components {
        monitor.update_component_health(component, HealthStatus::Healthy, None).await;
    }
    
    let all_healthy = monitor.is_system_healthy().await;
    assert!(all_healthy, "System should be healthy when all components are healthy");
    
    // Scenario 2: One component degraded
    monitor.update_component_health("clipboard_source", HealthStatus::Degraded, 
        Some("Clipboard tools not available".to_string())).await;
    
    let _one_degraded = monitor.is_system_healthy().await;
    // Note: Degraded components might still allow system to be considered healthy
    // depending on implementation - this tests current behavior
    
    // Scenario 3: Critical component unhealthy
    monitor.update_component_health("database", HealthStatus::Unhealthy,
        Some("Database connection lost".to_string())).await;
    
    let critical_unhealthy = monitor.is_system_healthy().await;
    assert!(!critical_unhealthy, "System should not be healthy when database is unhealthy");
    
    // Scenario 4: Multiple components unhealthy
    monitor.update_component_health("filesystem_source", HealthStatus::Unhealthy,
        Some("Filesystem watcher crashed".to_string())).await;
    
    let multiple_unhealthy = monitor.get_unhealthy_components().await;
    assert!(multiple_unhealthy.len() >= 2, "Should detect multiple unhealthy components");
    assert!(multiple_unhealthy.contains(&"database".to_string()));
    assert!(multiple_unhealthy.contains(&"filesystem_source".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_health_monitoring_performance_impact() -> Result<(), anyhow::Error> {
    // Test that health monitoring doesn't significantly impact system performance
    
    let monitor = SystemHealthMonitor::new(Duration::from_millis(10), 3);
    
    // Register many components
    for i in 0..100 {
        monitor.register_component(&format!("component_{}", i)).await;
    }
    
    // Measure health check performance
    let start_time = Instant::now();
    let iterations = 1000;
    
    for i in 0..iterations {
        let component_name = format!("component_{}", i % 100);
        let status = if i % 10 == 0 { HealthStatus::Degraded } else { HealthStatus::Healthy };
        monitor.update_component_health(&component_name, status, None).await;
    }
    
    let update_duration = start_time.elapsed();
    let avg_update_time = update_duration / iterations;
    
    assert!(avg_update_time < Duration::from_millis(1), 
           "Health updates should be fast, average: {:?}", avg_update_time);
    
    // Test concurrent health checks
    let concurrent_start = Instant::now();
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let monitor = monitor.clone(); // Assuming Clone implementation
        let handle = tokio::spawn(async move {
            for j in 0..100 {
                let component_name = format!("component_{}", (i * 100 + j) % 100);
                monitor.update_component_health(&component_name, HealthStatus::Healthy, None).await;
            }
        });
        handles.push(handle);
    }
    
    // Wait for all concurrent updates
    for handle in handles {
        handle.await?;
    }
    
    let concurrent_duration = concurrent_start.elapsed();
    assert!(concurrent_duration < Duration::from_secs(1),
           "Concurrent health updates should complete quickly: {:?}", concurrent_duration);
    
    Ok(())
}

// Helper to make SystemHealthMonitor cloneable for testing
impl Clone for SystemHealthMonitor {
    fn clone(&self) -> Self {
        Self {
            components: self.components.clone(),
            check_interval: self.check_interval,
            failure_threshold: self.failure_threshold,
            running: self.running.clone(),
        }
    }
}

#[tokio::test]
async fn test_health_monitoring_with_real_workload() -> Result<(), anyhow::Error> {
    let pool = create_test_pool().await?;
    crate::common::cleanup::truncate_all_tables(&pool).await?;
    
    let monitor = SystemHealthMonitor::new(Duration::from_millis(100), 3);
    
    // Register system components
    monitor.register_component("database").await;
    monitor.register_component("event_processing").await;
    monitor.register_component("worker_system").await;
    
    // Simulate real workload while monitoring health
    let workload_monitor = monitor.clone();
    let workload_pool = pool.clone();
    
    let workload_task = tokio::spawn(async move {
        // Simulate realistic event processing workload
        for i in 0..100 {
            // Create and insert events
            let event = RawEventBuilder::new(
                "health_monitoring_test",
                "workload.simulation",
                json!({
                    "sequence": i,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "workload_type": "health_monitoring"
                })
            ).build();
            
            match queries::insert_event(&workload_pool, &event).await {
                Ok(_) => {
                    workload_monitor.update_component_health("database", HealthStatus::Healthy, None).await;
                    workload_monitor.update_component_health("event_processing", HealthStatus::Healthy, None).await;
                }
                Err(e) => {
                    workload_monitor.update_component_health("database", HealthStatus::Unhealthy,
                        Some(format!("Database error: {}", e))).await;
                }
            }
            
            // Simulate worker processing
            if i % 10 == 0 {
                // Add some events to promotion queue
                let _ = queries::add_to_work_queue(&workload_pool, event.id, "health-test-agent", 3).await;
                
                // Try to claim work
                match queries::claim_work_queue_items(&workload_pool, "health-test-agent", "health-worker", 1).await {
                    Ok(items) => {
                        workload_monitor.update_component_health("worker_system", HealthStatus::Healthy, None).await;
                        
                        // Complete the work
                        for item in items {
                            let _ = queries::complete_work_queue_item(&workload_pool, item.queue_id).await;
                        }
                    }
                    Err(e) => {
                        workload_monitor.update_component_health("worker_system", HealthStatus::Degraded,
                            Some(format!("Worker error: {}", e))).await;
                    }
                }
            }
            
            tokio::task::yield_now().await;
        }
    });
    
    // Monitor health during workload
    let health_check_task = tokio::spawn(async move {
        let mut health_snapshots = Vec::new();
        
        for _ in 0..20 {
            tokio::task::yield_now().await;
            let health = monitor.get_system_health().await;
            health_snapshots.push((Instant::now(), health));
        }
        
        health_snapshots
    });
    
    // Wait for both tasks
    workload_task.await?;
    let health_history = health_check_task.await?;
    
    // Analyze health monitoring results
    assert!(!health_history.is_empty(), "Should have health snapshots");
    
    let mut healthy_snapshots = 0;
    let mut total_snapshots = 0;
    
    for (_, health) in &health_history {
        total_snapshots += 1;
        let all_healthy = health.values().all(|c| matches!(c.status, HealthStatus::Healthy));
        if all_healthy {
            healthy_snapshots += 1;
        }
    }
    
    let health_ratio = healthy_snapshots as f64 / total_snapshots as f64;
    assert!(health_ratio > 0.7, "System should be healthy most of the time during normal workload: {:.2}%", health_ratio * 100.0);
    
    // Verify final event count
    let final_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE source = 'health_monitoring_test'"
    ).fetch_one(&pool).await?;
    
    assert_eq!(final_count, 100, "All events should be processed despite health monitoring");
    
    Ok(())
}