use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::multi_location::{LocationStatus, MultiLocationCoordinator};

/// Storage health metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageHealthMetrics {
    pub total_locations: usize,
    pub available_locations: usize,
    pub healthy_locations: usize,
    pub total_capacity_gb: f64,
    pub used_capacity_gb: f64,
    pub replication_factor: f32,
    pub avg_health_score: f32,
    pub critical_errors: Vec<HealthAlert>,
}

/// Health alert types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthAlert {
    pub alert_type: HealthAlertType,
    pub location_id: Option<String>,
    pub message: String,
    pub severity: AlertSeverity,
    pub timestamp: SystemTime,
    pub auto_resolved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthAlertType {
    LocationUnavailable,
    DiskSpaceLow,
    ReplicationFactorLow,
    SyncFailure,
    CorruptionDetected,
    NetworkIssue,
    CapacityExceeded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

/// Configuration for health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMonitorConfig {
    pub check_interval_seconds: u64,
    pub disk_warning_threshold: f32,      // 0.8 = 80%
    pub disk_critical_threshold: f32,     // 0.95 = 95%
    pub min_replication_factor: f32,      // 2.0 = each file in 2+ locations
    pub min_healthy_locations: usize,     // Minimum locations that must be healthy
    pub alert_retention_hours: u64,       // How long to keep resolved alerts
    pub auto_healing_enabled: bool,       // Enable automatic problem resolution
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            check_interval_seconds: 300, // 5 minutes
            disk_warning_threshold: 0.8,
            disk_critical_threshold: 0.95,
            min_replication_factor: 2.0,
            min_healthy_locations: 2,
            alert_retention_hours: 48,
            auto_healing_enabled: true,
        }
    }
}

/// Health monitor for Git-annex multi-location storage
pub struct StorageHealthMonitor {
    config: HealthMonitorConfig,
    coordinator: Option<MultiLocationCoordinator>,
    active_alerts: HashMap<String, HealthAlert>,
    metrics_history: Vec<(SystemTime, StorageHealthMetrics)>,
    last_check: Option<SystemTime>,
}

impl StorageHealthMonitor {
    pub fn new(config: HealthMonitorConfig) -> Self {
        Self {
            config,
            coordinator: None,
            active_alerts: HashMap::new(),
            metrics_history: Vec::new(),
            last_check: None,
        }
    }

    /// Set the multi-location coordinator
    pub fn set_coordinator(&mut self, coordinator: MultiLocationCoordinator) {
        self.coordinator = Some(coordinator);
    }

    /// Start the health monitoring daemon
    pub async fn start_monitoring(&mut self) -> Result<()> {
        info!("Starting storage health monitoring daemon");

        let mut timer = interval(Duration::from_secs(self.config.check_interval_seconds));

        loop {
            timer.tick().await;
            
            if let Err(e) = self.perform_health_check().await {
                error!("Health check failed: {}", e);
            }
        }
    }

    /// Perform a comprehensive health check
    pub async fn perform_health_check(&mut self) -> Result<StorageHealthMetrics> {
        debug!("Performing storage health check");

        let coordinator = self.coordinator.as_ref()
            .context("Multi-location coordinator not set")?;

        let locations_status = coordinator.get_all_status();
        let metrics = self.calculate_metrics(&locations_status).await?;

        // Check for new alerts
        self.check_for_alerts(&locations_status, &metrics).await?;

        // Clean up old alerts
        self.cleanup_old_alerts();

        // Store metrics history
        self.metrics_history.push((SystemTime::now(), metrics.clone()));
        
        // Keep only recent history (last 24 hours)
        let cutoff = SystemTime::now() - Duration::from_secs(86400);
        self.metrics_history.retain(|(timestamp, _)| *timestamp > cutoff);

        self.last_check = Some(SystemTime::now());

        info!("Health check completed: {}/{} locations healthy", 
              metrics.healthy_locations, metrics.total_locations);

        Ok(metrics)
    }

    /// Calculate comprehensive storage metrics
    async fn calculate_metrics(&self, locations_status: &[LocationStatus]) -> Result<StorageHealthMetrics> {
        let total_locations = locations_status.len();
        let available_locations = locations_status.iter()
            .filter(|status| status.is_available)
            .count();
        
        let healthy_locations = locations_status.iter()
            .filter(|status| status.health_score > 0.7)
            .count();

        let total_capacity_gb = locations_status.iter()
            .filter_map(|status| status.disk_usage_gb)
            .sum::<f64>();

        let used_capacity_gb = locations_status.iter()
            .filter_map(|status| status.disk_usage_gb)
            .sum::<f64>(); // This would need actual used space calculation

        let avg_health_score = if available_locations > 0 {
            locations_status.iter()
                .filter(|status| status.is_available)
                .map(|status| status.health_score)
                .sum::<f32>() / available_locations as f32
        } else {
            0.0
        };

        // Calculate replication factor (simplified)
        let replication_factor = if total_locations > 0 {
            available_locations as f32
        } else {
            0.0
        };

        let critical_errors = self.get_critical_alerts();

        Ok(StorageHealthMetrics {
            total_locations,
            available_locations,
            healthy_locations,
            total_capacity_gb,
            used_capacity_gb,
            replication_factor,
            avg_health_score,
            critical_errors,
        })
    }

    /// Check for new health alerts
    async fn check_for_alerts(&mut self, locations_status: &[LocationStatus], metrics: &StorageHealthMetrics) -> Result<()> {
        // Check individual location health
        for status in locations_status {
            self.check_location_alerts(status).await?;
        }

        // Check system-wide health
        self.check_system_alerts(metrics).await?;

        Ok(())
    }

    /// Check alerts for a specific location
    async fn check_location_alerts(&mut self, status: &LocationStatus) -> Result<()> {
        let location_id = &status.location_id;

        // Check availability
        if !status.is_available {
            let alert_id = format!("unavailable_{}", location_id);
            if !self.active_alerts.contains_key(&alert_id) {
                let alert = HealthAlert {
                    alert_type: HealthAlertType::LocationUnavailable,
                    location_id: Some(location_id.clone()),
                    message: format!("Storage location '{}' is unavailable", location_id),
                    severity: AlertSeverity::Critical,
                    timestamp: SystemTime::now(),
                    auto_resolved: false,
                };
                
                self.active_alerts.insert(alert_id, alert);
                warn!("ALERT: Location {} is unavailable", location_id);
            }
        } else {
            // Resolve unavailability alert if location is back online
            let alert_id = format!("unavailable_{}", location_id);
            if let Some(alert) = self.active_alerts.get_mut(&alert_id) {
                alert.auto_resolved = true;
                info!("RESOLVED: Location {} is back online", location_id);
            }
        }

        // Check disk space
        if let Some(disk_usage) = status.disk_usage_gb {
            // This would need max capacity from location config to calculate percentage
            // For now, use a simplified check
            if disk_usage > 90.0 { // Assuming this is percentage
                let alert_id = format!("disk_space_{}", location_id);
                let severity = if disk_usage > 95.0 {
                    AlertSeverity::Emergency
                } else {
                    AlertSeverity::Warning
                };

                if !self.active_alerts.contains_key(&alert_id) {
                    let alert = HealthAlert {
                        alert_type: HealthAlertType::DiskSpaceLow,
                        location_id: Some(location_id.clone()),
                        message: format!("Disk space low on location '{}': {:.1}%", location_id, disk_usage),
                        severity,
                        timestamp: SystemTime::now(),
                        auto_resolved: false,
                    };
                    
                    self.active_alerts.insert(alert_id, alert);
                    warn!("ALERT: Low disk space on location {}: {:.1}%", location_id, disk_usage);
                }
            }
        }

        // Check health score
        if status.health_score < 0.3 {
            let alert_id = format!("health_score_{}", location_id);
            if !self.active_alerts.contains_key(&alert_id) {
                let alert = HealthAlert {
                    alert_type: HealthAlertType::SyncFailure,
                    location_id: Some(location_id.clone()),
                    message: format!("Location '{}' health score is critically low: {:.2}", location_id, status.health_score),
                    severity: AlertSeverity::Critical,
                    timestamp: SystemTime::now(),
                    auto_resolved: false,
                };
                
                self.active_alerts.insert(alert_id, alert);
                warn!("ALERT: Critical health score for location {}: {:.2}", location_id, status.health_score);
            }
        }

        // Check recent sync errors
        let recent_errors = status.sync_errors.iter()
            .filter(|e| e.timestamp.elapsed().unwrap_or(Duration::MAX) < Duration::from_secs(3600))
            .count();

        if recent_errors > 3 {
            let alert_id = format!("sync_errors_{}", location_id);
            if !self.active_alerts.contains_key(&alert_id) {
                let alert = HealthAlert {
                    alert_type: HealthAlertType::SyncFailure,
                    location_id: Some(location_id.clone()),
                    message: format!("Location '{}' has {} sync errors in the last hour", location_id, recent_errors),
                    severity: AlertSeverity::Warning,
                    timestamp: SystemTime::now(),
                    auto_resolved: false,
                };
                
                self.active_alerts.insert(alert_id, alert);
                warn!("ALERT: Multiple sync errors for location {}: {}", location_id, recent_errors);
            }
        }

        Ok(())
    }

    /// Check system-wide alerts
    async fn check_system_alerts(&mut self, metrics: &StorageHealthMetrics) -> Result<()> {
        // Check minimum healthy locations
        if metrics.healthy_locations < self.config.min_healthy_locations {
            let alert_id = "insufficient_healthy_locations".to_string();
            if !self.active_alerts.contains_key(&alert_id) {
                let alert = HealthAlert {
                    alert_type: HealthAlertType::ReplicationFactorLow,
                    location_id: None,
                    message: format!("Only {} healthy locations available, minimum required: {}", 
                                   metrics.healthy_locations, self.config.min_healthy_locations),
                    severity: AlertSeverity::Emergency,
                    timestamp: SystemTime::now(),
                    auto_resolved: false,
                };
                
                self.active_alerts.insert(alert_id, alert);
                error!("ALERT: Insufficient healthy storage locations");
            }
        }

        // Check replication factor
        if metrics.replication_factor < self.config.min_replication_factor {
            let alert_id = "low_replication_factor".to_string();
            if !self.active_alerts.contains_key(&alert_id) {
                let alert = HealthAlert {
                    alert_type: HealthAlertType::ReplicationFactorLow,
                    location_id: None,
                    message: format!("Replication factor {:.1} is below minimum {:.1}", 
                                   metrics.replication_factor, self.config.min_replication_factor),
                    severity: AlertSeverity::Critical,
                    timestamp: SystemTime::now(),
                    auto_resolved: false,
                };
                
                self.active_alerts.insert(alert_id, alert);
                error!("ALERT: Low replication factor");
            }
        }

        // Check overall system health
        if metrics.avg_health_score < 0.5 {
            let alert_id = "system_health_low".to_string();
            if !self.active_alerts.contains_key(&alert_id) {
                let alert = HealthAlert {
                    alert_type: HealthAlertType::CorruptionDetected,
                    location_id: None,
                    message: format!("Overall system health score is low: {:.2}", metrics.avg_health_score),
                    severity: AlertSeverity::Warning,
                    timestamp: SystemTime::now(),
                    auto_resolved: false,
                };
                
                self.active_alerts.insert(alert_id, alert);
                warn!("ALERT: Low overall system health");
            }
        }

        Ok(())
    }

    /// Get all critical alerts
    fn get_critical_alerts(&self) -> Vec<HealthAlert> {
        self.active_alerts
            .values()
            .filter(|alert| !alert.auto_resolved && matches!(alert.severity, AlertSeverity::Critical | AlertSeverity::Emergency))
            .cloned()
            .collect()
    }

    /// Clean up old resolved alerts
    fn cleanup_old_alerts(&mut self) {
        let cutoff = SystemTime::now() - Duration::from_secs(self.config.alert_retention_hours * 3600);
        
        self.active_alerts.retain(|_, alert| {
            if alert.auto_resolved {
                alert.timestamp > cutoff
            } else {
                true // Keep unresolved alerts
            }
        });
    }

    /// Get current metrics
    pub fn get_current_metrics(&self) -> Option<&StorageHealthMetrics> {
        self.metrics_history.last().map(|(_, metrics)| metrics)
    }

    /// Get all active alerts
    pub fn get_active_alerts(&self) -> Vec<&HealthAlert> {
        self.active_alerts
            .values()
            .filter(|alert| !alert.auto_resolved)
            .collect()
    }

    /// Get metrics history
    pub fn get_metrics_history(&self) -> &[(SystemTime, StorageHealthMetrics)] {
        &self.metrics_history
    }

    /// Attempt automatic healing for known issues
    pub async fn attempt_auto_healing(&mut self) -> Result<()> {
        if !self.config.auto_healing_enabled {
            return Ok(());
        }

        info!("Attempting automatic healing of storage issues");

        // Get coordinator reference
        let coordinator = match &mut self.coordinator {
            Some(coord) => coord,
            None => return Ok(()),
        };

        // Try to resolve issues
        for (_alert_id, alert) in &self.active_alerts {
            if alert.auto_resolved {
                continue;
            }

            match &alert.alert_type {
                HealthAlertType::SyncFailure => {
                    if let Some(location_id) = &alert.location_id {
                        info!("Attempting to auto-heal sync failure for location: {}", location_id);
                        if let Err(e) = coordinator.force_sync(location_id).await {
                            warn!("Auto-healing failed for location {}: {}", location_id, e);
                        } else {
                            info!("Auto-healing succeeded for location: {}", location_id);
                        }
                    }
                }
                HealthAlertType::LocationUnavailable => {
                    // Could implement automatic failover or retry logic here
                    debug!("Location unavailable - monitoring for recovery");
                }
                _ => {
                    // Other issues might require manual intervention
                    debug!("Alert type {:?} requires manual intervention", alert.alert_type);
                }
            }
        }

        Ok(())
    }

    /// Generate a health report
    pub fn generate_health_report(&self) -> String {
        let mut report = String::new();
        
        report.push_str("=== Storage Health Report ===\n\n");
        
        if let Some(metrics) = self.get_current_metrics() {
            report.push_str(&format!("Total Locations: {}\n", metrics.total_locations));
            report.push_str(&format!("Available Locations: {}\n", metrics.available_locations));
            report.push_str(&format!("Healthy Locations: {}\n", metrics.healthy_locations));
            report.push_str(&format!("Average Health Score: {:.2}\n", metrics.avg_health_score));
            report.push_str(&format!("Replication Factor: {:.1}\n", metrics.replication_factor));
            report.push_str(&format!("Total Capacity: {:.1} GB\n", metrics.total_capacity_gb));
            report.push_str(&format!("Used Capacity: {:.1} GB\n", metrics.used_capacity_gb));
        }

        let active_alerts = self.get_active_alerts();
        if !active_alerts.is_empty() {
            report.push_str("\n=== Active Alerts ===\n");
            for alert in active_alerts {
                report.push_str(&format!("[{:?}] {}\n", alert.severity, alert.message));
            }
        } else {
            report.push_str("\n✓ No active alerts\n");
        }

        if let Some(last_check) = self.last_check {
            if let Ok(elapsed) = last_check.elapsed() {
                report.push_str(&format!("\nLast Check: {:.0} seconds ago\n", elapsed.as_secs()));
            }
        }

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_monitor_creation() {
        let config = HealthMonitorConfig::default();
        let monitor = StorageHealthMonitor::new(config);
        
        assert_eq!(monitor.active_alerts.len(), 0);
        assert_eq!(monitor.metrics_history.len(), 0);
    }

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Emergency > AlertSeverity::Critical);
        assert!(AlertSeverity::Critical > AlertSeverity::Warning);
        assert!(AlertSeverity::Warning > AlertSeverity::Info);
    }

    #[test]
    fn test_health_report_generation() {
        let config = HealthMonitorConfig::default();
        let monitor = StorageHealthMonitor::new(config);
        
        let report = monitor.generate_health_report();
        assert!(report.contains("Storage Health Report"));
        assert!(report.contains("No active alerts"));
    }
}