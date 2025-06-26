use chrono::Utc;
use crate::{DbPool, DbPoolRef, JsonValue, Timestamp};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHeartbeat {
    pub component_name: String,
    pub timestamp: Timestamp,
    pub status: HealthStatus,
    pub uptime_seconds: u64,
    pub memory_usage_mb: u32,
    pub cpu_usage_percent: f32,
    pub events_processed_last_minute: u32,
    pub errors_last_hour: u32,
    pub last_error_message: Option<String>,
    pub binary_version: String,
    pub git_hash: String,
    pub build_time: String,
    pub metrics: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Failed,
}

#[derive(Debug, Error)]
pub enum HeartbeatError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("System metrics error: {0}")]
    SystemMetrics(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl ComponentHeartbeat {
    /// Collect system metrics and emit heartbeat to database
    pub async fn collect_and_emit(
        pool: DbPoolRef<'_>,
        component_name: &str,
    ) -> Result<(), HeartbeatError> {
        let heartbeat = Self::collect_metrics(component_name).await?;
        
        sqlx::query!(
            r#"
            INSERT INTO component_heartbeats 
            (id, component_name, timestamp, status, uptime_seconds, memory_usage_mb,
             cpu_usage_percent, events_processed_last_minute, errors_last_hour, 
             last_error_message, binary_version, git_hash, build_time, metrics)
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
            sinex_ulid::Ulid::new().to_string() as _,
            heartbeat.component_name,
            heartbeat.timestamp,
            heartbeat.status.to_string(),
            heartbeat.uptime_seconds as i64,
            heartbeat.memory_usage_mb as i32,
            heartbeat.cpu_usage_percent as f64,
            heartbeat.events_processed_last_minute as i32,
            heartbeat.errors_last_hour as i32,
            heartbeat.last_error_message,
            heartbeat.binary_version,
            heartbeat.git_hash,
            heartbeat.build_time,
            heartbeat.metrics
        ).execute(pool).await?;
        
        Ok(())
    }
    
    /// Collect current system metrics for this component
    async fn collect_metrics(component_name: &str) -> Result<Self, HeartbeatError> {
        let timestamp = Utc::now();
        
        // Get basic system metrics
        let (memory_usage_mb, cpu_usage_percent) = Self::get_system_metrics()?;
        
        // Calculate uptime (simplified - from process start time)
        let uptime_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| HeartbeatError::SystemMetrics(e.to_string()))?
            .as_secs();
        
        // Get version info from build constants
        let (binary_version, git_hash, build_time) = Self::get_version_info();
        
        // Component-specific metrics (to be implemented by each service)
        let (events_processed, errors_count, last_error) = Self::get_component_metrics(component_name).await;
        
        // Determine health status based on metrics
        let status = Self::determine_health_status(
            memory_usage_mb,
            cpu_usage_percent,
            errors_count,
        );
        
        Ok(ComponentHeartbeat {
            component_name: component_name.to_string(),
            timestamp,
            status,
            uptime_seconds,
            memory_usage_mb,
            cpu_usage_percent,
            events_processed_last_minute: events_processed,
            errors_last_hour: errors_count,
            last_error_message: last_error,
            binary_version,
            git_hash,
            build_time,
            metrics: serde_json::json!({}), // Extended metrics can be added here
        })
    }
    
    /// Get basic system metrics (memory, CPU)
    fn get_system_metrics() -> Result<(u32, f32), HeartbeatError> {
        // Try to get process memory usage
        let memory_usage_mb = Self::get_memory_usage()
            .unwrap_or(0);
        
        // CPU usage is more complex to measure accurately, start with 0
        let cpu_usage_percent = 0.0;
        
        Ok((memory_usage_mb, cpu_usage_percent))
    }
    
    /// Get current process memory usage in MB
    fn get_memory_usage() -> Option<u32> {
        // Read from /proc/self/status on Linux
        if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    // Parse "VmRSS: 12345 kB"
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u32>() {
                            return Some(kb / 1024); // Convert KB to MB
                        }
                    }
                }
            }
        }
        None
    }
    
    /// Get version information from build-time constants
    fn get_version_info() -> (String, String, String) {
        // These constants are generated at build time by the Nix build
        let binary_version = env!("CARGO_PKG_VERSION").to_string();
        
        // Try to include build info if it exists
        let git_hash = if let Ok(build_info) = std::fs::read_to_string("src/generated/build_info.rs") {
            // Parse GIT_HASH constant from build info
            build_info
                .lines()
                .find(|line| line.contains("GIT_HASH"))
                .and_then(|line| line.split('"').nth(1))
                .unwrap_or("unknown")
                .to_string()
        } else {
            "unknown".to_string()
        };
        
        let build_time = if let Ok(build_info) = std::fs::read_to_string("src/generated/build_info.rs") {
            // Parse BUILD_TIME constant from build info
            build_info
                .lines()
                .find(|line| line.contains("BUILD_TIME"))
                .and_then(|line| line.split('"').nth(1))
                .unwrap_or("unknown")
                .to_string()
        } else {
            "unknown".to_string()
        };
        
        (binary_version, git_hash, build_time)
    }
    
    /// Get component-specific metrics (to be overridden by implementations)
    async fn get_component_metrics(_component_name: &str) -> (u32, u32, Option<String>) {
        // Default implementation - actual services should override this
        // Returns: (events_processed_last_minute, errors_last_hour, last_error_message)
        (0, 0, None)
    }
    
    /// Determine health status based on current metrics
    fn determine_health_status(
        memory_usage_mb: u32,
        cpu_usage_percent: f32,
        errors_last_hour: u32,
    ) -> HealthStatus {
        // Simple health determination logic
        // These thresholds should be configurable in production
        if memory_usage_mb > 400 || cpu_usage_percent > 90.0 || errors_last_hour > 10 {
            HealthStatus::Failed
        } else if memory_usage_mb > 300 || cpu_usage_percent > 70.0 || errors_last_hour > 3 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }
    
    /// Get latest heartbeat for a specific component
    pub async fn get_latest_for_component(
        pool: DbPoolRef<'_>,
        component_name: &str,
    ) -> Result<Option<Self>, HeartbeatError> {
        let result = sqlx::query!(
            r#"
            SELECT component_name, timestamp, status, uptime_seconds, memory_usage_mb,
                   cpu_usage_percent, events_processed_last_minute, errors_last_hour,
                   last_error_message, binary_version, git_hash, build_time, metrics
            FROM component_heartbeats
            WHERE component_name = $1
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
            component_name
        ).fetch_optional(pool).await?;
        
        if let Some(row) = result {
            Ok(Some(ComponentHeartbeat {
                component_name: row.component_name,
                timestamp: row.timestamp.unwrap_or_else(Utc::now),
                status: row.status.parse().unwrap_or(HealthStatus::Failed),
                uptime_seconds: row.uptime_seconds.unwrap_or(0) as u64,
                memory_usage_mb: row.memory_usage_mb.unwrap_or(0) as u32,
                cpu_usage_percent: row.cpu_usage_percent.unwrap_or(0.0) as f32,
                events_processed_last_minute: row.events_processed_last_minute.unwrap_or(0) as u32,
                errors_last_hour: row.errors_last_hour.unwrap_or(0) as u32,
                last_error_message: row.last_error_message,
                binary_version: row.binary_version.unwrap_or_else(|| "unknown".to_string()),
                git_hash: row.git_hash.unwrap_or_else(|| "unknown".to_string()),
                build_time: row.build_time.unwrap_or_else(|| "unknown".to_string()),
                metrics: row.metrics.unwrap_or_else(|| serde_json::json!({})),
            }))
        } else {
            Ok(None)
        }
    }
    
    /// Get system health overview
    pub async fn get_system_health(pool: DbPoolRef<'_>) -> Result<SystemHealth, HeartbeatError> {
        let result = sqlx::query!(
            "SELECT * FROM get_system_health_status()"
        ).fetch_one(pool).await?;
        
        Ok(SystemHealth {
            overall_status: result.overall_status.unwrap_or_else(|| "unknown".to_string()),
            healthy_components: result.healthy_components.unwrap_or(0) as u32,
            degraded_components: result.degraded_components.unwrap_or(0) as u32,
            failed_components: result.failed_components.unwrap_or(0) as u32,
            total_components: result.total_components.unwrap_or(0) as u32,
            last_updated: result.last_updated.unwrap_or_else(Utc::now),
        })
    }
}

impl std::str::FromStr for HealthStatus {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "healthy" => Ok(HealthStatus::Healthy),
            "degraded" => Ok(HealthStatus::Degraded),
            "failed" => Ok(HealthStatus::Failed),
            _ => Err(format!("Invalid health status: {}", s)),
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Failed => write!(f, "failed"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub overall_status: String,
    pub healthy_components: u32,
    pub degraded_components: u32,
    pub failed_components: u32,
    pub total_components: u32,
    pub last_updated: Timestamp,
}

/// Heartbeat emission task that can be spawned in services
pub struct HeartbeatEmitter {
    pool: DbPool,
    component_name: String,
    interval_seconds: u64,
    metrics_provider: Option<Box<dyn MetricsProvider + Send + Sync>>,
}

/// Trait for components to provide custom metrics to heartbeats
pub trait MetricsProvider {
    fn get_events_processed_last_minute(&self) -> u32;
    fn get_errors_last_hour(&self) -> u32;
    fn get_last_error_message(&self) -> Option<String>;
    fn get_custom_metrics(&self) -> JsonValue {
        serde_json::json!({})
    }
}

impl HeartbeatEmitter {
    pub fn new(pool: DbPool, component_name: String, interval_seconds: u64) -> Self {
        Self {
            pool,
            component_name,
            interval_seconds,
            metrics_provider: None,
        }
    }
    
    pub fn with_metrics_provider<T: MetricsProvider + Send + Sync + 'static>(
        pool: DbPool, 
        component_name: String, 
        interval_seconds: u64, 
        provider: T
    ) -> Self {
        Self {
            pool,
            component_name,
            interval_seconds,
            metrics_provider: Some(Box::new(provider)),
        }
    }
    
    /// Run heartbeat emission loop (call from tokio::spawn)
    pub async fn run(self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(self.interval_seconds));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.collect_and_emit_with_provider().await {
                eprintln!("Failed to emit heartbeat for {}: {}", self.component_name, e);
                // Continue running even on errors
            }
        }
    }
    
    /// Collect metrics and emit heartbeat using the optional metrics provider
    async fn collect_and_emit_with_provider(&self) -> Result<(), HeartbeatError> {
        let mut heartbeat = ComponentHeartbeat::collect_metrics(&self.component_name).await?;
        
        // Override component-specific metrics if provider is available
        if let Some(ref provider) = self.metrics_provider {
            heartbeat.events_processed_last_minute = provider.get_events_processed_last_minute();
            heartbeat.errors_last_hour = provider.get_errors_last_hour();
            heartbeat.last_error_message = provider.get_last_error_message();
            heartbeat.metrics = provider.get_custom_metrics();
        }
        
        // Insert the heartbeat
        sqlx::query!(
            r#"
            INSERT INTO component_heartbeats 
            (id, component_name, timestamp, status, uptime_seconds, memory_usage_mb,
             cpu_usage_percent, events_processed_last_minute, errors_last_hour, 
             last_error_message, binary_version, git_hash, build_time, metrics)
            VALUES ($1::ulid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
            sinex_ulid::Ulid::new().to_string() as _,
            heartbeat.component_name,
            heartbeat.timestamp,
            heartbeat.status.to_string(),
            heartbeat.uptime_seconds as i64,
            heartbeat.memory_usage_mb as i32,
            heartbeat.cpu_usage_percent as f64,
            heartbeat.events_processed_last_minute as i32,
            heartbeat.errors_last_hour as i32,
            heartbeat.last_error_message,
            heartbeat.binary_version,
            heartbeat.git_hash,
            heartbeat.build_time,
            heartbeat.metrics
        ).execute(&self.pool).await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_health_status_parsing() {
        assert_eq!("healthy".parse::<HealthStatus>().unwrap(), HealthStatus::Healthy);
        assert_eq!("degraded".parse::<HealthStatus>().unwrap(), HealthStatus::Degraded);
        assert_eq!("failed".parse::<HealthStatus>().unwrap(), HealthStatus::Failed);
        assert!("invalid".parse::<HealthStatus>().is_err());
    }
    
    #[test]
    fn test_health_status_display() {
        assert_eq!(HealthStatus::Healthy.to_string(), "healthy");
        assert_eq!(HealthStatus::Degraded.to_string(), "degraded");
        assert_eq!(HealthStatus::Failed.to_string(), "failed");
    }
    
    #[test]
    fn test_health_status_determination() {
        // Healthy case
        assert_eq!(
            ComponentHeartbeat::determine_health_status(100, 20.0, 0),
            HealthStatus::Healthy
        );
        
        // Degraded case
        assert_eq!(
            ComponentHeartbeat::determine_health_status(350, 75.0, 5),
            HealthStatus::Degraded
        );
        
        // Failed case
        assert_eq!(
            ComponentHeartbeat::determine_health_status(450, 95.0, 15),
            HealthStatus::Failed
        );
    }
}