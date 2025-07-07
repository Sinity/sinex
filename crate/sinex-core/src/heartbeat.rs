use crate::{DbPool, DbPoolRef, JsonValue, Timestamp, ErrorContext, CoreError, ValidationChain};
use chrono::Utc;
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
    #[error("Core error: {0}")]
    Core(#[from] CoreError),
}

/// Configurable health check conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConditions {
    pub memory_thresholds: MemoryThresholds,
    pub cpu_thresholds: CpuThresholds,
    pub error_thresholds: ErrorThresholds,
    pub uptime_requirements: UptimeRequirements,
    pub component_specific: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryThresholds {
    pub healthy_max_mb: u32,
    pub degraded_max_mb: u32,
    pub failed_max_mb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuThresholds {
    pub healthy_max_percent: f32,
    pub degraded_max_percent: f32,
    pub failed_max_percent: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorThresholds {
    pub healthy_max_per_hour: u32,
    pub degraded_max_per_hour: u32,
    pub failed_max_per_hour: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UptimeRequirements {
    pub minimum_uptime_seconds: u64,
    pub require_stable_uptime: bool,
}

impl Default for HealthCheckConditions {
    fn default() -> Self {
        Self {
            memory_thresholds: MemoryThresholds {
                healthy_max_mb: 300,
                degraded_max_mb: 400,
                failed_max_mb: 500,
            },
            cpu_thresholds: CpuThresholds {
                healthy_max_percent: 70.0,
                degraded_max_percent: 85.0,
                failed_max_percent: 95.0,
            },
            error_thresholds: ErrorThresholds {
                healthy_max_per_hour: 3,
                degraded_max_per_hour: 10,
                failed_max_per_hour: 25,
            },
            uptime_requirements: UptimeRequirements {
                minimum_uptime_seconds: 30,
                require_stable_uptime: false,
            },
            component_specific: serde_json::json!({}),
        }
    }
}

impl ComponentHeartbeat {
    /// Collect system metrics and emit heartbeat to database with enhanced validation
    pub async fn collect_and_emit(
        pool: DbPoolRef<'_>,
        component_name: &str,
    ) -> Result<(), HeartbeatError> {
        Self::collect_and_emit_with_conditions(pool, component_name, &HealthCheckConditions::default()).await
    }

    /// Collect and emit heartbeat with custom health check conditions
    pub async fn collect_and_emit_with_conditions(
        pool: DbPoolRef<'_>,
        component_name: &str,
        conditions: &HealthCheckConditions,
    ) -> Result<(), HeartbeatError> {
        let heartbeat = Self::collect_metrics_with_conditions(component_name, conditions).await?;

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
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Collect current system metrics for this component
    async fn collect_metrics(component_name: &str) -> Result<Self, HeartbeatError> {
        Self::collect_metrics_with_conditions(component_name, &HealthCheckConditions::default()).await
    }

    /// Collect current system metrics with enhanced condition-based validation
    async fn collect_metrics_with_conditions(
        component_name: &str,
        conditions: &HealthCheckConditions,
    ) -> Result<Self, HeartbeatError> {
        // Validate component name first
        ValidationChain::validate(component_name.to_string(), "component_name")
            .not_empty()
            .min_length(2)
            .into_result()
            .map_err(HeartbeatError::Core)?;
        let timestamp = Utc::now();

        // Get basic system metrics with enhanced error context
        let (memory_usage_mb, cpu_usage_percent) = Self::get_system_metrics_enhanced(component_name)?;

        // Calculate uptime (simplified - from process start time)
        let uptime_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| HeartbeatError::SystemMetrics(e.to_string()))?
            .as_secs();

        // Get version info from build constants
        let (binary_version, git_hash, build_time) = Self::get_version_info();

        // Component-specific metrics (to be implemented by each service)
        let (events_processed, errors_count, last_error) =
            Self::get_component_metrics(component_name).await;

        // Determine health status using condition-based logic
        let status = Self::determine_health_status_with_conditions(
            memory_usage_mb,
            cpu_usage_percent,
            errors_count,
            uptime_seconds,
            conditions,
        )?;

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
    #[allow(dead_code)] // Future use for system monitoring
    fn get_system_metrics() -> Result<(u32, f32), HeartbeatError> {
        Self::get_system_metrics_enhanced("unknown")
    }

    /// Get system metrics with enhanced error context and validation
    fn get_system_metrics_enhanced(component_name: &str) -> Result<(u32, f32), HeartbeatError> {
        // Try to get process memory usage with enhanced error reporting
        let memory_usage_mb = Self::get_memory_usage_enhanced(component_name)?;

        // CPU usage is more complex to measure accurately, start with 0
        let cpu_usage_percent = 0.0;

        // Validate memory usage is reasonable
        if memory_usage_mb > 2048 {
            return Err(HeartbeatError::Core(
                ErrorContext::new(CoreError::Configuration("Memory usage exceeds reasonable limits".to_string()))
                    .with_operation("collect_system_metrics")
                    .with_context("component_name", component_name)
                    .with_context("memory_usage_mb", memory_usage_mb.to_string())
                    .with_context("limit_mb", "2048")
                    .build()
            ));
        }

        Ok((memory_usage_mb, cpu_usage_percent))
    }

    /// Get current process memory usage in MB
    #[allow(dead_code)] // Future use for system monitoring
    fn get_memory_usage() -> Option<u32> {
        Self::get_memory_usage_enhanced("unknown").ok()
    }

    /// Get current process memory usage with enhanced error context
    fn get_memory_usage_enhanced(component_name: &str) -> Result<u32, HeartbeatError> {
        // Read from /proc/self/status on Linux
        let proc_status_path = "/proc/self/status";
        
        let contents = std::fs::read_to_string(proc_status_path)
            .map_err(|e| HeartbeatError::Core(
                ErrorContext::new(CoreError::Io(format!("Failed to read process status: {}", e)))
                    .with_operation("get_memory_usage")
                    .with_context("component_name", component_name)
                    .with_context("proc_path", proc_status_path)
                    .with_context("suggestion", "Is /proc filesystem available?")
                    .build()
            ))?;
        
        for line in contents.lines() {
            if line.starts_with("VmRSS:") {
                // Parse "VmRSS: 12345 kB"
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let kb = parts[1].parse::<u32>()
                        .map_err(|e| HeartbeatError::Core(
                            ErrorContext::new(CoreError::Serialization(format!("Failed to parse memory value: {}", e)))
                                .with_operation("get_memory_usage")
                                .with_context("component_name", component_name)
                                .with_context("raw_line", line)
                                .with_context("memory_value", parts[1])
                                .build()
                        ))?;
                    return Ok(kb / 1024); // Convert KB to MB
                }
            }
        }
        
        Err(HeartbeatError::Core(
            ErrorContext::new(CoreError::Configuration("VmRSS line not found in /proc/self/status".to_string()))
                .with_operation("get_memory_usage")
                .with_context("component_name", component_name)
                .with_context("proc_path", proc_status_path)
                .with_context("content_lines", contents.lines().count().to_string())
                .build()
        ))
    }

    /// Get version information from build-time constants
    fn get_version_info() -> (String, String, String) {
        // These constants are generated at build time by the Nix build
        let binary_version = env!("CARGO_PKG_VERSION").to_string();

        // Try to include build info if it exists
        let git_hash =
            if let Ok(build_info) = std::fs::read_to_string("src/generated/build_info.rs") {
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

        let build_time =
            if let Ok(build_info) = std::fs::read_to_string("src/generated/build_info.rs") {
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

    /// Determine health status based on current metrics (legacy method)
    #[allow(dead_code)] // Legacy method kept for compatibility
    fn determine_health_status(
        memory_usage_mb: u32,
        cpu_usage_percent: f32,
        errors_last_hour: u32,
    ) -> HealthStatus {
        let conditions = HealthCheckConditions::default();
        Self::determine_health_status_with_conditions(
            memory_usage_mb,
            cpu_usage_percent,
            errors_last_hour,
            0, // uptime not considered in legacy method
            &conditions,
        ).unwrap_or(HealthStatus::Failed)
    }

    /// Determine health status using condition-based logic with comprehensive validation
    fn determine_health_status_with_conditions(
        memory_usage_mb: u32,
        cpu_usage_percent: f32,
        errors_last_hour: u32,
        uptime_seconds: u64,
        conditions: &HealthCheckConditions,
    ) -> Result<HealthStatus, HeartbeatError> {
        // Validate inputs first
        ValidationChain::validate(memory_usage_mb, "memory_usage_mb")
            .max(4096) // 4GB reasonable maximum
            .into_result()
            .map_err(HeartbeatError::Core)?;

        ValidationChain::validate(cpu_usage_percent, "cpu_usage_percent")
            .min(0.0)
            .max(100.0)
            .into_result()
            .map_err(HeartbeatError::Core)?;

        // Check uptime requirements
        if conditions.uptime_requirements.require_stable_uptime 
            && uptime_seconds < conditions.uptime_requirements.minimum_uptime_seconds {
            return Ok(HealthStatus::Degraded); // Not enough uptime for stable status
        }

        // Multi-condition health determination
        let mut failed_conditions = Vec::new();
        let mut degraded_conditions = Vec::new();

        // Memory condition checks
        if memory_usage_mb >= conditions.memory_thresholds.failed_max_mb {
            failed_conditions.push(format!("Memory usage {}MB exceeds failed threshold {}MB", 
                memory_usage_mb, conditions.memory_thresholds.failed_max_mb));
        } else if memory_usage_mb >= conditions.memory_thresholds.degraded_max_mb {
            degraded_conditions.push(format!("Memory usage {}MB exceeds degraded threshold {}MB", 
                memory_usage_mb, conditions.memory_thresholds.degraded_max_mb));
        } else if memory_usage_mb > conditions.memory_thresholds.healthy_max_mb {
            degraded_conditions.push(format!("Memory usage {}MB exceeds healthy threshold {}MB", 
                memory_usage_mb, conditions.memory_thresholds.healthy_max_mb));
        }

        // CPU condition checks
        if cpu_usage_percent >= conditions.cpu_thresholds.failed_max_percent {
            failed_conditions.push(format!("CPU usage {:.1}% exceeds failed threshold {:.1}%", 
                cpu_usage_percent, conditions.cpu_thresholds.failed_max_percent));
        } else if cpu_usage_percent >= conditions.cpu_thresholds.degraded_max_percent {
            degraded_conditions.push(format!("CPU usage {:.1}% exceeds degraded threshold {:.1}%", 
                cpu_usage_percent, conditions.cpu_thresholds.degraded_max_percent));
        } else if cpu_usage_percent > conditions.cpu_thresholds.healthy_max_percent {
            degraded_conditions.push(format!("CPU usage {:.1}% exceeds healthy threshold {:.1}%", 
                cpu_usage_percent, conditions.cpu_thresholds.healthy_max_percent));
        }

        // Error condition checks
        if errors_last_hour >= conditions.error_thresholds.failed_max_per_hour {
            failed_conditions.push(format!("Error count {} exceeds failed threshold {}", 
                errors_last_hour, conditions.error_thresholds.failed_max_per_hour));
        } else if errors_last_hour >= conditions.error_thresholds.degraded_max_per_hour {
            degraded_conditions.push(format!("Error count {} exceeds degraded threshold {}", 
                errors_last_hour, conditions.error_thresholds.degraded_max_per_hour));
        } else if errors_last_hour > conditions.error_thresholds.healthy_max_per_hour {
            degraded_conditions.push(format!("Error count {} exceeds healthy threshold {}", 
                errors_last_hour, conditions.error_thresholds.healthy_max_per_hour));
        }

        // Determine final status based on condition violations
        if !failed_conditions.is_empty() {
            tracing::warn!(
                memory_mb = memory_usage_mb,
                cpu_percent = cpu_usage_percent,
                errors_per_hour = errors_last_hour,
                uptime_seconds = uptime_seconds,
                failed_conditions = ?failed_conditions,
                "Component health status: FAILED"
            );
            Ok(HealthStatus::Failed)
        } else if !degraded_conditions.is_empty() {
            tracing::warn!(
                memory_mb = memory_usage_mb,
                cpu_percent = cpu_usage_percent,
                errors_per_hour = errors_last_hour,
                uptime_seconds = uptime_seconds,
                degraded_conditions = ?degraded_conditions,
                "Component health status: DEGRADED"
            );
            Ok(HealthStatus::Degraded)
        } else {
            tracing::debug!(
                memory_mb = memory_usage_mb,
                cpu_percent = cpu_usage_percent,
                errors_per_hour = errors_last_hour,
                uptime_seconds = uptime_seconds,
                "Component health status: HEALTHY"
            );
            Ok(HealthStatus::Healthy)
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
        )
        .fetch_optional(pool)
        .await?;

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
        let result = sqlx::query!("SELECT * FROM get_system_health_status()")
            .fetch_one(pool)
            .await?;

        Ok(SystemHealth {
            overall_status: result
                .overall_status
                .unwrap_or_else(|| "unknown".to_string()),
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
        provider: T,
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
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(self.interval_seconds));

        loop {
            interval.tick().await;

            if let Err(e) = self.collect_and_emit_with_provider().await {
                eprintln!(
                    "Failed to emit heartbeat for {}: {}",
                    self.component_name, e
                );
                // Continue running even on errors
            }
        }
    }

    /// Collect metrics and emit heartbeat using the optional metrics provider
    async fn collect_and_emit_with_provider(&self) -> Result<(), HeartbeatError> {
        let mut heartbeat = ComponentHeartbeat::collect_metrics(&self.component_name).await
            .map_err(|e| {
                tracing::error!(
                    component_name = %self.component_name,
                    error = %e,
                    "Failed to collect metrics for heartbeat"
                );
                e
            })?;

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
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_parsing() {
        assert_eq!(
            "healthy".parse::<HealthStatus>().unwrap(),
            HealthStatus::Healthy
        );
        assert_eq!(
            "degraded".parse::<HealthStatus>().unwrap(),
            HealthStatus::Degraded
        );
        assert_eq!(
            "failed".parse::<HealthStatus>().unwrap(),
            HealthStatus::Failed
        );
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
