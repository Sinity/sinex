use crate::{CoreError, DbPool, EventFactory, JsonValue, Timestamp, ValidationChain, event_type_constants};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use thiserror::Error;

// Legacy ComponentHeartbeat struct removed - use ProcessHeartbeatEmitter for event-based heartbeats

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

// Legacy ComponentHeartbeat implementation removed - keeping utility functions

/// Simple health status determination for event-based heartbeats
pub fn determine_health_status(
    memory_usage_mb: u32,
    cpu_usage_percent: f32,
    errors_last_hour: u32,
) -> HealthStatus {
    let conditions = HealthCheckConditions::default();
    determine_health_status_with_conditions(
        memory_usage_mb,
        cpu_usage_percent,
        errors_last_hour,
        60, // Default uptime
        &conditions,
    )
    .unwrap_or(HealthStatus::Failed)
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
        && uptime_seconds < conditions.uptime_requirements.minimum_uptime_seconds
    {
        return Ok(HealthStatus::Degraded); // Not enough uptime for stable status
    }

    // Multi-condition health determination
    let mut failed_conditions = Vec::new();
    let mut degraded_conditions = Vec::new();

    // Memory condition checks
    if memory_usage_mb >= conditions.memory_thresholds.failed_max_mb {
        failed_conditions.push(format!(
            "Memory usage {}MB exceeds failed threshold {}MB",
            memory_usage_mb, conditions.memory_thresholds.failed_max_mb
        ));
    } else if memory_usage_mb >= conditions.memory_thresholds.degraded_max_mb {
        degraded_conditions.push(format!(
            "Memory usage {}MB exceeds degraded threshold {}MB",
            memory_usage_mb, conditions.memory_thresholds.degraded_max_mb
        ));
    } else if memory_usage_mb > conditions.memory_thresholds.healthy_max_mb {
        degraded_conditions.push(format!(
            "Memory usage {}MB exceeds healthy threshold {}MB",
            memory_usage_mb, conditions.memory_thresholds.healthy_max_mb
        ));
    }

    // CPU condition checks
    if cpu_usage_percent >= conditions.cpu_thresholds.failed_max_percent {
        failed_conditions.push(format!(
            "CPU usage {:.1}% exceeds failed threshold {:.1}%",
            cpu_usage_percent, conditions.cpu_thresholds.failed_max_percent
        ));
    } else if cpu_usage_percent >= conditions.cpu_thresholds.degraded_max_percent {
        degraded_conditions.push(format!(
            "CPU usage {:.1}% exceeds degraded threshold {:.1}%",
            cpu_usage_percent, conditions.cpu_thresholds.degraded_max_percent
        ));
    } else if cpu_usage_percent > conditions.cpu_thresholds.healthy_max_percent {
        degraded_conditions.push(format!(
            "CPU usage {:.1}% exceeds healthy threshold {:.1}%",
            cpu_usage_percent, conditions.cpu_thresholds.healthy_max_percent
        ));
    }

    // Error condition checks
    if errors_last_hour >= conditions.error_thresholds.failed_max_per_hour {
        failed_conditions.push(format!(
            "Error count {} exceeds failed threshold {}",
            errors_last_hour, conditions.error_thresholds.failed_max_per_hour
        ));
    } else if errors_last_hour >= conditions.error_thresholds.degraded_max_per_hour {
        degraded_conditions.push(format!(
            "Error count {} exceeds degraded threshold {}",
            errors_last_hour, conditions.error_thresholds.degraded_max_per_hour
        ));
    } else if errors_last_hour > conditions.error_thresholds.healthy_max_per_hour {
        degraded_conditions.push(format!(
            "Error count {} exceeds healthy threshold {}",
            errors_last_hour, conditions.error_thresholds.healthy_max_per_hour
        ));
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

// Legacy ComponentHeartbeat implementation methods removed - using ProcessHeartbeatEmitter for events

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

/// Trait for components to provide custom metrics to heartbeats
pub trait MetricsProvider {
    fn get_events_processed_last_minute(&self) -> u32;
    fn get_errors_last_hour(&self) -> u32;
    fn get_last_error_message(&self) -> Option<String>;
    fn get_custom_metrics(&self) -> JsonValue {
        serde_json::json!({})
    }
}

// ============================================================================
// Event-Based Process Heartbeat Emitter (New Implementation)
// ============================================================================

/// Event-based heartbeat emitter that sends heartbeat events instead of table inserts
/// This is the new implementation that replaces ComponentHeartbeat table usage
pub struct ProcessHeartbeatEmitter {
    db_pool: DbPool,
    process_name: String,
    version: String,
    interval_seconds: u64,
    metrics_provider: Option<Box<dyn MetricsProvider + Send + Sync>>,
    event_factory: EventFactory,
}

impl ProcessHeartbeatEmitter {
    pub fn new(
        db_pool: DbPool,
        process_name: String,
        version: String,
        interval_seconds: u64,
    ) -> Self {
        Self {
            db_pool,
            process_name: process_name.clone(),
            version,
            interval_seconds,
            metrics_provider: None,
            event_factory: EventFactory::new("sinex.process"),
        }
    }

    pub fn with_metrics_provider<T: MetricsProvider + Send + Sync + 'static>(
        db_pool: DbPool,
        process_name: String,
        version: String,
        interval_seconds: u64,
        provider: T,
    ) -> Self {
        Self {
            db_pool,
            process_name: process_name.clone(),
            version,
            interval_seconds,
            metrics_provider: Some(Box::new(provider)),
            event_factory: EventFactory::new("sinex.process"),
        }
    }


    /// Emit a process started event
    pub async fn emit_process_started(&self) -> Result<(), HeartbeatError> {
        let payload = serde_json::json!({
            "process_name": self.process_name,
            "version": self.version,
            "started_at": Utc::now(),
            "pid": std::process::id(),
        });

        let event = self.event_factory.create_event(event_type_constants::process::PROCESS_STARTED, payload);
        
        // Insert event into core.events (let database generate ts_ingest from ULID)
        sqlx::query!(
            r#"
            INSERT INTO core.events (source, event_type, ts_orig, host, payload)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            event.source,
            event.event_type,
            event.ts_orig,
            event.host,
            event.payload
        )
        .execute(&self.db_pool)
        .await?;

        tracing::info!(
            process_name = %self.process_name,
            version = %self.version,
            "Process started event emitted"
        );

        Ok(())
    }

    /// Emit a process shutdown event
    pub async fn emit_process_shutdown(&self, reason: &str) -> Result<(), HeartbeatError> {
        let uptime = self.get_uptime_seconds();
        
        let payload = serde_json::json!({
            "process_name": self.process_name,
            "version": self.version,
            "shutdown_at": Utc::now(),
            "uptime_seconds": uptime,
            "reason": reason,
            "graceful": true,
        });

        let event = self.event_factory.create_event(event_type_constants::process::PROCESS_SHUTDOWN, payload);
        
        // Insert event into core.events (let database generate ts_ingest from ULID)
        sqlx::query!(
            r#"
            INSERT INTO core.events (source, event_type, ts_orig, host, payload)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            event.source,
            event.event_type,
            event.ts_orig,
            event.host,
            event.payload
        )
        .execute(&self.db_pool)
        .await?;

        tracing::info!(
            process_name = %self.process_name,
            uptime_seconds = uptime,
            "Process shutdown event emitted"
        );

        Ok(())
    }

    /// Run heartbeat emission loop (call from tokio::spawn)
    pub async fn run(self) {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(self.interval_seconds));

        loop {
            interval.tick().await;

            if let Err(e) = self.emit_heartbeat().await {
                tracing::error!(
                    process_name = %self.process_name,
                    error = %e,
                    "Failed to emit process heartbeat event"
                );
                // Continue running even on errors
            }
        }
    }

    /// Emit a single heartbeat event
    pub async fn emit_heartbeat(&self) -> Result<(), HeartbeatError> {
        let uptime = self.get_uptime_seconds();
        let memory_mb = self.get_memory_usage_mb();
        let cpu_percent = self.get_cpu_usage_percent();
        
        let (events_processed, errors_count) = if let Some(ref provider) = self.metrics_provider {
            (
                provider.get_events_processed_last_minute() as u64,
                provider.get_errors_last_hour() as u64,
            )
        } else {
            (0, 0)
        };

        let health_status = determine_health_status(
            memory_mb, 
            cpu_percent, 
            errors_count as u32
        ).to_string();

        let custom_metrics = if let Some(ref provider) = self.metrics_provider {
            serde_json::from_value::<HashMap<String, serde_json::Value>>(provider.get_custom_metrics()).unwrap_or_default()
        } else {
            HashMap::new()
        };

        let payload = serde_json::json!({
            "process_name": self.process_name,
            "version": self.version,
            "uptime_seconds": uptime,
            "memory_mb": memory_mb,
            "cpu_percent": cpu_percent,
            "events_processed": events_processed,
            "errors_count": errors_count,
            "health_status": health_status,
            "custom_metrics": custom_metrics,
        });

        let event = self.event_factory.create_event(event_type_constants::process::PROCESS_HEARTBEAT, payload);
        
        // Insert event into core.events (let database generate ts_ingest from ULID)
        sqlx::query!(
            r#"
            INSERT INTO core.events (source, event_type, ts_orig, host, payload)
            VALUES ($1, $2, $3, $4, $5)
            "#,
            event.source,
            event.event_type,
            event.ts_orig,
            event.host,
            event.payload
        )
        .execute(&self.db_pool)
        .await?;

        tracing::debug!(
            process_name = %self.process_name,
            health_status = %health_status,
            memory_mb = memory_mb,
            cpu_percent = cpu_percent,
            "Process heartbeat event emitted"
        );

        Ok(())
    }

    /// Get process uptime in seconds (simplified implementation)
    fn get_uptime_seconds(&self) -> u64 {
        // This is a simplified implementation - in production you'd track actual start time
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() % 86400 // Reset daily for demo purposes
    }

    /// Get memory usage in MB (simplified implementation)
    fn get_memory_usage_mb(&self) -> u32 {
        // In production, use proper system metrics
        // For now, return a reasonable default
        200
    }

    /// Get CPU usage percentage (simplified implementation)  
    fn get_cpu_usage_percent(&self) -> f32 {
        // In production, use proper system metrics
        // For now, return a reasonable default
        25.0
    }

    /// Start continuous heartbeat emission (for testing)
    pub async fn start_heartbeat_loop(&self) -> Result<(), HeartbeatError> {
        // Note: This is a simplified implementation for testing
        // In production, this would be managed with proper async cancellation
        self.emit_heartbeat().await
    }

    /// Stop continuous heartbeat emission (for testing)
    pub async fn stop_heartbeat_loop(&self) -> Result<(), HeartbeatError> {
        // Note: This is a simplified implementation for testing
        // In production, this would signal a running loop to stop
        Ok(())
    }

    /// Clone method needed for testing
    pub fn clone(&self) -> Self {
        ProcessHeartbeatEmitter {
            db_pool: self.db_pool.clone(),
            process_name: self.process_name.clone(),
            version: self.version.clone(),
            interval_seconds: self.interval_seconds,
            metrics_provider: None, // Cannot clone boxed trait object easily
            event_factory: EventFactory::new("sinex.process"),
        }
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
            determine_health_status(100, 20.0, 0),
            HealthStatus::Healthy
        );

        // Degraded case
        assert_eq!(
            determine_health_status(350, 75.0, 5),
            HealthStatus::Degraded
        );

        // Failed case
        assert_eq!(
            determine_health_status(450, 95.0, 15),
            HealthStatus::Failed
        );
    }
}
