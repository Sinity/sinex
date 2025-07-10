//! Health Check Framework
//!
//! Provides comprehensive health checking capabilities for services including
//! component-level health checks, aggregated reports, and health monitoring.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use async_trait::async_trait;

use crate::{ServiceResult, ComponentName, ServiceName};

/// Health status levels
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Component is functioning normally
    Healthy,
    /// Component has minor issues but is still functional
    Degraded,
    /// Component is not functioning properly
    Unhealthy,
    /// Component status cannot be determined
    Unknown,
}

impl HealthStatus {
    /// Check if status indicates the component is operational
    pub fn is_operational(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }
    
    /// Get numeric score for status (higher is better)
    pub fn score(&self) -> u8 {
        match self {
            HealthStatus::Healthy => 100,
            HealthStatus::Degraded => 75,
            HealthStatus::Unhealthy => 25,
            HealthStatus::Unknown => 0,
        }
    }
    
    /// Combine multiple health statuses (worst case)
    pub fn combine(statuses: &[HealthStatus]) -> HealthStatus {
        if statuses.is_empty() {
            return HealthStatus::Unknown;
        }
        
        if statuses.iter().any(|s| matches!(s, HealthStatus::Unhealthy)) {
            HealthStatus::Unhealthy
        } else if statuses.iter().any(|s| matches!(s, HealthStatus::Degraded)) {
            HealthStatus::Degraded
        } else if statuses.iter().any(|s| matches!(s, HealthStatus::Unknown)) {
            HealthStatus::Unknown
        } else {
            HealthStatus::Healthy
        }
    }
}

/// Individual component health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub component: ComponentName,
    /// Health status
    pub status: HealthStatus,
    /// Human-readable status message
    pub message: Option<String>,
    /// Check execution timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Time taken to perform the check
    pub check_duration: Duration,
    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ComponentHealth {
    /// Create a healthy component status
    pub fn healthy(component: impl Into<ComponentName>) -> Self {
        Self {
            component: component.into(),
            status: HealthStatus::Healthy,
            message: None,
            timestamp: chrono::Utc::now(),
            check_duration: Duration::default(),
            metadata: HashMap::new(),
        }
    }
    
    /// Create a degraded component status
    pub fn degraded(component: impl Into<ComponentName>, message: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: HealthStatus::Degraded,
            message: Some(message.into()),
            timestamp: chrono::Utc::now(),
            check_duration: Duration::default(),
            metadata: HashMap::new(),
        }
    }
    
    /// Create an unhealthy component status
    pub fn unhealthy(component: impl Into<ComponentName>, message: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: HealthStatus::Unhealthy,
            message: Some(message.into()),
            timestamp: chrono::Utc::now(),
            check_duration: Duration::default(),
            metadata: HashMap::new(),
        }
    }
    
    /// Create an unknown component status
    pub fn unknown(component: impl Into<ComponentName>, message: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: HealthStatus::Unknown,
            message: Some(message.into()),
            timestamp: chrono::Utc::now(),
            check_duration: Duration::default(),
            metadata: HashMap::new(),
        }
    }
    
    /// Add metadata to the health check
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
    
    /// Set the check duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.check_duration = duration;
        self
    }
}

/// Comprehensive health report for a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Service name
    pub service_name: ServiceName,
    /// Overall service health status
    pub status: HealthStatus,
    /// Report generation timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Individual component health checks
    pub checks: HashMap<ComponentName, ComponentHealth>,
    /// Additional service-level metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl HealthReport {
    /// Create a new health report
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        Self {
            service_name: service_name.into(),
            status: HealthStatus::Healthy,
            timestamp: chrono::Utc::now(),
            checks: HashMap::new(),
            metadata: HashMap::new(),
        }
    }
    
    /// Add a component health check
    pub fn add_check(mut self, check: ComponentHealth) -> Self {
        self.checks.insert(check.component.clone(), check);
        self.update_overall_status();
        self
    }
    
    /// Add multiple component health checks
    pub fn add_checks(mut self, checks: Vec<ComponentHealth>) -> Self {
        for check in checks {
            self.checks.insert(check.component.clone(), check);
        }
        self.update_overall_status();
        self
    }
    
    /// Add metadata to the report
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
    
    /// Get the overall health status
    pub fn overall_status(&self) -> &HealthStatus {
        &self.status
    }
    
    /// Check if service is healthy overall
    pub fn is_healthy(&self) -> bool {
        self.status.is_operational()
    }
    
    /// Get unhealthy components
    pub fn unhealthy_components(&self) -> Vec<&ComponentHealth> {
        self.checks
            .values()
            .filter(|check| !check.status.is_operational())
            .collect()
    }
    
    /// Update overall status based on component checks
    fn update_overall_status(&mut self) {
        let statuses: Vec<HealthStatus> = self.checks.values().map(|c| c.status.clone()).collect();
        self.status = HealthStatus::combine(&statuses);
    }
}

/// Trait for implementing custom health checks
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Component name this health check is for
    fn component_name(&self) -> &str;
    
    /// Perform the health check
    async fn check_health(&self) -> ServiceResult<ComponentHealth>;
    
    /// Get the timeout for this health check
    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
    
    /// Whether this check is critical for service health
    fn is_critical(&self) -> bool {
        true
    }
}

/// Built-in health checks for common scenarios
pub mod builtin {
    use super::*;
    use std::path::Path;
    use tokio::time::timeout;
    
    /// Health check that verifies a file exists
    pub struct FileExistsCheck {
        component: String,
        file_path: String,
    }
    
    impl FileExistsCheck {
        pub fn new(component: impl Into<String>, file_path: impl Into<String>) -> Self {
            Self {
                component: component.into(),
                file_path: file_path.into(),
            }
        }
    }
    
    #[async_trait]
    impl HealthCheck for FileExistsCheck {
        fn component_name(&self) -> &str {
            &self.component
        }
        
        async fn check_health(&self) -> ServiceResult<ComponentHealth> {
            let start_time = std::time::Instant::now();
            
            let health = if Path::new(&self.file_path).exists() {
                ComponentHealth::healthy(&self.component)
                    .with_metadata("file_path", serde_json::Value::String(self.file_path.clone()))
            } else {
                ComponentHealth::unhealthy(&self.component, format!("File not found: {}", self.file_path))
                    .with_metadata("file_path", serde_json::Value::String(self.file_path.clone()))
            };
            
            Ok(health.with_duration(start_time.elapsed()))
        }
    }
    
    /// Health check that verifies network connectivity
    pub struct NetworkConnectivityCheck {
        component: String,
        target_url: String,
        timeout_duration: Duration,
    }
    
    impl NetworkConnectivityCheck {
        pub fn new(component: impl Into<String>, target_url: impl Into<String>) -> Self {
            Self {
                component: component.into(),
                target_url: target_url.into(),
                timeout_duration: Duration::from_secs(10),
            }
        }
        
        pub fn with_timeout(mut self, timeout_duration: Duration) -> Self {
            self.timeout_duration = timeout_duration;
            self
        }
    }
    
    #[async_trait]
    impl HealthCheck for NetworkConnectivityCheck {
        fn component_name(&self) -> &str {
            &self.component
        }
        
        async fn check_health(&self) -> ServiceResult<ComponentHealth> {
            let start_time = std::time::Instant::now();
            
            let health = match timeout(self.timeout_duration, try_connect(&self.target_url)).await {
                Ok(Ok(())) => {
                    ComponentHealth::healthy(&self.component)
                        .with_metadata("target_url", serde_json::Value::String(self.target_url.clone()))
                }
                Ok(Err(e)) => {
                    ComponentHealth::unhealthy(&self.component, format!("Connection failed: {}", e))
                        .with_metadata("target_url", serde_json::Value::String(self.target_url.clone()))
                        .with_metadata("error", serde_json::Value::String(e.to_string()))
                }
                Err(_) => {
                    ComponentHealth::unhealthy(&self.component, "Connection timeout")
                        .with_metadata("target_url", serde_json::Value::String(self.target_url.clone()))
                        .with_metadata("timeout_ms", serde_json::Value::Number((self.timeout_duration.as_millis() as u64).into()))
                }
            };
            
            Ok(health.with_duration(start_time.elapsed()))
        }
        
        fn timeout(&self) -> Duration {
            self.timeout_duration
        }
    }
    
    async fn try_connect(url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Simple connectivity check - in practice you might want to use reqwest or similar
        let parsed = url::Url::parse(url)?;
        let host = parsed.host_str().ok_or("No host in URL")?;
        let port = parsed.port().unwrap_or(match parsed.scheme() {
            "http" => 80,
            "https" => 443,
            _ => return Err("Unsupported scheme".into()),
        });
        
        tokio::net::TcpStream::connect(format!("{}:{}", host, port)).await?;
        Ok(())
    }
    
    /// Health check that measures memory usage
    pub struct MemoryUsageCheck {
        component: String,
        warning_threshold_mb: u64,
        critical_threshold_mb: u64,
    }
    
    impl MemoryUsageCheck {
        pub fn new(component: impl Into<String>, warning_threshold_mb: u64, critical_threshold_mb: u64) -> Self {
            Self {
                component: component.into(),
                warning_threshold_mb,
                critical_threshold_mb,
            }
        }
    }
    
    #[async_trait]
    impl HealthCheck for MemoryUsageCheck {
        fn component_name(&self) -> &str {
            &self.component
        }
        
        async fn check_health(&self) -> ServiceResult<ComponentHealth> {
            let start_time = std::time::Instant::now();
            
            // Note: This is a simplified implementation
            // In practice, you'd use a proper system monitoring library
            let memory_usage_mb = get_current_memory_usage_mb();
            
            let health = if memory_usage_mb >= self.critical_threshold_mb {
                ComponentHealth::unhealthy(&self.component, format!("Memory usage critical: {}MB", memory_usage_mb))
            } else if memory_usage_mb >= self.warning_threshold_mb {
                ComponentHealth::degraded(&self.component, format!("Memory usage high: {}MB", memory_usage_mb))
            } else {
                ComponentHealth::healthy(&self.component)
            };
            
            Ok(health
                .with_metadata("memory_usage_mb", serde_json::Value::Number(memory_usage_mb.into()))
                .with_metadata("warning_threshold_mb", serde_json::Value::Number(self.warning_threshold_mb.into()))
                .with_metadata("critical_threshold_mb", serde_json::Value::Number(self.critical_threshold_mb.into()))
                .with_duration(start_time.elapsed()))
        }
    }
    
    fn get_current_memory_usage_mb() -> u64 {
        // Simplified implementation - just return a placeholder
        // In practice, you'd use system APIs or libraries like sysinfo
        0
    }
}

/// Health check manager that coordinates multiple health checks
pub struct HealthCheckManager {
    checks: Vec<Box<dyn HealthCheck>>,
    service_name: ServiceName,
}

impl HealthCheckManager {
    /// Create a new health check manager
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        Self {
            checks: Vec::new(),
            service_name: service_name.into(),
        }
    }
    
    /// Add a health check
    pub fn add_check(&mut self, check: Box<dyn HealthCheck>) {
        self.checks.push(check);
    }
    
    /// Run all health checks and generate a report
    pub async fn check_health(&self) -> ServiceResult<HealthReport> {
        let mut report = HealthReport::new(&self.service_name);
        
        for check in &self.checks {
            match tokio::time::timeout(check.timeout(), check.check_health()).await {
                Ok(Ok(component_health)) => {
                    report = report.add_check(component_health);
                }
                Ok(Err(e)) => {
                    let error_health = ComponentHealth::unhealthy(
                        check.component_name(),
                        format!("Health check failed: {}", e)
                    );
                    report = report.add_check(error_health);
                }
                Err(_) => {
                    let timeout_health = ComponentHealth::unhealthy(
                        check.component_name(),
                        format!("Health check timed out after {:?}", check.timeout())
                    );
                    report = report.add_check(timeout_health);
                }
            }
        }
        
        Ok(report)
    }
    
    /// Run health checks for specific components only
    pub async fn check_components(&self, components: &[&str]) -> ServiceResult<HealthReport> {
        let mut report = HealthReport::new(&self.service_name);
        
        for check in &self.checks {
            if components.contains(&check.component_name()) {
                match tokio::time::timeout(check.timeout(), check.check_health()).await {
                    Ok(Ok(component_health)) => {
                        report = report.add_check(component_health);
                    }
                    Ok(Err(e)) => {
                        let error_health = ComponentHealth::unhealthy(
                            check.component_name(),
                            format!("Health check failed: {}", e)
                        );
                        report = report.add_check(error_health);
                    }
                    Err(_) => {
                        let timeout_health = ComponentHealth::unhealthy(
                            check.component_name(),
                            format!("Health check timed out after {:?}", check.timeout())
                        );
                        report = report.add_check(timeout_health);
                    }
                }
            }
        }
        
        Ok(report)
    }
    
    /// Get list of all registered component names
    pub fn component_names(&self) -> Vec<&str> {
        self.checks.iter().map(|c| c.component_name()).collect()
    }
}