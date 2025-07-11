//! Service Status and Metrics Reporting
//!
//! Provides comprehensive status reporting and metrics collection for services.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::health::HealthStatus;
use crate::lifecycle::LifecycleState;
use crate::{ComponentName, ServiceError, ServiceName, ServiceResult};

/// Service status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatus {
    /// Service name
    pub service_name: ServiceName,
    /// Current lifecycle state
    pub state: LifecycleState,
    /// Health status
    pub health: HealthStatus,
    /// Service uptime
    pub uptime: Duration,
    /// Last status update timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Service version
    pub version: String,
    /// Host where service is running
    pub hostname: String,
    /// Service metrics summary
    pub metrics_summary: MetricsSummary,
    /// Additional status metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ServiceStatus {
    /// Create a new service status
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        Self {
            service_name: service_name.into(),
            state: LifecycleState::Created,
            health: HealthStatus::Unknown,
            uptime: Duration::default(),
            timestamp: chrono::Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            metrics_summary: MetricsSummary::default(),
            metadata: HashMap::new(),
        }
    }

    /// Update the service state
    pub fn with_state(mut self, state: LifecycleState) -> Self {
        self.state = state;
        self.timestamp = chrono::Utc::now();
        self
    }

    /// Update the health status
    pub fn with_health(mut self, health: HealthStatus) -> Self {
        self.health = health;
        self.timestamp = chrono::Utc::now();
        self
    }

    /// Update the uptime
    pub fn with_uptime(mut self, uptime: Duration) -> Self {
        self.uptime = uptime;
        self.timestamp = chrono::Utc::now();
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Update metrics summary
    pub fn with_metrics_summary(mut self, summary: MetricsSummary) -> Self {
        self.metrics_summary = summary;
        self.timestamp = chrono::Utc::now();
        self
    }

    /// Check if service is operational
    pub fn is_operational(&self) -> bool {
        self.state.is_healthy() && self.health.is_operational()
    }

    /// Get overall service score (0-100)
    pub fn overall_score(&self) -> u8 {
        let state_score = if self.state.is_healthy() { 100 } else { 0 };
        let health_score = self.health.score();

        // Weight state and health equally
        (state_score + health_score) / 2
    }
}

/// Summary of service metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    /// Total number of requests/operations processed
    pub total_operations: u64,
    /// Operations per second (recent rate)
    pub operations_per_second: f64,
    /// Average operation duration in milliseconds
    pub avg_operation_duration_ms: f64,
    /// Error rate percentage
    pub error_rate_percent: f64,
    /// Memory usage in MB
    pub memory_usage_mb: u64,
    /// CPU usage percentage
    pub cpu_usage_percent: f64,
    /// Number of active connections/sessions
    pub active_connections: u32,
}

impl Default for MetricsSummary {
    fn default() -> Self {
        Self {
            total_operations: 0,
            operations_per_second: 0.0,
            avg_operation_duration_ms: 0.0,
            error_rate_percent: 0.0,
            memory_usage_mb: 0,
            cpu_usage_percent: 0.0,
            active_connections: 0,
        }
    }
}

/// Detailed service metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMetrics {
    /// Service name
    pub service_name: ServiceName,
    /// Metrics collection timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Counter metrics (monotonically increasing values)
    pub counters: HashMap<String, u64>,
    /// Gauge metrics (point-in-time values)
    pub gauges: HashMap<String, f64>,
    /// Histogram metrics (distribution data)
    pub histograms: HashMap<String, HistogramData>,
    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ServiceMetrics {
    /// Create new service metrics
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        Self {
            service_name: service_name.into(),
            timestamp: chrono::Utc::now(),
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    /// Add a counter metric
    pub fn counter(mut self, name: impl Into<String>, value: u64) -> Self {
        self.counters.insert(name.into(), value);
        self
    }

    /// Add a gauge metric
    pub fn gauge(mut self, name: impl Into<String>, value: f64) -> Self {
        self.gauges.insert(name.into(), value);
        self
    }

    /// Add a histogram metric
    pub fn histogram(mut self, name: impl Into<String>, data: HistogramData) -> Self {
        self.histograms.insert(name.into(), data);
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Generate a metrics summary
    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            total_operations: self.counters.get("total_operations").copied().unwrap_or(0),
            operations_per_second: self
                .gauges
                .get("operations_per_second")
                .copied()
                .unwrap_or(0.0),
            avg_operation_duration_ms: self
                .gauges
                .get("avg_operation_duration_ms")
                .copied()
                .unwrap_or(0.0),
            error_rate_percent: self
                .gauges
                .get("error_rate_percent")
                .copied()
                .unwrap_or(0.0),
            memory_usage_mb: self
                .gauges
                .get("memory_usage_mb")
                .map(|v| *v as u64)
                .unwrap_or(0),
            cpu_usage_percent: self.gauges.get("cpu_usage_percent").copied().unwrap_or(0.0),
            active_connections: self
                .gauges
                .get("active_connections")
                .map(|v| *v as u32)
                .unwrap_or(0),
        }
    }
}

/// Histogram data for distribution metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramData {
    /// Sample count
    pub count: u64,
    /// Sum of all samples
    pub sum: f64,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Mean value
    pub mean: f64,
    /// Percentile values
    pub percentiles: HashMap<String, f64>, // e.g., "p50", "p95", "p99"
    /// Histogram buckets (upper bounds -> counts)
    pub buckets: HashMap<String, u64>,
}

impl HistogramData {
    /// Create a new histogram data
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            percentiles: HashMap::new(),
            buckets: HashMap::new(),
        }
    }

    /// Add a percentile value
    pub fn percentile(mut self, percentile: impl Into<String>, value: f64) -> Self {
        self.percentiles.insert(percentile.into(), value);
        self
    }

    /// Add a bucket count
    pub fn bucket(mut self, upper_bound: impl Into<String>, count: u64) -> Self {
        self.buckets.insert(upper_bound.into(), count);
        self
    }

    /// Set basic statistics
    pub fn with_stats(mut self, count: u64, sum: f64, min: f64, max: f64, mean: f64) -> Self {
        self.count = count;
        self.sum = sum;
        self.min = min;
        self.max = max;
        self.mean = mean;
        self
    }
}

impl Default for HistogramData {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for components that can report status
pub trait StatusReporter: Send + Sync {
    /// Component name
    fn component_name(&self) -> &str;

    /// Get current component status
    fn status(&self) -> ComponentStatus;

    /// Get component metrics
    fn metrics(&self) -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }
}

/// Status of an individual component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    /// Component name
    pub component: ComponentName,
    /// Component state
    pub state: ComponentState,
    /// Health status
    pub health: HealthStatus,
    /// Status message
    pub message: Option<String>,
    /// Last update timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Component metrics
    pub metrics: HashMap<String, serde_json::Value>,
}

/// Component state enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComponentState {
    /// Component is inactive
    Inactive,
    /// Component is initializing
    Initializing,
    /// Component is active and working
    Active,
    /// Component is shutting down
    ShuttingDown,
    /// Component has stopped
    Stopped,
    /// Component is in error state
    Error,
}

impl ComponentStatus {
    /// Create a new component status
    pub fn new(component: impl Into<ComponentName>, state: ComponentState) -> Self {
        Self {
            component: component.into(),
            state,
            health: HealthStatus::Unknown,
            message: None,
            timestamp: chrono::Utc::now(),
            metrics: HashMap::new(),
        }
    }

    /// Set health status
    pub fn with_health(mut self, health: HealthStatus) -> Self {
        self.health = health;
        self
    }

    /// Set status message
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Add metrics
    pub fn with_metrics(mut self, metrics: HashMap<String, serde_json::Value>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Check if component is operational
    pub fn is_operational(&self) -> bool {
        matches!(self.state, ComponentState::Active) && self.health.is_operational()
    }
}

/// Manages status reporting for multiple components
pub struct StatusManager {
    service_name: ServiceName,
    components: Arc<RwLock<HashMap<ComponentName, Box<dyn StatusReporter>>>>,
    service_start_time: chrono::DateTime<chrono::Utc>,
}

impl StatusManager {
    /// Create a new status manager
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        Self {
            service_name: service_name.into(),
            components: Arc::new(RwLock::new(HashMap::new())),
            service_start_time: chrono::Utc::now(),
        }
    }

    /// Register a status reporter
    pub fn register_component(&self, reporter: Box<dyn StatusReporter>) -> ServiceResult<()> {
        let component_name = reporter.component_name().to_string();

        let mut components = self
            .components
            .write()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire write lock: {}", e)))?;

        if components.contains_key(&component_name) {
            return Err(ServiceError::Configuration(format!(
                "Component '{}' already registered",
                component_name
            )));
        }

        components.insert(component_name, reporter);
        Ok(())
    }

    /// Unregister a status reporter
    pub fn unregister_component(&self, component_name: &str) -> ServiceResult<()> {
        let mut components = self
            .components
            .write()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire write lock: {}", e)))?;

        components.remove(component_name);
        Ok(())
    }

    /// Get status for all components
    pub fn component_statuses(&self) -> ServiceResult<HashMap<ComponentName, ComponentStatus>> {
        let components = self
            .components
            .read()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire read lock: {}", e)))?;

        let mut statuses = HashMap::new();
        for (name, reporter) in components.iter() {
            statuses.insert(name.clone(), reporter.status());
        }

        Ok(statuses)
    }

    /// Get overall service status
    pub fn service_status(&self, state: LifecycleState) -> ServiceResult<ServiceStatus> {
        let component_statuses = self.component_statuses()?;

        // Determine overall health from component health
        let health_statuses: Vec<HealthStatus> = component_statuses
            .values()
            .map(|status| status.health.clone())
            .collect();
        let overall_health = HealthStatus::combine(&health_statuses);

        // Calculate uptime
        let uptime = chrono::Utc::now()
            .signed_duration_since(self.service_start_time)
            .to_std()
            .unwrap_or_default();

        // Generate metrics summary
        let metrics_summary = self.generate_metrics_summary(&component_statuses)?;

        Ok(ServiceStatus::new(&self.service_name)
            .with_state(state)
            .with_health(overall_health)
            .with_uptime(uptime)
            .with_metrics_summary(metrics_summary))
    }

    /// Get detailed service metrics
    pub fn service_metrics(&self) -> ServiceResult<ServiceMetrics> {
        let components = self
            .components
            .read()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire read lock: {}", e)))?;

        let mut metrics = ServiceMetrics::new(&self.service_name);

        // Collect metrics from all components
        for (component_name, reporter) in components.iter() {
            let component_metrics = reporter.metrics();

            // Prefix component metrics with component name
            for (key, value) in component_metrics {
                let prefixed_key = format!("{}.{}", component_name, key);
                metrics.metadata.insert(prefixed_key, value);
            }
        }

        Ok(metrics)
    }

    fn generate_metrics_summary(
        &self,
        component_statuses: &HashMap<ComponentName, ComponentStatus>,
    ) -> ServiceResult<MetricsSummary> {
        // This is a simplified implementation
        // In practice, you'd aggregate actual metrics from components

        let total_components = component_statuses.len() as u64;
        let active_components = component_statuses
            .values()
            .filter(|status| status.is_operational())
            .count() as u64;

        Ok(MetricsSummary {
            total_operations: total_components,
            operations_per_second: 0.0,
            avg_operation_duration_ms: 0.0,
            error_rate_percent: if total_components > 0 {
                ((total_components - active_components) as f64 / total_components as f64) * 100.0
            } else {
                0.0
            },
            memory_usage_mb: 0,
            cpu_usage_percent: 0.0,
            active_connections: active_components as u32,
        })
    }
}

/// Simple implementation of StatusReporter for basic components
pub struct SimpleStatusReporter {
    component_name: ComponentName,
    state: Arc<RwLock<ComponentState>>,
    health: Arc<RwLock<HealthStatus>>,
    message: Arc<RwLock<Option<String>>>,
    metrics: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl SimpleStatusReporter {
    /// Create a new simple status reporter
    pub fn new(component_name: impl Into<ComponentName>) -> Self {
        Self {
            component_name: component_name.into(),
            state: Arc::new(RwLock::new(ComponentState::Inactive)),
            health: Arc::new(RwLock::new(HealthStatus::Unknown)),
            message: Arc::new(RwLock::new(None)),
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update component state
    pub fn set_state(&self, state: ComponentState) -> ServiceResult<()> {
        let mut current_state = self
            .state
            .write()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire write lock: {}", e)))?;
        *current_state = state;
        Ok(())
    }

    /// Update health status
    pub fn set_health(&self, health: HealthStatus) -> ServiceResult<()> {
        let mut current_health = self
            .health
            .write()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire write lock: {}", e)))?;
        *current_health = health;
        Ok(())
    }

    /// Update status message
    pub fn set_message(&self, message: impl Into<String>) -> ServiceResult<()> {
        let mut current_message = self
            .message
            .write()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire write lock: {}", e)))?;
        *current_message = Some(message.into());
        Ok(())
    }

    /// Update metrics
    pub fn set_metrics(&self, metrics: HashMap<String, serde_json::Value>) -> ServiceResult<()> {
        let mut current_metrics = self
            .metrics
            .write()
            .map_err(|e| ServiceError::Runtime(format!("Failed to acquire write lock: {}", e)))?;
        *current_metrics = metrics;
        Ok(())
    }
}

impl StatusReporter for SimpleStatusReporter {
    fn component_name(&self) -> &str {
        &self.component_name
    }

    fn status(&self) -> ComponentStatus {
        let state = self
            .state
            .read()
            .unwrap_or_else(|_| panic!("Poisoned lock"))
            .clone();
        let health = self
            .health
            .read()
            .unwrap_or_else(|_| panic!("Poisoned lock"))
            .clone();
        let message = self
            .message
            .read()
            .unwrap_or_else(|_| panic!("Poisoned lock"))
            .clone();
        let metrics = self
            .metrics
            .read()
            .unwrap_or_else(|_| panic!("Poisoned lock"))
            .clone();

        ComponentStatus::new(&self.component_name, state)
            .with_health(health)
            .with_message(message.unwrap_or_default())
            .with_metrics(metrics)
    }

    fn metrics(&self) -> HashMap<String, serde_json::Value> {
        self.metrics
            .read()
            .unwrap_or_else(|_| panic!("Poisoned lock"))
            .clone()
    }
}
