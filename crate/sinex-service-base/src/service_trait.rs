//! Core Service Trait
//!
//! Defines the unified interface that all Sinex services must implement.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

use crate::{ServiceId, ServiceName};
use crate::health::HealthReport;
use crate::status::ServiceMetrics;

/// Result type for service operations
pub type ServiceResult<T> = Result<T, ServiceError>;

/// Errors that can occur during service operations
#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Service initialization failed: {0}")]
    Initialization(String),
    
    #[error("Service startup failed: {0}")]
    Startup(String),
    
    #[error("Service shutdown failed: {0}")]
    Shutdown(String),
    
    #[error("Service health check failed: {0}")]
    HealthCheck(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("Dependency error: {0}")]
    Dependency(String),
    
    #[error("Resource error: {0}")]
    Resource(String),
    
    #[error("Runtime error: {0}")]
    Runtime(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Other error: {0}")]
    Other(String),
}

/// Context passed to services during operations
#[derive(Debug, Clone)]
pub struct ServiceContext {
    /// Unique service instance ID
    pub service_id: ServiceId,
    
    /// Human-readable service name
    pub service_name: ServiceName,
    
    /// Service version
    pub version: String,
    
    /// Hostname where service is running
    pub hostname: String,
    
    /// Service configuration
    pub config: HashMap<String, serde_json::Value>,
    
    /// Service dependencies
    pub dependencies: Vec<ServiceId>,
    
    /// Start time
    pub start_time: chrono::DateTime<chrono::Utc>,
}

impl ServiceContext {
    /// Create a new service context
    pub fn new(service_name: impl Into<ServiceName>) -> Self {
        let service_name = service_name.into();
        let service_id = format!("{}-{}", service_name, sinex_ulid::Ulid::new());
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        
        Self {
            service_id,
            service_name,
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname,
            config: HashMap::new(),
            dependencies: Vec::new(),
            start_time: chrono::Utc::now(),
        }
    }
    
    /// Set configuration value
    pub fn set_config(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.config.insert(key.into(), value);
    }
    
    /// Get configuration value
    pub fn get_config(&self, key: &str) -> Option<&serde_json::Value> {
        self.config.get(key)
    }
    
    /// Add dependency
    pub fn add_dependency(&mut self, dependency: impl Into<ServiceId>) {
        self.dependencies.push(dependency.into());
    }
    
    /// Get service uptime
    pub fn uptime(&self) -> chrono::Duration {
        chrono::Utc::now() - self.start_time
    }
}

/// Service capability flags
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCapabilities {
    /// Supports graceful shutdown
    pub supports_graceful_shutdown: bool,
    
    /// Supports health checks
    pub supports_health_checks: bool,
    
    /// Supports configuration reload
    pub supports_config_reload: bool,
    
    /// Supports metrics reporting
    pub supports_metrics: bool,
    
    /// Supports dependency management
    pub supports_dependencies: bool,
    
    /// Custom capabilities
    pub custom: HashMap<String, bool>,
}

impl Default for ServiceCapabilities {
    fn default() -> Self {
        Self {
            supports_graceful_shutdown: true,
            supports_health_checks: true,
            supports_config_reload: false,
            supports_metrics: false,
            supports_dependencies: false,
            custom: HashMap::new(),
        }
    }
}

/// Core trait that all Sinex services must implement
#[async_trait]
pub trait Service: Send + Sync {
    /// Service name (used for identification and logging)
    fn name(&self) -> &str;
    
    /// Service capabilities
    fn capabilities(&self) -> ServiceCapabilities {
        ServiceCapabilities::default()
    }
    
    /// Initialize the service with given context
    async fn initialize(&mut self, context: ServiceContext) -> ServiceResult<()>;
    
    /// Start the service
    async fn start(&mut self) -> ServiceResult<()>;
    
    /// Stop the service gracefully
    async fn stop(&mut self) -> ServiceResult<()>;
    
    /// Perform health check
    async fn health_check(&self) -> ServiceResult<HealthReport> {
        Ok(HealthReport {
            service_name: self.name().to_string(),
            status: crate::health::HealthStatus::Healthy,
            timestamp: chrono::Utc::now(),
            checks: HashMap::new(),
            metadata: HashMap::new(),
        })
    }
    
    /// Get service metrics
    async fn metrics(&self) -> ServiceResult<ServiceMetrics> {
        Ok(ServiceMetrics {
            service_name: self.name().to_string(),
            timestamp: chrono::Utc::now(),
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
            metadata: HashMap::new(),
        })
    }
    
    /// Reload configuration (if supported)
    async fn reload_config(&mut self, _config: HashMap<String, serde_json::Value>) -> ServiceResult<()> {
        Err(ServiceError::Other("Configuration reload not supported".to_string()))
    }
    
    /// Handle service-specific commands
    async fn handle_command(&mut self, _command: &str, _args: Vec<String>) -> ServiceResult<String> {
        Err(ServiceError::Other("Commands not supported".to_string()))
    }
    
    /// Get service status
    async fn status(&self) -> ServiceResult<crate::status::ServiceStatus>;
}

/// Service factory trait for creating services
pub trait ServiceFactory: Send + Sync {
    /// Service type name
    fn service_type(&self) -> &str;
    
    /// Create a new service instance
    fn create_service(&self) -> Box<dyn Service>;
    
    /// Validate configuration for this service type
    fn validate_config(&self, config: &HashMap<String, serde_json::Value>) -> ServiceResult<()> {
        let _ = config;
        Ok(())
    }
}

/// Registry for service factories
pub struct ServiceRegistry {
    factories: HashMap<String, Box<dyn ServiceFactory>>,
}

impl ServiceRegistry {
    /// Create a new service registry
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
    
    /// Register a service factory
    pub fn register<F: ServiceFactory + 'static>(&mut self, factory: F) {
        let service_type = factory.service_type().to_string();
        self.factories.insert(service_type, Box::new(factory));
    }
    
    /// Create a service instance by type
    pub fn create_service(&self, service_type: &str) -> ServiceResult<Box<dyn Service>> {
        self.factories
            .get(service_type)
            .map(|factory| factory.create_service())
            .ok_or_else(|| ServiceError::Configuration(format!("Unknown service type: {}", service_type)))
    }
    
    /// Get all registered service types
    pub fn service_types(&self) -> Vec<&str> {
        self.factories.keys().map(|s| s.as_str()).collect()
    }
    
    /// Validate configuration for a service type
    pub fn validate_config(&self, service_type: &str, config: &HashMap<String, serde_json::Value>) -> ServiceResult<()> {
        self.factories
            .get(service_type)
            .map(|factory| factory.validate_config(config))
            .unwrap_or_else(|| Err(ServiceError::Configuration(format!("Unknown service type: {}", service_type))))
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}