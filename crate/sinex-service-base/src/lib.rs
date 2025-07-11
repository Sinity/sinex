//! Service Base Framework
//!
//! This crate provides a unified foundation for all Sinex services, including
//! lifecycle management, configuration, health checks, and graceful shutdown handling.

pub mod config;
pub mod health;
pub mod lifecycle;
pub mod service_trait;
pub mod shutdown;
pub mod status;

// Re-export core service types
pub use config::{ConfigManager, ConfigSource, ServiceConfig};
pub use health::{ComponentHealth, HealthCheck, HealthReport, HealthStatus};
pub use lifecycle::{LifecycleEvent, LifecycleManager, LifecycleState, ServiceLifecycle};
pub use service_trait::{Service, ServiceContext, ServiceError, ServiceResult};
pub use shutdown::{GracefulShutdown, ShutdownManager, ShutdownSignal};
pub use status::{ServiceMetrics, ServiceStatus, StatusReporter};

// Common types
pub type ServiceId = String;
pub type ServiceName = String;
pub type ComponentName = String;
