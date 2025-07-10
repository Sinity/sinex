//! Service Base Framework
//!
//! This crate provides a unified foundation for all Sinex services, including
//! lifecycle management, configuration, health checks, and graceful shutdown handling.

pub mod service_trait;
pub mod lifecycle;
pub mod health;
pub mod config;
pub mod shutdown;
pub mod status;

// Re-export core service types
pub use service_trait::{Service, ServiceContext, ServiceError, ServiceResult};
pub use lifecycle::{ServiceLifecycle, LifecycleManager, LifecycleEvent, LifecycleState};
pub use health::{HealthCheck, HealthStatus, HealthReport, ComponentHealth};
pub use config::{ServiceConfig, ConfigManager, ConfigSource};
pub use shutdown::{ShutdownManager, ShutdownSignal, GracefulShutdown};
pub use status::{ServiceStatus, StatusReporter, ServiceMetrics};

// Common types
pub type ServiceId = String;
pub type ServiceName = String;
pub type ComponentName = String;