//! Preflight verification system for satellite deployment.
//!
//! This module provides comprehensive preflight checks to ensure satellites can
//! operate correctly before they begin processing events. Preflight verification
//! prevents runtime failures by validating all dependencies and prerequisites.
//!
//! ## Verification Categories
//!
//! - **Configuration**: Validate all required configuration values
//! - **Database**: Check database connectivity and schema compatibility
//! - **Resources**: Verify filesystem access, permissions, and disk space
//! - **Services**: Ensure external services (NATS, ingestd) are reachable
//!
//! ## Usage
//!
//! Preflight checks are automatically run by the satellite SDK before starting
//! event processing. Failed checks will prevent satellite startup with detailed
//! error information.

pub mod configuration;
pub mod database;
pub mod resources;
pub mod services;
pub mod verification;

// validate_toml_file is now private to the configuration module
pub use services::verify_service_dependencies;
pub use verification::run_preflight_checks;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VerificationStatus {
    Pass,
    Warning,
    Fail,
}
