//! # Integration Tests
//!
//! Tests that verify different components work together correctly without requiring
//! complete end-to-end system validation. Integration tests focus on the boundaries
//! and interactions between major system components.
//!
//! ## Scope & Purpose
//!
//! **Integration tests verify:**
//! - Component interactions and interfaces
//! - Database operations with business logic
//! - Event flow between components
//! - Configuration parsing and validation
//! - Service startup and coordination
//! - Failure handling across component boundaries
//!
//! **Integration tests do NOT:**
//! - Test complete end-to-end workflows (see `system/`)
//! - Test individual functions in isolation (see `unit/`)
//! - Test extreme edge cases or attacks (see `adversarial/`)
//!
//! ## Test Categories
//!
//! ### Core Component Integration
//! - **`database/`**: Database operations with business logic
//! - **`collector/`**: Event collection and coordination
//! - **`worker/`**: Event processing and work distribution
//! - **`event_sources/`**: Event source implementations
//! - **`agent/`**: Agent lifecycle and communication
//!
//! ### System Integration
//! - **`failure_modes/`**: Graceful degradation and error handling
//! - **`infrastructure/`**: Infrastructure component coordination
//!
//! ### Specific Integration Tests
//! - Configuration validation across components
//! - Health monitoring integration
//! - Git Annex storage integration
//! - Query interface functionality
//! - System startup coordination
//! - Failure recovery mechanisms
//! - Deployment validation
//!
//! ## Running Integration Tests
//!
//! ```bash
//! cargo test --test integration           # All integration tests
//! cargo test --test integration::database # Database integration only
//! just test-integration                   # Via just command
//! ```
//!
//! ## Test Infrastructure
//!
//! Integration tests use shared database pools with transaction isolation,
//! providing faster test execution while maintaining perfect isolation between tests.

// === Core Component Integration ===

/// Database operations integrated with business logic
pub mod database;

/// Consolidated database integration tests (replaces database/*)
pub mod database_test;

/// Event collection and coordination testing
pub mod collector;

/// Event processing and work distribution testing
pub mod worker;

/// Event source implementation testing
pub mod event_sources;

/// Agent lifecycle and communication testing
pub mod agent;

// === System Integration ===

/// Failure handling across component boundaries
pub mod failure_modes;

/// Infrastructure component coordination
pub mod infrastructure;

// === Specific Integration Tests ===

/// Query interface functionality testing
pub mod query_interface_test;

/// System startup coordination testing
pub mod full_system_startup_test;

/// Failure recovery mechanism testing
pub mod failure_recovery_integration_test;

/// Health monitoring integration testing
pub mod health_monitoring_integration_test;

/// Git Annex storage integration testing
pub mod git_annex_full_integration_test;


/// Deployment validation testing
pub mod deployment_validation_test;


