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
//! ### Consolidated Integration Tests
//! - **`database_test`**: Database operations with business logic
//! - **`event_sources_test`**: Event source implementations and coordination
//! - **`worker_test`**: Event processing and work distribution  
//! - **`collector_test`**: Event collection and coordination
//! - **`failure_modes_test`**: Graceful degradation and error handling
//! - **`system_integration_test`**: High-level system coordination
//!
//! ### Additional Integration Coverage
//! - Configuration validation across components
//! - Health monitoring integration  
//! - Git Annex storage integration
//! - Query interface functionality
//! - Failure recovery mechanisms
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

/// Consolidated database integration tests
pub mod database_test;

/// Consolidated event source integration tests  
pub mod event_sources_test;

// === Specific Integration Tests ===
// These tests have been consolidated into their respective test files


