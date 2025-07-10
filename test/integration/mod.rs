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

/// Worker processing and distribution tests
pub mod worker_test;

/// Event collector coordination tests
pub mod collector_test;

/// Failure mode handling tests
pub mod failure_modes_test;

/// System-wide integration tests
pub mod system_integration_test;

/// Search service integration tests
pub mod search_service_test;

/// PKM service integration tests
pub mod pkm_service_test;

/// Analytics service integration tests
pub mod analytics_service_test;

/// Content service integration tests
pub mod content_service_test;

/// BlobManager integration tests
pub mod blob_manager_test;

/// RPC handlers request/response tests
pub mod rpc_handlers_test;


