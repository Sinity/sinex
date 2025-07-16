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
use sinex_satellite_sdk::EventSourceContext;

pub mod database_test;

/// Schema validation integration tests
pub mod schema_validation_test;

/// Consolidated event source integration tests (ingestor satellite architecture)
pub mod event_sources_test;

/// Collector test - OBSOLETE (replaced by ingestd architecture)
// pub mod collector_test;

/// Automaton processing and event handling tests (replaces worker_test)
// Note: Functionality covered by existing satellite_architecture_test.rs
// pub mod automaton_processing_test;

/// Ingestor coordination tests (replaces collector_test) 
// Note: Functionality covered by existing event_sources_test.rs
// pub mod ingestor_coordination_test;

/// Failure mode handling tests (updated for satellite architecture)
// Disabled - references obsolete sinex_core imports (EventSource, EventSourceContext)
// pub mod failure_modes_test;

/// System-wide integration tests (updated for satellite architecture)
// Disabled - references obsolete sinex_collector and sinex_core imports
// pub mod system_integration_test;

/// Redis Consumer Group fault tolerance tests (replaces orphaned work recovery)
pub mod redis_consumer_group_fault_tolerance_test;

/// PEL (Pending Entry List) recovery tests
pub mod pel_recovery_test;

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
// Disabled - references missing RPC handler functions
// pub mod rpc_handlers_test;

// === Preflight Verification System Tests ===

/// Preflight comprehensive integration tests
pub mod preflight_integration_test;

/// Preflight failure scenarios and error handling tests
pub mod preflight_failure_scenarios_test;

/// Preflight timeout, performance and graceful shutdown tests
pub mod preflight_timeout_performance_test;

/// Preflight rollback mechanisms and recovery tests
pub mod preflight_rollback_recovery_test;

/// Typed clipboard event integration tests
pub mod typed_clipboard_integration_test;

/// Scanner test integration
pub mod scanner_test;

/// Import deduplication tests
pub mod import_deduplication_test;

/// Process event tests
pub mod process_event_test;

/// Edge case coverage tests for dual-mode refactoring
pub mod edge_case_coverage_test;

/// Critical failure modes testing  
pub mod critical_failure_modes_test;

/// Version tracking integration tests
pub mod version_tracking_integration_test;

/// Satellite architecture integration tests
pub mod satellite_architecture_test;

/// Comprehensive satellite integration tests
// Disabled - references obsolete ServerConfig and IngestServer (needs architecture update)
// pub mod satellite_comprehensive_test;

/// Checkpoint persistence and recovery integration tests
pub mod checkpoint_persistence_test;

/// Provenance tracking integration tests  
pub mod provenance_tracking_test;

/// End-to-end workflow integration tests
pub mod end_to_end_workflows_test;
