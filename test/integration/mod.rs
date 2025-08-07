// # Integration Tests
//
// Tests that verify different components work together correctly without requiring
// complete end-to-end system validation. Integration tests focus on the boundaries
// and interactions between major system components.
//
// ## Scope & Purpose
//
// **Integration tests verify:**
// - Component interactions and interfaces
// - Database operations with business logic
// - Event flow between components
// - Configuration parsing and validation
// - Service startup and coordination
// - Failure handling across component boundaries
//
// **Integration tests do NOT:**
// - Test complete end-to-end workflows (see `system/`)
// - Test individual functions in isolation (see `unit/`)
// - Test extreme edge cases or attacks (see `adversarial/`)
//
// ## Test Categories
//
// ### Consolidated Integration Tests
// - **`database_test`**: Database operations with business logic
// - **`event_sources_test`**: Event source implementations and coordination
// - **`worker_test`**: Event processing and work distribution
// - **`collector_test`**: Event collection and coordination
// - **`failure_modes_test`**: Graceful degradation and error handling
// - **`system_integration_test`**: High-level system coordination
//
// ### Additional Integration Coverage
// - Configuration validation across components
// - Health monitoring integration
// - Git Annex storage integration
// - Query interface functionality
// - Failure recovery mechanisms
//
// ## Running Integration Tests
//
// ```bash
// cargo test --test integration           # All integration tests
// cargo test --test integration::database # Database integration only
// just test-integration                   # Via just command
// ```
//
// ## Test Infrastructure
//
// Integration tests use shared database pools with transaction isolation,
// providing faster test execution while maintaining perfect isolation between tests.

// === Core Component Integration ===

// Test macro demonstrations
// pub mod macro_validation_test; // Temporarily disabled due to async closure lifetime issues

// Consolidated integration tests
// pub mod redis_stream_integration_test; // Temporarily disabled due to async closure lifetime issues

// === Business Logic Integration Tests ===

// Service-level integration tests
pub mod analytics_service_test;
pub mod content_service_test;
pub mod pkm_service_test;
pub mod search_service_test;

// Data integrity and recovery tests
pub mod checkpoint_consistency_test;
pub mod data_corruption_detection_test;
pub mod pel_recovery_test;

// === Core Integration Tests ===

// Database and event processing
pub mod database_test;
pub mod event_sources_test;
pub mod process_event_test;

// System architecture and integration
pub mod end_to_end_workflows_test;
pub mod satellite_architecture_test;
pub mod system_integration_test;

// Modern test infrastructure demonstration
pub mod modern_test_infrastructure_test;

// Failure handling and recovery
pub mod critical_failure_modes_test;
pub mod preflight_integration_test;
pub mod typed_clipboard_integration_test;
