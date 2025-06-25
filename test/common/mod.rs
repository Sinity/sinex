//! Common test utilities and helpers

#![allow(dead_code)] // Test utilities may not all be used
#![allow(unused_variables)] // Test patterns

use sinex_test_macros::sinex_test;

// Test prelude for standardized imports
pub mod prelude;

// Database helper functions and macros
pub mod database;
pub mod database_helpers;
pub use database_helpers::*;

// Event builder patterns
pub mod event_builders;

// Coverage assurance utilities
pub mod coverage_assurance;

// Schema test utilities
pub mod schema_test_utils;

// Validation test utilities
pub mod validation_test_utils;

// Worker test utilities
pub mod worker_test_utils;

// Test database management
pub mod test_database;

// Test context for unified test setup
pub mod test_context;
pub use test_context::{TestContext, TestConfig};

// Timing optimizations for tests
pub mod timing_optimization;