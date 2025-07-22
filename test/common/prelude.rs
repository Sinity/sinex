// # Sinex Test Prelude
//
// Comprehensive test infrastructure prelude providing all necessary imports for
// writing consistent, maintainable tests across the Sinex project.
//
// ## Core Features
// - **Unified Test Infrastructure**: `#[sinex_test]` and `TestContext`
// - **Database Operations**: Shared pool access and common queries
// - **Event Creation**: `EventFactory` and test event utilities
// - **Timing & Synchronization**: Smart waiting and concurrency primitives
// - **Assertions**: Enhanced assertion macros with better error output
//
// ## Usage
// ```rust
// use crate::common::prelude::*;
//
// #[sinex_test]
// async fn test_example(ctx: TestContext) -> TestResult {
//     let event = EventFactory::new("source").create_event("type", json!({}));
//     insert_event(ctx.pool(), &event).await?;
//     ctx.wait_for_event_count(1).await?;
//     Ok(())
// }
// ```

// ===== Standard Library =====
pub use std::collections::{HashMap, HashSet};
pub use std::fmt::Debug;
pub use std::str::FromStr;
pub use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
pub use std::time::{Duration, Instant};
// ===== Error Handling =====
pub use anyhow::Result as AnyhowResult;
/// Standard error type for test functions
pub type TestResult = AnyhowResult<()>;
pub use sinex_error::{CoreError, ValidationError};
// ===== Serialization =====
pub use serde_json::{json, Value};
// ===== Time and Date =====
pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
// ===== Sinex Core Types =====
pub use sinex_channel::{
    BackpressureManager, ChannelMonitor, ChannelReceiverExt, ChannelSenderExt,
};
pub use sinex_config::{parse_duration, ConfigValue};
pub use sinex_core_types::{MultiValidator, ValidationChain};
pub use sinex_error::ResultExt;
pub use sinex_db::{run_migrations, DbPool, RawEvent};
pub use sinex_ulid::Ulid;
// ===== Async Runtime =====
pub use tokio::sync::mpsc;
pub use tokio::time::timeout;
// ===== Database =====
// ===== Testing Utilities =====
pub use async_trait::async_trait;
pub use futures::future::join_all;
pub use tempfile::TempDir;
// ===== Test Infrastructure =====
// Common modules
// Test context - THE way to write tests
// Event factory and builders - THE way to create events
pub use crate::common::builders::EventBuilder;
pub use sinex_events::{sources, event_types, EventFactory};
// Database helpers available in crate::common::database_helpers
// Test macro - THE way to define tests
pub use sinex_test_macros::sinex_test;
// Test context - THE way to write tests (only import when needed)
#[allow(unused_imports)]
pub use crate::common::test_context::TestContext;
// ===== Timing Helpers =====
pub use sinex_utils::wait_helpers::{
    wait_for_event_count, wait_for_condition_or_timeout,
    wait_for_database_ready,
};
pub use crate::common::{wait_for_filtered_event_count, assert_event_inserted_with_context};
// ===== Common Functions =====
// Event operations
pub use crate::common::insert_event;
// Query shortcuts
pub use sinex_db::{events::get_event_by_id, insert_event_with_validator};
// Test helper functions from common/mod.rs
// Satellite architecture testing utilities
// ===== Enhanced Assertions =====
// Enhanced assertions module is not available
// ===== Mock Types for Testing =====
pub use crate::common::mocks::{
    AtuinHistoryImporter, EventSourceContext, FilesystemMonitor,
    ShellHistoryMonitor,
};
// ===== Worker Test Utilities =====
// Worker test utilities module is not available
// ===== Constants =====

// ===== Test Query Helpers and Builders =====
// Query helpers for test-friendly database operations
pub use crate::common::query_helpers::{TestQueries, CheckpointRecord, OperationRecord};

// Test data builders with fluent interfaces
pub use crate::common::builders::{
    TestEventBuilder, TestCheckpointBuilder, BatchEventBuilder, 
    TestScenarioBuilder, TestEvents
};

// Test fixtures for reusable test data
pub use crate::common::fixtures::{
    FixtureHandle, UserSessionFixture, PopulatedCheckpointsFixture,
    ErrorScenariosFixture, PerformanceDatasetFixture, PreWarmedFixture,
    // Common fixture functions
    standard_user_session, user_session_with_params, empty_database,
    populated_checkpoints, error_scenarios, performance_dataset,
    performance_dataset_with_size, pre_warmed_database,
};

// Test macros for common patterns
pub use crate::{
    test_event_insertion, test_invalid_event, test_batch_events,
    test_checkpoint_flow, test_concurrent_operations, test_time_range_query,
    test_event_filter, test_with_scenario, parameterized_test, test_event_flow,
};

// Snapshot testing utilities
// Snapshot testing module is not available

// Error testing utilities and patterns
pub use crate::common::error_helpers::{
    ErrorAssert, CoreErrorVariant, ErrorScenarioBuilder, 
    CommonErrorScenarios, ErrorRecovery, ErrorPropagation
};

// Error testing macros - import specific macros as needed
pub use crate::{
    assert_error_type, assert_error_contains, assert_error_context,
    test_error_case, test_error_propagation, test_recovery,
    test_validation_error, test_concurrent_errors, test_error_context,
    test_constraint_violation, test_timeout_error, test_error_transformation,
    test_error_idempotency, test_error_with_rollback, test_event_processing_error,
    test_cascading_errors, test_partial_failure,
};
