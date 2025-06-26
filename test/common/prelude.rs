//! # Sinex Test Prelude
//! 
//! Comprehensive test infrastructure prelude providing all necessary imports for
//! writing consistent, maintainable tests across the Sinex project.
//!
//! ## Core Features
//! - **Unified Test Infrastructure**: `#[sinex_test]` and `TestContext`
//! - **Database Operations**: Shared pool access and common queries  
//! - **Event Creation**: `RawEventBuilder` and test event utilities
//! - **Timing & Synchronization**: Smart waiting and concurrency primitives
//! - **Assertions**: Enhanced assertion macros with better error output
//!
//! ## Usage
//! ```rust
//! use crate::common::prelude::*;
//!
//! #[sinex_test]
//! async fn test_example(ctx: TestContext) -> TestResult {
//!     let event = RawEventBuilder::new("source", "type", json!({})).build();
//!     insert_event(ctx.pool(), &event).await?;
//!     ctx.wait_for_event_count(1).await?;
//!     Ok(())
//! }
//! ```
// ===== Standard Library =====
pub use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering}};
pub use std::time::{Duration, Instant};
pub use std::collections::{HashMap, HashSet};
pub use std::str::FromStr;
pub use std::path::{Path, PathBuf};
pub use std::fmt::Debug;
// ===== Error Handling =====
pub use anyhow::Result;
/// Standard error type for test functions
pub type TestResult = Result<(), Box<dyn std::error::Error>>;
// ===== Serialization =====
pub use serde::{Serialize, Deserialize};
pub use serde_json::{json, Value};
// ===== Sinex Core Types =====
pub use sinex_ulid::Ulid;
pub use sinex_core::{
    EventSource, EventSourceContext, 
    RawEventBuilder,
    sources, event_type_constants,
    ValidationChain, MultiValidator, JsonType, ErrorContext, ResultExt, CoreError,
    ConfigExtractor, ConfigValidator, parse_duration, ConfigValue,
    ChannelSenderExt, ChannelReceiverExt, ChannelMonitor, BackpressureManager,
    unified_collector::{EventRegistry, create_registry},
};
pub use sinex_db::{
    queries, run_migrations,
    RawEvent,
    DbPool,
    prelude::{AgentManifest, WorkQueueItem, QueueStatus},
};
// ===== Async Runtime =====
pub use tokio::sync::{mpsc, Barrier};
pub use tokio::time::{sleep, interval, timeout};
// ===== Database =====
// ===== Testing Utilities =====
pub use futures::future::join_all;
pub use tempfile::{TempDir, NamedTempFile};
pub use async_trait::async_trait;
// ===== Test Infrastructure =====
// Common modules
pub use crate::common::{
    events,
    database_helpers,
    event_sources,
    event_builders::EventBuilder,
};
// Test context - THE way to write tests
// Event builders - THE way to create events
// Database helpers
// NEW: Unified database access
pub use crate::common::database::{TestPool, CleanupStrategy, TestPoolExt};
pub use crate::common::database_helpers::{
    create_test_event, 
    // create_test_agent, purge_old_work_queue_items - available but unused currently
};
pub use crate::common::{create_test_db_pool};
// Test macro - THE way to define tests
pub use sinex_test_macros::sinex_test;
// Test context - THE way to write tests (only import when needed)
#[allow(unused_imports)]
pub use crate::common::test_context::TestContext;
// ===== Timing Helpers =====
pub use crate::common::timing_optimization::wait_helpers::{
    wait_for_filtered_event_count,
    wait_for_work_queue_count,
    wait_for_work_queue_status_count,
    wait_for_condition_or_timeout,
};
// ===== Common Functions =====
// Event operations
pub use crate::common::insert_event;
// Query shortcuts
pub use sinex_db::queries::{
    add_to_work_queue,
    claim_work_queue_items,
    complete_work_queue_item,
    fail_work_queue_item,
    insert_raw_event,
    calculate_queue_depth_metrics,
};
// ===== Enhanced Assertions =====
pub use crate::common::enhanced_assertions::{
    assert_with_validation, assert_eq_with_context, assert_with_context,
    assert_event_inserted_with_context, assert_completes_within,
    assert_validation_passes, assert_validation_fails,
    assert_channel_send_success, assert_channel_send_timeout,
    assert_config_valid, assert_config_extraction,
    assert_database_state, assert_events_equivalent,
    TestAssertionBatch,
};
// ===== Configuration Testing =====
pub use crate::common::config_test_utils::{
    test_configs, validation as config_validation, extraction as config_extraction,
    TestConfigFactory, scenarios as config_scenarios,
    DatabaseTestConfig, CollectorTestConfig, SourcesTestConfig,
};
// ===== Channel Testing =====
pub use crate::common::channel_test_utils::{
    TestChannelSetup, behavior as channel_behavior, backpressure as channel_backpressure,
    performance as channel_performance, monitoring as channel_monitoring,
    scenarios as channel_scenarios, ChannelPerformanceReport, ChannelHealthReport,
};
// ===== Assertion Enhancements =====
// Use pretty_assertions for better diffs
// ===== Constants =====
