//! # Sinex Test Prelude
//!
//! Comprehensive test infrastructure prelude providing all necessary imports for
//! writing consistent, maintainable tests across the Sinex project.
//!
//! ## Core Features
//! - **Unified Test Infrastructure**: `#[sinex_test]` and `TestContext`
//! - **Database Operations**: Shared pool access and common queries
//! - **Event Creation**: `EventFactory` and test event utilities
//! - **Timing & Synchronization**: Smart waiting and concurrency primitives
//! - **Assertions**: Enhanced assertion macros with better error output
//!
//! ## Usage
//! ```rust
//! use crate::common::prelude::*;
//!
//! #[sinex_test]
//! async fn test_example(ctx: TestContext) -> TestResult {
//!     let event = EventFactory::new("source").create_event("type", json!({}));
//!     insert_event(ctx.pool(), &event).await?;
//!     ctx.wait_for_event_count(1).await?;
//!     Ok(())
//! }
//! ```
// ===== Standard Library =====
pub use std::collections::{HashMap, HashSet};
pub use std::fmt::Debug;
pub use std::path::{Path, PathBuf};
pub use std::str::FromStr;
pub use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    Arc,
};
pub use std::time::{Duration, Instant};
// ===== Error Handling =====
pub use anyhow::Result;
/// Standard error type for test functions
pub type TestResult = Result<(), Box<dyn std::error::Error>>;
// ===== Serialization =====
pub use serde::{Deserialize, Serialize};
pub use serde_json::{json, Value};
// ===== Sinex Core Types =====
pub use sinex_core::{
    event_type_constants, parse_duration, sources,
    unified_collector::{EventRegistry},
    BackpressureManager, ChannelMonitor, ChannelReceiverExt, ChannelSenderExt, ConfigExtractor,
    ConfigValidator, ConfigValue, CoreError, EventSource, EventSourceContext, MultiValidator,
    EventFactory, ResultExt, ValidationChain,
};
pub use sinex_collector::create_registry_with_auto_registration as create_registry;
pub use sinex_db::{
    prelude::{AgentManifest, QueueStatus, WorkQueueItem},
    queries, run_migrations, DbPool, RawEvent,
};
pub use sinex_ulid::Ulid;
// ===== Async Runtime =====
pub use tokio::sync::{mpsc, Barrier};
pub use tokio::time::{interval, sleep, timeout};
// ===== Database =====
// ===== Testing Utilities =====
pub use async_trait::async_trait;
pub use futures::future::join_all;
pub use tempfile::{NamedTempFile, TempDir};
// ===== Test Infrastructure =====
// Common modules
pub use crate::common::{database_helpers, event_sources, events};
// Test context - THE way to write tests
// Event factory - THE way to create events
pub use sinex_core::EventFactory;
// Database helpers
// NEW: Unified database access
// pub use crate::common::create_test_db_pool;
pub use crate::common::database::{CleanupStrategy, TestPool, TestPoolExt};
pub use crate::common::database_helpers::{
    create_test_event,
    // create_test_agent, purge_old_work_queue_items - available but unused currently
};
// Test macro - THE way to define tests
pub use sinex_test_macros::sinex_test;
// Test context - THE way to write tests (only import when needed)
#[allow(unused_imports)]
pub use crate::common::test_context::TestContext;
// ===== Timing Helpers =====
pub use crate::common::timing_optimization::wait_helpers::{
    wait_for_condition_or_timeout, wait_for_filtered_event_count, wait_for_work_queue_count,
    wait_for_work_queue_status_count,
};
// ===== Common Functions =====
// Event operations
pub use crate::common::insert_event;
// Query shortcuts
pub use sinex_db::queries::{
    add_to_work_queue, calculate_queue_depth_metrics, claim_work_queue_items,
    complete_work_queue_item, fail_work_queue_item, insert_raw_event,
};
// ===== Enhanced Assertions =====
pub use crate::common::enhanced_assertions::{
    assert_channel_send_success, assert_database_state, assert_eq_with_context,
    assert_event_inserted_with_context, assert_events_equivalent, assert_validation_passes,
    assert_with_context, assert_with_validation, TestAssertionBatch,
};
// ===== Configuration Testing =====
// pub use crate::common::config_test_utils::{
//     extraction as config_extraction, scenarios as config_scenarios, test_configs,
//     validation as config_validation, CollectorTestConfig, DatabaseTestConfig, SourcesTestConfig,
//     TestConfigFactory,
// };
// ===== Channel Testing =====
// pub use crate::common::channel_test_utils::{
//     backpressure as channel_backpressure, behavior as channel_behavior,
//     monitoring as channel_monitoring, performance as channel_performance,
//     scenarios as channel_scenarios, ChannelHealthReport, ChannelPerformanceReport,
//     TestChannelSetup,
// };
// ===== Assertion Enhancements =====
// Use pretty_assertions for better diffs
// ===== Constants =====
