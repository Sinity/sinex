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
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    Arc,
};
pub use std::time::{Duration, Instant};
// ===== Error Handling =====
pub use anyhow::Result as AnyhowResult;
/// Standard error type for test functions
pub type TestResult = AnyhowResult<()>;
pub use sinex_core_types::CoreError;
pub use sinex_validation::Result as ValidationResult;
// ===== Serialization =====
pub use serde::{Deserialize, Serialize};
pub use serde_json::{json, Value};
// ===== Time and Date =====
pub use chrono::{DateTime, Duration as ChronoDuration, Utc};
// ===== Sinex Core Types =====
pub use sinex_channel::{
    BackpressureManager, ChannelMonitor, ChannelReceiverExt, ChannelSenderExt,
};
pub use sinex_config::{parse_duration, ConfigValue};
pub use sinex_core_types::{event_type_constants, MultiValidator, ResultExt, ValidationChain};
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
pub use crate::common::event_builders::EventBuilder;
pub use sinex_events::{sources, event_types, EventFactory};
// Database helpers available in crate::common::database_helpers
// Test macro - THE way to define tests
pub use sinex_test_macros::sinex_test;
// Test context - THE way to write tests (only import when needed)
#[allow(unused_imports)]
pub use crate::common::test_context::TestContext;
// ===== Timing Helpers =====
pub use crate::common::timing_optimization::wait_helpers::wait_for_filtered_event_count;
// ===== Common Functions =====
// Event operations
pub use crate::common::insert_event;
// Query shortcuts
pub use sinex_db::{events, events::get_event_by_id, insert_event_with_validator};
// Test helper functions from common/mod.rs
pub use crate::common::{get_events_by_type, get_events_in_time_range, get_recent_events};
// Satellite architecture testing utilities
pub use crate::common::{count_events_from_source, start_test_ingestd, start_test_ingestd_at_path};
// ===== Enhanced Assertions =====
pub use crate::common::enhanced_assertions::{
    assert_channel_send_success, assert_eq_with_context, assert_event_inserted_with_context,
    assert_events_equivalent, assert_validation_passes, assert_with_context,
    assert_with_validation, TestAssertionBatch,
};
// ===== Mock Types for Testing =====
pub use crate::common::mocks::{
    AtuinHistoryImporter, ClipboardMonitor, EventSourceContext, FilesystemMonitor, RedisClient,
    ShellHistoryMonitor, TerminalMonitor,
};
// ===== Worker Test Utilities =====
pub use crate::common::worker_test_utils::{
    claim_work_queue_items, clear_work_queue, complete_work_queue_item, create_work_item,
    ensure_work_queue_table, get_work_queue_status, WorkQueueItem,
};
// ===== Constants =====
