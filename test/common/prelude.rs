//! Test prelude for standardized imports across the Sinex test suite
//! 
//! This module provides commonly used imports to reduce boilerplate
//! and ensure consistency across test files.
//!
//! Usage:
//! ```rust
//! use crate::common::prelude::*;
//! ```
// ===== Standard Library =====
pub use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering}};
pub use std::time::{Duration, Instant};
pub use std::collections::{HashMap, HashSet};
pub use std::str::FromStr;
pub use std::path::{Path, PathBuf};
// ===== Error Handling =====
pub use anyhow::Result;
// ===== Serialization =====
pub use serde::{Serialize, Deserialize};
pub use serde_json::{json, Value};
// ===== Sinex Core Types =====
pub use sinex_ulid::Ulid;
pub use sinex_core::{
    EventSource, EventSourceContext, 
    RawEvent, RawEventBuilder,
    sources, event_type_constants,
};
pub use sinex_db::{
    queries, run_migrations,
};
// ===== Async Runtime =====
pub use tokio::sync::{mpsc, Barrier};
pub use tokio::time::{sleep, interval, timeout};
// ===== Database =====
pub use sqlx::PgPool;
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
// ===== Assertion Enhancements =====
// Use pretty_assertions for better diffs
// ===== Constants =====
