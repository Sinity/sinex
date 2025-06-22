//! Test prelude for standardized imports across the Sinex test suite
//! 
//! This module provides commonly used imports to reduce boilerplate
//! and ensure consistency across test files.
//!
//! Usage:
//! ```rust
//! use crate::common::prelude::*;
//! ```

// Standard library imports used in most tests
pub use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering}};
pub use std::time::{Duration, Instant};
pub use std::collections::{HashMap, HashSet};
pub use std::str::FromStr;

// Error handling
pub use anyhow::Result;

// Serialization and JSON
pub use serde::{Serialize, Deserialize};
pub use serde_json::{json, Value};

// Sinex core types
pub use sinex_ulid::Ulid;
pub use sinex_core::{EventSource, EventSourceContext, RawEvent, RawEventBuilder};
pub use sinex_db::{queries, run_migrations};

// Async runtime and synchronization
pub use tokio::sync::{mpsc, Barrier};
pub use tokio::time::{sleep, interval};

// Database 
pub use sqlx::PgPool;

// Testing utilities
pub use futures::future::join_all;
pub use tempfile::TempDir;
pub use async_trait::async_trait;
pub use std::boxed::Box;

// Test infrastructure from common module
pub use crate::common::{
    events,
    database_helpers,
    event_sources,
};

// Test macros - keeping basic ones
pub use crate::{
    test_with_transaction, test_with_shared_pool
};

// Timing optimization helpers
pub use crate::common::timing_optimization::wait_helpers::{
    wait_for_filtered_event_count,
    wait_for_work_queue_count,
    wait_for_work_queue_status_count,
    wait_for_condition_or_timeout,
};

// Constants commonly used in tests
pub use sinex_core::sources;

