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
pub use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicU32, AtomicUsize, Ordering}};
pub use std::time::{Duration, Instant};
pub use std::collections::{HashMap, HashSet};
pub use std::path::{Path, PathBuf};
pub use std::str::FromStr;

// Error handling
pub use anyhow::{Result, Context as AnyhowContext};

// Serialization and JSON
pub use serde::{Serialize, Deserialize};
pub use serde_json::{json, Value};

// Sinex core types
pub use sinex_ulid::Ulid;
pub use sinex_core::{EventSource, EventSourceContext, RawEvent, RawEventBuilder};
pub use sinex_db::{models::RawEvent as DbRawEvent, queries, create_test_pool, run_migrations};

// Async runtime and synchronization
pub use tokio::sync::{mpsc, Barrier, Mutex, RwLock};
pub use tokio::time::{sleep, timeout, interval};
pub use tokio::task;

// Database 
pub use sqlx::PgPool;

// Testing utilities
pub use futures::future::join_all;
pub use tempfile::TempDir;
pub use async_trait::async_trait;
pub use std::pin::Pin;
pub use std::boxed::Box;

// External utilities  
pub use rand::Rng;

// Test infrastructure from common module
pub use crate::common::{
    events,
    create_test_db_pool,
    create_test_agent,
    resources,
    database_helpers,
    event_sources,
};

// Test macros
pub use crate::{
    test_with_pool, integration_test, test_with_agent, workload_test,
    test_with_transaction, test_with_shared_pool, test_with_transaction_agent
};

// Timing optimization helpers
pub use crate::common::timing_optimization::wait_helpers::{
    wait_for_event_count,
    wait_for_filtered_event_count,
    wait_for_work_queue_count,
    wait_for_work_queue_status_count,
    wait_for_condition,
    wait_for_condition_or_timeout,
};

// Constants commonly used in tests
pub use sinex_core::event_type_constants;
pub use sinex_core::sources;

