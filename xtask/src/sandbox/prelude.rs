pub use color_eyre::eyre::{bail, eyre, Error, Result, WrapErr};
pub use futures::future::BoxFuture;
pub use once_cell::sync::Lazy;
pub use serde_json::{json, Value as JsonValue};
pub use sinex_db::{DbPool, DbPoolExt};
pub use sinex_primitives::prelude::*;
pub use sinex_primitives::{
    DynamicPayload, Event, EventSource, EventType, Id, OffsetDateTime, SinexError, Timestamp, Ulid,
};

pub type EventId = Id<Event>;
pub use sqlx::Postgres;
pub use std::sync::Arc;
pub use std::time::Duration;
pub use tokio::time::sleep;

// Proptest re-exports
pub use proptest::prelude::*;

// Macro re-exports
pub use xtask_macros::{sinex_bench, sinex_prop, sinex_proptest, sinex_serial_test, sinex_test};

pub use super::assertions::EventAssert;
pub use super::context::{Sandbox, SandboxFailureSnapshot, SandboxHandle};
pub use super::db::cleanup_config::{CleanupConfig, CleanupMethod, TableCleanupStrategy};
pub use super::db::{reset_database, verify_clean_state};
pub use super::nats::{EphemeralNats, EphemeralNatsBuilder, TlsConfig};
pub use super::orchestrator::{
    start_test_ingestd_with_config, TestIngestdConfig, TestIngestdHandle,
};
pub use super::preflight::*;
pub use super::timing::{Timeouts, TimingUtils, WaitHelpers};

// Type aliases
pub type TestContext = Sandbox;
pub type TestResult<T> = color_eyre::Result<T>;
pub type SandboxResult<T> = color_eyre::Result<T>;
