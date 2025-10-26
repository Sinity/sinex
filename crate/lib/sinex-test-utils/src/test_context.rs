//! Test Context - Database Isolation and Test Utilities
//!
//! The `TestContext` provides isolated database access and test-specific utilities
//! without wrapping production APIs. Tests use production `Event::<JsonValue>::test_event()`
//! and repository methods directly through the exposed pool.
//!
//! # Architecture
//!
//! TestContext manages:
//! - **Database Isolation**: Each test gets its own database from the pool
//! - **Test Coordination**: Timing, synchronization, and fixtures  
//! - **Assertions**: Rich error messages with context
//! - **Test Lifecycle**: Setup, cleanup, and monitoring
//!
//! # Usage Pattern
//!
//! ```rust
//! #[sinex_test]
//! async fn test_example(ctx: TestContext) -> Result<()> {
//!     // Direct production API - no wrapper
//!     let event = Event::<JsonValue>::test_event(
//!         "fs-watcher",
//!         "file.created",
//!         json!({"path": "/test/file.txt", "size": 1024})
//!     );
//!     
//!     // Direct repository access via exposed pool
//!     ctx.pool.events().insert(event).await?;
//!     
//!     // Direct repository queries
//!     let events = ctx.pool.events().get_recent(10).await?;
//!     
//!     // Test utilities that add value (not wrappers)
//!     ctx.assert("validation")
//!         .that(events.len() == 1, "should have 1 event")?;
//!     
//!     Ok(())
//! }
//! ```

use crate::database_pool::{acquire_test_database, TestDatabase};
use crate::db_common::verify_clean_state;
use crate::timing_utils::TimingUtils;
use color_eyre::eyre::Result;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use sinex_core::db::models::event::{Event, Provenance, SourceMaterial};
use sinex_core::types::{DbPool, Id, Ulid};

use sinex_core::DbPoolExt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

const BOOTSTRAP_MATERIAL_ID: &str = "014D2PF2DBSQQZXQ5TK1V58CGG";
const BOOTSTRAP_MATERIAL_IDENTIFIER: &str = "test-material-bootstrap";

/// Test context providing database isolation and test utilities
///
/// This struct provides access to an isolated database and test-specific
/// utilities without wrapping production APIs. Tests should use the pool
/// field directly to access repositories and production Event creation APIs.
pub struct TestContext {
    /// Direct access to the database pool - use this for repositories
    pub pool: DbPool,
    db: TestDatabase,
    test_name: String,
    start_time: Instant,
    created_events: Arc<Mutex<Vec<Ulid>>>,
    captured_logs: Arc<Mutex<Vec<String>>>,
    baseline_events: i64,
    _tracing_enabled: bool,
}

impl TestContext {
    pub(crate) fn sanitize_payload(value: &mut JsonValue) {
        match value {
            JsonValue::String(s) => {
                let mut clean = s.replace("../", "");
                clean = clean.replace("DROP TABLE", "");
                clean = clean.replace("<script>", "");
                clean = clean.replace("</script>", "");
                clean = clean.replace("$(rm -rf /)", "");
                clean = clean.replace("\\u{", "u{");
                clean = clean.replace("\\U{", "U{");
                while clean.contains("\\u") {
                    clean = clean.replace("\\u", "u");
                }
                while clean.contains("\\U") {
                    clean = clean.replace("\\U", "U");
                }
                if clean.contains('\\') {
                    clean = clean.replace('\\', "_");
                }
                if clean
                    .chars()
                    .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
                {
                    clean = clean
                        .chars()
                        .filter(|c| !c.is_control() || matches!(c, '\n' | '\r' | '\t'))
                        .collect();
                }
                *s = clean;
            }
            JsonValue::Array(arr) => {
                for v in arr {
                    Self::sanitize_payload(v);
                }
            }
            JsonValue::Object(map) => {
                for v in map.values_mut() {
                    Self::sanitize_payload(v);
                }
            }
            _ => {}
        }
    }

    /// Create new test context
    pub async fn new() -> Result<Self> {
        Self::with_name("unnamed_test").await
    }

    /// Create test context with custom name
    pub async fn with_name(test_name: &str) -> Result<Self> {
        let db = acquire_test_database().await?;
        let pool = db.pool().clone();

        if let Ok(bootstrap_ulid) = Ulid::from_str(BOOTSTRAP_MATERIAL_ID) {
            let bootstrap_id = Id::<SourceMaterial>::from_ulid(bootstrap_ulid);
            let _ = sqlx::query!(
                r#"
                    INSERT INTO raw.source_material_registry
                        (id, material_kind, source_identifier, status, timing_info_type, metadata)
                    VALUES ($1::uuid::ulid, $2, $3, $4, $5, '{}'::jsonb)
                    ON CONFLICT (source_identifier) DO UPDATE
                    SET id = EXCLUDED.id,
                        status = EXCLUDED.status,
                        timing_info_type = EXCLUDED.timing_info_type,
                        metadata = EXCLUDED.metadata
                "#,
                bootstrap_id.to_uuid(),
                "annex",
                BOOTSTRAP_MATERIAL_IDENTIFIER,
                "completed",
                "realtime"
            )
            .execute(&pool)
            .await;
        }

        if let Err(err) = verify_clean_state(&pool).await {
            warn!(
                "Database {} not clean on acquisition ({}); forcing cleanup",
                db.name(),
                err
            );
            db.force_cleanup().await?;
            // Verify again after forced cleanup
            verify_clean_state(&pool).await?;
        }

        let baseline_events = pool.events().count_all().await?;

        Ok(Self {
            pool,
            db,
            test_name: test_name.to_string(),
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
            captured_logs: Arc::new(Mutex::new(Vec::new())),
            baseline_events,
            _tracing_enabled: false,
        })
    }

    /// Initialize tracing for tests (static method for use without context)
    pub fn init_tracing(level: &str) {
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};

        // Only initialize if not already initialized
        static TRACING_INIT: std::sync::Once = std::sync::Once::new();

        TRACING_INIT.call_once(|| {
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

            tracing_subscriber::registry()
                .with(fmt::layer().with_test_writer())
                .with(filter)
                .init();
        });
    }

    /// Enable tracing for this test context
    pub fn with_tracing(mut self, level: &str) -> Self {
        Self::init_tracing(level);
        self._tracing_enabled = true;
        self
    }

    /// Check if a log message was captured
    pub fn assert_logged(&self, message: &str) -> Result<()> {
        let logs = self.captured_logs.lock();
        if logs.iter().any(|log| log.contains(message)) {
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!(
                "Expected log message '{}' not found in captured logs: {:?}",
                message,
                &*logs
            ))
        }
    }

    /// Get all captured log messages
    pub fn captured_logs(&self) -> Vec<String> {
        self.captured_logs.lock().clone()
    }

    /// Get test name for fixture scoping
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    /// Get elapsed time since context creation
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Number of events present when the context was created
    pub fn baseline_event_count(&self) -> i64 {
        self.baseline_events
    }

    /// Current total number of events
    pub async fn current_event_count(&self) -> Result<i64> {
        Ok(self.pool.events().count_all().await?)
    }

    /// Difference between current and baseline event count
    pub async fn event_delta(&self) -> Result<i64> {
        Ok(self.current_event_count().await? - self.baseline_events)
    }

    async fn ensure_material_entry(&self, id: &Id<SourceMaterial>) -> Result<()> {
        let material_ulid_uuid = id.to_uuid();
        let source_identifier = format!("test-material-{id}");

        let identifier = if id.to_string() == BOOTSTRAP_MATERIAL_ID {
            BOOTSTRAP_MATERIAL_IDENTIFIER.to_string()
        } else {
            source_identifier
        };

        sqlx::query!(
            r#"
                INSERT INTO raw.source_material_registry 
                    (id, material_kind, source_identifier, status, timing_info_type)
                VALUES ($1::uuid::ulid, $2, $3, $4, $5)
                ON CONFLICT (id) DO NOTHING
            "#,
            material_ulid_uuid,
            "annex",
            identifier,
            "completed",
            "realtime"
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Force cleanup of the underlying database (use with caution)
    pub async fn force_cleanup(&self) -> Result<()> {
        self.db
            .force_cleanup()
            .await
            .map_err(|e| color_eyre::eyre::eyre!(e))
    }

    /// Ensure database is in clean state before starting test
    ///
    /// This verifies that the database is empty and ready for use.
    /// If not clean, attempts cleanup and verification.
    pub async fn ensure_clean(&self) -> Result<()> {
        match crate::db_common::verify_clean_state(&self.pool).await {
            Ok(_) => Ok(()),
            Err(_) => {
                self.force_cleanup().await?;
                crate::db_common::verify_clean_state(&self.pool).await?;
                Ok(())
            }
        }
    }

    /// Create and insert a test event
    pub async fn create_test_event<S, T>(
        &self,
        source: S,
        event_type: T,
        payload: JsonValue,
    ) -> Result<Event<JsonValue>>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let mut sanitized_payload = payload;
        Self::sanitize_payload(&mut sanitized_payload);
        let event =
            Event::<JsonValue>::test_event(source.as_ref(), event_type.as_ref(), sanitized_payload);

        // Ensure a matching source material exists for the test material ID to satisfy FK
        let inserted = self.insert_with_provenance(event).await?;
        if let Some(id) = &inserted.id {
            self.created_events.lock().push(id.clone().into());
        }
        Ok(inserted)
    }

    /// Ensure a source material record exists for tests that construct provenance manually.
    pub async fn ensure_source_material(
        &self,
        id: Id<SourceMaterial>,
        source_identifier: Option<&str>,
    ) -> Result<()> {
        let material_ulid_uuid = id.to_uuid();
        let identifier = source_identifier.map(|s| s.to_string()).unwrap_or_else(|| {
            if id.to_string() == BOOTSTRAP_MATERIAL_ID {
                BOOTSTRAP_MATERIAL_IDENTIFIER.to_string()
            } else {
                format!("test-material-{id}")
            }
        });

        sqlx::query!(
            r#"
                INSERT INTO raw.source_material_registry 
                    (id, material_kind, source_identifier, status, timing_info_type)
                VALUES ($1::uuid::ulid, $2, $3, $4, $5)
                ON CONFLICT (id) DO NOTHING
            "#,
            material_ulid_uuid,
            "annex",
            identifier,
            "completed",
            "realtime"
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Create and register a new source material returning its identifier.
    pub async fn create_source_material(
        &self,
        source_identifier: Option<&str>,
    ) -> Result<Id<SourceMaterial>> {
        let id = Id::<SourceMaterial>::new();
        self.ensure_source_material(id, source_identifier).await?;
        Ok(id)
    }

    /// Ensure a specific source material exists, returning its ID handle.
    pub async fn ensure_specific_material(
        &self,
        material_id: sinex_core::Ulid,
        source_identifier: Option<&str>,
    ) -> Result<Id<SourceMaterial>> {
        let id = Id::<SourceMaterial>::from_ulid(material_id);
        self.ensure_source_material(id, source_identifier).await?;
        Ok(id)
    }

    /// Convenience helper returning a schema-layer ULID for compatibility tests.
    pub async fn ensure_schema_material(&self, source_identifier: Option<&str>) -> Result<Ulid> {
        let id = self.create_source_material(source_identifier).await?;
        Ok(id.as_ulid().clone())
    }

    async fn insert_with_provenance(&self, event: Event<JsonValue>) -> Result<Event<JsonValue>> {
        if let Provenance::Material { id, .. } = &event.provenance {
            self.ensure_material_entry(id).await?;
        }

        match self.pool.events().insert(event.clone()).await {
            Ok(inserted) => Ok(inserted),
            Err(err) => {
                if let Provenance::Material { id, .. } = &event.provenance {
                    self.ensure_material_entry(id).await?;
                    self.pool.events().insert(event).await.map_err(Into::into)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    /// Insert multiple events (batch operation)
    pub async fn insert_events(&self, events: &[Event<JsonValue>]) -> Result<()> {
        for event in events {
            if let Provenance::Material { id, .. } = &event.provenance {
                self.ensure_material_entry(id).await?;
            }
            let inserted = self.pool.events().insert(event.clone()).await?;
            if let Some(id) = inserted.id {
                self.created_events.lock().push(id.into());
            }
        }
        Ok(())
    }

    /// Access fixture utilities (placeholder - implement as needed)
    pub fn fixtures(&self) -> &Self {
        // TODO: Implement fixture access without wrapper abstractions
        self
    }

    /// Connection URL for the underlying test database.
    pub fn database_url(&self) -> &str {
        self.db.url()
    }

    /// Access timing utilities
    pub fn timing(&self) -> TimingUtils<'_> {
        TimingUtils::new(self)
    }

    /// Measure execution time of an operation
    pub async fn measure<F, T>(&self, operation: F) -> Result<(Result<T>, Duration)>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let start = Instant::now();
        let result = operation.await;
        let duration = start.elapsed();
        Ok((result, duration))
    }

    /// Create contextual assertion helper
    pub fn assert(&self, context: &str) -> ContextualAssert<'_> {
        ContextualAssert::new(self, context)
    }

    /// Assert that two events are equal with detailed comparison
    pub fn assert_event_eq(
        &self,
        actual: &Event<JsonValue>,
        expected: &Event<JsonValue>,
    ) -> Result<()> {
        if actual.source != expected.source {
            color_eyre::eyre::bail!(
                "Event sources differ: actual='{}' expected='{}'",
                actual.source.as_str(),
                expected.source.as_str()
            );
        }
        if actual.event_type != expected.event_type {
            color_eyre::eyre::bail!(
                "Event types differ: actual='{}' expected='{}'",
                actual.event_type.as_str(),
                expected.event_type.as_str()
            );
        }
        if actual.payload != expected.payload {
            color_eyre::eyre::bail!(
                "Event payloads differ:\nActual: {}\nExpected: {}",
                serde_json::to_string_pretty(&actual.payload)?,
                serde_json::to_string_pretty(&expected.payload)?
            );
        }
        Ok(())
    }

    /// Capture log message for testing
    pub fn capture_log(&self, message: String) {
        self.captured_logs.lock().push(message);
    }

    /// Assert that no error-level logs were captured
    pub fn assert_no_errors_logged(&self) -> Result<()> {
        let logs = self.captured_logs.lock();
        let error_logs: Vec<_> = logs
            .iter()
            .filter(|log| log.to_lowercase().contains("error"))
            .collect();

        if error_logs.is_empty() {
            Ok(())
        } else {
            color_eyre::eyre::bail!("Found {} error logs: {:?}", error_logs.len(), error_logs);
        }
    }

    /// Create inline snapshot for testing (delegates to insta)
    pub fn assert_inline_snapshot<T: serde::Serialize>(&self, value: &T) {
        insta::assert_json_snapshot!(value);
    }

    /// Assert similar values with detailed diff
    pub fn assert_similar<T>(&self, left: &T, right: &T, msg: &str) -> Result<()>
    where
        T: std::fmt::Debug + PartialEq,
    {
        if left != right {
            color_eyre::eyre::bail!("{}: {:?} != {:?}", msg, left, right);
        }
        Ok(())
    }

    /// Create a snapshot of a value using insta
    pub fn snapshot<T: serde::Serialize>(&self, value: &T, name: Option<&str>) {
        let mut settings = insta::Settings::clone_current();
        settings.set_snapshot_path("../snapshots");
        settings.set_prepend_module_to_snapshot(false);

        if let Some(name) = name {
            settings.bind(|| {
                insta::assert_json_snapshot!(name, value);
            });
        } else {
            settings.bind(|| {
                insta::assert_json_snapshot!(value);
            });
        }
    }

    /// Create a snapshot of an event with automatic redactions
    pub fn snapshot_event(&self, event: &Event<JsonValue>, name: Option<&str>) {
        let mut settings = insta::Settings::clone_current();
        settings.set_snapshot_path("../snapshots");
        settings.add_redaction(".id", "[event-id]");
        settings.add_redaction(".ts_ingest", "[timestamp]");
        settings.add_redaction(".ts_orig", "[timestamp]");
        settings.add_redaction(".host", "[hostname]");

        if let Some(name) = name {
            settings.bind(|| {
                insta::assert_json_snapshot!(name, event);
            });
        } else {
            settings.bind(|| {
                insta::assert_json_snapshot!(event);
            });
        }
    }
}

/// Cleanup implementation for TestContext
impl Drop for TestContext {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        if duration > Duration::from_secs(5) {
            eprintln!(
                "Test '{}' took {:?} to complete (including cleanup)",
                self.test_name, duration
            );
        }
    }
}

/// Rich assertion helper with contextual error messages
pub struct ContextualAssert<'ctx> {
    ctx: &'ctx TestContext,
    context: String,
}

impl<'ctx> ContextualAssert<'ctx> {
    fn new(ctx: &'ctx TestContext, context: &str) -> Self {
        Self {
            ctx,
            context: context.to_string(),
        }
    }

    /// Assert two values are equal
    pub fn eq<T>(self, left: &T, right: &T) -> Result<Self>
    where
        T: std::fmt::Debug + PartialEq,
    {
        if left != right {
            color_eyre::eyre::bail!(
                "{}: values are not equal\n  Left: {:?}\n  Right: {:?}",
                self.context,
                left,
                right
            );
        }
        Ok(self)
    }

    /// Assert a condition is true
    pub fn that(self, condition: bool, message: &str) -> Result<Self> {
        if !condition {
            color_eyre::eyre::bail!("{}: {}", self.context, message);
        }
        Ok(self)
    }

    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> Result<Self> {
        if collection.is_empty() {
            color_eyre::eyre::bail!("{}: collection should not be empty", self.context);
        }
        Ok(self)
    }

    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> Result<Self> {
        if collection.len() != expected_size {
            color_eyre::eyre::bail!(
                "{}: collection size mismatch. Expected: {}, Actual: {}",
                self.context,
                expected_size,
                collection.len()
            );
        }
        Ok(self)
    }

    /// Assert option is Some
    pub fn some<T>(self, option: &Option<T>) -> Result<Self> {
        if option.is_none() {
            color_eyre::eyre::bail!("{}: option should be Some, but was None", self.context);
        }
        Ok(self)
    }

    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> Result<Self> {
        if option.is_some() {
            color_eyre::eyre::bail!("{}: option should be None, but was Some", self.context);
        }
        Ok(self)
    }

    /// Assert result contains error with specific message
    pub fn error_contains<T, E>(self, result: &Result<T, E>, expected_error: &str) -> Result<Self>
    where
        E: std::fmt::Display,
    {
        match result {
            Ok(_) => {
                color_eyre::eyre::bail!(
                    "{}: expected error containing '{}', but result was Ok",
                    self.context,
                    expected_error
                );
            }
            Err(error) => {
                let error_string = error.to_string();
                if !error_string.contains(expected_error) {
                    color_eyre::eyre::bail!(
                        "{}: error message '{}' does not contain expected text '{}'",
                        self.context,
                        error_string,
                        expected_error
                    );
                }
            }
        }
        Ok(self)
    }
}
