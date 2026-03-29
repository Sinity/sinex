//! Test Context - Database Isolation and Test Utilities
//!
//! The `Sandbox` provides isolated database access and test-specific utilities
//! without wrapping production APIs. Tests use production `test_event()`
//! and repository methods directly through the exposed pool.
//!
//! # Architecture
//!
//! Sandbox manages:
//! - **Database Isolation**: Each test gets its own database from the pool
//! - **Test Coordination**: Timing and synchronization
//! - **Assertions**: Rich error messages with context
//! - **Test Lifecycle**: Setup, cleanup, and monitoring
//!
//! # Usage Pattern
//!
//! ```ignore
//! #[sinex_test]
//! async fn test_example(ctx: Sandbox) -> TestResult<()> {
//!     // Direct production API - no wrapper
//!     let event = test_event(
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

use crate::sandbox::prelude::*;
use std::collections::HashSet;

use crate::sandbox::assertions::{ContextualAssert, EventAssert};
use crate::sandbox::background::{
    BACKGROUND_TIMEOUT_SECS, BackgroundRegistry, CLEANUP_AWAIT_SECS, CLEANUP_HANDLES,
    await_pending_cleanups,
};
use crate::sandbox::coordination::PipelineNamespace;
use crate::sandbox::coordination::PipelineScope;
use crate::sandbox::db::pool::{TestDatabase, acquire_test_database};
use crate::sandbox::db::{reset_database, verify_clean_state};
use crate::sandbox::events::{CreatedEventInfo, EventPublisher, cleanup_created_records};
use crate::sandbox::nats::EphemeralNats;
use crate::sandbox::nats::NatsSetup;
use crate::sandbox::nats::create_or_open_kv_store;
use crate::sandbox::nats::shared_nats_handle;
use crate::sandbox::prelude::TestResult;
use crate::sandbox::snapshot_helper::{self, FailureContext};
use crate::sandbox::timing::TimingUtils;
use async_nats::{Client as NatsClient, jetstream};
use color_eyre::eyre::{WrapErr, eyre};
use futures::FutureExt;
use futures::StreamExt;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_primitives::events::Publishable;
use sinex_primitives::{Event, Id, SourceMaterial, Uuid};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OnceCell as AsyncOnceCell;
use tokio::task::JoinHandle;
use tracing::warn;

fn format_cleanup_failure_context(
    message: &str,
    namespace: &str,
    diagnostics: &crate::sandbox::db::pool::CleanupDiagnostics,
    snapshot: Option<BackgroundSnapshot>,
) -> String {
    let (pending, labels) = match snapshot {
        Some(snapshot) => {
            let label_list = if snapshot.labels.is_empty() {
                "none".to_string()
            } else {
                snapshot.labels.join(", ")
            };
            (snapshot.pending, label_list)
        }
        None => (0, "none".to_string()),
    };

    format!(
        "{message}\nnamespace={}\nactive_hooks={}\nactive_hook_count={}\n{}",
        namespace,
        labels,
        pending,
        diagnostics.format_for_error()
    )
}

fn load_env_filter(default_filter: &str) -> TestResult<tracing_subscriber::EnvFilter> {
    let Some(raw) = std::env::var_os("RUST_LOG") else {
        return Ok(tracing_subscriber::EnvFilter::new(default_filter));
    };
    let raw = raw
        .into_string()
        .map_err(|_| eyre!("RUST_LOG is not valid UTF-8"))?;
    tracing_subscriber::EnvFilter::try_new(&raw)
        .map_err(|error| eyre!("Invalid RUST_LOG directive `{raw}`: {error}"))
}

fn background_invocation_id() -> TestResult<Option<i64>> {
    let Some(raw) = std::env::var_os("XTASK_BG_INVOCATION_ID") else {
        return Ok(None);
    };
    let raw = raw
        .into_string()
        .map_err(|_| eyre!("XTASK_BG_INVOCATION_ID is not valid UTF-8"))?;
    let invocation_id = raw
        .parse::<i64>()
        .map_err(|error| eyre!("Invalid XTASK_BG_INVOCATION_ID `{raw}`: {error}"))?;
    Ok(Some(invocation_id))
}

pub struct Sandbox {
    /// Direct access to the database pool - use this for repositories
    pub pool: DbPool,
    db: TestDatabase,
    test_name: String,
    start_time: Instant,
    created_events: Arc<Mutex<Vec<CreatedEventInfo>>>,
    background: Arc<AsyncMutex<BackgroundRegistry>>,
    captured_logs: Arc<Mutex<Vec<String>>>,
    baseline_events: i64,
    _tracing_enabled: bool,
    nats: Option<Arc<EphemeralNats>>,
    nats_client: Option<NatsClient>,
    nats_mode: NatsMode,
    env: SinexEnvironment,
    pipeline_namespace: PipelineNamespace,
    _reaper: Arc<NamespaceReaper>,
    /// Lazy-initialized shared NATS for property tests (doesn't consume self)
    lazy_shared_nats: Arc<AsyncOnceCell<(Arc<EphemeralNats>, NatsClient)>>,
}

struct NamespaceReaper {
    namespace: PipelineNamespace,
    nats: Mutex<Option<NatsClient>>,
}

impl Drop for NamespaceReaper {
    fn drop(&mut self) {
        if let Some(client) = self.nats.lock().take() {
            let prefix = self.namespace.prefix().to_string();
            // Spawn NATS stream cleanup and register the handle so
            // await_pending_cleanups() will wait for it before the next test.
            let handle = tokio::spawn(async move {
                let js = async_nats::jetstream::new(client);

                // List all streams and delete those starting with our prefix
                let mut streams = js.streams();
                while let Some(Ok(stream)) = streams.next().await {
                    if stream.config.name.starts_with(&prefix) {
                        if let Err(error) = js.delete_stream(stream.config.name.clone()).await {
                            warn!(
                                stream = %stream.config.name,
                                error = %error,
                                "Failed to delete namespaced test stream during cleanup"
                            );
                        }
                    }
                }
            });

            CLEANUP_HANDLES
                .lock()
                .expect("CLEANUP_HANDLES lock poisoned")
                .push(handle);
        }
    }
}

/// NATS initialization mode for test context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NatsMode {
    /// No NATS configured
    None,
    /// Dedicated ephemeral NATS instance
    Dedicated,
    /// Shared process-wide NATS instance
    Shared,
}

#[derive(Clone)]
pub struct BackgroundSnapshot {
    pub pending: usize,
    pub labels: Vec<String>,
}

#[derive(Clone)]
pub struct SandboxFailureSnapshot {
    test_name: String,
    baseline_events: i64,
    start_time: Instant,
    captured_logs: Arc<Mutex<Vec<String>>>,
    background: Arc<AsyncMutex<BackgroundRegistry>>,
}

impl SandboxFailureSnapshot {
    #[must_use]
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    #[must_use]
    pub fn baseline_event_count(&self) -> i64 {
        self.baseline_events
    }

    #[must_use]
    pub fn elapsed_ms(&self) -> u128 {
        self.start_time.elapsed().as_millis()
    }

    #[must_use]
    pub fn captured_logs(&self) -> Vec<String> {
        self.captured_logs.lock().clone()
    }

    #[must_use]
    pub fn background_snapshot(&self) -> BackgroundSnapshot {
        match self.background.try_lock() {
            Ok(guard) => BackgroundSnapshot {
                pending: guard.pending_count(),
                labels: guard.labels(),
            },
            Err(_) => BackgroundSnapshot {
                pending: 0,
                labels: Vec::new(),
            },
        }
    }
}

impl Sandbox {
    /// Accessor for the shared database pool.
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Publish a test event through the ingestion pipeline.
    pub async fn publish<P: Publishable>(&self, payload: P) -> TestResult<Event<JsonValue>> {
        // Use the trait implementation from events.rs
        EventPublisher::publish(self, payload).await
    }

    /// Recursively sanitize a JSON payload (strings and object keys).
    pub fn sanitize_payload(value: &mut JsonValue) {
        match value {
            JsonValue::String(s) => {
                *s = Self::sanitize_string(s);
            }
            JsonValue::Array(arr) => {
                for v in arr {
                    Self::sanitize_payload(v);
                }
            }
            JsonValue::Object(map) => {
                // Sanitize nested values first
                for v in map.values_mut() {
                    Self::sanitize_payload(v);
                }

                // Sanitize keys by renaming entries where needed
                let mut renames = Vec::new();
                for key in map.keys() {
                    let sanitized = Self::sanitize_string(key);
                    if sanitized != *key {
                        renames.push((key.clone(), sanitized));
                    }
                }
                for (old, new) in renames {
                    if let Some(mut value) = map.remove(&old) {
                        // Value already sanitized, but ensure nested structures stay clean.
                        Self::sanitize_payload(&mut value);
                        map.insert(new, value);
                    }
                }
            }
            _ => {}
        }
    }

    fn sanitize_string(raw: &str) -> String {
        let mut clean = raw.replace("../", "");
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
        clean
    }

    /// Create new test context
    pub async fn new() -> TestResult<Self> {
        Self::with_name("unnamed_test").await
    }

    /// Create test context with custom name
    pub async fn with_name(test_name: &str) -> TestResult<Self> {
        let db = acquire_test_database().await?;
        let pool = db.pool().clone();

        await_pending_cleanups().await;

        // Verify the slot is clean. Under nextest (separate processes per test), the
        // background cleanup thread of the PREVIOUS test process may not have finished
        // before the process exited. If we find residual data, clean inline rather than
        // erroring — this is the normal case, not an exceptional one.
        if verify_clean_state(&pool).await.is_err() {
            reset_database(&pool).await.map_err(|e| {
                let diagnostics = db.cleanup_diagnostics();
                e.wrap_err(format_cleanup_failure_context(
                    "inline cleanup after dirty acquisition failed",
                    test_name,
                    &diagnostics,
                    None,
                ))
            })?;
            // Re-seed fixture data after inline cleanup
            crate::sandbox::db::pool::seed_test_fixtures(&pool).await?;
        }

        // Baseline event count is lazy — most tests never call event_delta() or
        // baseline_event_count(), so the SELECT COUNT(*) on the hypertable is wasted.
        // It's set to 0 here and computed on first access via baseline_event_count_lazy().
        let baseline_events = 0;

        let pipeline_namespace = PipelineNamespace::new(test_name);

        let ctx = Self {
            pool,
            db,
            test_name: test_name.to_string(),
            start_time: Instant::now(),
            created_events: Arc::new(Mutex::new(Vec::new())),
            background: Arc::new(AsyncMutex::new(BackgroundRegistry::default())),
            captured_logs: Arc::new(Mutex::new(Vec::new())),
            baseline_events,
            _tracing_enabled: false,
            nats: None,
            nats_client: None,
            nats_mode: NatsMode::None,
            env: sinex_primitives::environment(),
            pipeline_namespace: pipeline_namespace.clone(),
            _reaper: Arc::new(NamespaceReaper {
                namespace: pipeline_namespace,
                nats: Mutex::new(None),
            }),
            lazy_shared_nats: Arc::new(AsyncOnceCell::new()),
        };

        Ok(ctx)
    }

    /// Configure NATS for this test context using a fluent builder.
    #[must_use]
    pub fn with_nats(self) -> NatsSetup {
        NatsSetup::new(self)
    }

    /// Internal: Set NATS state (used by `NatsSetup` builder).
    pub(crate) fn set_nats(
        &mut self,
        nats: Option<Arc<EphemeralNats>>,
        client: Option<NatsClient>,
        mode: NatsMode,
    ) {
        self.nats = nats;
        self.nats_client = client;
        self.nats_mode = mode;
    }

    /// Internal: Register client with reaper for cleanup (used by `NatsSetup` builder).
    pub(crate) fn register_reaper_client(&self, client: NatsClient) {
        self._reaper.nats.lock().replace(client);
    }

    /// Get the NATS client for this test context
    #[must_use]
    #[allow(clippy::panic)] // Deliberate: programmer error if NATS not initialized
    pub fn nats_client(&self) -> NatsClient {
        // First check the primary nats_client field
        if let Some(client) = &self.nats_client {
            return client.clone();
        }
        // Fall back to lazy_shared_nats if initialized
        if let Some((_, client)) = self.lazy_shared_nats.get() {
            return client.clone();
        }
        panic!(
            "NATS not initialized - call with_nats(), with_shared_nats(), or ensure_nats() first"
        )
    }

    /// Lazily initialize shared NATS without consuming self.
    pub async fn ensure_nats(&self) -> TestResult<NatsClient> {
        // If already initialized via with_nats/with_shared_nats, use that
        if let Some(client) = &self.nats_client {
            return Ok(client.clone());
        }

        // Otherwise, lazily initialize shared NATS
        let (_, client) = self
            .lazy_shared_nats
            .get_or_try_init(|| async {
                let nats = shared_nats_handle().await?;
                let client = nats.connect().await?;
                Ok::<_, color_eyre::Report>((nats, client))
            })
            .await?;

        Ok(client.clone())
    }

    /// Lazily get `JetStream` context without consuming self (for property tests).
    pub async fn ensure_jetstream(&self) -> TestResult<jetstream::Context> {
        let client = self.ensure_nats().await?;
        Ok(jetstream::new(client))
    }

    /// Lazily get checkpoint KV bucket without consuming self (for property tests).
    pub async fn ensure_checkpoint_kv(&self) -> TestResult<jetstream::kv::Store> {
        let js = self.ensure_jetstream().await?;
        let prefix = self.pipeline_namespace().prefix();
        let bucket = sinex_node_sdk::checkpoint::checkpoint_bucket_name(Some(prefix));
        let kv_store = create_or_open_kv_store(
            &js,
            jetstream::kv::Config {
                bucket: bucket.clone(),
                history: 64,
                ..Default::default()
            },
        )
        .await?;
        Ok(kv_store)
    }

    /// Access the underlying `EphemeralNats` handle (lifetime-managed by the context).
    pub fn nats_handle(&self) -> TestResult<Arc<EphemeralNats>> {
        // First check the primary nats field
        if let Some(nats) = &self.nats {
            return Ok(nats.clone());
        }
        // Fall back to lazy_shared_nats if initialized
        if let Some((nats, _)) = self.lazy_shared_nats.get() {
            return Ok(nats.clone());
        }
        Err(eyre!(
            "NATS not initialized - call with_nats() or with_shared_nats()"
        ))
    }

    /// Get a `JetStream` context bound to this test's NATS instance.
    pub async fn jetstream(&self) -> TestResult<jetstream::Context> {
        let nats = self.nats_handle()?;
        nats.jetstream().await
    }

    /// Get (or create) the default checkpoint KV bucket for tests.
    pub async fn checkpoint_kv(&self) -> TestResult<jetstream::kv::Store> {
        let js = self.jetstream().await?;
        let prefix = self.pipeline_namespace().prefix();
        let bucket = sinex_node_sdk::checkpoint::checkpoint_bucket_name(Some(prefix));
        let kv_store = create_or_open_kv_store(
            &js,
            jetstream::kv::Config {
                bucket: bucket.clone(),
                history: 64,
                ..Default::default()
            },
        )
        .await?;
        Ok(kv_store)
    }

    /// Get the Sinex environment for this test context
    #[must_use]
    pub fn env(&self) -> &SinexEnvironment {
        &self.env
    }

    /// Access the per-test `JetStream` namespace used for pipeline resources.
    #[must_use]
    pub fn pipeline_namespace(&self) -> &PipelineNamespace {
        &self.pipeline_namespace
    }

    /// Create a pipeline scope that resets the DB slot and starts ingestd.
    pub async fn pipeline(&self) -> TestResult<PipelineScope<'_>> {
        PipelineScope::new(self).await
    }

    pub(crate) fn ensure_shared_nats(&self) -> TestResult<()> {
        match self.nats_mode {
            NatsMode::Shared => Ok(()),
            NatsMode::Dedicated => Err(eyre!(
                "PipelineScope requires shared NATS; call with_shared_nats() instead of with_nats()"
            )),
            NatsMode::None => {
                // Check if lazy_shared_nats was initialized (for property tests)
                if self.lazy_shared_nats.initialized() {
                    Ok(())
                } else {
                    Err(eyre!(
                        "PipelineScope requires shared NATS; call with_shared_nats() first"
                    ))
                }
            }
        }
    }

    /// Reset the underlying database slot and verify it is clean.
    pub async fn reset_database_slot(&self) -> TestResult<()> {
        self.quiesce_background_tasks().await?;
        reset_database(&self.pool).await?;
        verify_clean_state(&self.pool).await?;
        Ok(())
    }

    /// Get the NATS server URL if NATS is enabled
    #[must_use]
    pub fn nats_url(&self) -> Option<String> {
        self.nats.as_ref().map(|n| n.client_url().to_string())
    }

    /// Initialize tracing for tests (static method for use without context)
    pub fn init_tracing(level: &str) {
        use tracing_subscriber::{fmt, prelude::*};

        // Only initialize if not already initialized
        static TRACING_INIT: std::sync::Once = std::sync::Once::new();

        TRACING_INIT.call_once(|| {
            let filter = load_env_filter(level)
                .unwrap_or_else(|error| panic!("failed to initialize sandbox tracing: {error}"));

            tracing_subscriber::registry()
                .with(fmt::layer().with_test_writer())
                .with(filter)
                .init();
        });
    }

    /// Enable tracing for this test context
    #[must_use]
    pub fn with_tracing(mut self, level: &str) -> Self {
        Self::init_tracing(level);
        self._tracing_enabled = true;
        self
    }

    /// Check if a log message was captured
    pub fn assert_logged(&self, message: &str) -> TestResult<()> {
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
    #[must_use]
    pub fn captured_logs(&self) -> Vec<String> {
        self.captured_logs.lock().clone()
    }

    /// Get test name for fixture scoping
    #[must_use]
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    /// Get elapsed time since context creation
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Number of events present when the context was created
    #[must_use]
    pub fn baseline_event_count(&self) -> i64 {
        self.baseline_events
    }

    /// Events created since test context was initialized
    pub async fn event_delta(&self) -> TestResult<i64> {
        Ok(self.pool.events().count_all().await? - self.baseline_events)
    }

    /// D8: Attach a NATS consumer snapshot to this test's history record.
    ///
    /// Serializes `snapshot` as JSON and stores it against this test's record in the
    /// xtask history DB. The invocation ID is read from the `XTASK_BG_INVOCATION_ID`
    /// environment variable set by `xtask test --bg`; no-op if not running under xtask.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snap = nats_helper.consumer_snapshot("sinex.consumer").await?;
    /// ctx.record_nats_context(&serde_json::to_value(&snap)?);
    /// ```
    pub fn record_nats_context(&self, snapshot: &serde_json::Value) {
        let inv_id = match background_invocation_id() {
            Ok(Some(inv_id)) => inv_id,
            Ok(None) => return,
            Err(error) => {
                eprintln!("⚠ Failed to read XTASK_BG_INVOCATION_ID: {error}");
                return;
            }
        };
        let cfg = crate::config::config();
        match crate::history::HistoryDb::open(&cfg.history_db_path()) {
            Ok(db) => {
                if let Err(error) = db.record_test_nats_context(inv_id, &self.test_name, snapshot)
                {
                    eprintln!(
                        "⚠️ failed to record NATS context for test '{}' (invocation {inv_id}): {error:#}",
                        self.test_name
                    );
                }
            }
            Err(error) => {
                eprintln!(
                    "⚠️ failed to open history DB for test '{}' NATS context (invocation {inv_id}): {error:#}",
                    self.test_name
                );
            }
        }
    }

    /// Capture snapshot metadata that survives if the context is moved.
    #[must_use]
    pub fn failure_snapshot(&self) -> SandboxFailureSnapshot {
        SandboxFailureSnapshot {
            test_name: self.test_name.clone(),
            baseline_events: self.baseline_events,
            start_time: self.start_time,
            captured_logs: Arc::clone(&self.captured_logs),
            background: self.background.clone(),
        }
    }

    pub(crate) fn record_created_event(&self, event_id: Uuid, material_id: Option<Uuid>) {
        self.created_events.lock().push(CreatedEventInfo {
            event_id,
            material_id,
        });
    }

    /// Force cleanup of the underlying database (use with caution)
    pub async fn force_cleanup(&self) -> TestResult<()> {
        // Ensure no background work is still touching the database before wiping it.
        self.quiesce_background_tasks().await?;
        self.db.force_cleanup().await.wrap_err_with(|| {
            let diagnostics = self.db.cleanup_diagnostics();
            let snapshot = self.background_snapshot();
            format_cleanup_failure_context(
                "cleanup failed",
                self.pipeline_namespace.prefix(),
                &diagnostics,
                Some(snapshot),
            )
        })
    }

    /// Ensure database is in clean state before starting test
    ///
    /// This verifies that the database is empty and ready for use.
    /// If not clean, returns an error with diagnostics.
    pub async fn ensure_clean(&self) -> TestResult<()> {
        self.quiesce_background_tasks().await?;
        match verify_clean_state(&self.pool).await {
            Ok(()) => Ok(()),
            Err(err) => {
                let diagnostics = self.db.cleanup_diagnostics();
                Err(err).wrap_err_with(|| {
                    let snapshot = self.background_snapshot();
                    format_cleanup_failure_context(
                        "database slot not clean before test",
                        self.pipeline_namespace.prefix(),
                        &diagnostics,
                        Some(snapshot),
                    )
                })
            }
        }
    }

    /// Register a background task that must complete before the database is returned to the pool.
    /// Use this for fire-and-forget helpers started inside a test.
    pub async fn register_background_task(&self, label: impl Into<String>, handle: JoinHandle<()>) {
        let mut guard = self.background.lock().await;
        guard.add_task(label, handle);
    }

    /// Register a background resource (e.g., process handle) as a tracked task.
    ///
    /// The registration is awaited directly so the task is guaranteed to be in
    /// the registry before the caller continues — no detached spawn race.
    pub async fn register_background_handle<T>(&self, label: impl Into<String>, handle: T)
    where
        T: Send + 'static,
    {
        let task_handle = tokio::spawn(async move {
            let _ = handle;
            let () = tokio::task::yield_now().await;
        });
        self.background.lock().await.add_task(label, task_handle);
    }

    /// Spawn and track a background task that will be awaited during cleanup.
    ///
    /// The registration is awaited directly so the task is guaranteed to be in
    /// the registry before the caller continues — no detached spawn race.
    pub async fn spawn_background<F>(&self, label: impl Into<String>, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(fut);
        self.background.lock().await.add_task(label, handle);
    }

    /// Register a custom shutdown hook to run before the context gives the database back.
    pub async fn register_shutdown_hook<F>(&self, label: impl Into<String>, hook: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut guard = self.background.lock().await;
        guard.add_hook(label, hook.boxed());
    }

    /// Register a background task or handle by name and optional join handle.
    ///
    /// Useful for process handles or runtime-managed resources.
    /// Wait for background tasks and shutdown hooks to finish. Called automatically on drop,
    /// but available for tests that want deterministic cleanup points.
    pub async fn quiesce_background_tasks(&self) -> TestResult<()> {
        let mut guard = self.background.lock().await;
        guard.quiesce_tasks_only().await;
        Ok(())
    }

    /// Assert that no background tasks or hooks remain pending.
    pub async fn assert_idle(&self) -> TestResult<()> {
        let guard = self.background.lock().await;
        if guard.pending_count() == 0 {
            return Ok(());
        }
        Err(color_eyre::eyre::eyre!(
            "Background registry not idle: {} pending ({:?})",
            guard.pending_count(),
            guard.labels()
        ))
    }

    #[must_use]
    pub fn background_snapshot(&self) -> BackgroundSnapshot {
        match self.background.try_lock() {
            Ok(guard) => BackgroundSnapshot {
                pending: guard.pending_count(),
                labels: guard.labels(),
            },
            Err(_) => BackgroundSnapshot {
                pending: 0,
                labels: Vec::new(),
            },
        }
    }

    /// Ensure a source material record exists for tests that construct provenance manually.
    ///
    /// If a record with the given ID already exists, this is a no-op.
    /// This avoids FK constraint issues from trying to update existing source materials.
    pub async fn ensure_source_material(
        &self,
        id: Id<SourceMaterial>,
        source_identifier: Option<&str>,
    ) -> TestResult<()> {
        let material_uuid = id.to_uuid();
        // Include the ID in the identifier to avoid source_identifier uniqueness conflicts.
        // Each unique id gets its own unique source_identifier.
        let identifier = source_identifier
            .map_or_else(|| format!("test-material-{id}"), |s| format!("{s}-{id}"));

        // Use INSERT with ON CONFLICT DO NOTHING to avoid FK violations.
        // If the record already exists (by id), we don't need to update it.
        sqlx::query(
            r"
                INSERT INTO raw.source_material_registry
                    (id, material_kind, source_identifier, status, timing_info_type)
                VALUES ($1::uuid, $2, $3, $4, $5)
                ON CONFLICT (id) DO NOTHING
            ",
        )
        .bind(material_uuid)
        .bind("annex")
        .bind(&identifier)
        .bind("completed")
        .bind("realtime")
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Create and register a new source material returning its identifier.
    pub async fn create_source_material(
        &self,
        source_identifier: Option<&str>,
    ) -> TestResult<Id<SourceMaterial>> {
        let id = Id::<SourceMaterial>::new();
        self.ensure_source_material(id, source_identifier).await?;
        Ok(id)
    }

    /// Ensure a specific source material exists, returning its ID handle.
    pub async fn ensure_specific_material(
        &self,
        material_id: uuid::Uuid,
        source_identifier: Option<&str>,
    ) -> TestResult<Id<SourceMaterial>> {
        let id = Id::<SourceMaterial>::from_uuid(material_id);
        self.ensure_source_material(id, source_identifier).await?;
        Ok(id)
    }

    /// Convenience helper returning a schema-layer UUIDv7 for tests.
    pub async fn ensure_schema_material(
        &self,
        source_identifier: Option<&str>,
    ) -> TestResult<Uuid> {
        let id = self.create_source_material(source_identifier).await?;
        Ok(*id.as_uuid())
    }

    /// Connection URL for the underlying test database.
    #[must_use]
    pub fn database_url(&self) -> &str {
        self.db.url()
    }

    /// Name of the dedicated database slot backing this context.
    #[must_use]
    pub fn database_name(&self) -> &str {
        self.db.name()
    }

    /// Access timing utilities
    #[must_use]
    pub fn timing(&self) -> TimingUtils<'_> {
        TimingUtils::new(self)
    }

    /// Measure execution time of an operation
    pub async fn measure<F, T, E>(&self, operation: F) -> TestResult<(StdResult<T, E>, Duration)>
    where
        F: std::future::Future<Output = StdResult<T, E>>,
    {
        let start = Instant::now();
        let result = operation.await;
        let duration = start.elapsed();
        Ok((result, duration))
    }

    /// Create contextual assertion helper
    #[must_use]
    pub fn assert(&self, context: &str) -> ContextualAssert {
        ContextualAssert::new(context)
    }

    /// Assert that two events are equal with detailed comparison
    pub fn assert_event_eq(
        &self,
        actual: &Event<JsonValue>,
        expected: &Event<JsonValue>,
    ) -> TestResult<()> {
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

    // ========== Event Assertion API ==========

    /// Create a fluent event assertion builder for composable filters.
    #[must_use]
    pub fn assert_event(&self) -> EventAssert<'_> {
        EventAssert::new(self)
    }

    /// Assert that a collection of events has unique IDs.
    pub fn assert_unique_event_ids(&self, events: &[Event<JsonValue>]) -> TestResult<()> {
        let mut seen = HashSet::new();
        for event in events {
            if let Some(id) = event.id.as_ref()
                && !seen.insert(id.to_string())
            {
                color_eyre::eyre::bail!("Duplicate event id detected: {id}");
            }
        }
        Ok(())
    }

    /// Capture log message for testing
    pub fn capture_log(&self, message: String) {
        self.captured_logs.lock().push(message);
    }

    /// Assert that no error-level logs were captured
    pub fn assert_no_errors_logged(&self) -> TestResult<()> {
        let logs = self.captured_logs.lock();
        let error_logs: Vec<_> = logs
            .iter()
            .filter(|log| looks_like_error_log(log))
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

#[cfg(test)]
mod tests {
    // Inline because these helpers are private to sandbox context initialization/parsing.
    use super::*;

    struct EnvGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: Option<std::ffi::OsString>) -> Self {
            let original = std::env::var_os(key);
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    #[test]
    fn load_env_filter_defaults_when_rust_log_is_missing() {
        let _guard = EnvGuard::set("RUST_LOG", None);

        let filter = load_env_filter("info").expect("default filter should load");

        assert_eq!(filter.to_string(), "info");
    }

    #[test]
    fn load_env_filter_rejects_invalid_rust_log_directive() {
        let _guard = EnvGuard::set("RUST_LOG", Some(std::ffi::OsString::from("[broken")));

        let error = load_env_filter("info").expect_err("invalid directive should fail");

        assert!(error.to_string().contains("Invalid RUST_LOG directive `[broken`"));
    }

    #[cfg(unix)]
    #[test]
    fn load_env_filter_rejects_non_utf8_rust_log() {
        use std::os::unix::ffi::OsStringExt;

        let _guard = EnvGuard::set("RUST_LOG", Some(std::ffi::OsString::from_vec(vec![0xff])));

        let error = load_env_filter("info").expect_err("non-utf8 directive should fail");

        assert!(error.to_string().contains("RUST_LOG is not valid UTF-8"));
    }

    #[test]
    fn background_invocation_id_defaults_to_none_when_missing() {
        let _guard = EnvGuard::set("XTASK_BG_INVOCATION_ID", None);

        assert_eq!(
            background_invocation_id().expect("missing invocation ID should be allowed"),
            None
        );
    }

    #[test]
    fn background_invocation_id_rejects_invalid_integer() {
        let _guard = EnvGuard::set(
            "XTASK_BG_INVOCATION_ID",
            Some(std::ffi::OsString::from("not-a-number")),
        );

        let error =
            background_invocation_id().expect_err("invalid invocation ID should not be ignored");

        assert!(error
            .to_string()
            .contains("Invalid XTASK_BG_INVOCATION_ID `not-a-number`"));
    }

    #[cfg(unix)]
    #[test]
    fn background_invocation_id_rejects_non_utf8_value() {
        use std::os::unix::ffi::OsStringExt;

        let _guard = EnvGuard::set(
            "XTASK_BG_INVOCATION_ID",
            Some(std::ffi::OsString::from_vec(vec![0xff])),
        );

        let error =
            background_invocation_id().expect_err("non-utf8 invocation ID should not be ignored");

        assert!(error
            .to_string()
            .contains("XTASK_BG_INVOCATION_ID is not valid UTF-8"));
    }
}

fn looks_like_error_log(log: &str) -> bool {
    let lower = log.to_ascii_lowercase();
    lower.starts_with("error")
        || lower.contains("[error]")
        || lower.contains(" level=error")
        || lower.contains("level=\"error\"")
        || lower.contains(" error:")
}

/// Cleanup implementation for Sandbox
impl Drop for Sandbox {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let snapshot = self.failure_snapshot();
            snapshot_helper::persist_failure(
                self.test_name(),
                "Sandbox dropped during panic",
                FailureContext::Snapshot(snapshot),
            );
        }
        // Ensure any registered background work is flushed before returning the database.
        let registry = self.background.clone();
        let quiesce_fut = async move {
            let _ = tokio::time::timeout(Duration::from_secs(BACKGROUND_TIMEOUT_SECS), async {
                registry.lock().await.quiesce().await;
            })
            .await;
        };
        // Avoid block_in_place on current-thread runtimes; instead enter the runtime if available
        // so Tokio timers and tasks can make progress while we wait for background shutdown.
        match Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| {
                    let _guard = handle.enter();
                    futures::executor::block_on(quiesce_fut);
                });
            }
            Ok(_handle) => {
                // For current-thread runtimes, move cleanup onto a dedicated thread with its own runtime
                // to avoid deadlocking the executor.
                let (tx, rx) = mpsc::channel();
                thread::spawn(move || {
                    if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        rt.block_on(quiesce_fut);
                    } else {
                        futures::executor::block_on(quiesce_fut);
                    }
                    let _ = tx.send(());
                });
                let _ = rx.recv_timeout(Duration::from_secs(CLEANUP_AWAIT_SECS));
            }
            Err(_) => {
                // Issue 116: No runtime available, spawn blocking thread with its own runtime
                let (tx, rx) = mpsc::channel();
                thread::spawn(move || {
                    if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        rt.block_on(quiesce_fut);
                    } else {
                        // Last resort: try futures executor, but this may fail
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            futures::executor::block_on(quiesce_fut);
                        }));
                    }
                    let _ = tx.send(());
                });
                let _ = rx.recv_timeout(Duration::from_secs(CLEANUP_AWAIT_SECS));
            }
        }

        let pool = self.pool.clone();
        let records = {
            let mut guard = self.created_events.lock();
            guard.drain(..).collect::<Vec<_>>()
        };

        if !records.is_empty() {
            if let Ok(handle) = Handle::try_current() {
                let cleanup_pool = pool;
                let cleanup_records = records;
                let join_handle = handle.spawn(async move {
                    if let Err(err) = cleanup_created_records(cleanup_pool, cleanup_records).await {
                        warn!("Sandbox cleanup failed: {}", err);
                    }
                });

                // Always succeeds: CLEANUP_HANDLES uses std::sync::Mutex, not try_lock().
                CLEANUP_HANDLES
                    .lock()
                    .expect("CLEANUP_HANDLES lock poisoned")
                    .push(join_handle);
            } else {
                // Issue 116: No runtime available, spawn blocking thread with its own runtime
                let (tx, rx) = mpsc::channel();
                thread::spawn(move || {
                    if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        if let Err(err) = rt.block_on(cleanup_created_records(pool, records)) {
                            warn!("Sandbox cleanup failed: {}", err);
                        }
                    } else {
                        // Last resort: try futures executor, but catch any panic
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            if let Err(err) =
                                futures::executor::block_on(cleanup_created_records(pool, records))
                            {
                                warn!("Sandbox cleanup failed without runtime: {}", err);
                            }
                        }));
                    }
                    let _ = tx.send(());
                });
                let _ = rx.recv_timeout(Duration::from_secs(20));
            }
        }

        let duration = self.start_time.elapsed();
        if duration > Duration::from_secs(5) {
            eprintln!(
                "Test '{}' took {:?} to complete (including cleanup)",
                self.test_name, duration
            );
        }
    }
}
