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
//! - **Test Coordination**: Timing and synchronization
//! - **Assertions**: Rich error messages with context
//! - **Test Lifecycle**: Setup, cleanup, and monitoring
//!
//! # Usage Pattern
//!
//! ```rust
//! #[sinex_test]
//! async fn test_example(ctx: TestContext) -> TestResult<()> {
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
use crate::db_common::{self, verify_clean_state};
use crate::nats::{
    shared_ephemeral_nats_with_key, EphemeralNats, EphemeralNatsBuilder, SharedNatsProfile,
};
use crate::pipeline::{shared_nats_handle, shared_secure_nats_handle, PipelineHarness};
use crate::pipeline_namespace::PipelineNamespace;
use crate::pipeline_scope::PipelineScope;
use crate::satellite_management_utils::{
    start_test_ingestd_with_config, TestIngestdConfig, TestIngestdHandle,
};
use crate::snapshot_helper::{self, FailureContext};
use crate::timing_utils::{TimingUtils, WaitHelpers, DEFAULT_WAIT_SECS};
use crate::TestResult;
use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, WrapErr};
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::StreamExt;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde_json::Value as JsonValue;
use sinex_core::db::models::event::{Event, Provenance, SourceMaterial};
use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::environment::SinexEnvironment;
use sinex_core::types::{DbPool, Id, Timestamp, Ulid};
use sinex_core::OffsetKind;
use std::result::Result as StdResult;

use sinex_core::DbPoolExt;
use std::collections::HashSet;
use std::mem;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

const BOOTSTRAP_MATERIAL_ID: &str = "014D2PF2DBSQQZXQ5TK1V58CGG";
const BOOTSTRAP_MATERIAL_IDENTIFIER: &str = "test-material-bootstrap";

fn format_cleanup_failure_context(
    message: &str,
    namespace: &str,
    diagnostics: &crate::database_pool::CleanupDiagnostics,
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

/// Test context providing database isolation and test utilities
///
/// This struct provides access to an isolated database and test-specific
/// utilities without wrapping production APIs. Tests should use the pool
/// field directly to access repositories and production Event creation APIs.
#[derive(Clone, Debug)]
pub(crate) struct CreatedEventInfo {
    event_id: Ulid,
    material_id: Option<Ulid>,
}

static CLEANUP_HANDLES: Lazy<AsyncMutex<Vec<tokio::task::JoinHandle<()>>>> =
    Lazy::new(|| AsyncMutex::new(Vec::new()));

const CLEANUP_AWAIT_SECS: u64 = 2;
const BACKGROUND_TIMEOUT_SECS: u64 = 10;

async fn await_pending_cleanups() {
    let timeout = Duration::from_secs(CLEANUP_AWAIT_SECS);

    let mut handles = CLEANUP_HANDLES.lock().await;
    let pending = mem::take(&mut *handles);
    drop(handles);

    for mut handle in pending {
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!("Background cleanup task failed: {}", err);
            }
            Err(_) => {
                handle.abort();
                warn!(
                    "Background cleanup task exceeded {:?}; aborting to avoid cross-test deadlocks",
                    timeout
                );
            }
        }
    }
}

pub struct TestContext {
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
    pipeline_ingestd: Arc<AsyncMutex<Option<TestIngestdHandle>>>,
    _reaper: Arc<NamespaceReaper>,
}

struct NamespaceReaper {
    namespace: PipelineNamespace,
    nats: Mutex<Option<NatsClient>>,
}

impl Drop for NamespaceReaper {
    fn drop(&mut self) {
        if let Some(client) = self.nats.lock().take() {
            let prefix = self.namespace.prefix().to_string();
            // Spawn a detached task to clean up JetStream streams
            // We can't await here, so we fire-and-forget
            tokio::spawn(async move {
                let js = async_nats::jetstream::new(client);

                // List all streams and delete those starting with our prefix
                let mut streams = js.streams();
                while let Some(Ok(stream)) = streams.next().await {
                    if stream.config.name.starts_with(&prefix) {
                        let _ = js.delete_stream(stream.config.name).await;
                    }
                }
            });
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NatsMode {
    None,
    Dedicated,
    Shared,
}

#[derive(Clone)]
pub struct BackgroundSnapshot {
    pub pending: usize,
    pub labels: Vec<String>,
}

#[derive(Clone)]
pub struct TestContextFailureSnapshot {
    test_name: String,
    baseline_events: i64,
    start_time: Instant,
    captured_logs: Arc<Mutex<Vec<String>>>,
    background: Arc<AsyncMutex<BackgroundRegistry>>,
}

impl TestContextFailureSnapshot {
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    pub fn baseline_events(&self) -> i64 {
        self.baseline_events
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.start_time.elapsed().as_millis()
    }

    pub fn captured_logs(&self) -> Vec<String> {
        self.captured_logs.lock().clone()
    }

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

/// Lightweight handle exposing pool and background registry for global hooks.
#[derive(Clone)]
pub struct TestContextHandle {
    pub pool: DbPool,
    pub(crate) background: Arc<AsyncMutex<BackgroundRegistry>>,
}

impl TestContextHandle {
    pub async fn quiesce_background_tasks(&self) {
        let mut guard = self.background.lock().await;
        guard.quiesce_tasks_only().await;
    }
}

impl TestContext {
    thread_local! {
        static CURRENT_CTX: std::cell::RefCell<Option<TestContextHandle>> = const { std::cell::RefCell::new(None) };
    }

    /// Attach this context to the current thread for retrieval by helpers.
    pub(crate) fn install_current(&self) {
        let handle = TestContextHandle {
            pool: self.pool.clone(),
            background: self.background.clone(),
        };
        Self::CURRENT_CTX.with(|cell| {
            *cell.borrow_mut() = Some(handle);
        });
    }

    /// Clear the current-thread handle (used on drop).
    pub(crate) fn clear_current(&self) {
        Self::CURRENT_CTX.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }

    /// Best-effort access to the current TestContext handle (pool + background).
    pub fn try_current() -> Option<TestContextHandle> {
        Self::CURRENT_CTX.with(|cell| cell.borrow().clone())
    }
    /// Accessor for the shared database pool.
    pub fn pool(&self) -> &DbPool {
        &self.pool
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
            let diagnostics = db.cleanup_diagnostics();
            return Err(err).wrap_err_with(|| {
                format_cleanup_failure_context(
                    "database slot not clean on acquisition",
                    test_name,
                    &diagnostics,
                    None,
                )
            });
        }

        let baseline_events = pool.events().count_all().await?;

        let pipeline_namespace = PipelineNamespace::new(test_name);

        Ok(Self {
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
            env: sinex_core::environment().clone(),
            pipeline_namespace: pipeline_namespace.clone(),
            pipeline_ingestd: Arc::new(AsyncMutex::new(None)),
            _reaper: Arc::new(NamespaceReaper {
                namespace: pipeline_namespace,
                nats: Mutex::new(None),
            }),
        })
    }

    /// Enable NATS/JetStream infrastructure for this test context
    ///
    /// This starts an ephemeral NATS server with JetStream enabled
    /// and connects a client to it. Required for JetStream integration tests.
    /// Enable NATS/JetStream with custom configuration
    pub async fn with_nats_builder(mut self, builder: EphemeralNatsBuilder) -> TestResult<Self> {
        let nats = builder.start().await?;
        let client = nats.connect().await?;
        let shutdown_proc = nats.process_handle();
        self.register_background_handle("nats-server", shutdown_proc.clone());
        self.register_shutdown_hook("nats-shutdown", async move {
            if let Some(mut child) = shutdown_proc.lock().await.take() {
                let _ = child.start_kill();
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
            }
        })
        .await;
        self.nats_client = Some(client.clone());
        self.nats = Some(Arc::new(nats));
        self.nats_mode = NatsMode::Dedicated;

        // Register client with reaper for cleaning
        self._reaper.nats.lock().replace(client);

        self.install_current();
        Ok(self)
    }

    /// Enable NATS/JetStream infrastructure for this test context
    ///
    /// This starts an ephemeral NATS server with JetStream enabled
    /// and connects a client to it. Required for JetStream integration tests.
    pub async fn with_nats(self) -> TestResult<Self> {
        self.with_nats_builder(EphemeralNats::builder()).await
    }

    /// Attach to the shared process-wide NATS instance, required for pipeline scopes.
    fn secure_shared_nats_requested() -> bool {
        matches!(
            std::env::var("SINEX_TEST_USE_TLS")
                .unwrap_or_default()
                .to_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        )
    }

    fn shared_nats_token() -> Option<String> {
        std::env::var("SINEX_TEST_NATS_TOKEN")
            .ok()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty())
    }

    fn shared_nats_key_override() -> Option<String> {
        std::env::var("SINEX_TEST_NATS_SHARED_KEY")
            .ok()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
    }

    fn shared_nats_config_file() -> Option<PathBuf> {
        std::env::var("SINEX_TEST_NATS_CONFIG_FILE")
            .ok()
            .map(|path| PathBuf::from(path.trim()))
            .filter(|path| !path.as_os_str().is_empty())
    }

    fn shared_nats_token_key(token: &str, secure_tls: bool) -> String {
        let hash = blake3::hash(token.as_bytes());
        if secure_tls {
            format!("auth-token-tls-{}", hash.to_hex())
        } else {
            format!("auth-token-{}", hash.to_hex())
        }
    }

    fn shared_nats_config_key(config_file: &PathBuf, secure_tls: bool) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(config_file.to_string_lossy().as_bytes());
        hasher.update(if secure_tls { b":tls" } else { b":plain" });
        format!("config-{}", hasher.finalize().to_hex())
    }

    pub async fn with_shared_nats(mut self) -> TestResult<Self> {
        let token = Self::shared_nats_token();
        let secure_tls = Self::secure_shared_nats_requested();
        let config_file = Self::shared_nats_config_file();
        let key_override = Self::shared_nats_key_override();
        let mut builder = if secure_tls {
            SharedNatsProfile::SecureTls.builder()
        } else {
            EphemeralNats::builder()
        };
        if let Some(config_path) = &config_file {
            builder = builder.with_config_file(config_path.clone());
        }
        if let Some(token) = &token {
            builder = builder.with_auth_token(token.clone());
        }
        let nats = if key_override.is_some() || token.is_some() || config_file.is_some() {
            let key = if let Some(key) = key_override {
                if let Some(token) = &token {
                    let hash = blake3::hash(token.as_bytes());
                    format!("{key}-token-{}", hash.to_hex())
                } else {
                    key
                }
            } else if let Some(token) = &token {
                Self::shared_nats_token_key(token, secure_tls)
            } else if let Some(config_path) = &config_file {
                Self::shared_nats_config_key(config_path, secure_tls)
            } else {
                unreachable!("shared NATS key selection exhausted")
            };
            shared_ephemeral_nats_with_key(&key, builder).await?
        } else if secure_tls {
            shared_secure_nats_handle().await?
        } else {
            shared_nats_handle().await?
        };
        let client = nats.connect().await?;
        self.nats_client = Some(client.clone());
        self.nats = Some(nats);
        self.nats_mode = NatsMode::Shared;

        // Register client with reaper for cleaning
        self._reaper.nats.lock().replace(client);

        self.install_current();
        Ok(self)
    }

    /// Attach to a shared NATS instance identified by the provided key.
    pub async fn with_shared_nats_builder(
        mut self,
        key: &str,
        builder: EphemeralNatsBuilder,
    ) -> TestResult<Self> {
        let nats = crate::nats::shared_ephemeral_nats_with_key(key, builder).await?;
        let client = nats.connect().await?;
        self.nats_client = Some(client.clone());
        self.nats = Some(nats);
        self.nats_mode = NatsMode::Shared;

        // Register client with reaper for cleaning
        self._reaper.nats.lock().replace(client);

        self.install_current();
        Ok(self)
    }

    /// Explicitly opt into the TLS-enabled shared NATS profile.
    pub async fn with_secure_shared_nats(mut self) -> TestResult<Self> {
        let nats = shared_secure_nats_handle().await?;
        let client = nats.connect().await?;
        self.nats_client = Some(client.clone());
        self.nats = Some(nats);
        self.nats_mode = NatsMode::Shared;

        // Register client with reaper for cleaning
        self._reaper.nats.lock().replace(client);

        self.install_current();
        Ok(self)
    }

    /// Get the NATS client for this test context
    ///
    /// Panics if NATS was not enabled via `with_nats()` or `with_shared_nats()`.
    pub fn nats_client(&self) -> NatsClient {
        self.nats_client
            .clone()
            .expect("NATS not initialized - call with_nats() or with_shared_nats() first")
    }

    /// Access the underlying EphemeralNats handle (lifetime-managed by the context).
    pub fn nats_handle(&self) -> TestResult<Arc<EphemeralNats>> {
        self.nats
            .as_ref()
            .cloned()
            .ok_or_else(|| eyre!("NATS not initialized - call with_nats() or with_shared_nats()"))
    }

    /// Get a JetStream context bound to this test's NATS instance.
    pub async fn jetstream(&self) -> TestResult<jetstream::Context> {
        let nats = self.nats.as_ref().ok_or_else(|| {
            eyre!("NATS not initialized - call with_nats() or with_shared_nats()")
        })?;
        nats.jetstream().await
    }

    /// Get (or create) the default checkpoint KV bucket for tests.
    pub async fn checkpoint_kv(&self) -> TestResult<jetstream::kv::Store> {
        let js = self.jetstream().await?;
        let bucket = sinex_node_sdk::checkpoint::checkpoint_bucket_name(None);
        let kv_store = match js
            .create_key_value(jetstream::kv::Config {
                bucket: bucket.clone(),
                history: 64,
                ..Default::default()
            })
            .await
        {
            Ok(store) => Ok(store),
            Err(_) => js.get_key_value(bucket).await,
        }?;
        Ok(kv_store)
    }

    /// Get the Sinex environment for this test context
    pub fn env(&self) -> &SinexEnvironment {
        &self.env
    }

    /// Access the per-test JetStream namespace used for pipeline resources.
    pub fn pipeline_namespace(&self) -> &PipelineNamespace {
        &self.pipeline_namespace
    }

    /// Create a pipeline scope that resets the DB slot and starts ingestd.
    pub async fn pipeline_scope(&self) -> TestResult<PipelineScope<'_>> {
        PipelineScope::new(self).await
    }

    async fn ensure_pipeline_ingestd(&self) -> TestResult<()> {
        self.ensure_shared_nats()?;
        let mut guard = self.pipeline_ingestd.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let nats = self.nats_handle()?;
        let namespace = self.pipeline_namespace().prefix().to_string();
        let mut config = TestIngestdConfig {
            nats_url: nats.client_url().to_string(),
            database_url: self.database_url().to_string(),
            work_dir: None,
            namespace: Some(namespace),
            ..Default::default()
        };
        config.batch_size = 32;
        config.consumer_fetch_max_messages = 32;
        config.consumer_fetch_timeout_ms = 200;
        let handle = start_test_ingestd_with_config(config, Some(self)).await?;
        *guard = Some(handle);
        drop(guard);

        let ingestd_handle = self.pipeline_ingestd.clone();
        self.register_shutdown_hook("pipeline-ingestd-shutdown", async move {
            if let Some(mut handle) = ingestd_handle.lock().await.take() {
                let _ = handle.stop().await;
            }
        })
        .await;
        Ok(())
    }

    pub(crate) fn ensure_shared_nats(&self) -> TestResult<()> {
        match self.nats_mode {
            NatsMode::Shared => Ok(()),
            NatsMode::Dedicated => Err(eyre!(
                "PipelineScope requires shared NATS; call with_shared_nats() instead of with_nats()"
            )),
            NatsMode::None => Err(eyre!(
                "PipelineScope requires shared NATS; call with_shared_nats() first"
            )),
        }
    }

    /// Reset the underlying database slot and verify it is clean.
    pub async fn reset_database_slot(&self) -> TestResult<()> {
        self.quiesce_background_tasks().await?;
        db_common::reset_database(&self.pool).await?;
        db_common::verify_clean_state(&self.pool).await?;
        Ok(())
    }

    /// Get the NATS server URL if NATS is enabled
    pub fn nats_url(&self) -> Option<String> {
        self.nats.as_ref().map(|n| n.client_url().to_string())
    }

    /// Launch ingestd attached to this context and return a pipeline harness.
    pub async fn pipeline(&self) -> TestResult<PipelineHarness<'_>> {
        if self.nats.is_none() {
            return Err(eyre!(
                "Pipeline harness requires NATS - call with_nats() or with_shared_nats() first"
            ));
        }
        PipelineHarness::new(self).await
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

    /// Capture snapshot metadata that survives if the context is moved.
    pub fn failure_snapshot(&self) -> TestContextFailureSnapshot {
        TestContextFailureSnapshot {
            test_name: self.test_name.clone(),
            baseline_events: self.baseline_events,
            start_time: self.start_time,
            captured_logs: Arc::clone(&self.captured_logs),
            background: self.background.clone(),
        }
    }

    /// Current total number of events
    pub async fn current_event_count(&self) -> TestResult<i64> {
        Ok(self.pool.events().count_all().await?)
    }

    /// Difference between current and baseline event count
    pub async fn event_delta(&self) -> TestResult<i64> {
        Ok(self.current_event_count().await? - self.baseline_events)
    }

    async fn ensure_material_entry(&self, id: &Id<SourceMaterial>) -> TestResult<()> {
        let material_ulid_uuid = id.to_uuid();
        let source_identifier = format!("test-material-{id}");

        let identifier = if id.to_string() == BOOTSTRAP_MATERIAL_ID {
            BOOTSTRAP_MATERIAL_IDENTIFIER.to_string()
        } else {
            source_identifier
        };

        let update_result = sqlx::query!(
            r#"
                UPDATE raw.source_material_registry
                SET id = $1::uuid::ulid,
                    material_kind = $2,
                    status = $4,
                    timing_info_type = $5
                WHERE source_identifier = $3
            "#,
            material_ulid_uuid,
            "annex",
            identifier,
            "completed",
            "realtime"
        )
        .execute(&self.pool)
        .await?;

        if update_result.rows_affected() == 0 {
            sqlx::query!(
                r#"
                    INSERT INTO raw.source_material_registry 
                        (id, material_kind, source_identifier, status, timing_info_type)
                    VALUES ($1::uuid::ulid, $2, $3, $4, $5)
                    ON CONFLICT (id) DO UPDATE
                    SET material_kind = EXCLUDED.material_kind,
                        status = EXCLUDED.status,
                        timing_info_type = EXCLUDED.timing_info_type,
                        source_identifier = EXCLUDED.source_identifier
                "#,
                material_ulid_uuid,
                "annex",
                identifier,
                "completed",
                "realtime"
            )
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn ensure_temporal_ledger_entry(
        &self,
        id: &Id<SourceMaterial>,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
        offset_kind: OffsetKind,
        ts_capture: DateTime<Utc>,
    ) -> TestResult<()> {
        let offset_start = match offset_start {
            Some(start) => start,
            None => return Ok(()),
        };
        let offset_end = offset_end.unwrap_or(offset_start);
        let offset_kind = match offset_kind {
            OffsetKind::Byte => "byte",
            OffsetKind::Line => "line",
            OffsetKind::Record => "rowid",
            OffsetKind::Character => "logical",
        };

        sqlx::query!(
            r#"
                INSERT INTO raw.temporal_ledger
                    (source_material_id, offset_start, offset_end, offset_kind, ts_capture, precision, clock, source_type)
                VALUES ($1::uuid::ulid, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (source_material_id, offset_start) DO NOTHING
            "#,
            id.to_uuid(),
            offset_start,
            offset_end,
            offset_kind,
            ts_capture,
            "bounded",
            "wall",
            "realtime_capture"
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    fn record_created_event(&self, event_id: Ulid, material_id: Option<Ulid>) {
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
        match crate::db_common::verify_clean_state(&self.pool).await {
            Ok(_) => Ok(()),
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
    pub fn register_background_handle<T>(&self, label: impl Into<String>, handle: T)
    where
        T: Send + 'static,
    {
        let registry = self.background.clone();
        let lbl = label.into();
        tokio::spawn(async move {
            registry.lock().await.add_task(
                lbl,
                tokio::spawn(async move {
                    let _ = handle;
                    let _ = tokio::task::yield_now().await;
                }),
            );
        });
    }

    /// Spawn and track a background task that will be awaited during cleanup.
    pub fn spawn_background<F>(&self, label: impl Into<String>, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let registry = self.background.clone();
        let lbl = label.into();
        let handle = tokio::spawn(fut);
        tokio::spawn(async move {
            registry.lock().await.add_task(lbl, handle);
        });
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

    /// Publish and persist a test event through the ingestion pipeline.
    ///
    /// This replaces the legacy direct-insert helpers with a pipeline-first path that:
    /// 1. Builds a sanitized `Event<JsonValue>`
    /// 2. Ensures a backing source material record exists
    /// 3. Publishes the payload through NATS/ingestd
    /// 4. Waits for persistence and returns the stored event
    pub async fn publish_json_event<S, T>(
        &self,
        source: S,
        event_type: T,
        payload: JsonValue,
    ) -> TestResult<Event<JsonValue>>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        self.publish_json_event_with_overrides(source, event_type, payload, None)
            .await
    }

    /// Publish and persist a test event with an explicit timestamp override.
    pub async fn publish_json_event_with_timestamp<S, T>(
        &self,
        source: S,
        event_type: T,
        payload: JsonValue,
        timestamp: Timestamp,
    ) -> TestResult<Event<JsonValue>>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        self.publish_json_event_with_overrides(source, event_type, payload, Some(timestamp))
            .await
    }

    async fn publish_json_event_with_overrides<S, T>(
        &self,
        source: S,
        event_type: T,
        payload: JsonValue,
        timestamp_override: Option<Timestamp>,
    ) -> TestResult<Event<JsonValue>>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        if self.nats.is_none() {
            return Err(eyre!(
                "publish_json_event requires NATS - call with_nats() or with_shared_nats() first"
            ));
        }

        let mut sanitized_payload = payload;
        TestContext::sanitize_payload(&mut sanitized_payload);

        let mut event =
            Event::<JsonValue>::test_event(source.as_ref(), event_type.as_ref(), sanitized_payload);
        if let Some(ts) = timestamp_override {
            event.ts_orig = Some(ts);
        }
        if event.ingestor_version.is_none() {
            event.ingestor_version = Some("test-ingestor".to_string());
        }
        let _event_id = event.id.get_or_insert_with(Id::new).clone();

        let material_id = Id::<SourceMaterial>::new();
        self.ensure_source_material(material_id, Some(source.as_ref()))
            .await?;
        let material_ulid = material_id.as_ulid().clone();
        event.provenance = Provenance::from_material(material_id, 0, None, None);

        let persisted_id = self.publish_test_event(&event).await?;
        let published_event_id = Id::<Event<JsonValue>>::from_ulid(persisted_id);
        WaitHelpers::wait_for_event_id(&self.pool, published_event_id.clone(), DEFAULT_WAIT_SECS)
            .await?;

        let stored = self
            .pool
            .events()
            .get_by_id(published_event_id.clone())
            .await?
            .ok_or_else(|| {
                eyre!(
                    "Event {} not found after pipeline publish",
                    published_event_id
                )
            })?;

        let cleanup_material = match &stored.provenance {
            Provenance::Material { id, .. } => Some(id.as_ulid().clone()),
            _ => Some(material_ulid),
        };
        self.record_created_event(published_event_id.as_ulid().clone(), cleanup_material);

        Ok(stored)
    }

    /// Ensure a source material record exists for tests that construct provenance manually.
    pub async fn ensure_source_material(
        &self,
        id: Id<SourceMaterial>,
        source_identifier: Option<&str>,
    ) -> TestResult<()> {
        let material_ulid_uuid = id.to_uuid();
        let identifier = source_identifier.map(|s| s.to_string()).unwrap_or_else(|| {
            if id.to_string() == BOOTSTRAP_MATERIAL_ID {
                BOOTSTRAP_MATERIAL_IDENTIFIER.to_string()
            } else {
                format!("test-material-{id}")
            }
        });

        let update_result = sqlx::query!(
            r#"
                UPDATE raw.source_material_registry
                SET id = $1::uuid::ulid,
                    material_kind = $2,
                    status = $4,
                    timing_info_type = $5
                WHERE source_identifier = $3
            "#,
            material_ulid_uuid,
            "annex",
            identifier,
            "completed",
            "realtime"
        )
        .execute(&self.pool)
        .await?;

        if update_result.rows_affected() == 0 {
            sqlx::query!(
                r#"
                    INSERT INTO raw.source_material_registry 
                        (id, material_kind, source_identifier, status, timing_info_type)
                    VALUES ($1::uuid::ulid, $2, $3, $4, $5)
                    ON CONFLICT (id) DO UPDATE
                    SET material_kind = EXCLUDED.material_kind,
                        status = EXCLUDED.status,
                        timing_info_type = EXCLUDED.timing_info_type,
                        source_identifier = EXCLUDED.source_identifier
                "#,
                material_ulid_uuid,
                "annex",
                identifier,
                "completed",
                "realtime"
            )
            .execute(&self.pool)
            .await?;
        }

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
        material_id: sinex_core::Ulid,
        source_identifier: Option<&str>,
    ) -> TestResult<Id<SourceMaterial>> {
        let id = Id::<SourceMaterial>::from_ulid(material_id);
        self.ensure_source_material(id, source_identifier).await?;
        Ok(id)
    }

    /// Convenience helper returning a schema-layer ULID for tests.
    pub async fn ensure_schema_material(
        &self,
        source_identifier: Option<&str>,
    ) -> TestResult<Ulid> {
        let id = self.create_source_material(source_identifier).await?;
        Ok(id.as_ulid().clone())
    }

    /// Connection URL for the underlying test database.
    pub fn database_url(&self) -> &str {
        self.db.url()
    }

    /// Name of the dedicated database slot backing this context.
    pub fn database_name(&self) -> &str {
        self.db.name()
    }

    /// Access timing utilities
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

    /// Assert the total event count matches expectation.
    pub async fn assert_event_count(&self, expected: usize) -> TestResult<usize> {
        let count = self.pool.events().count_all().await? as usize;
        if count != expected {
            color_eyre::eyre::bail!("Expected {expected} events, found {count}");
        }
        Ok(count)
    }

    /// Assert the total event count is at least the expectation.
    pub async fn assert_event_count_at_least(&self, expected: usize) -> TestResult<usize> {
        let count = self.pool.events().count_all().await? as usize;
        if count < expected {
            color_eyre::eyre::bail!("Expected at least {expected} events, found {count}");
        }
        Ok(count)
    }

    /// Assert the event count for a source matches expectation.
    pub async fn assert_event_count_by_source(
        &self,
        source: &str,
        expected: usize,
    ) -> TestResult<usize> {
        let event_source = sinex_core::EventSource::new(source);
        let count = self.pool.events().count_by_source(&event_source).await? as usize;
        if count != expected {
            color_eyre::eyre::bail!("Expected {expected} events for '{source}', found {count}");
        }
        Ok(count)
    }

    /// Assert the event count for a source is at least the expectation.
    pub async fn assert_event_count_by_source_at_least(
        &self,
        source: &str,
        expected: usize,
    ) -> TestResult<usize> {
        let event_source = sinex_core::EventSource::new(source);
        let count = self.pool.events().count_by_source(&event_source).await? as usize;
        if count < expected {
            color_eyre::eyre::bail!(
                "Expected at least {expected} events for '{source}', found {count}"
            );
        }
        Ok(count)
    }

    /// Assert that a collection of events has unique IDs.
    pub fn assert_unique_event_ids(&self, events: &[Event<JsonValue>]) -> TestResult<()> {
        let mut seen = HashSet::new();
        for event in events {
            if let Some(id) = event.id.as_ref() {
                if !seen.insert(id.to_string()) {
                    color_eyre::eyre::bail!("Duplicate event id detected: {id}");
                }
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

impl TestContext {
    /// Publish a test event to the ingestion pipeline via NATS.
    ///
    /// This is the preferred method for "Pipeline-First" testing. It sends the event
    /// to NATS (simulating a satellite), where it will be picked up by ingestd,
    /// validated, and inserted into the database.
    ///
    /// The event ID is returned so tests can wait for it using `WaitHelpers`.
    pub async fn publish_test_event(&self, event: &Event<JsonValue>) -> TestResult<Ulid> {
        self.ensure_pipeline_ingestd().await?;
        let client = self.nats_client();
        let mut envelope = event.clone();
        if envelope.ingestor_version.is_none() {
            envelope.ingestor_version = Some("test-ingestd".to_string());
        }
        let payload = serde_json::to_vec(&envelope)?;

        let base_subject = format!("events.raw.{}", event.source);
        let subject = self.pipeline_namespace().subject(&base_subject);

        client.publish(subject, payload.into()).await?;

        // Return the ID so the caller can wait for it
        Ok(event
            .id
            .clone()
            .expect("Test event must have an ID")
            .as_ulid()
            .clone())
    }
}

async fn cleanup_created_records(pool: DbPool, records: Vec<CreatedEventInfo>) -> TestResult<()> {
    if records.is_empty() {
        return Ok(());
    }

    let event_ids: Vec<Uuid> = records
        .iter()
        .map(|info| ulid_to_uuid(info.event_id))
        .collect();

    if !event_ids.is_empty() {
        sqlx::query!(
            "DELETE FROM core.events WHERE id = ANY(($1::uuid[])::ulid[])",
            &event_ids
        )
        .execute(&pool)
        .await?;
    }

    let material_set: HashSet<Uuid> = records
        .iter()
        .filter_map(|info| info.material_id.map(ulid_to_uuid))
        .collect();
    let material_ids: Vec<Uuid> = material_set.into_iter().collect();

    if !material_ids.is_empty() {
        sqlx::query!(
            "DELETE FROM raw.source_material_registry WHERE id = ANY(($1::uuid[])::ulid[])",
            &material_ids
        )
        .execute(&pool)
        .await?;
    }

    Ok(())
}

#[derive(Default)]
pub(crate) struct BackgroundRegistry {
    tasks: Vec<(String, JoinHandle<()>)>,
    shutdown_hooks: Vec<(String, BoxFuture<'static, ()>)>,
}

impl BackgroundRegistry {
    fn background_timeout_secs() -> u64 {
        BACKGROUND_TIMEOUT_SECS
    }

    fn pending_count(&self) -> usize {
        self.tasks.len() + self.shutdown_hooks.len()
    }

    fn add_task(&mut self, label: impl Into<String>, handle: JoinHandle<()>) {
        self.tasks.push((label.into(), handle));
    }

    fn add_hook(&mut self, label: impl Into<String>, hook: BoxFuture<'static, ()>) {
        self.shutdown_hooks.push((label.into(), hook));
    }

    fn labels(&self) -> Vec<String> {
        self.tasks
            .iter()
            .map(|(l, _)| l.clone())
            .chain(self.shutdown_hooks.iter().map(|(l, _)| l.clone()))
            .collect()
    }

    async fn run_shutdown_hooks(&mut self, timeout_secs: u64) {
        // Run shutdown hooks first so tasks can observe the signal.
        let hooks = std::mem::take(&mut self.shutdown_hooks);
        for (label, hook) in hooks {
            if let Err(err) = tokio::time::timeout(Duration::from_secs(timeout_secs), hook).await {
                warn!(%label, ?err, "Timeout waiting for shutdown hook");
            }
        }
    }

    async fn wait_for_tasks(&mut self, timeout_secs: u64) {
        // Wait for tracked background tasks to finish, aborting on timeout.
        let tasks = std::mem::take(&mut self.tasks);
        for (label, handle) in tasks {
            let mut handle = handle;
            let timeout_sleep = tokio::time::sleep(Duration::from_secs(timeout_secs));
            tokio::pin!(timeout_sleep);

            tokio::select! {
                result = &mut handle => {
                    match result {
                        Ok(()) => {}
                        Err(join_err) => warn!(%label, error = %join_err, "Background task join failed"),
                    }
                }
                _ = &mut timeout_sleep => {
                    warn!(%label, "Background task did not finish within timeout; aborting");
                    handle.abort();
                    let _ = handle.await;
                }
            };
        }
    }

    async fn quiesce(&mut self) {
        let timeout_secs = Self::background_timeout_secs();
        self.run_shutdown_hooks(timeout_secs).await;
        self.wait_for_tasks(timeout_secs).await;
    }

    async fn quiesce_tasks_only(&mut self) {
        let timeout_secs = Self::background_timeout_secs();
        self.wait_for_tasks(timeout_secs).await;
    }
}

/// Cleanup implementation for TestContext
impl Drop for TestContext {
    fn drop(&mut self) {
        self.clear_current();
        if std::thread::panicking() {
            let snapshot = self.failure_snapshot();
            snapshot_helper::persist_failure(
                self.test_name(),
                "TestContext dropped during panic",
                FailureContext::Snapshot(snapshot),
            );
        }
        // Ensure any registered background work is flushed before returning the database.
        let registry = self.background.clone();
        let quiesce_fut = async move {
            let _ = tokio::time::timeout(Duration::from_secs(15), async {
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
                let _ = rx.recv_timeout(Duration::from_secs(20));
            }
            Err(_) => {
                futures::executor::block_on(quiesce_fut);
            }
        }

        let pool = self.pool.clone();
        let records = {
            let mut guard = self.created_events.lock();
            guard.drain(..).collect::<Vec<_>>()
        };

        if !records.is_empty() {
            if let Ok(handle) = Handle::try_current() {
                let cleanup_pool = pool.clone();
                let cleanup_records = records.clone();
                let mut join_handle = Some(handle.spawn(async move {
                    if let Err(err) = cleanup_created_records(cleanup_pool, cleanup_records).await {
                        warn!("TestContext cleanup failed: {}", err);
                    }
                }));

                if let Ok(mut guard) = CLEANUP_HANDLES.try_lock() {
                    if let Some(join) = join_handle.take() {
                        guard.push(join);
                    }
                }

                if let Some(join) = join_handle {
                    handle.spawn(async move {
                        if let Err(err) = join.await {
                            warn!("Detached cleanup task failed: {}", err);
                        }
                    });
                }
            } else if let Err(err) =
                futures::executor::block_on(cleanup_created_records(pool.clone(), records))
            {
                warn!("TestContext cleanup failed without runtime: {}", err);
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

/// Rich assertion helper with contextual error messages
pub struct ContextualAssert {
    context: String,
}

impl ContextualAssert {
    fn new(context: &str) -> Self {
        Self {
            context: context.to_string(),
        }
    }

    /// Assert two values are equal
    pub fn eq<T>(self, left: &T, right: &T) -> TestResult<Self>
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
    pub fn that(self, condition: bool, message: &str) -> TestResult<Self> {
        if !condition {
            color_eyre::eyre::bail!("{}: {}", self.context, message);
        }
        Ok(self)
    }

    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> TestResult<Self> {
        if collection.is_empty() {
            color_eyre::eyre::bail!("{}: collection should not be empty", self.context);
        }
        Ok(self)
    }

    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> TestResult<Self> {
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
    pub fn some<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_none() {
            color_eyre::eyre::bail!("{}: option should be Some, but was None", self.context);
        }
        Ok(self)
    }

    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_some() {
            color_eyre::eyre::bail!("{}: option should be None, but was Some", self.context);
        }
        Ok(self)
    }

    /// Assert result contains error with specific message
    pub fn error_contains<T, E>(
        self,
        result: &Result<T, E>,
        expected_error: &str,
    ) -> TestResult<Self>
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
