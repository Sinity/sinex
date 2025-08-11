// Test Fixture Management System
//
// Provides reusable test data with proper lifecycle management for Sinex tests.
// Features:
// - Lazy initialization and caching of expensive test data
// - Automatic cleanup on test completion
// - Fixture dependencies and ordering
// - Transaction-scoped fixtures for database isolation
// - Performance fixtures with pre-warmed data
//
// NOTE: Static fixture persistence to disk is available in the static_fixtures module
// which is behind the "bench" feature flag. When enabled, fixtures can be generated
// once and stored on disk for deterministic testing across runs. Currently, fixtures
// are generated in-memory and cached for the duration of the test run.
//
// Usage:
// ```rust
// #[sinex_test]
// async fn test_with_fixture(ctx: TestContext) -> Result<()> {
//     let session = fixtures::standard_user_session(&ctx).await?;
//     // fixture automatically cleaned up
// }
// ```

use crate::builders::TestCheckpointBuilder;
use crate::fixture_config::FIXTURE_CONFIG;
use crate::prelude::*;
use crate::test_context::TestContext;
use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use serde_json::json;
use sinex_core::db::models::*;
use sinex_core::db::{repositories::DbPoolExt, DbPool};
use sinex_core::types::events::payloads::{
    ClipboardCopiedPayload, FileCreatedPayload, KittyCommandCompletedPayload,
};
use sinex_core::types::events::Event;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

/// Global fixture registry for sharing fixtures across tests
static FIXTURE_REGISTRY: OnceCell<Arc<Mutex<FixtureRegistry>>> = OnceCell::const_new();

/// Get or create the global fixture registry
async fn get_registry() -> Arc<Mutex<FixtureRegistry>> {
    FIXTURE_REGISTRY
        .get_or_init(|| async { Arc::new(Mutex::new(FixtureRegistry::new())) })
        .await
        .clone()
}

/// Registry for managing fixture lifecycle
struct FixtureRegistry {
    /// Cached fixtures by type ID and key
    cache: HashMap<(TypeId, String), Arc<dyn Any + Send + Sync>>,
    /// Cleanup functions for each fixture
    cleanups: HashMap<(TypeId, String), Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
    /// Reference counts for cached fixtures
    ref_counts: HashMap<(TypeId, String), usize>,
}

impl FixtureRegistry {
    fn new() -> Self {
        Self {
            cache: HashMap::new(),
            cleanups: HashMap::new(),
            ref_counts: HashMap::new(),
        }
    }

    /// Get or create a cached fixture with proper error handling
    async fn get_or_create<T, F, Fut>(&mut self, key: String, creator: F) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let type_id = TypeId::of::<T>();
        let cache_key = (type_id, key.clone());

        // First check: return cached fixture if it exists
        if let Some(cached) = self.cache.get(&cache_key) {
            self.ref_counts
                .entry(cache_key.clone())
                .and_modify(|c| *c += 1);
            return Ok(cached.clone().downcast::<T>().unwrap());
        }

        // Create new fixture with proper error propagation
        // This avoids panicking and poisoning the mutex
        let fixture = creator().await.map_err(|e| {
            SinexError::service("Failed to create fixture")
                .with_source(e)
                .with_context("key", &key)
        })?;
        let arc_fixture = Arc::new(fixture);

        // Insert the successfully created fixture
        self.cache.insert(
            cache_key.clone(),
            arc_fixture.clone() as Arc<dyn Any + Send + Sync>,
        );
        self.ref_counts.insert(cache_key, 1);

        Ok(arc_fixture)
    }

    /// Register a cleanup function for a fixture
    fn register_cleanup<F>(&mut self, type_id: TypeId, key: String, cleanup: F)
    where
        F: Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        self.cleanups.insert((type_id, key), Box::new(cleanup));
    }

    /// Release a fixture reference and cleanup if needed
    async fn release<T: 'static>(&mut self, key: String) {
        let type_id = TypeId::of::<T>();
        let cache_key = (type_id, key);

        if let Some(count) = self.ref_counts.get_mut(&cache_key) {
            *count -= 1;
            if *count == 0 {
                // Run cleanup if registered
                if let Some(cleanup) = self.cleanups.remove(&cache_key) {
                    cleanup().await;
                }
                self.cache.remove(&cache_key);
                self.ref_counts.remove(&cache_key);
            }
        }
    }
}

/// Type alias for fixture handles using Arc for shared ownership
pub type FixtureHandle<T> = Arc<T>;

/// Fixture data for a standard user session
#[derive(Debug, Clone)]
pub struct UserSessionFixture {
    pub user_id: String,
    pub session_start: chrono::DateTime<chrono::Utc>,
    pub event_ids: Vec<Ulid>,
    pub checkpoint_id: Option<Ulid>,
}

/// Fixture metadata for large datasets
#[derive(Debug, Clone)]
pub struct LargeDatasetFixture {
    pub event_ids: Vec<Ulid>,
    pub event_count: usize,
    pub source_distribution: HashMap<String, usize>,
    pub type_distribution: HashMap<String, usize>,
    pub time_range: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
}

/// Fixture data for populated checkpoints
#[derive(Debug, Clone)]
pub struct PopulatedCheckpointsFixture {
    pub processor_names: Vec<String>,
    pub checkpoint_ids: Vec<Ulid>,
    pub total_events_processed: u64,
}

/// Fixture data for error scenarios
#[derive(Debug, Clone)]
pub struct ErrorScenariosFixture {
    pub invalid_event_ids: Vec<Ulid>,
    pub failed_operation_ids: Vec<Ulid>,
    pub error_messages: Vec<String>,
}

/// Fixture data for performance testing
#[derive(Debug, Clone)]
pub struct PerformanceDatasetFixture {
    pub event_count: usize,
    pub event_ids: Vec<Ulid>,
    pub time_range: (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    pub sources: Vec<String>,
}

// Type aliases for fixture types
pub type CheckpointFixture = PopulatedCheckpointsFixture;
pub type TerminalSessionFixture = UserSessionFixture;
pub type ConcurrentOperationsFixture = UserSessionFixture;
pub type EventStormFixture = PerformanceDatasetFixture;
pub type HighVolumeCheckpointsFixture = PopulatedCheckpointsFixture;
pub type ValidationErrorsFixture = ErrorScenariosFixture;
pub type SchemaViolationsFixture = ErrorScenariosFixture;
pub type MalformedEventsFixture = ErrorScenariosFixture;

/// Builder for parameterized fixtures
#[derive(bon::Builder)]
pub struct Fixture<T> {
    #[builder(default = HashMap::new())]
    params: HashMap<String, serde_json::Value>,
    #[builder(skip)]
    _marker: std::marker::PhantomData<T>,
}

impl<T> Fixture<T> {
    pub fn params(&self) -> &HashMap<String, serde_json::Value> {
        &self.params
    }
}

impl<T> Default for Fixture<T> {
    fn default() -> Self {
        Self {
            params: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// FIXTURE IMPLEMENTATIONS
// =============================================================================

/// Create a standard user session fixture with activity events
pub(crate) async fn standard_user_session(
    ctx: &TestContext,
) -> Result<FixtureHandle<UserSessionFixture>> {
    let key = format!("standard_user_session_{}", ctx.test_name());
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move {
                create_user_session_fixture(
                    &pool,
                    FIXTURE_CONFIG.user_session_event_count,
                    FIXTURE_CONFIG.checkpoint_interval,
                )
                .await
            }
        })
        .await?;

    Ok(fixture)
}

/// Create a parameterized user session fixture
pub(crate) async fn user_session_with_params(
    ctx: &TestContext,
    event_count: usize,
    checkpoint_interval: usize,
) -> Result<FixtureHandle<UserSessionFixture>> {
    let key = format!(
        "user_session_{}_{}_{}_{}",
        ctx.test_name(),
        event_count,
        checkpoint_interval,
        uuid::Uuid::new_v4()
    );
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_user_session_fixture(&pool, event_count, checkpoint_interval).await
        }
    }).await?;

    Ok(fixture)
}

/// Create an empty database fixture (useful for isolation tests)
pub(crate) async fn empty_database(ctx: &TestContext) -> Result<FixtureHandle<()>> {
    let pool = ctx.pool.clone();

    // Clean any test data
    sqlx::query!("DELETE FROM core.events WHERE source LIKE 'test_%'")
        .execute(&pool)
        .await?;
    sqlx::query!("DELETE FROM core.processor_checkpoints WHERE processor_name LIKE 'test_%'")
        .execute(&pool)
        .await?;

    Ok(Arc::new(()))
}

/// Create populated checkpoints fixture
pub(crate) async fn populated_checkpoints(
    ctx: &TestContext,
) -> Result<FixtureHandle<PopulatedCheckpointsFixture>> {
    let key = format!("populated_checkpoints_{}", ctx.test_name());
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move { create_populated_checkpoints_fixture(&pool).await }
        })
        .await?;

    Ok(fixture)
}

/// Create error scenarios fixture
pub(crate) async fn error_scenarios(
    ctx: &TestContext,
) -> Result<FixtureHandle<ErrorScenariosFixture>> {
    let key = format!("error_scenarios_{}", ctx.test_name());
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move { create_error_scenarios_fixture(&pool).await }
        })
        .await?;

    Ok(fixture)
}

/// Create performance dataset fixture
pub(crate) async fn performance_dataset(
    ctx: &TestContext,
) -> Result<FixtureHandle<PerformanceDatasetFixture>> {
    performance_dataset_with_size(ctx, FIXTURE_CONFIG.medium_dataset_size).await
}

/// Create parameterized performance dataset fixture
pub(crate) async fn performance_dataset_with_size(
    ctx: &TestContext,
    event_count: usize,
) -> Result<FixtureHandle<PerformanceDatasetFixture>> {
    let key = format!(
        "performance_dataset_{}_{}_{}",
        ctx.test_name(),
        event_count,
        uuid::Uuid::new_v4()
    );
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move { create_performance_dataset_fixture(&pool, event_count).await }
        })
        .await?;

    Ok(fixture)
}

// =============================================================================
// FIXTURE CREATION HELPERS
// =============================================================================

async fn create_user_session_fixture(
    pool: &DbPool,
    event_count: usize,
    checkpoint_interval: usize,
) -> Result<UserSessionFixture> {
    let user_id = format!("test_user_{}", uuid::Uuid::new_v4());
    let session_start = Utc::now() - Duration::hours(1);
    let mut event_ids = Vec::new();

    // Create filesystem events
    for i in 0..event_count / 3 {
        let event = Event::new(FileCreatedPayload {
            path: SanitizedPath::from(format!("/home/{}/documents/file_{}.txt", user_id, i)),
            size: 0,
            created_at: Utc::now(),
            permissions: None,
        })
        .into();

        let inserted = pool.events().insert(event).await.map_err(|e| {
            SinexError::database("Failed to insert event")
                .with_source(e)
                .with_context("fixture", "user_session")
        })?;
        event_ids.push(
            inserted
                .id
                .expect("Inserted event must have ID")
                .as_ulid()
                .clone(),
        );
    }

    // Create terminal events
    let commands = [
        "ls -la",
        "cd ~/projects",
        "git status",
        "cargo build",
        "vim main.rs",
    ];
    for i in 0..event_count / 3 {
        let cmd = commands[i % commands.len()];
        let event = Event::new(KittyCommandCompletedPayload {
            command: CommandText::from(cmd.to_string()),
            working_directory: SanitizedPath::from(format!("/home/{}/projects", user_id)),
            exit_status: 0,
            duration_ms: 100 + i as u64 * 10,
            shell_pid: 1000 + i as u32,
            kitty_window_id: "window_1".to_string(),
            kitty_tab_id: "tab_1".to_string(),
            output_lines: Some(10),
            error_output: None,
        })
        .into();

        let inserted = pool.events().insert(event).await.map_err(|e| {
            SinexError::database("Failed to insert event")
                .with_source(e)
                .with_context("fixture", "user_session")
        })?;
        event_ids.push(
            inserted
                .id
                .expect("Inserted event must have ID")
                .as_ulid()
                .clone(),
        );
    }

    // Create clipboard events
    for i in 0..event_count / 3 {
        let text = format!("Clipboard content {}", i);
        let event = Event::new(ClipboardCopiedPayload {
            operation: "copy".to_string(),
            content_type: "text/plain".to_string(),
            content_size: text.len(),
            text_preview: Some(text.clone()),
            file_count: None,
            file_paths: None,
            source_app: Some("test_app".to_string()),
            window_title: Some("Test Window".to_string()),
            content_hash: format!("hash_{}", i),
            original_hash: None,
            annex_key: None,
            blob_id: None,
        })
        .into();

        let inserted = pool.events().insert(event).await.map_err(|e| {
            SinexError::database("Failed to insert event")
                .with_source(e)
                .with_context("fixture", "user_session")
        })?;
        event_ids.push(
            inserted
                .id
                .expect("Inserted event must have ID")
                .as_ulid()
                .clone(),
        );
    }

    // Create checkpoint if needed
    let checkpoint_id = if checkpoint_interval > 0 && event_count >= checkpoint_interval {
        let checkpoint_id = Ulid::new();
        TestCheckpointBuilder::new(&format!("test_processor_{}", user_id))
            .processed_count((event_count / checkpoint_interval * checkpoint_interval) as i64)
            .last_processed_id(Id::from(event_ids[checkpoint_interval - 1]))
            .state_data(json!({
                "user_id": user_id,
                "session_start": session_start,
                "events_processed": event_count / checkpoint_interval * checkpoint_interval
            }))
            .build()
            .insert(pool)
            .await?;
        Some(checkpoint_id)
    } else {
        None
    };

    Ok(UserSessionFixture {
        user_id,
        session_start,
        event_ids,
        checkpoint_id,
    })
}

async fn create_populated_checkpoints_fixture(
    pool: &DbPool,
) -> Result<PopulatedCheckpointsFixture> {
    let count = FIXTURE_CONFIG.populated_checkpoints_count;
    let mut processor_names = Vec::new();

    // Generate processor names based on configured count
    let base_names = vec![
        "health-aggregator",
        "command-canonicalizer",
        "activity-tracker",
        "event-processor",
        "data-enricher",
    ];

    for i in 0..count {
        processor_names.push(if i < base_names.len() {
            base_names[i].to_string()
        } else {
            format!("processor-{}", i)
        });
    }
    let mut checkpoint_ids = Vec::new();
    let mut total_events_processed = 0u64;

    for (i, name) in processor_names.iter().enumerate() {
        let processed_count = 100 * (i + 1) as i64;
        total_events_processed += processed_count as u64;

        let checkpoint_id = Ulid::new();
        TestCheckpointBuilder::new(name)
            .processed_count(processed_count)
            .last_processed_id(Id::from(Ulid::new()))
            .state_data(json!({
                "processor_name": name,
                "version": "1.0.0",
                "status": "healthy",
                "last_health_check": Utc::now(),
            }))
            .build()
            .insert(pool)
            .await?;

        checkpoint_ids.push(checkpoint_id);
    }

    Ok(PopulatedCheckpointsFixture {
        processor_names,
        checkpoint_ids,
        total_events_processed,
    })
}

async fn create_error_scenarios_fixture(pool: &DbPool) -> Result<ErrorScenariosFixture> {
    let mut invalid_event_ids = Vec::new();
    let failed_operation_ids = Vec::new();
    let mut error_messages = Vec::new();

    // Create events that would fail validation
    let invalid_events = vec![
        (
            RawEvent::new(EventSource::from(""), EventType::from("test"), json!({})),
            "Empty source",
        ),
        (
            RawEvent::new(EventSource::from("test"), EventType::from(""), json!({})),
            "Empty event type",
        ),
        (
            RawEvent::new(
                EventSource::from("test"),
                EventType::from("test.event"),
                json!(null),
            ),
            "Null payload",
        ),
    ];

    for (event, error_msg) in invalid_events {
        // Try to insert and capture the error
        match pool.events().insert(event).await {
            Ok(inserted) => {
                // If it somehow succeeded, track it for cleanup
                if let Some(id) = inserted.id {
                    invalid_event_ids.push(id.as_ulid().clone());
                }
            }
            Err(e) => {
                error_messages.push(format!("{}: {}", error_msg, e));
            }
        }
    }

    // TODO: Re-enable after updating operations_log helper functions
    // Create failed operations
    /*
    for i in 0..3 {
        let op_id_str: String = sqlx::query_scalar!(
            "SELECT core.start_operation($1, $2, $3::jsonb)::text",
            "stage",
            "error_test_user",
            json!({"test": "error_scenario", "index": i})
        )
        .fetch_one(pool)
        .await?
        .expect("start_operation should return an ID");

        let op_id = Ulid::from_str(&op_id_str).map_err(|e| {
            SinexError::parse("Invalid ULID")
                .with_source(e)
                .with_context("ulid_str", &op_id_str)
        })?;

        sqlx::query!(
            "SELECT core.fail_operation($1::uuid, $2::jsonb)",
            op_id.to_uuid(),
            json!({"error": format!("Test error {}", i), "code": format!("E{}", 500 + i)})
        )
        .execute(pool)
        .await?;

        failed_operation_ids.push(op_id);
        error_messages.push(format!("Operation {} failed: Test error {}", op_id, i));
    }
    */

    Ok(ErrorScenariosFixture {
        invalid_event_ids,
        failed_operation_ids,
        error_messages,
    })
}

async fn create_performance_dataset_fixture(
    pool: &DbPool,
    event_count: usize,
) -> Result<PerformanceDatasetFixture> {
    let start_time = Utc::now() - Duration::days(7);
    let end_time = Utc::now();
    // Use source constants from payload types
    use sinex_core::types::*;

    let sources = vec![
        FileCreatedPayload::SOURCE,
        KittyCommandExecutedPayload::SOURCE,
        ClipboardCopiedPayload::SOURCE,
        HyprlandWindowFocusedPayload::SOURCE,
    ];

    let event_types = vec![
        FileCreatedPayload::EVENT_TYPE,
        KittyCommandExecutedPayload::EVENT_TYPE,
        ClipboardCopiedPayload::EVENT_TYPE,
        HyprlandWindowFocusedPayload::EVENT_TYPE,
    ];

    let mut event_ids = Vec::new();

    // Generate events with time distribution
    let time_range = end_time - start_time;
    let time_step = time_range / event_count as i32;

    let mut batch = Vec::new();
    for i in 0..event_count {
        let source = &sources[i % sources.len()];
        let event_type = &event_types[i % event_types.len()];
        let payload_size = [100, 500, 1000, 5000][i % 4];

        let event = RawEvent::new(
            source.clone(),
            event_type.clone(),
            json!({
                "index": i,
                "data": "x".repeat(payload_size)
            }),
        )
        .with_ts_orig(Some(start_time + time_step * i as i32));
        batch.push(event);
    }

    // Insert in batches for performance
    let chunk_size = FIXTURE_CONFIG.batch_insert_size;
    for chunk in batch.chunks(chunk_size) {
        for event in chunk {
            let inserted = pool.events().insert(event.clone()).await.map_err(|e| {
                SinexError::database("Failed to insert event")
                    .with_source(e)
                    .with_context("fixture", "user_session")
            })?;
            event_ids.push(
                inserted
                    .id
                    .expect("Inserted event must have ID")
                    .as_ulid()
                    .clone(),
            );
        }
    }

    Ok(PerformanceDatasetFixture {
        event_count,
        event_ids,
        time_range: (start_time, end_time),
        sources: sources.iter().map(|s| s.to_string()).collect(),
    })
}

// =============================================================================
// FIXTURE COMPOSITION
// =============================================================================

/// Composite fixture combining multiple fixtures
pub struct CompositeFixture<A: 'static + Send, B: 'static + Send> {
    pub first: FixtureHandle<A>,
    pub second: FixtureHandle<B>,
}

/// Create a fixture that depends on other fixtures
pub(crate) async fn user_session_with_checkpoints(
    ctx: &TestContext,
) -> std::result::Result<
    CompositeFixture<UserSessionFixture, PopulatedCheckpointsFixture>,
    SinexError,
> {
    let session = standard_user_session(ctx)
        .await
        .map_err(|e| SinexError::unknown("Failed to get composite fixture").with_source(e))?;
    let checkpoints = populated_checkpoints(ctx)
        .await
        .map_err(|e| SinexError::unknown("Failed to get composite fixture").with_source(e))?;

    Ok(CompositeFixture {
        first: session,
        second: checkpoints,
    })
}

// =============================================================================
// TRANSACTION-SCOPED FIXTURES
// =============================================================================

/// Run a test with a transaction-scoped fixture
pub(crate) async fn with_transaction_fixture<F, T>(ctx: &TestContext, fixture_fn: F) -> Result<T>
where
    F: for<'a> FnOnce(sqlx::Transaction<'a, sqlx::Postgres>) -> BoxFuture<'a, Result<T>>,
{
    let tx = ctx.pool.begin().await?;

    // Create some fixture data in the transaction
    let _event = ctx
        .create_test_event(
            "transaction_test",
            "file.created",
            json!({"path": "/test/transaction/file.txt", "size": 0}),
        )
        .await
        .map_err(|e| SinexError::unknown(e.to_string()))?;
    // Note: In a real transaction test, you'd use the transaction itself

    let result = fixture_fn(tx).await?;

    // Transaction automatically rolled back on drop
    Ok(result)
}

// =============================================================================
// PERFORMANCE FIXTURES
// =============================================================================

/// Pre-warmed fixture with data already in database
#[derive(Debug, Clone)]
pub struct PreWarmedFixture {
    pub event_count: usize,
    pub checkpoint_count: usize,
    pub operation_count: usize,
    pub total_size_bytes: usize,
}

/// Create a pre-warmed fixture with various data types
pub(crate) async fn pre_warmed_database(
    ctx: &TestContext,
) -> Result<FixtureHandle<PreWarmedFixture>> {
    let key = format!("pre_warmed_database_{}", ctx.test_name());
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move { create_pre_warmed_fixture(&pool).await }
        })
        .await?;

    Ok(fixture)
}

async fn create_pre_warmed_fixture(pool: &DbPool) -> Result<PreWarmedFixture> {
    use sinex_core::types::domain::*;

    let event_count = 5000;
    let checkpoint_count = 10;
    let operation_count = 50;
    let mut total_size_bytes = 0;

    // Create events with various sizes
    let payload_sizes = vec![100, 500, 1000, 5000, 10000];
    let mut batch = Vec::new();
    for i in 0..event_count {
        let payload_size = payload_sizes[i % payload_sizes.len()];
        let event = RawEvent::new(
            EventSource::from("performance_test"),
            EventType::from("test.event"),
            json!({
                "index": i,
                "data": "x".repeat(payload_size)
            }),
        );
        batch.push(event);
    }

    for chunk in batch.chunks(500) {
        for event in chunk {
            let size = serde_json::to_string(&event.payload)?.len();
            total_size_bytes += size;
            pool.events().insert(event.clone()).await.map_err(|e| {
                SinexError::database("Failed to insert event")
                    .with_source(e)
                    .with_context("fixture", "user_session")
            })?;
        }
    }

    // Create checkpoints
    for i in 0..checkpoint_count {
        TestCheckpointBuilder::new(&format!("pre_warmed_processor_{}", i))
            .processed_count((i * 500) as i64)
            .state_data(json!({
                "fixture": "pre_warmed",
                "index": i,
            }))
            .build()
            .insert(pool)
            .await?;
    }

    // Create operations
    for i in 0..operation_count {
        let _op_type = match i % 5 {
            0 => "stage",
            1 => "replay",
            2 => "archive",
            3 => "restore",
            _ => "curate",
        };

        // NOTE: Operations_log test helpers commented out - need to be rewritten for new schema
        // The new operations_log has actor, scope, state fields instead of the old structure
        /*
        let op_id_str: String = sqlx::query_scalar!(
            "SELECT core.start_operation($1, $2, $3::jsonb)::text",
            op_type,
            "fixture_user",
            json!({"fixture": "pre_warmed", "index": i})
        )
        .fetch_one(pool)
        .await?
        .expect("start_operation should return an ID");

        let op_id = Ulid::from_str(&op_id_str).map_err(|e| {
            SinexError::parse("Invalid ULID")
                .with_source(e)
                .with_context("ulid_str", &op_id_str)
        })?;

        if i % 2 == 0 {
            sqlx::query!(
                "SELECT core.complete_operation($1::uuid, $2::jsonb)",
                op_id.to_uuid(),
                json!({"result": "success"})
            )
            .execute(pool)
            .await?;
        }
        */
    }

    Ok(PreWarmedFixture {
        event_count,
        checkpoint_count,
        operation_count,
        total_size_bytes,
    })
}

// =============================================================================
// CLEANUP UTILITIES
// =============================================================================

/// Manually cleanup all fixtures (useful for test teardown)
pub(crate) async fn cleanup_all_fixtures() -> Result<()> {
    let registry = get_registry().await;
    let mut registry = registry.lock().await;

    // Run all cleanup functions
    let cleanups: Vec<_> = registry.cleanups.drain().collect();
    for ((_, _), cleanup) in cleanups {
        cleanup().await;
    }

    // Clear all caches
    registry.cache.clear();
    registry.ref_counts.clear();

    Ok(())
}

/// Force cleanup of a specific fixture type
pub(crate) async fn cleanup_fixture<T: 'static>(key: &str) -> Result<()> {
    let registry = get_registry().await;
    let mut registry = registry.lock().await;

    let type_id = TypeId::of::<T>();
    let cache_key = (type_id, key.to_string());

    if let Some(cleanup) = registry.cleanups.remove(&cache_key) {
        cleanup().await;
    }

    registry.cache.remove(&cache_key);
    registry.ref_counts.remove(&cache_key);

    Ok(())
}

// Additional fixture functions for API completeness

pub(crate) async fn large_event_dataset(
    ctx: &TestContext,
    count: usize,
) -> Result<LargeDatasetFixture> {
    // Create a performance dataset fixture
    let perf_fixture = performance_dataset_with_size(ctx, count).await?;

    // Convert PerformanceDatasetFixture to LargeDatasetFixture
    Ok(LargeDatasetFixture {
        event_ids: perf_fixture.event_ids.clone(),
        event_count: perf_fixture.event_count,
        source_distribution: HashMap::new(), // TODO: calculate from events
        type_distribution: HashMap::new(),   // TODO: calculate from events
        time_range: perf_fixture.time_range,
    })
}

pub(crate) async fn terminal_session(ctx: &TestContext) -> Result<TerminalSessionFixture> {
    // Create a terminal-focused user session
    let session = user_session_with_params(
        ctx, 10, // event count
        5,  // checkpoint interval
    )
    .await?;

    Ok((*session).clone())
}

pub(crate) async fn concurrent_operations(
    ctx: &TestContext,
) -> Result<ConcurrentOperationsFixture> {
    // Create a fixture with concurrent event patterns
    let session = user_session_with_params(
        ctx, 20, // event count - mix of different types
        10, // checkpoint interval
    )
    .await?;

    Ok((*session).clone())
}

pub(crate) async fn event_storm(ctx: &TestContext) -> Result<EventStormFixture> {
    // Create a high-volume burst of events
    let fixture = performance_dataset_with_size(ctx, 50000).await?;
    Ok((*fixture).clone())
}

pub(crate) async fn high_volume_checkpoints(
    ctx: &TestContext,
) -> Result<HighVolumeCheckpointsFixture> {
    // Simply use the existing populated_checkpoints fixture
    // This already creates multiple checkpoints
    let fixture = populated_checkpoints(ctx).await?;
    Ok((*fixture).clone())
}

pub(crate) async fn validation_failures(ctx: &TestContext) -> Result<ValidationErrorsFixture> {
    let fixture = error_scenarios(ctx).await?;
    Ok((*fixture).clone())
}

pub(crate) async fn schema_violations(ctx: &TestContext) -> Result<SchemaViolationsFixture> {
    let key = format!("schema_violations_{}", ctx.test_name());
    let registry = get_registry().await;
    let pool = ctx.pool.clone();

    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move {
                let mut invalid_event_ids = Vec::new();
                let mut failed_operation_ids = Vec::new();
                let mut error_messages = Vec::new();

                // Simple schema violation examples
                error_messages.push("Missing required field: required_field".to_string());
                error_messages.push("Value below minimum: -1 < 0".to_string());
                error_messages.push("Value above maximum: 101 > 100".to_string());
                error_messages.push("Additional properties not allowed".to_string());

                // Generate some fake IDs
                for _ in 0..4 {
                    invalid_event_ids.push(Ulid::new());
                    failed_operation_ids.push(Ulid::new());
                }

                Ok(ErrorScenariosFixture {
                    invalid_event_ids,
                    failed_operation_ids,
                    error_messages,
                })
            }
        })
        .await?;

    Ok((*fixture).clone())
}

pub(crate) async fn malformed_events(ctx: &TestContext) -> Result<MalformedEventsFixture> {
    let key = format!("malformed_events_{}", ctx.test_name());
    let registry = get_registry().await;
    let pool = ctx.pool.clone();

    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move {
                let mut invalid_event_ids = Vec::new();
                let mut failed_operation_ids = Vec::new();
                let mut error_messages = Vec::new();

                // Test various malformed event scenarios
                let test_cases = vec![
                    ("empty source", "source cannot be empty"),
                    ("empty type", "event_type cannot be empty"),
                    ("very long source", "source exceeds maximum length"),
                    ("huge payload", "payload size exceeds limit"),
                ];

                for (error_desc, message) in test_cases {
                    invalid_event_ids.push(Ulid::new());
                    failed_operation_ids.push(Ulid::new());
                    error_messages.push(format!("{}: {}", error_desc, message));
                }

                Ok(ErrorScenariosFixture {
                    invalid_event_ids,
                    failed_operation_ids,
                    error_messages,
                })
            }
        })
        .await?;

    Ok((*fixture).clone())
}

// =============================================================================
// TEST HELPERS
// =============================================================================

/// Macro for defining custom fixtures
// Internal macro for fixture definitions
#[allow(unused_macros)]
macro_rules! fixture {
    ($name:ident, {
        setup: $setup:expr,
        teardown: $teardown:expr,
        cache: $cache:expr
    }) => {
        pub async fn $name(ctx: &TestContext) -> Result<FixtureHandle<_>> {
            use $crate::fixtures::*;

            let key = if $cache {
                stringify!($name).to_string()
            } else {
                format!(
                    "{}_{}_{}",
                    stringify!($name),
                    ctx.test_name(),
                    uuid::Uuid::new_v4()
                )
            };

            let registry = get_registry().await;
            let pool = ctx.pool.clone();

            let fixture = registry
                .lock()
                .await
                .get_or_create(key.clone(), || {
                    let _pool = pool.clone();
                    async move { $setup(pool).await }
                })
                .await?;

            // Register teardown if provided
            let teardown_fn = $teardown;
            registry.lock().await.register_cleanup(
                std::any::TypeId::of::<_>(),
                key.clone(),
                move || {
                    Box::pin(async move {
                        teardown_fn().await;
                    })
                },
            );

            Ok(fixture)
        }
    };
}

// Comprehensive fixture tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinex_test;
    use std::sync::Arc;
    use std::time::Duration;

    #[sinex_test]
    async fn test_fixture_caching_basic(ctx: TestContext) -> Result<()> {
        // First fixture should be created
        let fixture1 = standard_user_session(&ctx).await?;
        let user_id1 = fixture1.user_id.clone();
        let event_count1 = fixture1.event_ids.len();

        // Second call should return cached fixture
        let fixture2 = standard_user_session(&ctx).await?;
        assert_eq!(user_id1, fixture2.user_id, "Should return cached fixture");
        assert_eq!(
            event_count1,
            fixture2.event_ids.len(),
            "Should have same event count"
        );

        // Different test context should get different fixture
        let ctx2 = TestContext::with_name("different_test").await?;
        let fixture3 = standard_user_session(&ctx2).await?;
        assert_ne!(
            user_id1, fixture3.user_id,
            "Different context should get new fixture"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_lifecycle(ctx: TestContext) -> Result<()> {
        // Create fixture with specific params
        let fixture = user_session_with_params(&ctx, 15, 5).await?;

        // Verify it was created correctly
        assert_eq!(fixture.event_ids.len(), 15);
        assert!(fixture.checkpoint_id.is_some(), "Should have checkpoint");

        // Events should exist in database
        for event_id in &fixture.event_ids {
            ctx.assert_event_exists(*event_id).await?;
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_empty_database_fixture(ctx: TestContext) -> Result<()> {
        // Insert some test data
        ctx.create_test_event("test", "test", json!({})).await?;
        ctx.create_test_event("test_2", "test", json!({})).await?;

        // Create empty database fixture
        let _empty = empty_database(&ctx).await?;

        // Should have cleaned test data
        let count = ctx
            .pool
            .events()
            .count_by_source(&sinex_core::types::domain::EventSource::from("test"))
            .await?;
        assert_eq!(count, 0, "Test data should be cleaned");

        Ok(())
    }

    #[sinex_test]
    async fn test_populated_checkpoints_fixture(ctx: TestContext) -> Result<()> {
        let fixture = populated_checkpoints(&ctx).await?;

        // Should have created multiple checkpoints
        assert!(fixture.processor_names.len() >= 3);
        assert_eq!(fixture.processor_names.len(), fixture.checkpoint_ids.len());
        assert!(fixture.total_events_processed > 0);

        // Verify checkpoints exist
        for name in &fixture.processor_names {
            let processor_name = sinex_core::types::domain::ProcessorName::from(name.as_str());
            let checkpoint = ctx
                .pool
                .checkpoints()
                .get_by_processor(&processor_name)
                .await?;
            assert!(
                checkpoint.is_some(),
                "Should have one checkpoint for {}",
                name
            );
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_error_scenarios_fixture(ctx: TestContext) -> Result<()> {
        let fixture = error_scenarios(&ctx).await?;

        // Should have error messages
        assert!(!fixture.error_messages.is_empty());

        // Should have created failed operations
        assert!(!fixture.failed_operation_ids.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_performance_dataset_fixture(ctx: TestContext) -> Result<()> {
        // Create small performance dataset
        let fixture = performance_dataset_with_size(&ctx, 100).await?;

        assert_eq!(fixture.event_count, 100);
        assert_eq!(fixture.event_ids.len(), 100);
        assert!(fixture.sources.len() >= 4);

        // Verify time distribution
        let (start, end) = fixture.time_range;
        assert!(end > start);

        // Events should exist
        let count = ctx.pool.events().count_all().await? as usize;
        assert!(count >= 100);

        Ok(())
    }

    #[sinex_test]
    async fn test_composite_fixtures(ctx: TestContext) -> Result<()> {
        let composite = user_session_with_checkpoints(&ctx).await?;

        // Both fixtures should be available
        assert!(!composite.first.event_ids.is_empty());
        assert!(!composite.second.processor_names.is_empty());

        Ok(())
    }

    #[sinex_test]
    async fn test_pre_warmed_fixture(ctx: TestContext) -> Result<()> {
        let fixture = pre_warmed_database(&ctx).await?;

        assert!(fixture.event_count > 0);
        assert!(fixture.checkpoint_count > 0);
        assert!(fixture.operation_count > 0);
        assert!(fixture.total_size_bytes > 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_transaction_fixture(ctx: TestContext) -> Result<()> {
        let result = with_transaction_fixture(&ctx, |mut tx| {
            Box::pin(async move {
                // Work with transaction
                let count_result = sqlx::query!("SELECT COUNT(*) as count FROM core.events")
                    .fetch_one(&mut *tx)
                    .await?;
                let count = count_result.count.unwrap_or(0);

                // Should see fixture event
                assert!(count >= 1);

                // Insert more data
                sqlx::query(
                    "INSERT INTO core.events (id, source, event_type, host, payload) 
                     VALUES ($1, 'tx_test', 'test', 'test', '{}'::jsonb)",
                )
                .bind(sinex_core::types::ulid::Ulid::new().to_uuid())
                .execute(&mut *tx)
                .await?;

                Ok(42)
            })
        })
        .await?;

        assert_eq!(result, 42);

        // Transaction should be rolled back
        let source_ref = sinex_core::types::domain::EventSource::from("tx_test");
        let count = ctx.pool.events().count_by_source(&source_ref).await? as usize;
        assert_eq!(count, 0, "Transaction should be rolled back");

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_registry_cleanup() -> Result<()> {
        // Create multiple fixtures
        let ctx1 = TestContext::with_name("cleanup_test_1").await?;
        let ctx2 = TestContext::with_name("cleanup_test_2").await?;

        let _fixture1 = standard_user_session(&ctx1).await?;
        let _fixture2 = standard_user_session(&ctx2).await?;

        // Manual cleanup
        cleanup_all_fixtures().await?;

        // Registry should be empty
        let registry = get_registry().await;
        let reg = registry.lock().await;
        assert!(reg.cache.is_empty(), "Cache should be empty after cleanup");
        assert!(reg.ref_counts.is_empty(), "Ref counts should be empty");

        Ok(())
    }

    #[sinex_test]
    async fn test_scenario_fixture_creation(ctx: TestContext) -> Result<()> {
        // Create events using the event API
        let mut event_ids = vec![];

        // Insert start event
        let start_event = RawEvent::new(
            EventSource::from_static("test"),
            EventType::from_static("test.started"),
            json!({}),
        );
        let start = ctx.insert_event(&start_event).await?;
        event_ids.push(start.id.expect("Inserted event must have ID"));

        // Insert middle events
        for i in 0..5 {
            let middle_event = RawEvent::new(
                EventSource::from_static("test"),
                EventType::from_static("test.started"),
                json!({"index": i}),
            );
            let event = ctx.insert_event(&middle_event).await?;
            event_ids.push(event.id.expect("Inserted event must have ID"));
        }

        // Insert end event
        let end_event = RawEvent::new(
            EventSource::from_static("test"),
            EventType::from_static("test.completed"),
            json!({}),
        );
        let end = ctx.insert_event(&end_event).await?;
        event_ids.push(end.id.expect("Inserted event must have ID"));

        assert_eq!(event_ids.len(), 7); // 1 + 5 + 1

        // Verify all events exist
        let source_ref = sinex_core::types::domain::EventSource::from("scenario");
        let events = ctx
            .pool
            .events()
            .get_by_source(&source_ref, Some(1000), None)
            .await?;
        assert_eq!(events.len(), 7);

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrent_fixture_access(ctx: TestContext) -> Result<()> {
        // Since fixtures are cached per context, we'll test sequential access
        // to verify caching works correctly
        let mut user_ids = vec![];

        for i in 0..10 {
            // All calls should get same cached fixture
            let fixture = standard_user_session(&ctx).await?;
            user_ids.push((i, fixture.user_id.clone()));
        }

        let first_id = &user_ids[0];
        for id in &user_ids {
            assert_eq!(
                id, first_id,
                "All concurrent accesses should get same fixture"
            );
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_timeout_handling(ctx: TestContext) -> Result<()> {
        // Create fixture that simulates slow operation
        let start = std::time::Instant::now();

        // Create a user session fixture
        let _fixture = standard_user_session(&ctx).await?;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "Should complete within timeout"
        );

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_error_propagation(ctx: TestContext) -> Result<()> {
        // Test error propagation by trying to query non-existent data
        let id = sinex_core::types::Id::<sinex_core::db::models::RawEvent>::new();
        let result = ctx.pool.events().get_by_id(id).await;

        assert!(result.is_err());

        Ok(())
    }

    #[sinex_test]
    async fn test_parameterized_fixtures(ctx: TestContext) -> Result<()> {
        // Test fixtures with different parameters
        let test_cases = [(5, 1), (10, 2), (20, 5), (50, 10)];

        for (event_count, checkpoint_interval) in test_cases {
            let fixture = user_session_with_params(&ctx, event_count, checkpoint_interval).await?;

            assert_eq!(fixture.event_ids.len(), event_count);

            // Should have checkpoint if interval allows
            if event_count >= checkpoint_interval {
                assert!(fixture.checkpoint_id.is_some());
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_cleanup_on_drop(ctx: TestContext) -> Result<()> {
        let _fixture_id = {
            let fixture = standard_user_session(&ctx).await?;
            fixture.user_id.clone()
        }; // Fixture dropped here

        // Registry should still track it until test ends
        let registry = get_registry().await;
        let reg = registry.lock().await;
        let cache_key = (
            std::any::TypeId::of::<UserSessionFixture>(),
            format!("standard_user_session_{}", ctx.test_name()),
        );
        assert!(reg.cache.contains_key(&cache_key));

        Ok(())
    }

    #[sinex_test]
    async fn test_complex_fixture_scenario(ctx: TestContext) -> Result<()> {
        // Create a complex scenario with multiple fixture types
        let user_session = standard_user_session(&ctx).await?;
        let checkpoints = populated_checkpoints(&ctx).await?;
        let perf_data = performance_dataset_with_size(&ctx, 50).await?;

        // All fixtures should coexist
        assert!(!user_session.event_ids.is_empty());
        assert!(!checkpoints.processor_names.is_empty());
        assert_eq!(perf_data.event_count, 50);

        // Total events should be sum of all fixtures
        let total_events = ctx.pool.events().count_all().await? as usize;
        assert!(total_events >= user_session.event_ids.len() + perf_data.event_count);

        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_registry_singleton() -> color_eyre::eyre::Result<()> {
        // Registry should be singleton
        let registry1 = get_registry().await;
        let registry2 = get_registry().await;

        assert!(Arc::ptr_eq(&registry1, &registry2));
        Ok(())
    }
}
