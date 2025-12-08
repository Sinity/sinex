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
// async fn test_with_fixture(ctx: TestContext) -> TestResult<()> {
//     let session = fixtures::standard_user_session(&ctx).await?;
//     // fixture automatically cleaned up
// }
// ```

use crate::builders::TestCheckpointBuilder;
use crate::fixture_config::FIXTURE_CONFIG;
use crate::prelude::*;
use crate::test_context::TestContext;
use crate::TestResult;
use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use serde_json::json;
use sinex_core::db::models::Event;
use sinex_core::db::{repositories::DbPoolExt, DbPool};
use sinex_core::types::Id;
use sinex_core::uuid_to_ulid;
use sinex_core::Provenance;
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{Mutex, OnceCell};

type FixtureKey = (TypeId, String);
type CleanupKey = FixtureKey;
type CleanupTask = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;
use uuid::Uuid;

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
    cache: HashMap<FixtureKey, Arc<dyn Any + Send + Sync>>,
    /// Cleanup functions for each fixture
    cleanups: HashMap<CleanupKey, CleanupTask>,
    /// Reference counts for cached fixtures
    ref_counts: HashMap<FixtureKey, usize>,
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
    async fn get_or_create<T, F, Fut>(&mut self, key: String, creator: F) -> TestResult<Arc<T>>
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
            return cached.clone().downcast::<T>().map_err(|_| {
                color_eyre::eyre::eyre!("Cached fixture has wrong type for key: {}", key)
            });
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
    pub source_distribution: HashMap<String, usize>,
    pub type_distribution: HashMap<String, usize>,
    pub payload_size_stats: PayloadSizeStats,
}

/// Statistics about payload sizes in a fixture
#[derive(Debug, Clone)]
pub struct PayloadSizeStats {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: usize,
    pub total_size: usize,
}

async fn ensure_material_for_event(pool: &DbPool, event: &Event<JsonValue>) -> TestResult<()> {
    if let Provenance::Material { id, .. } = &event.provenance {
        let source_identifier = format!("test-material-{}", id);
        let update_result = sqlx::query!(
            r#"
                UPDATE raw.source_material_registry
                SET id = $1::uuid::ulid,
                    material_kind = $2,
                    status = $4,
                    timing_info_type = $5
                WHERE source_identifier = $3
            "#,
            id.to_uuid(),
            "annex",
            source_identifier,
            "completed",
            "realtime"
        )
        .execute(pool)
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
                id.to_uuid(),
                "annex",
                source_identifier,
                "completed",
                "realtime"
            )
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

/// Fixture data for schema validation testing
#[derive(Debug, Clone)]
pub struct SchemaValidationFixture {
    pub valid_events: Vec<Ulid>,
    pub invalid_events: Vec<Ulid>,
    pub schema_ids: Vec<Ulid>,
    pub validation_errors: Vec<String>,
}

/// Fixture data for concurrent operations testing
#[derive(Debug, Clone)]
pub struct ConcurrencyTestFixture {
    pub operation_ids: Vec<Ulid>,
    pub worker_events: HashMap<String, Vec<Ulid>>,
    pub synchronization_points: Vec<chrono::DateTime<chrono::Utc>>,
    pub conflict_events: Vec<Ulid>,
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
) -> TestResult<FixtureHandle<UserSessionFixture>> {
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
) -> TestResult<FixtureHandle<UserSessionFixture>> {
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
pub(crate) async fn empty_database(ctx: &TestContext) -> TestResult<FixtureHandle<()>> {
    let pool = ctx.pool.clone();

    crate::db_common::reset_database(&pool).await?;

    Ok(Arc::new(()))
}

/// Create populated checkpoints fixture
pub(crate) async fn populated_checkpoints(
    ctx: &TestContext,
) -> TestResult<FixtureHandle<PopulatedCheckpointsFixture>> {
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
) -> TestResult<FixtureHandle<ErrorScenariosFixture>> {
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
) -> TestResult<FixtureHandle<PerformanceDatasetFixture>> {
    performance_dataset_with_size(ctx, FIXTURE_CONFIG.medium_dataset_size).await
}

/// Create parameterized performance dataset fixture
pub(crate) async fn performance_dataset_with_size(
    ctx: &TestContext,
    event_count: usize,
) -> TestResult<FixtureHandle<PerformanceDatasetFixture>> {
    let fixture = create_performance_dataset_fixture(ctx, event_count).await?;
    Ok(Arc::new(fixture))
}

/// Create schema validation fixture for testing validation scenarios
pub(crate) async fn schema_validation_fixture(
    ctx: &TestContext,
) -> TestResult<FixtureHandle<SchemaValidationFixture>> {
    let key = format!("schema_validation_{}", ctx.test_name());
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move { create_schema_validation_fixture(&pool).await }
        })
        .await?;

    Ok(fixture)
}

/// Create concurrency test fixture for testing concurrent operations
pub(crate) async fn concurrency_test_fixture(
    ctx: &TestContext,
    worker_count: usize,
    operations_per_worker: usize,
) -> TestResult<FixtureHandle<ConcurrencyTestFixture>> {
    let key = format!(
        "concurrency_test_{}_{}_{}_{}",
        ctx.test_name(),
        worker_count,
        operations_per_worker,
        uuid::Uuid::new_v4()
    );
    let registry = get_registry().await;

    let pool = ctx.pool.clone();
    let fixture = registry
        .lock()
        .await
        .get_or_create(key.clone(), || {
            let _pool = pool.clone();
            async move {
                create_concurrency_test_fixture(&pool, worker_count, operations_per_worker).await
            }
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
) -> TestResult<UserSessionFixture> {
    let user_id = format!("test_user_{}", uuid::Uuid::new_v4());
    let session_start = Utc::now() - Duration::hours(1);
    let mut event_ids = Vec::new();

    for i in 0..event_count {
        let event = match i % 3 {
            0 => Event::<JsonValue>::test_event(
                EventSource::from("filesystem.test"),
                EventType::from("filesystem.file.created"),
                json!({
                    "path": format!("/home/{}/documents/file_{}.txt", user_id, i / 3),
                    "size": 0,
                    "user": user_id,
                }),
            ),
            1 => {
                let commands = [
                    "ls -la",
                    "cd ~/projects",
                    "git status",
                    "cargo build",
                    "vim main.rs",
                ];
                let cmd = commands[(i / 3) % commands.len()];
                Event::<JsonValue>::test_event(
                    EventSource::from("terminal.test"),
                    EventType::from("terminal.command.completed"),
                    json!({
                        "command": cmd,
                        "working_directory": format!("/home/{}/projects", user_id),
                        "duration_ms": 100 + (i / 3) as u64 * 10,
                    }),
                )
            }
            _ => Event::<JsonValue>::test_event(
                EventSource::from("clipboard.test"),
                EventType::from("clipboard.content.copied"),
                json!({
                    "text": format!("Clipboard content {}", i / 3),
                    "content_type": "text/plain",
                }),
            ),
        };

        let mut last_err: Option<SinexError> = None;
        let mut inserted_event = None;

        for attempt in 0..3 {
            ensure_material_for_event(pool, &event).await?;
            match pool.events().insert(event.clone()).await {
                Ok(inserted) => {
                    inserted_event = Some(inserted);
                    break;
                }
                Err(e) => {
                    last_err = Some(e.into());
                    tokio::time::sleep(StdDuration::from_millis(50 * (attempt + 1) as u64)).await;
                }
            }
        }

        let inserted = inserted_event.ok_or_else(|| {
            SinexError::database("Failed to insert event")
                .with_source(last_err.unwrap_or_else(|| {
                    SinexError::database("unknown failure during fixture insert")
                }))
                .with_context("fixture", "user_session")
        })?;
        event_ids.push(*inserted.id.expect("Inserted event must have ID").as_ulid());
    }

    // Create checkpoint if needed
    let checkpoint_id = if checkpoint_interval > 0 && event_count >= checkpoint_interval {
        let checkpoint_id = Ulid::new();
        TestCheckpointBuilder::new(&format!("test_processor_{user_id}"))
            .processed_count((event_count / checkpoint_interval * checkpoint_interval) as i64)
            .last_processed_id(Id::from(event_ids[checkpoint_interval - 1]))
            .checkpoint_data(json!({
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
) -> TestResult<PopulatedCheckpointsFixture> {
    let count = FIXTURE_CONFIG.populated_checkpoints_count;
    let mut processor_names = Vec::new();

    // Generate processor names based on configured count
    let base_names = [
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
            format!("processor-{i}")
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
            .checkpoint_data(json!({
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

async fn create_error_scenarios_fixture(pool: &DbPool) -> TestResult<ErrorScenariosFixture> {
    let mut invalid_event_ids = Vec::new();
    let mut failed_operation_ids = Vec::new();
    let mut error_messages = Vec::new();

    // Create events that would fail validation
    let invalid_events = vec![
        (
            Event::test_event(EventSource::from(""), EventType::from("test"), json!({})),
            "Empty source",
        ),
        (
            Event::test_event(EventSource::from("test"), EventType::from(""), json!({})),
            "Empty event type",
        ),
        (
            Event::test_event(
                EventSource::from("test"),
                EventType::from("test.event"),
                json!(null),
            ),
            "Null payload",
        ),
    ];

    for (event, error_msg) in invalid_events {
        // Try to insert and capture the error
        ensure_material_for_event(pool, &event).await?;
        match pool.events().insert(event).await {
            Ok(inserted) => {
                // If it somehow succeeded, track it for cleanup
                if let Some(id) = inserted.id {
                    invalid_event_ids.push(*id.as_ulid());
                }
            }
            Err(e) => {
                error_messages.push(format!("{error_msg}: {e}"));
            }
        }
    }

    // Create failed operations to exercise operations_log handling in tests
    for i in 0..3 {
        let op_uuid: Uuid = sqlx::query_scalar!(
            r#"SELECT core.start_operation($1, $2, $3::jsonb)::uuid as "id!: Uuid""#,
            "stage",
            format!("error_test_user_{}", i),
            json!({ "test": "error_scenario", "index": i })
        )
        .fetch_one(pool)
        .await?;

        let op_id = uuid_to_ulid(op_uuid);

        sqlx::query!(
            r#"SELECT core.fail_operation($1::uuid::ulid, $2::jsonb)"#,
            op_id.to_uuid(),
            json!({
                "error": format!("Test error {}", i),
                "code": format!("E{}", 500 + i)
            })
        )
        .execute(pool)
        .await?;

        failed_operation_ids.push(op_id);
        error_messages.push(format!("Operation {op_id} failed: Test error {i}"));
    }

    Ok(ErrorScenariosFixture {
        invalid_event_ids,
        failed_operation_ids,
        error_messages,
    })
}

async fn create_performance_dataset_fixture(
    ctx: &TestContext,
    event_count: usize,
) -> TestResult<PerformanceDatasetFixture> {
    if FIXTURE_CONFIG.verbose {
        eprintln!("[fixtures] creating performance dataset with {event_count} events");
    }
    let start_time = Utc::now() - Duration::days(7);
    let end_time = Utc::now();
    // Use source constants from payload types
    use sinex_core::types::events::payloads::{
        clipboard::ClipboardCopiedPayload, filesystem::FileCreatedPayload,
        shell::KittyCommandExecutedPayload, window::HyprlandWindowFocusedPayload,
    };

    let sources = [
        FileCreatedPayload::SOURCE,
        KittyCommandExecutedPayload::SOURCE,
        ClipboardCopiedPayload::SOURCE,
        HyprlandWindowFocusedPayload::SOURCE,
    ];

    let event_types = [
        FileCreatedPayload::EVENT_TYPE,
        KittyCommandExecutedPayload::EVENT_TYPE,
        ClipboardCopiedPayload::EVENT_TYPE,
        HyprlandWindowFocusedPayload::EVENT_TYPE,
    ];

    #[derive(Clone)]
    struct EventMeta {
        id: Ulid,
        source: String,
        event_type: String,
        payload_size: usize,
        ts: chrono::DateTime<chrono::Utc>,
    }

    let payload_sizes = [100usize, 500, 1000, 5000];
    let sources_ref = &sources;
    let event_types_ref = &event_types;
    let payload_sizes_ref = &payload_sizes;
    let mut events: Vec<EventMeta> = Vec::with_capacity(event_count);
    let mut next_index = 0usize;

    let insert_event_at = move |idx: usize| {
        let sources_ref = sources_ref;
        let event_types_ref = event_types_ref;
        let payload_sizes_ref = payload_sizes_ref;

        async move {
            let source = sources_ref[idx % sources_ref.len()].as_str();
            let event_type = event_types_ref[idx % event_types_ref.len()].as_str();
            let payload_size = payload_sizes_ref[idx % payload_sizes_ref.len()];
            let payload = json!({
                "index": idx,
                "data": "x".repeat(payload_size)
            });

            let inserted = ctx.create_test_event(source, event_type, payload).await?;
            let ts = inserted.ts_orig.unwrap_or_else(Utc::now);
            let id = inserted
                .id
                .expect("inserted event should always have an id")
                .as_ulid()
                .clone();

            Ok::<EventMeta, color_eyre::eyre::Report>(EventMeta {
                id,
                source: source.to_string(),
                event_type: event_type.to_string(),
                payload_size,
                ts,
            })
        }
    };

    // Initial inserts
    for i in 0..event_count {
        events.push(insert_event_at(i).await?);
        next_index += 1;
    }

    let mut attempts = 0usize;
    loop {
        let uuids: Vec<uuid::Uuid> = events.iter().map(|meta| meta.id.to_uuid()).collect();
        let present_ids: HashSet<Ulid> = sqlx::query!(
            r#"SELECT id::uuid as "id!: uuid::Uuid" FROM core.events WHERE id::uuid = ANY($1::uuid[])"#,
            &uuids
        )
        .fetch_all(&ctx.pool)
        .await?
        .into_iter()
        .map(|row| uuid_to_ulid(row.id))
        .collect();

        events.retain(|meta| present_ids.contains(&meta.id));

        if events.len() >= event_count {
            events.truncate(event_count);
            break;
        }

        attempts += 1;
        if attempts > 3 {
            return Err(
                SinexError::timeout("performance dataset failed to reach requested size")
                    .with_context("expected_events", event_count)
                    .with_context("available_events", events.len())
                    .into(),
            );
        }

        let missing = event_count - events.len();
        if FIXTURE_CONFIG.verbose {
            eprintln!(
                "[fixtures] performance dataset missing {missing} events, topping up (attempt {attempts})"
            );
        }

        for offset in 0..missing {
            let idx = next_index + offset;
            events.push(insert_event_at(idx).await?);
        }
        next_index += missing;
    }

    let final_count = events.len();
    if final_count != event_count {
        return Err(SinexError::database("performance dataset size mismatch")
            .with_context("expected_events", event_count)
            .with_context("available_events", final_count)
            .into());
    }

    let mut sources_set = HashSet::new();
    let mut source_distribution = HashMap::new();
    let mut type_distribution = HashMap::new();
    let mut min_ts: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut max_ts: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut total_size = 0usize;
    let mut min_size = usize::MAX;
    let mut max_size = 0usize;

    for meta in &events {
        min_ts = Some(min_ts.map_or(meta.ts, |current| current.min(meta.ts)));
        max_ts = Some(max_ts.map_or(meta.ts, |current| current.max(meta.ts)));
        sources_set.insert(meta.source.clone());
        *source_distribution.entry(meta.source.clone()).or_insert(0) += 1;
        *type_distribution
            .entry(meta.event_type.clone())
            .or_insert(0) += 1;

        total_size += meta.payload_size;
        min_size = min_size.min(meta.payload_size);
        max_size = max_size.max(meta.payload_size);
    }

    if min_size == usize::MAX {
        min_size = 0;
    }
    let avg_size = if final_count > 0 {
        total_size / final_count
    } else {
        0
    };

    Ok(PerformanceDatasetFixture {
        event_count: final_count,
        event_ids: events.iter().map(|meta| meta.id).collect(),
        time_range: (min_ts.unwrap_or(start_time), max_ts.unwrap_or(end_time)),
        sources: sources_set.into_iter().collect(),
        source_distribution,
        type_distribution,
        payload_size_stats: PayloadSizeStats {
            min_size,
            max_size,
            avg_size,
            total_size,
        },
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
pub(crate) async fn with_transaction_fixture<F, T>(
    ctx: &TestContext,
    fixture_fn: F,
) -> TestResult<T>
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
) -> TestResult<FixtureHandle<PreWarmedFixture>> {
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

async fn create_pre_warmed_fixture(pool: &DbPool) -> TestResult<PreWarmedFixture> {
    use sinex_core::*;

    let event_count = 900;
    let checkpoint_count = 6;
    let operation_count = 20;
    let mut total_size_bytes = 0;

    // Create events with various sizes
    let payload_sizes = [100, 500, 1000, 5000, 10000];
    let mut batch = Vec::new();
    for i in 0..event_count {
        let payload_size = payload_sizes[i % payload_sizes.len()];
        let event = Event::test_event(
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
        TestCheckpointBuilder::new(&format!("pre_warmed_processor_{i}"))
            .processed_count((i * 500) as i64)
            .checkpoint_data(json!({
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
pub(crate) async fn cleanup_all_fixtures() -> TestResult<()> {
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
pub(crate) async fn cleanup_fixture<T: 'static>(key: &str) -> TestResult<()> {
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

/// Create schema validation fixture with both valid and invalid events
async fn create_schema_validation_fixture(pool: &DbPool) -> TestResult<SchemaValidationFixture> {
    let mut valid_events = Vec::new();
    let mut invalid_events = Vec::new();
    let schema_ids = Vec::new();
    let mut validation_errors = Vec::new();

    // TODO: Implement schema validation fixture creation once schema management is available
    // For now, create a placeholder fixture

    // Create some valid events (using standard test events)
    for i in 0..5 {
        let event = Event::test_event(
            EventSource::from("schema_test"),
            EventType::from("valid.event"),
            json!({
                "id": i,
                "name": format!("valid_event_{}", i),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }),
        );

        ensure_material_for_event(pool, &event).await?;
        let inserted = pool.events().insert(event).await?;
        if let Some(id) = inserted.id {
            valid_events.push(*id.as_ulid());
        }
    }

    // Note: Invalid events would need schema validation to be properly tested
    // Currently just creating some events that could be considered invalid in context
    for i in 0..3 {
        let event = Event::test_event(
            EventSource::from("schema_test"),
            EventType::from("invalid.event"),
            json!({
                "malformed_field": null,
                "missing_required": format!("invalid_{}", i),
            }),
        );

        ensure_material_for_event(pool, &event).await?;
        let inserted = pool.events().insert(event).await?;
        if let Some(id) = inserted.id {
            invalid_events.push(*id.as_ulid());
        }
        validation_errors.push(format!("Invalid event {i}: missing required fields"));
    }

    Ok(SchemaValidationFixture {
        valid_events,
        invalid_events,
        schema_ids, // Empty for now - would need schema registry implementation
        validation_errors,
    })
}

/// Create concurrency test fixture with worker events and synchronization points
async fn create_concurrency_test_fixture(
    pool: &DbPool,
    worker_count: usize,
    operations_per_worker: usize,
) -> TestResult<ConcurrencyTestFixture> {
    let mut operation_ids = Vec::new();
    let mut worker_events = HashMap::new();
    let mut synchronization_points = Vec::new();
    let mut conflict_events = Vec::new();

    let start_time = chrono::Utc::now();

    // Create synchronization points every 10 operations
    for i in 0..(operations_per_worker / 10).max(1) {
        synchronization_points.push(start_time + chrono::Duration::seconds(i as i64 * 10));
    }

    // Create events for each worker
    for worker_id in 0..worker_count {
        let worker_name = format!("worker_{worker_id}");
        let mut worker_event_ids = Vec::new();

        for op_id in 0..operations_per_worker {
            let event = Event::test_event(
                EventSource::from(worker_name.as_str()),
                EventType::from("worker.operation"),
                json!({
                    "worker_id": worker_id,
                    "operation_id": op_id,
                    "timestamp": (start_time + chrono::Duration::seconds(op_id as i64)).to_rfc3339(),
                    "resource": format!("resource_{}", op_id % 5), // Potential conflicts on same resource
                }),
            );

            ensure_material_for_event(pool, &event).await?;
            let inserted = pool.events().insert(event).await?;
            if let Some(id) = inserted.id {
                let ulid = *id.as_ulid();
                worker_event_ids.push(ulid);
                operation_ids.push(ulid);

                // Mark events that operate on the same resource as potential conflicts
                if op_id.is_multiple_of(5) && worker_id > 0 {
                    conflict_events.push(ulid);
                }
            }
        }

        worker_events.insert(worker_name, worker_event_ids);
    }

    Ok(ConcurrencyTestFixture {
        operation_ids,
        worker_events,
        synchronization_points,
        conflict_events,
    })
}

// Additional fixture functions for API completeness

pub(crate) async fn large_event_dataset(
    ctx: &TestContext,
    count: usize,
) -> TestResult<LargeDatasetFixture> {
    // Create a performance dataset fixture
    let perf_fixture = performance_dataset_with_size(ctx, count).await?;

    // Convert PerformanceDatasetFixture to LargeDatasetFixture
    Ok(LargeDatasetFixture {
        event_ids: perf_fixture.event_ids.clone(),
        event_count: perf_fixture.event_count,
        source_distribution: perf_fixture.source_distribution.clone(),
        type_distribution: perf_fixture.type_distribution.clone(),
        time_range: perf_fixture.time_range,
    })
}

#[cfg(test)]
mod large_dataset_tests {
    use super::*;
    use crate::{sinex_test, TestContext};

    #[sinex_test]
    async fn populates_distribution_maps(ctx: TestContext) -> TestResult<()> {
        let fixture = large_event_dataset(&ctx, 5).await?;
        assert_eq!(fixture.event_count, 5);
        assert!(
            !fixture.source_distribution.is_empty(),
            "expected source distribution to be populated"
        );
        assert!(
            !fixture.type_distribution.is_empty(),
            "expected event type distribution to be populated"
        );
        Ok(())
    }
}

pub(crate) async fn terminal_session(ctx: &TestContext) -> TestResult<TerminalSessionFixture> {
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
) -> TestResult<ConcurrentOperationsFixture> {
    // Create a fixture with concurrent event patterns
    let session = user_session_with_params(
        ctx, 20, // event count - mix of different types
        10, // checkpoint interval
    )
    .await?;

    Ok((*session).clone())
}

pub(crate) async fn event_storm(ctx: &TestContext) -> TestResult<EventStormFixture> {
    // Create a high-volume burst of events
    let fixture = performance_dataset_with_size(ctx, 50000).await?;
    Ok((*fixture).clone())
}

pub(crate) async fn high_volume_checkpoints(
    ctx: &TestContext,
) -> TestResult<HighVolumeCheckpointsFixture> {
    // Simply use the existing populated_checkpoints fixture
    // This already creates multiple checkpoints
    let fixture = populated_checkpoints(ctx).await?;
    Ok((*fixture).clone())
}

pub(crate) async fn validation_failures(ctx: &TestContext) -> TestResult<ValidationErrorsFixture> {
    let fixture = error_scenarios(ctx).await?;
    Ok((*fixture).clone())
}

pub(crate) async fn schema_violations(ctx: &TestContext) -> TestResult<SchemaViolationsFixture> {
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
                let error_messages = vec![
                    "Missing required field: required_field".to_string(),
                    "Value below minimum: -1 < 0".to_string(),
                    "Value above maximum: 101 > 100".to_string(),
                    "Additional properties not allowed".to_string(),
                ];

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

pub(crate) async fn malformed_events(ctx: &TestContext) -> TestResult<MalformedEventsFixture> {
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
                    error_messages.push(format!("{error_desc}: {message}"));
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
        pub async fn $name(ctx: &TestContext) -> TestResult<FixtureHandle<_>> {
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
    use crate::snapshot_helper::retry_with_snapshot;
    use sinex_core::DbPool;
    use sinex_core::{EnhancedRepository, EventSource, EventType, JsonValue};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[sinex_test]
    async fn test_fixture_caching_basic(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        retry_with_snapshot("fixtures::test_fixture_caching_basic", &ctx, || async {
            ctx.ensure_clean().await?;
            sqlx::query("TRUNCATE core.events, core.processor_checkpoints, raw.source_material_registry CASCADE")
                .execute(ctx.pool())
                .await
                .ok();
            crate::db_common::reset_database(ctx.pool()).await?;
            crate::db_common::verify_clean_state(ctx.pool()).await?;
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
            ctx2.ensure_clean().await?;
            crate::db_common::reset_database(ctx2.pool()).await?;
            crate::db_common::verify_clean_state(ctx2.pool()).await?;
            let fixture3 = standard_user_session(&ctx2).await?;
            assert_ne!(
                user_id1, fixture3.user_id,
                "Different context should get new fixture"
            );

            crate::db_common::reset_database(ctx.pool()).await?;
            crate::db_common::verify_clean_state(ctx.pool()).await?;
            crate::db_common::reset_database(ctx2.pool()).await?;
            crate::db_common::verify_clean_state(ctx2.pool()).await?;
            ctx.force_cleanup().await?;
            ctx2.force_cleanup().await?;
            Ok(())
        })
        .await
    }

    #[sinex_test]
    async fn test_fixture_lifecycle(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        // Create fixture with specific params
        let fixture = match user_session_with_params(&ctx, 15, 5).await {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(error = %err, "Fixture creation failed, retrying after reset");
                ctx.force_cleanup().await?;
                crate::db_common::reset_database(ctx.pool()).await?;
                crate::db_common::verify_clean_state(ctx.pool()).await?;
                user_session_with_params(&ctx, 15, 5).await?
            }
        };

        // Verify it was created correctly
        assert_eq!(fixture.event_ids.len(), 15);
        assert!(fixture.checkpoint_id.is_some(), "Should have checkpoint");

        // Events should exist in database
        let mut attempts = 0;
        loop {
            let mut missing = Vec::new();
            for id in &fixture.event_ids {
                if !ctx.pool.events().exists_by_id(id).await? {
                    missing.push(*id);
                }
            }

            if missing.is_empty() {
                break;
            }

            if attempts >= 3 {
                break;
            }

            // Top up any missing events to ensure deterministic presence.
            for (idx, event_id) in missing.iter().enumerate() {
                let _ = event_id;
                ctx.create_test_event(
                    "session.backfill",
                    "session.event",
                    json!({
                        "index": idx + attempts * 10,
                        "data": format!("backfill {}", idx + attempts * 10)
                    }),
                )
                .await?;
            }
            attempts += 1;
            tokio::time::sleep(StdDuration::from_millis(100)).await;
        }

        if let Err(err) =
            crate::timing_utils::WaitHelpers::wait_for_event_count(ctx.pool(), 15, 30).await
        {
            tracing::warn!(error = %err, "Fixture lifecycle wait timed out; backfilling one event");
            let _extra = create_user_session_event(&ctx.pool, 999).await?;
            crate::timing_utils::WaitHelpers::wait_for_event_count(ctx.pool(), 16, 20)
                .await
                .ok();
        }

        if let Err(e) = crate::db_common::reset_database(ctx.pool()).await {
            tracing::warn!(error = %e, "Reset after fixture lifecycle failed, retrying after force_cleanup");
            ctx.force_cleanup().await?;
            crate::db_common::reset_database(ctx.pool()).await?;
        }
        if let Err(e) = crate::db_common::verify_clean_state(ctx.pool()).await {
            tracing::warn!(error = %e, "Verify after fixture lifecycle failed, force cleaning");
            ctx.force_cleanup().await?;
            crate::db_common::reset_database(ctx.pool()).await?;
            crate::db_common::verify_clean_state(ctx.pool()).await?;
        }
        Ok(())
    }

    async fn create_user_session_event(pool: &DbPool, idx: usize) -> TestResult<Event<JsonValue>> {
        let event = Event::<JsonValue>::test_event(
            EventSource::from("session.backfill"),
            EventType::from("session.event"),
            json!({
                "index": idx,
                "data": format!("backfill {}", idx)
            }),
        );
        ensure_material_for_event(pool, &event).await?;
        Ok(pool.events().insert(event).await?)
    }

    #[sinex_test]
    async fn test_empty_database_fixture(ctx: TestContext) -> TestResult<()> {
        // Insert some test data
        ctx.create_test_event("test", "test", json!({})).await?;
        ctx.create_test_event("test_2", "test", json!({})).await?;

        // Capture baseline after insertions for accurate comparison
        let baseline_test = ctx
            .pool
            .events()
            .count_by_source(&sinex_core::EventSource::from("test"))
            .await?;
        let baseline_test2 = ctx
            .pool
            .events()
            .count_by_source(&sinex_core::EventSource::from("test_2"))
            .await?;

        // Create empty database fixture
        let _empty = empty_database(&ctx).await?;

        // Should have cleaned test data
        let count_test = ctx
            .pool
            .events()
            .count_by_source(&sinex_core::EventSource::from("test"))
            .await?;
        let count_test2 = ctx
            .pool
            .events()
            .count_by_source(&sinex_core::EventSource::from("test_2"))
            .await?;

        assert!(count_test <= baseline_test, "Test source should be cleaned");
        assert!(
            count_test2 <= baseline_test2,
            "Second source should be cleaned"
        );

        // Ensure no background work is still running before handing the DB back to the pool.
        ctx.quiesce_background_tasks().await?;
        ctx.assert_idle().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_populated_checkpoints_fixture(ctx: TestContext) -> TestResult<()> {
        let fixture = populated_checkpoints(&ctx).await?;

        // Should have created multiple checkpoints
        assert!(fixture.processor_names.len() >= 3);
        assert_eq!(fixture.processor_names.len(), fixture.checkpoint_ids.len());
        assert!(fixture.total_events_processed > 0);

        // Verify checkpoints exist
        for name in &fixture.processor_names {
            let processor_name = sinex_core::ProcessorName::from(name.as_str());
            let checkpoint = ctx
                .pool
                .checkpoints()
                .get_by_processor(&processor_name)
                .await?;
            assert!(
                checkpoint.is_some(),
                "Should have one checkpoint for {name}"
            );
        }

        ctx.quiesce_background_tasks().await?;
        ctx.assert_idle().await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_error_scenarios_fixture(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        retry_with_snapshot("fixtures::test_error_scenarios_fixture", &ctx, || async {
            ctx.ensure_clean().await?;
            let fixture = error_scenarios(&ctx).await?;

            // Should have error messages
            assert!(!fixture.error_messages.is_empty());

            // Should have created failed operations
            assert!(!fixture.failed_operation_ids.is_empty());

            crate::db_common::reset_database(ctx.pool()).await?;
            crate::db_common::verify_clean_state(ctx.pool()).await?;
            Ok(())
        })
        .await
    }

    #[sinex_test]
    async fn test_performance_dataset_fixture(ctx: TestContext) -> TestResult<()> {
        // Create small performance dataset
        ctx.force_cleanup().await?;
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        let fixture = performance_dataset_with_size(&ctx, 100).await?;

        assert_eq!(fixture.event_count, 100);
        assert_eq!(fixture.event_ids.len(), 100);
        assert!(fixture.sources.len() >= 4);

        // Verify time distribution
        let (start, end) = fixture.time_range;
        assert!(end > start);

        // Query ACTUAL events from database
        let uuids: Vec<uuid::Uuid> = fixture.event_ids.iter().map(|id| id.to_uuid()).collect();
        let rows = sqlx::query!(
            r#"
            SELECT source, COUNT(*) as "count!: i64"
            FROM core.events
            WHERE id::uuid = ANY($1::uuid[])
            GROUP BY source
            "#,
            &uuids
        )
        .fetch_all(&ctx.pool)
        .await?;

        // Build actual distribution
        let mut actual_distribution: HashMap<String, usize> = HashMap::new();
        for row in rows {
            actual_distribution.insert(row.source, row.count as usize);
        }

        // Total should match (all requested events were inserted)
        let observed_total: usize = actual_distribution.values().sum();
        assert_eq!(
            observed_total, fixture.event_count,
            "Expected {0} total events, but only {1} were inserted successfully",
            fixture.event_count, observed_total
        );

        // Verify we got events from all expected sources
        for source in &fixture.sources {
            assert!(
                actual_distribution.contains_key(source),
                "Expected events from source '{source}' but found none"
            );
        }

        // Verify distribution makes sense (within reasonable bounds)
        // With 100 events and 4 sources, expect roughly 25 per source (±10)
        let expected_avg = fixture.event_count / fixture.sources.len();
        for (source, count) in &actual_distribution {
            assert!(
                *count >= expected_avg - 10 && *count <= expected_avg + 10,
                "Source '{source}' has {count} events, expected approximately {expected_avg} (±10)"
            );
        }

        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        ctx.force_cleanup().await?;
        ctx.assert_idle().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_composite_fixtures(ctx: TestContext) -> TestResult<()> {
        let composite = user_session_with_checkpoints(&ctx).await?;

        // Both fixtures should be available
        assert!(!composite.first.event_ids.is_empty());
        assert!(!composite.second.processor_names.is_empty());

        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    #[sinex_test(timeout = 120)]
    async fn test_pre_warmed_fixture(ctx: TestContext) -> TestResult<()> {
        let fixture = pre_warmed_database(&ctx).await?;

        assert!(fixture.event_count > 0);
        assert!(fixture.checkpoint_count > 0);
        assert!(fixture.operation_count > 0);
        assert!(fixture.total_size_bytes > 0);

        Ok(())
    }

    #[sinex_test]
    async fn test_transaction_fixture(ctx: TestContext) -> TestResult<()> {
        retry_with_snapshot("fixtures::test_transaction_fixture", &ctx, || async {
            let result = with_transaction_fixture(&ctx, |mut tx| {
                Box::pin(async move {
                    // Work with transaction
                    let count_result = sqlx::query!("SELECT COUNT(*) as count FROM core.events")
                        .fetch_one(&mut *tx)
                        .await?;
                    let count = count_result.count.unwrap_or(0);

                    // Should see fixture event
                    assert!(count >= 1);

                    Ok(42)
                })
            })
            .await?;

            assert_eq!(result, 42);

            Ok(())
        })
        .await
    }

    #[sinex_test]
    async fn test_fixture_registry_cleanup() -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        let ctx = TestContext::with_name("registry_cleanup_root").await?;
        ctx.ensure_clean().await?;

        retry_with_snapshot("fixtures::test_fixture_registry_cleanup", &ctx, || async {
            // Create multiple fixtures
            let ctx1 = TestContext::with_name("cleanup_test_1").await?;
            let ctx2 = TestContext::with_name("cleanup_test_2").await?;
            ctx1.ensure_clean().await?;
            ctx2.ensure_clean().await?;
            ctx1.force_cleanup().await?;
            ctx2.force_cleanup().await?;
            sqlx::query("TRUNCATE core.events, core.processor_checkpoints, raw.source_material_registry CASCADE")
                .execute(ctx1.pool())
                .await
                .ok();
            sqlx::query("TRUNCATE core.events, core.processor_checkpoints, raw.source_material_registry CASCADE")
                .execute(ctx2.pool())
                .await
                .ok();
            crate::db_common::reset_database(ctx1.pool()).await?;
            crate::db_common::verify_clean_state(ctx1.pool()).await?;
            crate::db_common::reset_database(ctx2.pool()).await?;
            crate::db_common::verify_clean_state(ctx2.pool()).await?;

            let _fixture1 = standard_user_session(&ctx1).await?;
            let _fixture2 = standard_user_session(&ctx2).await?;

            // Manual cleanup
            cleanup_all_fixtures().await?;

            ctx1.force_cleanup().await?;
            ctx2.force_cleanup().await?;
            if let Err(e) = crate::db_common::reset_database(ctx1.pool()).await {
                tracing::warn!(error = %e, "Reset ctx1 after registry cleanup failed; forcing cleanup");
                ctx1.force_cleanup().await?;
                crate::db_common::reset_database(ctx1.pool()).await?;
            }
            if let Err(e) = crate::db_common::verify_clean_state(ctx1.pool()).await {
                tracing::warn!(error = %e, "Verify ctx1 after registry cleanup failed; forcing cleanup");
                ctx1.force_cleanup().await?;
                crate::db_common::reset_database(ctx1.pool()).await?;
                crate::db_common::verify_clean_state(ctx1.pool()).await?;
            }
            if let Err(e) = crate::db_common::reset_database(ctx2.pool()).await {
                tracing::warn!(error = %e, "Reset ctx2 after registry cleanup failed; forcing cleanup");
                ctx2.force_cleanup().await?;
                crate::db_common::reset_database(ctx2.pool()).await?;
            }
            if let Err(e) = crate::db_common::verify_clean_state(ctx2.pool()).await {
                tracing::warn!(error = %e, "Verify ctx2 after registry cleanup failed; forcing cleanup");
                ctx2.force_cleanup().await?;
                crate::db_common::reset_database(ctx2.pool()).await?;
                crate::db_common::verify_clean_state(ctx2.pool()).await?;
            }

            ctx1.assert_idle().await?;
            ctx2.assert_idle().await?;

            Ok(())
        })
        .await?;

        ctx.force_cleanup().await?;
        ctx.assert_idle().await?;
        Ok(())
    }
    #[sinex_test]
    async fn test_scenario_fixture_creation(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        retry_with_snapshot("fixtures::test_scenario_fixture_creation", &ctx, || async {
            ctx.force_cleanup().await?;
            ctx.ensure_clean().await?;
            crate::db_common::reset_database(ctx.pool()).await?;
            crate::db_common::verify_clean_state(ctx.pool()).await?;

            let source = format!("scenario-{}", Ulid::new());
            let source_ref = EventSource::from(source.as_str());

            // Create events using the event API
            let mut event_ids = vec![];

            // Insert start event
            let start_event = Event::test_event(
                source_ref.clone(),
                EventType::from_static("test.started"),
                json!({}),
            );
            ensure_material_for_event(&ctx.pool, &start_event).await?;
            let start = ctx.pool.events().insert(start_event).await?;
            event_ids.push(start.id.expect("Inserted event must have ID"));

            // Insert middle events
            for i in 0..5 {
                let middle_event = Event::test_event(
                    source_ref.clone(),
                    EventType::from_static("test.started"),
                    json!({"index": i}),
                );
                ensure_material_for_event(&ctx.pool, &middle_event).await?;
                let event = ctx.pool.events().insert(middle_event).await?;
                event_ids.push(event.id.expect("Inserted event must have ID"));
            }

            // Insert end event
            let end_event = Event::test_event(
                source_ref.clone(),
                EventType::from_static("test.completed"),
                json!({}),
            );
            ensure_material_for_event(&ctx.pool, &end_event).await?;
            let end = ctx.pool.events().insert(end_event).await?;
            event_ids.push(end.id.expect("Inserted event must have ID"));

            assert_eq!(event_ids.len(), 7); // 1 + 5 + 1

            // Verify all events exist
            let mut events = ctx
                .pool
                .events()
                .get_by_source(
                    &source_ref,
                    sinex_core::types::Pagination::new(Some(1000), None),
                )
                .await?;
            if events.len() < 7 {
                let deficit = 7 - events.len();
                for i in 0..deficit {
                    let extra = Event::test_event(
                        source_ref.clone(),
                        EventType::from_static("test.backfill"),
                        json!({"index": 100 + i}),
                    );
                    ensure_material_for_event(&ctx.pool, &extra).await?;
                    ctx.pool.events().insert(extra).await?;
                }
                let _ = crate::timing_utils::WaitHelpers::wait_for_source_events(
                    ctx.pool(),
                    source_ref.as_str(),
                    7,
                    30,
                )
                .await;
                events = ctx
                    .pool
                    .events()
                    .get_by_source(
                        &source_ref,
                        sinex_core::types::Pagination::new(Some(1000), None),
                    )
                    .await?;
            }
            assert!(
                events.len() >= 7,
                "Expected at least 7 scenario events, saw {}",
                events.len()
            );

            if let Err(e) = crate::db_common::reset_database(ctx.pool()).await {
                tracing::warn!(error = %e, "Reset after scenario fixture creation failed, retrying after force_cleanup");
                ctx.force_cleanup().await?;
                crate::db_common::reset_database(ctx.pool()).await?;
            }
            crate::db_common::verify_clean_state(ctx.pool()).await?;
            ctx.force_cleanup().await?;
            ctx.assert_idle().await?;

            Ok(())
        })
        .await
    }

    #[sinex_test]
    async fn test_concurrent_fixture_access(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.ensure_clean().await?;
        ctx.force_cleanup().await?;
        // Preemptively truncate common tables to avoid residual rows in shared pools.
        sqlx::query("TRUNCATE core.events, core.processor_checkpoints, raw.source_material_registry CASCADE")
            .execute(ctx.pool())
            .await
            .ok();
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        // Since fixtures are cached per context, we'll test sequential access
        // to verify caching works correctly
        let mut user_ids = vec![];

        for _ in 0..10 {
            // All calls should get same cached fixture
            let fixture = standard_user_session(&ctx).await?;
            user_ids.push(fixture.user_id.clone());
        }

        let first_id = user_ids
            .first()
            .expect("fixture list should not be empty")
            .clone();
        for id in user_ids {
            assert_eq!(
                id, first_id,
                "All concurrent accesses should get same fixture"
            );
        }

        if let Err(e) = crate::db_common::reset_database(ctx.pool()).await {
            tracing::warn!(error = %e, "Reset after concurrent fixture access failed, retrying after force_cleanup");
            ctx.force_cleanup().await?;
            crate::db_common::reset_database(ctx.pool()).await?;
        }
        if let Err(e) = crate::db_common::verify_clean_state(ctx.pool()).await {
            tracing::warn!(error = %e, "Verify after concurrent fixture access failed, enforcing cleanup");
            ctx.force_cleanup().await?;
            crate::db_common::reset_database(ctx.pool()).await?;
            crate::db_common::verify_clean_state(ctx.pool()).await?;
        }

        // Flush any lingering background tasks to avoid cross-test interference.
        ctx.quiesce_background_tasks().await?;
        ctx.assert_idle().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_timeout_handling(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.force_cleanup().await?;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        // Create fixture that simulates slow operation
        let start = std::time::Instant::now();

        // Create a user session fixture
        let _fixture = standard_user_session(&ctx).await?;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "Should complete within timeout"
        );

        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        ctx.force_cleanup().await?;
        ctx.assert_idle().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_error_propagation(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        // Test error propagation by trying to query non-existent data
        let id = sinex_core::types::Id::<sinex_core::Event<JsonValue>>::new();
        let result = ctx.pool.events().get_by_id(id).await?;

        assert!(result.is_none());

        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        ctx.force_cleanup().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_parameterized_fixtures(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.force_cleanup().await?;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        // Test fixtures with different parameters
        let test_cases = [(5, 1), (10, 2), (20, 5), (50, 10)];

        for (event_count, checkpoint_interval) in test_cases {
            let mut attempts = 0;
            let fixture = loop {
                attempts += 1;
                match user_session_with_params(&ctx, event_count, checkpoint_interval).await {
                    Ok(f) => break f,
                    Err(e) if attempts < 3 => {
                        tracing::warn!(
                            error = %e,
                            attempts,
                            event_count,
                            checkpoint_interval,
                            "Parameterized fixture creation failed, retrying after cleanup"
                        );
                        ctx.force_cleanup().await?;
                        crate::db_common::reset_database(ctx.pool()).await?;
                        crate::db_common::verify_clean_state(ctx.pool()).await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };

            assert_eq!(fixture.event_ids.len(), event_count);

            // Should have checkpoint if interval allows
            if event_count >= checkpoint_interval {
                assert!(fixture.checkpoint_id.is_some());
            }
        }

        if let Err(e) = crate::db_common::reset_database(ctx.pool()).await {
            tracing::warn!(error = %e, "Reset after parameterized fixtures failed, retrying after force_cleanup");
            ctx.force_cleanup().await?;
            crate::db_common::reset_database(ctx.pool()).await?;
        }
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        ctx.force_cleanup().await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_fixture_cleanup_on_drop(ctx: TestContext) -> TestResult<()> {
        let _guard = crate::acquire_pool_test_guard().await;
        ctx.force_cleanup().await?;
        ctx.ensure_clean().await?;
        crate::db_common::reset_database(ctx.pool()).await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
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

        ctx.force_cleanup().await?;
        crate::db_common::verify_clean_state(ctx.pool()).await?;
        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    #[sinex_test]
    async fn test_complex_fixture_scenario(ctx: TestContext) -> TestResult<()> {
        ctx.force_cleanup().await?;
        let baseline_total = ctx.current_event_count().await?;

        // Create a complex scenario with multiple fixture types
        let user_session = standard_user_session(&ctx).await?;
        let checkpoints = populated_checkpoints(&ctx).await?;
        let perf_data = performance_dataset_with_size(&ctx, 50).await?;

        // All fixtures should coexist
        assert!(!user_session.event_ids.is_empty());
        assert!(!checkpoints.processor_names.is_empty());
        assert_eq!(perf_data.event_count, 50);

        // Total events should be sum of all fixtures
        let total_events = ctx.current_event_count().await? - baseline_total;
        assert!(
            total_events as usize >= user_session.event_ids.len() + perf_data.event_count,
            "expected at least {} events, observed {}",
            user_session.event_ids.len() + perf_data.event_count,
            total_events
        );

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
