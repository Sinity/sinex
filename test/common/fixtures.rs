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
// Usage:
// ```rust
// #[sinex_test]
// async fn test_with_fixture(ctx: TestContext) -> TestResult {
//     let session = fixtures::standard_user_session(&ctx).await?;
//     // fixture automatically cleaned up
// }
// ```

use crate::common::prelude::*;
use crate::common::test_context::TestContext;
use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder, BatchEventBuilder};
use crate::common::builders::EventBuilder;
use sinex_events::{EventFactory, event_types, sources};
use sinex_core_utils::ResourceGuard;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
use std::any::{Any, TypeId};
use chrono::{Duration, Utc};
use serde_json::json;
use std::str::FromStr;
use futures::future::BoxFuture;

/// Global fixture registry for sharing fixtures across tests
static FIXTURE_REGISTRY: OnceCell<Arc<Mutex<FixtureRegistry>>> = OnceCell::const_new();

/// Get or create the global fixture registry
async fn get_registry() -> Arc<Mutex<FixtureRegistry>> {
    FIXTURE_REGISTRY
        .get_or_init(|| async {
            Arc::new(Mutex::new(FixtureRegistry::new()))
        })
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

    /// Get or create a cached fixture
    async fn get_or_create<T, F, Fut>(
        &mut self,
        key: String,
        creator: F,
    ) -> Arc<T>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let type_id = TypeId::of::<T>();
        let cache_key = (type_id, key.clone());

        if let Some(cached) = self.cache.get(&cache_key) {
            self.ref_counts.entry(cache_key.clone()).and_modify(|c| *c += 1);
            return cached.clone().downcast::<T>().unwrap();
        }

        // Create new fixture
        let fixture = creator().await.expect("Fixture creation failed");
        let arc_fixture = Arc::new(fixture);
        
        self.cache.insert(cache_key.clone(), arc_fixture.clone() as Arc<dyn Any + Send + Sync>);
        self.ref_counts.insert(cache_key, 1);
        
        arc_fixture
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

/// Type alias for fixture handles using generic ResourceGuard
pub type FixtureHandle<T> = ResourceGuard<T>;

/// Fixture data for a standard user session
#[derive(Debug, Clone)]
pub struct UserSessionFixture {
    pub user_id: String,
    pub session_start: chrono::DateTime<chrono::Utc>,
    pub event_ids: Vec<Ulid>,
    pub checkpoint_id: Option<Ulid>,
}

/// Fixture data for populated checkpoints
#[derive(Debug, Clone)]
pub struct PopulatedCheckpointsFixture {
    pub automaton_names: Vec<String>,
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

/// Builder for parameterized fixtures
pub struct FixtureBuilder<T> {
    params: HashMap<String, serde_json::Value>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FixtureBuilder<T> {
    pub fn new() -> Self {
        Self {
            params: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn param(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn params(&self) -> &HashMap<String, serde_json::Value> {
        &self.params
    }
}

// =============================================================================
// FIXTURE IMPLEMENTATIONS
// =============================================================================

/// Create a standard user session fixture with activity events
pub async fn standard_user_session(ctx: &TestContext) -> AnyhowResult<FixtureHandle<UserSessionFixture>> {
    let key = format!("standard_user_session_{}", ctx.test_name());
    let registry = get_registry().await;
    
    let pool = ctx.pool().clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_user_session_fixture(&pool, 30, 10).await
        }
    }).await;

    Ok(FixtureHandle::new(fixture, key))
}

/// Create a parameterized user session fixture
pub async fn user_session_with_params(
    ctx: &TestContext,
    event_count: usize,
    checkpoint_interval: usize,
) -> AnyhowResult<FixtureHandle<UserSessionFixture>> {
    let key = format!("user_session_{}_{}_{}_{}", ctx.test_name(), event_count, checkpoint_interval, uuid::Uuid::new_v4());
    let registry = get_registry().await;
    
    let pool = ctx.pool().clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_user_session_fixture(&pool, event_count, checkpoint_interval).await
        }
    }).await;

    Ok(FixtureHandle::new(fixture, key))
}

/// Create an empty database fixture (useful for isolation tests)
pub async fn empty_database(ctx: &TestContext) -> AnyhowResult<FixtureHandle<()>> {
    let pool = ctx.pool().clone();
    
    // Create fixture
    let fixture = {
        // Clean any test data
        sqlx::query!("DELETE FROM core.events WHERE source LIKE 'test_%'")
            .execute(&pool)
            .await?;
        sqlx::query!("DELETE FROM core.automaton_checkpoints WHERE automaton_name LIKE 'test_%'")
            .execute(&pool)
            .await?;
        ()
    };
    
    // Create ResourceGuard with cleanup
    let cleanup = |_fixture: ()| async {
        // No cleanup needed for empty database fixture
    };
    
    Ok(ResourceGuard::new(fixture, cleanup))
}

/// Create populated checkpoints fixture
pub async fn populated_checkpoints(ctx: &TestContext) -> AnyhowResult<FixtureHandle<PopulatedCheckpointsFixture>> {
    let key = format!("populated_checkpoints_{}", ctx.test_name());
    let registry = get_registry().await;
    
    let pool = ctx.pool().clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_populated_checkpoints_fixture(&pool).await
        }
    }).await;

    Ok(FixtureHandle::new(fixture, key))
}

/// Create error scenarios fixture
pub async fn error_scenarios(ctx: &TestContext) -> AnyhowResult<FixtureHandle<ErrorScenariosFixture>> {
    let key = format!("error_scenarios_{}", ctx.test_name());
    let registry = get_registry().await;
    
    let pool = ctx.pool().clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_error_scenarios_fixture(&pool).await
        }
    }).await;

    Ok(FixtureHandle::new(fixture, key))
}

/// Create performance dataset fixture
pub async fn performance_dataset(ctx: &TestContext) -> AnyhowResult<FixtureHandle<PerformanceDatasetFixture>> {
    performance_dataset_with_size(ctx, 10000).await
}

/// Create parameterized performance dataset fixture
pub async fn performance_dataset_with_size(
    ctx: &TestContext,
    event_count: usize,
) -> AnyhowResult<FixtureHandle<PerformanceDatasetFixture>> {
    let key = format!("performance_dataset_{}_{}_{}", ctx.test_name(), event_count, uuid::Uuid::new_v4());
    let registry = get_registry().await;
    
    let pool = ctx.pool().clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_performance_dataset_fixture(&pool, event_count).await
        }
    }).await;

    Ok(FixtureHandle::new(fixture, key))
}

// =============================================================================
// FIXTURE CREATION HELPERS
// =============================================================================

async fn create_user_session_fixture(
    pool: &DbPool,
    event_count: usize,
    checkpoint_interval: usize,
) -> AnyhowResult<UserSessionFixture> {
    let user_id = format!("test_user_{}", uuid::Uuid::new_v4());
    let session_start = Utc::now() - Duration::hours(1);
    let mut event_ids = Vec::new();

    // Create filesystem events
    for i in 0..event_count / 3 {
        let event = EventBuilder::filesystem()
            .path(&format!("/home/{}/documents/file_{}.txt", user_id, i))
            .created()
            .build();
        
        let inserted = sinex_db::insert_event_with_validator(pool, &event, None).await?;
        event_ids.push(inserted.id);
    }

    // Create terminal events
    let commands = ["ls -la", "cd ~/projects", "git status", "cargo build", "vim main.rs"];
    for i in 0..event_count / 3 {
        let cmd = commands[i % commands.len()];
        let event = EventBuilder::terminal()
            .command(cmd)
            .success()
            .duration_ms(100 + i as u64 * 10)
            .build();
        
        let inserted = sinex_db::insert_event_with_validator(pool, &event, None).await?;
        event_ids.push(inserted.id);
    }

    // Create clipboard events
    for i in 0..event_count / 3 {
        let event = EventBuilder::clipboard()
            .text(&format!("Clipboard content {}", i))
            .build();
        
        let inserted = sinex_db::insert_event_with_validator(pool, &event, None).await?;
        event_ids.push(inserted.id);
    }

    // Create checkpoint if needed
    let checkpoint_id = if checkpoint_interval > 0 && event_count >= checkpoint_interval {
        let checkpoint_id = Ulid::new();
        TestCheckpointBuilder::new(&format!("test_automaton_{}", user_id))
            .with_processed_count((event_count / checkpoint_interval * checkpoint_interval) as i64)
            .with_last_processed(&event_ids[checkpoint_interval - 1].to_string())
            .with_state(json!({
                "user_id": user_id,
                "session_start": session_start,
                "events_processed": event_count / checkpoint_interval * checkpoint_interval
            }))
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

async fn create_populated_checkpoints_fixture(pool: &DbPool) -> AnyhowResult<PopulatedCheckpointsFixture> {
    let automaton_names = vec![
        "health-aggregator".to_string(),
        "command-canonicalizer".to_string(),
        "activity-tracker".to_string(),
    ];
    let mut checkpoint_ids = Vec::new();
    let mut total_events_processed = 0u64;

    for (i, name) in automaton_names.iter().enumerate() {
        let processed_count = 100 * (i + 1) as i64;
        total_events_processed += processed_count as u64;
        
        let checkpoint_id = Ulid::new();
        TestCheckpointBuilder::new(name)
            .with_processed_count(processed_count)
            .with_last_processed(&Ulid::new().to_string())
            .with_state(json!({
                "automaton_name": name,
                "version": "1.0.0",
                "status": "healthy",
                "last_health_check": Utc::now(),
            }))
            .insert(pool)
            .await?;
        
        checkpoint_ids.push(checkpoint_id);
    }

    Ok(PopulatedCheckpointsFixture {
        automaton_names,
        checkpoint_ids,
        total_events_processed,
    })
}

async fn create_error_scenarios_fixture(pool: &DbPool) -> AnyhowResult<ErrorScenariosFixture> {
    let mut invalid_event_ids = Vec::new();
    let mut failed_operation_ids = Vec::new();
    let mut error_messages = Vec::new();

    // Create events that would fail validation
    let invalid_events = vec![
        (EventFactory::new("").create_event("test", json!({})), "Empty source"),
        (EventFactory::new("test").create_event("", json!({})), "Empty event type"),
        (EventFactory::new("test").create_event("test.event", json!(null)), "Null payload"),
    ];

    for (event, error_msg) in invalid_events {
        // Try to insert and capture the error
        match sinex_db::insert_event_with_validator(pool, &event, None).await {
            Ok(inserted) => {
                // If it somehow succeeded, track it for cleanup
                invalid_event_ids.push(inserted.id);
            }
            Err(e) => {
                error_messages.push(format!("{}: {}", error_msg, e));
            }
        }
    }

    // Create failed operations
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

        let op_id = Ulid::from_str(&op_id_str)?;
        
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

    Ok(ErrorScenariosFixture {
        invalid_event_ids,
        failed_operation_ids,
        error_messages,
    })
}

async fn create_performance_dataset_fixture(
    pool: &DbPool,
    event_count: usize,
) -> AnyhowResult<PerformanceDatasetFixture> {
    let start_time = Utc::now() - Duration::days(7);
    let end_time = Utc::now();
    let sources = vec![
        sources::FS.to_string(),
        sources::SHELL_KITTY.to_string(),
        sources::CLIPBOARD.to_string(),
        sources::WM_HYPRLAND.to_string(),
    ];
    
    let event_types = vec![
        event_types::filesystem::FILE_CREATED.to_string(),
        event_types::shell::COMMAND_EXECUTED.to_string(),
        event_types::clipboard::COPIED.to_string(),
        event_types::window_manager::WINDOW_FOCUSED.to_string(),
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
        
        let mut builder = TestEventBuilder::new(source, event_type)
            .with_field("index", json!(i))
            .with_field("data", json!("x".repeat(payload_size)))
            .with_timestamp(start_time + time_step * i as i32);
            
        batch.push(builder.build());
    }

    // Insert in batches for performance
    let chunk_size = 1000;
    for chunk in batch.chunks(chunk_size) {
        for event in chunk {
            let inserted = sinex_db::insert_event_with_validator(pool, event, None).await?;
            event_ids.push(inserted.id);
        }
    }

    Ok(PerformanceDatasetFixture {
        event_count,
        event_ids,
        time_range: (start_time, end_time),
        sources,
    })
}

// =============================================================================
// FIXTURE COMPOSITION
// =============================================================================

/// Composite fixture combining multiple fixtures
pub struct CompositeFixture<A: 'static, B: 'static> {
    pub first: FixtureHandle<A>,
    pub second: FixtureHandle<B>,
}

/// Create a fixture that depends on other fixtures
pub async fn user_session_with_checkpoints(
    ctx: &TestContext,
) -> AnyhowResult<CompositeFixture<UserSessionFixture, PopulatedCheckpointsFixture>> {
    let session = standard_user_session(ctx).await?;
    let checkpoints = populated_checkpoints(ctx).await?;
    
    Ok(CompositeFixture {
        first: session,
        second: checkpoints,
    })
}

// =============================================================================
// TRANSACTION-SCOPED FIXTURES
// =============================================================================

/// Run a test with a transaction-scoped fixture
pub async fn with_transaction_fixture<F, Fut, T>(
    ctx: &TestContext,
    fixture_fn: F,
) -> AnyhowResult<T>
where
    F: FnOnce(sqlx::Transaction<'_, sqlx::Postgres>) -> Fut,
    Fut: std::future::Future<Output = AnyhowResult<T>>,
{
    let mut tx = ctx.pool().begin().await?;
    
    // Create some fixture data in the transaction
    let event = EventBuilder::filesystem()
        .path("/test/transaction/file.txt")
        .created()
        .build();
    use sinex_db::queries::EventQueries;
    let record = EventQueries::insert_event(
        event.source.clone(),
        event.event_type.clone(),
        event.host.clone(),
        event.payload.clone(),
        event.ts_orig,
        event.ingestor_version.clone(),
        event.payload_schema_id,
        event.source_event_ids.clone()
    )
    .execute_tx(&mut tx)
    .await?;
    
    let result = fixture_fn(tx).await?;
    
    // Transaction automatically rolled back on drop
    Ok(result)
}

// =============================================================================
// PERFORMANCE FIXTURES
// =============================================================================

/// Pre-warmed fixture with data already in database
pub struct PreWarmedFixture {
    pub event_count: usize,
    pub checkpoint_count: usize,
    pub operation_count: usize,
    pub total_size_bytes: usize,
}

/// Create a pre-warmed fixture with various data types
pub async fn pre_warmed_database(ctx: &TestContext) -> AnyhowResult<FixtureHandle<PreWarmedFixture>> {
    let key = format!("pre_warmed_database_{}", ctx.test_name());
    let registry = get_registry().await;
    
    let pool = ctx.pool().clone();
    let fixture = registry.lock().await.get_or_create(key.clone(), || {
        let pool = pool.clone();
        async move {
            create_pre_warmed_fixture(&pool).await
        }
    }).await;

    Ok(FixtureHandle::new(fixture, key))
}

async fn create_pre_warmed_fixture(pool: &DbPool) -> AnyhowResult<PreWarmedFixture> {
    let event_count = 5000;
    let checkpoint_count = 10;
    let operation_count = 50;
    let mut total_size_bytes = 0;

    // Create events with various sizes
    let payload_sizes = vec![100, 500, 1000, 5000, 10000];
    let mut batch = Vec::new();
    for i in 0..event_count {
        let payload_size = payload_sizes[i % payload_sizes.len()];
        let event = TestEventBuilder::new("performance_test", "test.event")
            .with_field("index", json!(i))
            .with_field("data", json!("x".repeat(payload_size)))
            .build();
        batch.push(event);
    }

    for chunk in batch.chunks(500) {
        for event in chunk {
            let size = serde_json::to_string(&event.payload)?.len();
            total_size_bytes += size;
            sinex_db::insert_event_with_validator(pool, event, None).await?;
        }
    }

    // Create checkpoints
    for i in 0..checkpoint_count {
        TestCheckpointBuilder::new(&format!("pre_warmed_automaton_{}", i))
            .with_processed_count((i * 500) as i64)
            .with_state(json!({
                "fixture": "pre_warmed",
                "index": i,
            }))
            .insert(pool)
            .await?;
    }

    // Create operations
    for i in 0..operation_count {
        let op_type = match i % 5 {
            0 => "stage",
            1 => "replay",
            2 => "archive",
            3 => "restore",
            _ => "curate",
        };

        let op_id_str: String = sqlx::query_scalar!(
            "SELECT core.start_operation($1, $2, $3::jsonb)::text",
            op_type,
            "fixture_user",
            json!({"fixture": "pre_warmed", "index": i})
        )
        .fetch_one(pool)
        .await?
        .expect("start_operation should return an ID");

        let op_id = Ulid::from_str(&op_id_str)?;
        
        if i % 2 == 0 {
            sqlx::query!(
                "SELECT core.complete_operation($1::uuid, $2::jsonb)",
                op_id.to_uuid(),
                json!({"result": "success"})
            )
            .execute(pool)
            .await?;
        }
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
pub async fn cleanup_all_fixtures() -> AnyhowResult<()> {
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
pub async fn cleanup_fixture<T: 'static>(key: &str) -> AnyhowResult<()> {
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

// =============================================================================
// TEST HELPERS
// =============================================================================

/// Macro for defining custom fixtures
#[macro_export]
macro_rules! fixture {
    ($name:ident, {
        setup: $setup:expr,
        teardown: $teardown:expr,
        cache: $cache:expr
    }) => {
        pub async fn $name(ctx: &TestContext) -> AnyhowResult<FixtureHandle<_>> {
            use $crate::common::fixtures::*;
            
            let key = if $cache {
                stringify!($name).to_string()
            } else {
                format!("{}_{}_{}", stringify!($name), ctx.test_name(), uuid::Uuid::new_v4())
            };
            
            let registry = get_registry().await;
            let pool = ctx.pool().clone();
            
            let fixture = registry.lock().await.get_or_create(key.clone(), || {
                let pool = pool.clone();
                async move { $setup(pool).await }
            }).await;
            
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
            
            Ok(FixtureHandle::new(fixture, key))
        }
    };
}