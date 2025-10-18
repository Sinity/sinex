#![doc = include_str!("../doc/service.md")]

//! Main ingestion service implementation.

// Local crate imports
use crate::{
    config::IngestdConfig,
    proto::{
        ingest_service_server::{IngestService as IngestServiceTrait, IngestServiceServer},
        BatchResponse, EventBatch, HealthRequest, HealthResponse, IngestResponse,
        RawEvent as ProtoEvent,
    },
    validator::EventValidator,
    IngestdResult, SinexError,
};

// External crates
use ahash::AHashMap;
use async_nats::{jetstream, Client as NatsClient};
use chrono::Utc;
use sinex_core::environment as sinex_environment;
use sinex_core::{
    db::{
        models::{event::EventId, Event, Provenance},
        query_helpers::{ulid_to_uuid, uuid_to_ulid},
    },
    types::{
        domain::{EventSource, EventType, HostName},
        Ulid,
    },
    JsonValue, OffsetKind,
};
use sqlx::PgPool;
use tonic::{transport::Server, Request, Response, Status};

// Standard library and common crates
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tokio::{
    net::UnixListener,
    sync::{Mutex, RwLock},
    time::{interval, Duration},
};
use tracing::{debug, error, info, instrument, warn};

// Shared ingestor version as a compile-time constant
const INGESTOR_VERSION: &str = "0.4.2";

/// Helper function to create a shutdown signal future
async fn shutdown_signal(shutdown_flag: &Arc<AtomicBool>) {
    loop {
        if shutdown_flag.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Cache for NATS subject strings to avoid repeated allocations
#[derive(Debug, Default)]
pub struct SubjectCache {
    cache: Mutex<AHashMap<(String, String), Arc<String>>>,
}

impl SubjectCache {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(AHashMap::new()),
        }
    }

    /// Get or create a cached subject string for the given source and event type
    pub async fn get_subject(&self, source: &str, event_type: &str) -> Arc<String> {
        let key = (source.to_string(), event_type.to_string());

        // Fast path: check if already in cache
        {
            let cache = self.cache.lock().await;
            if let Some(cached) = cache.get(&key) {
                return cached.clone();
            }
        }

        // Slow path: create and cache the subject, namespaced by environment
        let env = sinex_environment();
        let base = format!(
            "events.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        );
        let subject = Arc::new(env.nats_subject(&base));

        let mut cache = self.cache.lock().await;
        // Double-check in case another task inserted while we were waiting
        if let Some(cached) = cache.get(&key) {
            return cached.clone();
        }

        cache.insert(key, subject.clone());
        subject
    }

    /// Get the current cache size for monitoring
    pub async fn len(&self) -> usize {
        self.cache.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// Main ingestion service
pub struct IngestService {
    config: IngestdConfig,
    db_pool: Option<PgPool>,
    nats_client: Option<NatsClient>,
    jetstream: Option<jetstream::Context>,
    validator: Arc<RwLock<EventValidator>>,
    stats: Arc<IngestStats>,
    shutdown_flag: Arc<AtomicBool>,
    event_buffer: Arc<Mutex<Vec<Event<JsonValue>>>>,
    last_flush: Arc<Mutex<SystemTime>>,
    subject_cache: Arc<SubjectCache>,
}

impl IngestService {
    /// Create a new ingestion service
    pub async fn new(config: IngestdConfig) -> IngestdResult<Self> {
        info!("Initializing ingestion service");

        // Initialize database pool
        let db_pool = if config.dry_run {
            None
        } else {
            let pool = config
                .get_db_options()
                .connect(&config.database_url)
                .await?;
            Some(pool)
        };

        // Initialize NATS client and JetStream
        let (nats_client, jetstream) = if config.dry_run {
            (None, None)
        } else {
            let client = async_nats::connect(&config.nats_url).await.map_err(|e| {
                SinexError::network(format!("Failed to connect to NATS: {e}"))
                    .with_operation("service.connect_nats")
                    .with_context("nats_url", config.nats_url.clone())
            })?;
            let js = jetstream::new(client.clone());

            // Create or get the events stream (subjects namespaced by environment)
            let env = sinex_environment();
            let stream_config = jetstream::stream::Config {
                name: config.nats_stream_name.clone(),
                subjects: vec![env.nats_subject("events.>")],
                retention: jetstream::stream::RetentionPolicy::Limits,
                max_messages: 10_000_000,
                max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
                storage: jetstream::stream::StorageType::File,
                num_replicas: 1,
                ..Default::default()
            };

            match js.get_or_create_stream(stream_config).await {
                Ok(_) => info!(
                    "Connected to NATS JetStream stream: {}",
                    config.nats_stream_name
                ),
                Err(e) => error!("Failed to create/get stream: {}", e),
            }

            (Some(client), Some(js))
        };

        // Initialize event validator
        let validator = if let Some(ref pool) = db_pool {
            // First, synchronize schemas from codebase to database
            if !config.dry_run {
                match crate::schema_sync::synchronize_schemas(pool).await {
                    Ok(sync_result) => {
                        info!(
                            discovered = sync_result.discovered,
                            created = sync_result.created,
                            updated = sync_result.updated,
                            unchanged = sync_result.unchanged,
                            "Schema synchronization completed"
                        );
                    }
                    Err(e) => {
                        error!("Failed to synchronize schemas: {}", e);
                        // Continue anyway - we can still use existing schemas
                    }
                }
            }

            EventValidator::load_schemas_from_db(pool, config.validate_schemas).await?
        } else {
            EventValidator::new(false)
        };

        // Initialize telemetry (we'll set up the channel after service is created)

        let service = Self {
            config: config.clone(),
            db_pool,
            nats_client,
            jetstream,
            validator: Arc::new(RwLock::new(validator)),
            stats: Arc::new(IngestStats::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            event_buffer: Arc::new(Mutex::new(Vec::with_capacity(config.batch_size))),
            last_flush: Arc::new(Mutex::new(SystemTime::now())),
            subject_cache: Arc::new(SubjectCache::new()),
        };

        info!("Ingestion service initialized");
        Ok(service)
    }

    /// Run the ingestion service
    pub async fn run(&mut self) -> IngestdResult<()> {
        info!("Starting ingestion service");

        // Ensure socket directory exists
        if let Some(parent) = std::path::Path::new(&self.config.socket_path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Remove existing socket file (use direct remove_file to avoid TOCTOU)
        // This is atomic and handles non-existent files gracefully
        match tokio::fs::remove_file(&self.config.socket_path).await {
            Ok(()) => {
                debug!("Removed existing socket file: {}", self.config.socket_path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("Socket file does not exist: {}", self.config.socket_path);
            }
            Err(e) => {
                warn!(
                    "Failed to remove socket file {}: {}",
                    self.config.socket_path, e
                );
                // Continue anyway - bind will fail if socket is in use
            }
        }

        // Create Unix Domain Socket listener
        let listener = UnixListener::bind(&self.config.socket_path)?;
        info!("Listening on Unix socket: {}", self.config.socket_path);

        // Set socket permissions (readable/writable by group)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o660);
            tokio::fs::set_permissions(&self.config.socket_path, perms).await?;
        }

        // Create gRPC service
        let grpc_service = IngestServiceImpl {
            service: self.clone(),
        };

        // Start background tasks
        let stats = self.stats.clone();
        let shutdown_flag = self.shutdown_flag.clone();

        // Start outbox processor task
        if let Some(ref pool) = self.db_pool {
            if let Some(ref js) = self.jetstream {
                self.start_outbox_processor_task(pool.clone(), js.clone())
                    .await;
            }
        }

        // Stats logging task with panic recovery
        let stats_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        stats.log_stats();
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        break;
                    }
                }
            }
        });

        // Monitor the stats task for panics
        tokio::spawn(async move {
            if let Err(e) = stats_handle.await {
                error!("Stats logging task panicked: {:?}", e);
            }
        });

        // Periodic flush task
        self.start_flush_task().await;

        // Schema reload task
        if let Some(ref pool) = self.db_pool {
            self.start_schema_reload_task(pool.clone()).await;
        }

        // Notify systemd that we're ready
        if let Err(e) = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]) {
            warn!("Failed to notify systemd ready state: {}", e);
        }

        // Serve gRPC requests
        Server::builder()
            .add_service(IngestServiceServer::new(grpc_service))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await
            .map_err(|e| {
                SinexError::service(format!("gRPC server error: {e}"))
                    .with_operation("service.start_grpc_server")
                    .with_context("socket_path", self.config.socket_path.clone())
            })?;

        info!("Ingestion service stopped");
        Ok(())
    }

    /// Start periodic flush task
    async fn start_flush_task(&self) {
        let event_buffer = self.event_buffer.clone();
        let last_flush = self.last_flush.clone();
        let config = self.config.clone();
        let db_pool = self.db_pool.clone();
        // JetStream context not used here; outbox processor handles NATS publishing
        let shutdown_flag = self.shutdown_flag.clone();
        let stats = self.stats.clone();
        let subject_cache = self.subject_cache.clone();
        let validator = self.validator.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check if we should flush
                        let should_flush = {
                            let buffer = event_buffer.lock().await;
                            let last_flush_time = *last_flush.lock().await;

                            buffer.len() >= config.batch_size
                                || (!buffer.is_empty() && last_flush_time.elapsed().unwrap_or_default().as_secs() >= config.batch_timeout_secs)
                        };

                        if should_flush {
                            let validator_guard = validator.read().await;
                            Self::flush_events_static(
                                &event_buffer,
                                &last_flush,
                                &config,
                                db_pool.as_ref(),
                                &stats,
                                Some(&*subject_cache),
                                Some(&*validator_guard),
                                ).await;
                        }
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        // Final flush on shutdown
                        let validator_guard = validator.read().await;
                        Self::flush_events_static(
                            &event_buffer,
                            &last_flush,
                            &config,
                            db_pool.as_ref(),
                            &stats,
                            Some(&*subject_cache),
                            Some(&*validator_guard),
                        ).await;
                        break;
                    }
                }
            }
        });
    }

    /// Start outbox processor task for transactional outbox pattern
    async fn start_outbox_processor_task(&self, pool: PgPool, js: jetstream::Context) {
        let shutdown_flag = self.shutdown_flag.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match Self::process_outbox(&pool, &js).await {
                            Ok(processed) => {
                                if processed > 0 {
                                    debug!("Processed {} outbox entries", processed);
                                }
                            }
                            Err(e) => {
                                error!("Failed to process outbox: {}", e);
                                stats.nats_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        // Final outbox processing on shutdown
                        match Self::process_outbox(&pool, &js).await {
                            Ok(processed) => {
                                if processed > 0 {
                                    info!("Final outbox processing: {} entries", processed);
                                }
                            }
                            Err(e) => error!("Failed final outbox processing: {}", e),
                        }
                        break;
                    }
                }
            }
        });
    }

    /// Process outbox entries: read, publish to NATS, delete
    ///
    /// Optimized version that batches NATS publishes and database operations
    /// for better async performance and reduced latency.
    /// Uses proper transactions to ensure atomicity between outbox reads and deletes.
    async fn process_outbox(pool: &PgPool, js: &jetstream::Context) -> IngestdResult<u32> {
        #[derive(sqlx::FromRow)]
        struct OutboxEntry {
            id: i64,
            event_id: sqlx::types::Uuid,
            subject: String,
            payload: Vec<u8>,
        }

        // Begin transaction for atomic read-and-lock operation
        let mut tx = pool.begin().await?;

        // Read and lock pending outbox entries (limit 100 for batching)
        // Use SELECT FOR UPDATE to prevent concurrent processing of same entries
        let pending = sqlx::query_as::<_, OutboxEntry>(
            "SELECT id, (event_id)::uuid AS event_id, destination as subject, payload
             FROM core.transactional_outbox
             WHERE status = 'pending'
             ORDER BY created_at
             LIMIT 100
             FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await?;

        if pending.is_empty() {
            tx.rollback().await?;
            return Ok(0);
        }

        // Prepare all publish operations concurrently
        let mut publish_futures = Vec::new();
        let mut entry_data = Vec::new();

        for entry in &pending {
            let mut headers = async_nats::HeaderMap::new();
            let msg_id = uuid_to_ulid(entry.event_id).to_string();
            headers.insert("Nats-Msg-Id", msg_id.as_str());

            let publish_future = js.publish_with_headers(
                entry.subject.clone(),
                headers,
                entry.payload.clone().into(),
            );
            publish_futures.push(publish_future);
            entry_data.push((entry.id, entry.subject.clone()));
        }

        // Execute all NATS publishes concurrently
        let publish_results = futures::future::join_all(publish_futures).await;

        // Collect IDs of successfully published entries for transactional deletion
        let mut successful_ids = Vec::new();
        let mut processed = 0;

        for (result, (entry_id, subject)) in publish_results.into_iter().zip(entry_data.into_iter())
        {
            match result {
                Ok(_) => {
                    successful_ids.push(entry_id);
                    processed += 1;
                }
                Err(e) => {
                    error!(
                        "Failed to publish outbox entry {} (subject: {}) to NATS: {}",
                        entry_id, subject, e
                    );
                    // Keep entry in outbox for retry - it will remain locked until tx ends
                }
            }
        }

        // Transactionally delete all successfully published entries
        if !successful_ids.is_empty() {
            match sqlx::query("DELETE FROM core.transactional_outbox WHERE id = ANY($1)")
                .bind(&successful_ids)
                .execute(&mut *tx)
                .await
            {
                Ok(result) => {
                    let deleted_count = result.rows_affected();
                    if deleted_count != successful_ids.len() as u64 {
                        warn!(
                            "Expected to delete {} outbox entries but deleted {}",
                            successful_ids.len(),
                            deleted_count
                        );
                    }
                    // Commit transaction - this makes the deletions atomic
                    tx.commit().await?;
                }
                Err(e) => {
                    error!(
                        "Failed to batch delete {} outbox entries: {}",
                        successful_ids.len(),
                        e
                    );
                    // Rollback transaction on delete failure
                    tx.rollback().await?;
                    return Err(e.into());
                }
            }
        } else {
            // No successful publishes, rollback to release locks
            tx.rollback().await?;
        }

        Ok(processed)
    }

    /// Start schema reload task
    async fn start_schema_reload_task(&self, pool: PgPool) {
        let validator = self.validator.clone();
        let shutdown_flag = self.shutdown_flag.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(300)); // Reload every 5 minutes

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut validator_guard = validator.write().await;
                        if let Err(e) = validator_guard.reload_schemas(&pool).await {
                            warn!("Failed to reload schemas: {}", e);
                        }
                    }
                    _ = shutdown_signal(&shutdown_flag) => {
                        break;
                    }
                }
            }
        });
    }

    /// Flush events to database using transactional outbox pattern
    async fn flush_events_static(
        event_buffer: &Arc<Mutex<Vec<Event<JsonValue>>>>,
        last_flush: &Arc<Mutex<SystemTime>>,
        config: &IngestdConfig,
        db_pool: Option<&PgPool>,
        stats: &IngestStats,
        subject_cache: Option<&SubjectCache>,
        _validator: Option<&EventValidator>,
    ) {
        // Take events from buffer
        let events = {
            let mut buffer = event_buffer.lock().await;
            if buffer.is_empty() {
                return;
            }
            std::mem::take(&mut *buffer)
        };

        let event_count = events.len();
        debug!("Flushing {} events", event_count);

        if config.dry_run {
            info!("DRY RUN: Would flush {} events", event_count);
            stats
                .events_processed
                .fetch_add(event_count as u64, Ordering::Relaxed);
            *last_flush.lock().await = SystemTime::now();
            return;
        }

        // Write to database with transactional outbox pattern
        // This handles both event insertion and outbox entries for NATS publishing
        if let Some(pool) = db_pool {
            if let Err(e) = Self::batch_write_to_db(pool, &events, subject_cache).await {
                error!("Failed to write events to database: {}", e);
                // Note: This is in a static context, so telemetry is not available here
                // Consider refactoring to pass telemetry if needed
                stats.db_errors.fetch_add(1, Ordering::Relaxed);
                let mut buffer = event_buffer.lock().await;
                // Prepend failed events so they are retried on next flush
                let mut requeue = events;
                if requeue.is_empty() {
                    return;
                }
                if buffer.is_empty() {
                    *buffer = requeue;
                } else {
                    requeue.extend(buffer.drain(..));
                    *buffer = requeue;
                }
                return;
            }
        }

        stats
            .events_processed
            .fetch_add(event_count as u64, Ordering::Relaxed);
        stats.batches_processed.fetch_add(1, Ordering::Relaxed);
        *last_flush.lock().await = SystemTime::now();

        debug!("Successfully flushed {} events", event_count);
    }

    /// Batch write events to database using UNNEST for true batching
    ///
    /// This implements:
    /// - True batch insert using UNNEST instead of N+1 pattern
    /// - Transactional outbox pattern: INSERT events and outbox entries in same transaction
    async fn batch_write_to_db(
        pool: &PgPool,
        events: &[Event<JsonValue>],
        subject_cache: Option<&SubjectCache>,
    ) -> IngestdResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Begin transaction for atomicity
        let mut tx = pool.begin().await?;
        let event_count = events.len();
        let mut outbox_entries = Vec::with_capacity(event_count);

        for event in events {
            let mut event = event.clone();
            let event_id_ulid = if let Some(existing_id) = event.id.as_ref() {
                *existing_id.as_ulid()
            } else {
                let new_id = Ulid::new();
                event.id = Some(EventId::from_ulid(new_id));
                new_id
            };

            if event.ts_orig.is_none() {
                event.ts_orig = Some(Utc::now());
            }

            let ts_orig = event.ts_orig.expect("ts_orig ensured above");
            let payload_schema_id = event.payload_schema_id.map(ulid_to_uuid);

            let (
                source_event_ids_db,
                source_material_uuid,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind_db,
            ) = match &event.provenance {
                Provenance::Material {
                    id,
                    anchor_byte,
                    offset_start,
                    offset_end,
                    offset_kind,
                } => (
                    None,
                    Some(ulid_to_uuid(*id.as_ulid())),
                    Some(*anchor_byte),
                    *offset_start,
                    *offset_end,
                    Some(Self::offset_kind_to_str(*offset_kind).to_string()),
                ),
                Provenance::Synthesis {
                    source_event_ids, ..
                } => {
                    let ids = source_event_ids
                        .iter()
                        .map(|id| ulid_to_uuid(*id.as_ulid()))
                        .collect::<Vec<_>>();
                    (Some(ids), None, None, None, None, None)
                }
            };

            let associated_blob_ids_db = event
                .associated_blob_ids
                .as_ref()
                .map(|ids| ids.iter().map(|id| ulid_to_uuid(*id)).collect::<Vec<_>>());

            sqlx::query(
                r#"
                INSERT INTO core.events (
                    id,
                    source,
                    event_type,
                    host,
                    payload,
                    ts_orig,
                    ingestor_version,
                    payload_schema_id,
                    source_event_ids,
                    source_material_id,
                    anchor_byte,
                    offset_start,
                    offset_end,
                    offset_kind,
                    associated_blob_ids
                ) VALUES (
                    ($1::uuid)::ulid,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    ($8::uuid)::ulid,
                    $9::uuid[]::ulid[],
                    ($10::uuid)::ulid,
                    $11,
                    $12,
                    $13,
                    $14,
                    $15::uuid[]::ulid[]
                )
                "#,
            )
            .bind(ulid_to_uuid(event_id_ulid))
            .bind(event.source.as_str())
            .bind(event.event_type.as_str())
            .bind(event.host.as_str())
            .bind(&event.payload)
            .bind(ts_orig)
            .bind(event.ingestor_version.as_deref())
            .bind(payload_schema_id)
            .bind(source_event_ids_db)
            .bind(source_material_uuid)
            .bind(anchor_byte)
            .bind(offset_start)
            .bind(offset_end)
            .bind(offset_kind_db)
            .bind(associated_blob_ids_db)
            .execute(&mut *tx)
            .await?;

            let subject = if let Some(cache) = subject_cache {
                cache
                    .get_subject(event.source.as_str(), event.event_type.as_str())
                    .await
            } else {
                let env = sinex_environment();
                let base = format!(
                    "events.{}.{}",
                    event.source.as_str().replace('.', "_"),
                    event.event_type.as_str().replace('.', "_")
                );
                Arc::new(env.nats_subject(&base))
            };

            let serialized_event = serde_json::to_vec(&event)?;
            outbox_entries.push((event_id_ulid, (*subject).clone(), serialized_event));
        }

        // Insert outbox entries for NATS publishing
        for (event_id, subject, payload) in outbox_entries {
            sqlx::query!(
                r#"
                INSERT INTO core.transactional_outbox (
                    event_id, destination, payload, status, created_at
                ) VALUES (
                    $1::ulid, $2, $3, 'pending', NOW()
                )
                "#,
                event_id as Ulid,
                subject,
                payload
            )
            .execute(&mut *tx)
            .await?;
        }

        // Commit transaction
        tx.commit().await?;

        debug!(
            "Successfully wrote {} events to core.events with outbox entries",
            events.len()
        );
        Ok(())
    }

    fn offset_kind_to_str(kind: OffsetKind) -> &'static str {
        match kind {
            OffsetKind::Byte => "byte",
            OffsetKind::Line => "line",
            OffsetKind::Record => "rowid",
            OffsetKind::Character => "logical",
        }
    }

    /// Add event to buffer
    async fn add_event_to_buffer(&self, event: Event<JsonValue>) -> IngestdResult<()> {
        let _event_type = &event.event_type;
        let _start = std::time::Instant::now();

        let mut buffer = self.event_buffer.lock().await;
        buffer.push(event);

        // Check if we should flush immediately
        if buffer.len() >= self.config.batch_size {
            drop(buffer); // Release lock before flushing

            let validator_guard = self.validator.read().await;
            Self::flush_events_static(
                &self.event_buffer,
                &self.last_flush,
                &self.config,
                self.db_pool.as_ref(),
                &self.stats,
                Some(&self.subject_cache),
                Some(&*validator_guard),
            )
            .await;
        }

        Ok(())
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> IngestdResult<()> {
        info!("Initiating graceful shutdown");

        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Final flush
        let validator_guard = self.validator.read().await;
        Self::flush_events_static(
            &self.event_buffer,
            &self.last_flush,
            &self.config,
            self.db_pool.as_ref(),
            &self.stats,
            Some(&self.subject_cache),
            Some(&*validator_guard),
        )
        .await;

        // Clean up socket file (using direct remove_file to avoid TOCTOU)
        match tokio::fs::remove_file(&self.config.socket_path).await {
            Ok(()) => {
                debug!(
                    "Removed socket file during shutdown: {}",
                    self.config.socket_path
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("Socket file already removed: {}", self.config.socket_path);
            }
            Err(e) => {
                warn!("Failed to remove socket file during shutdown: {}", e);
            }
        }

        // Close database connections
        if let Some(pool) = &self.db_pool {
            pool.close().await;
        }

        info!("Graceful shutdown completed");
        Ok(())
    }
}

impl Clone for IngestService {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            db_pool: self.db_pool.clone(),
            nats_client: self.nats_client.clone(),
            jetstream: self.jetstream.clone(),
            validator: self.validator.clone(),
            stats: self.stats.clone(),
            shutdown_flag: self.shutdown_flag.clone(),
            event_buffer: self.event_buffer.clone(),
            last_flush: self.last_flush.clone(),
            subject_cache: self.subject_cache.clone(),
        }
    }
}

/// gRPC service implementation
#[derive(Clone)]
struct IngestServiceImpl {
    service: IngestService,
}

#[tonic::async_trait]
impl IngestServiceTrait for IngestServiceImpl {
    #[instrument(skip(self, request))]
    async fn ingest_event(
        &self,
        request: Request<ProtoEvent>,
    ) -> Result<Response<IngestResponse>, Status> {
        let proto_event = request.into_inner();

        // Convert proto event to RawEvent
        let raw_event = match self.proto_to_event(proto_event).await {
            Ok(event) => event,
            Err(e) => {
                self.service
                    .stats
                    .validation_errors
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Response::new(IngestResponse {
                    success: false,
                    error: Some(format!("Event conversion failed: {e}")),
                    event_id: None,
                }));
            }
        };

        // Validate event
        let validation_result = {
            let validator = self.service.validator.read().await;
            match validator.validate_event(&raw_event) {
                Ok(v) => v,
                Err(e) => return Err(crate::sinex_error_to_status(*e)),
            }
        };

        if !validation_result.should_accept() {
            self.service
                .stats
                .validation_errors
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(IngestResponse {
                success: false,
                error: validation_result.error_message(),
                event_id: None,
            }));
        }

        let event_id = raw_event
            .id
            .as_ref()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".into());

        // Add to buffer
        if let Err(e) = self.service.add_event_to_buffer(raw_event).await {
            error!("Failed to add event to buffer: {}", e);
            return Ok(Response::new(IngestResponse {
                success: false,
                error: Some(format!("Internal error: {e}")),
                event_id: None,
            }));
        }

        self.service
            .stats
            .events_received
            .fetch_add(1, Ordering::Relaxed);

        Ok(Response::new(IngestResponse {
            success: true,
            error: None,
            event_id: Some(event_id),
        }))
    }

    #[instrument(skip(self, request))]
    async fn ingest_batch(
        &self,
        request: Request<EventBatch>,
    ) -> Result<Response<BatchResponse>, Status> {
        let batch = request.into_inner();
        let event_count = batch.events.len();

        if event_count == 0 {
            return Ok(Response::new(BatchResponse {
                success: true,
                error: None,
                event_ids: vec![],
                processed_count: 0,
                failed_count: 0,
            }));
        }

        // Process events in parallel batches for better async performance
        let mut event_ids = Vec::with_capacity(event_count);
        let mut processed_count = 0;
        let mut failed_count = 0;

        // Process up to 20 events concurrently to balance throughput and resource usage
        let batch_size = std::cmp::min(event_count, 20);

        for chunk in batch.events.chunks(batch_size) {
            // Convert all proto events to raw events concurrently
            let conversion_futures: Vec<_> = chunk
                .iter()
                .map(|proto_event| self.proto_to_event(proto_event.clone()))
                .collect();

            let conversion_results = futures::future::join_all(conversion_futures).await;

            // Validate all successfully converted events concurrently
            let mut validation_futures = Vec::new();
            let mut raw_events = Vec::new();

            for result in conversion_results {
                match result {
                    Ok(raw_event) => {
                        raw_events.push(raw_event.clone());
                        let validator = self.service.validator.clone();
                        validation_futures.push(async move {
                            let validator_guard = validator.read().await;
                            validator_guard.validate_event(&raw_event)
                        });
                    }
                    Err(e) => {
                        warn!("Failed to convert proto event: {}", e);
                        failed_count += 1;
                        self.service
                            .stats
                            .validation_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            if !validation_futures.is_empty() {
                let validation_results = futures::future::join_all(validation_futures).await;

                // Process validation results and add valid events to buffer
                for (raw_event, validation_result) in
                    raw_events.into_iter().zip(validation_results.into_iter())
                {
                    match validation_result {
                        Ok(result) if result.should_accept() => {
                            let event_id = raw_event
                                .id
                                .as_ref()
                                .map(|id| id.to_string())
                                .unwrap_or_else(|| "unknown".into());
                            event_ids.push(event_id);

                            if let Err(e) = self.service.add_event_to_buffer(raw_event).await {
                                error!("Failed to add event to buffer: {}", e);
                                failed_count += 1;
                            } else {
                                processed_count += 1;
                            }
                        }
                        Ok(_) => {
                            failed_count += 1;
                            self.service
                                .stats
                                .validation_errors
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            error!("Failed to validate event: {}", e);
                            failed_count += 1;
                            self.service
                                .stats
                                .validation_errors
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }

        self.service
            .stats
            .events_received
            .fetch_add(processed_count, Ordering::Relaxed);

        Ok(Response::new(BatchResponse {
            success: failed_count == 0,
            error: if failed_count > 0 {
                Some(format!("{failed_count} events failed validation"))
            } else {
                None
            },
            event_ids,
            processed_count: processed_count as u32,
            failed_count: failed_count as u32,
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let healthy = !self.service.shutdown_flag.load(Ordering::Relaxed);

        Ok(Response::new(HealthResponse {
            healthy,
            status: if healthy {
                "healthy".to_string()
            } else {
                "shutting down".to_string()
            },
            message: None,
        }))
    }
}

impl IngestServiceImpl {
    /// Convert protobuf event to RawEvent
    async fn proto_to_event(&self, proto: ProtoEvent) -> IngestdResult<Event<JsonValue>> {
        // Validate and parse JSON payload
        let payload = sinex_core::types::validate_json(&proto.payload).map_err(|e| {
            SinexError::validation(format!("Invalid JSON payload: {e}"))
                .with_operation("service.parse_json_payload")
        })?;

        let _blob_id = proto
            .blob_id
            .map(|blob_id_str| {
                Ulid::from_str(&blob_id_str).map_err(|e| {
                    SinexError::validation(format!("Invalid blob ID: {e}"))
                        .with_operation("service.parse_blob_id")
                })
            })
            .transpose()?;

        // Look up schema ID from our in-memory cache
        let source = EventSource::new(proto.source);
        let event_type = EventType::new(proto.event_type);
        let schema_id = {
            let validator = self.service.validator.read().await;
            validator
                .get_schema_id(&source, &event_type)
                .and_then(|id_arc| Ulid::from_str(&id_arc).ok())
        };

        // Create a dynamic event; provenance is required by type system.
        // Use a synthesized provenance with a bootstrap parent to satisfy invariants.
        let mut event = Event::create(
            source.clone(),
            event_type.clone(),
            payload,
            Provenance::from_synthesis_safe(EventId::new(), vec![]),
        )
        .with_host(HostName::new(proto.host))
        .with_ingestor_version(INGESTOR_VERSION);

        if let Some(id) = schema_id {
            event = event.with_schema_id(id);
        }

        Ok(event)
    }
}

/// Statistics for the ingestion service
#[derive(Debug)]
struct IngestStats {
    events_received: AtomicU64,
    events_processed: AtomicU64,
    batches_processed: AtomicU64,
    validation_errors: AtomicU64,
    db_errors: AtomicU64,
    nats_errors: AtomicU64,
    start_time: SystemTime,
}

impl IngestStats {
    fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            validation_errors: AtomicU64::new(0),
            db_errors: AtomicU64::new(0),
            nats_errors: AtomicU64::new(0),
            start_time: SystemTime::now(),
        }
    }

    fn log_stats(&self) {
        let uptime = self.start_time.elapsed().unwrap_or_default().as_secs();
        let events_received = self.events_received.load(Ordering::Relaxed);
        let events_processed = self.events_processed.load(Ordering::Relaxed);
        let batches_processed = self.batches_processed.load(Ordering::Relaxed);
        let validation_errors = self.validation_errors.load(Ordering::Relaxed);
        let db_errors = self.db_errors.load(Ordering::Relaxed);
        let nats_errors = self.nats_errors.load(Ordering::Relaxed);

        let events_per_sec = if uptime > 0 {
            events_processed as f64 / uptime as f64
        } else {
            0.0
        };

        info!(
            uptime_secs = uptime,
            events_received = events_received,
            events_processed = events_processed,
            batches_processed = batches_processed,
            validation_errors = validation_errors,
            db_errors = db_errors,
            nats_errors = nats_errors,
            events_per_sec = format!("{:.2}", events_per_sec),
            "Ingestion service statistics"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::IngestService;
    use async_nats::jetstream::{
        consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
        stream::{Config as StreamConfig, RetentionPolicy},
    };
    use futures::StreamExt;
    use serde_json::json;
    use sinex_core::types::ulid::Ulid;
    use sinex_test_utils::prelude::*;
    use std::time::Duration;

    #[ignore = "requires local NATS JetStream"]
    #[sinex_test]
    async fn test_process_outbox_publishes_and_cleans_up(
        ctx: TestContext,
    ) -> color_eyre::eyre::Result<()> {
        let client = match async_nats::connect("localhost:4222").await {
            Ok(client) => client,
            Err(e) => {
                eprintln!(
                    "⚠️  Skipping JetStream integration test (failed to connect to NATS: {e})"
                );
                return Ok(());
            }
        };
        let jetstream = async_nats::jetstream::new(client.clone());

        let stream_name = format!("test_outbox_{}", Ulid::new());
        let subject = format!("sinex.test.events.{}", Ulid::new());

        jetstream
            .create_stream(StreamConfig {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::Limits,
                ..Default::default()
            })
            .await?;

        let event_id = Ulid::new();
        let payload_bytes = serde_json::to_vec(&json!({ "hello": "world" }))?;

        sqlx::query!(
            "INSERT INTO core.transactional_outbox (event_id, destination, payload, status, created_at)
             VALUES ($1::ulid, $2, $3, 'pending', NOW())",
            event_id as Ulid,
            subject.clone(),
            payload_bytes.clone()
        )
        .execute(&ctx.pool)
        .await?;

        let processed = IngestService::process_outbox(&ctx.pool, &jetstream).await?;
        assert_eq!(processed, 1);

        let remaining: Option<i64> =
            sqlx::query_scalar!("SELECT COUNT(*) FROM core.transactional_outbox")
                .fetch_one(&ctx.pool)
                .await?;
        assert_eq!(remaining.unwrap_or(0), 0);

        let consumer_name = format!("{}_consumer", stream_name);
        let stream = jetstream
            .get_stream(&stream_name)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("failed to fetch stream: {e}"))?;
        let consumer_config = ConsumerConfig {
            name: Some(consumer_name.clone()),
            durable_name: Some(consumer_name.clone()),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            filter_subject: subject.clone(),
            ..Default::default()
        };

        if stream
            .get_consumer::<ConsumerConfig>(&consumer_name)
            .await
            .is_err()
        {
            stream
                .create_consumer(consumer_config.clone())
                .await
                .map_err(|e| color_eyre::eyre::eyre!("failed to create consumer: {e}"))?;
        }

        let consumer = stream
            .get_consumer::<ConsumerConfig>(&consumer_name)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("failed to get consumer: {e}"))?;

        let mut messages = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(2))
            .messages()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("failed to fetch messages: {e}"))?;

        let message = messages
            .next()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("expected outbox publication"))?
            .map_err(|e| color_eyre::eyre::eyre!("failed to receive message: {e}"))?;

        assert_eq!(message.payload.to_vec(), payload_bytes);
        message
            .ack()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("failed to ack message: {e}"))?;

        jetstream.delete_stream(&stream_name).await?;
        drop(client);

        Ok(())
    }
}
