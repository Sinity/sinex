//! Main ingestion service implementation

use crate::{
    config::IngestdConfig,
    proto::{
        ingest_service_server::{IngestService as IngestServiceTrait, IngestServiceServer},
        BatchResponse, EventBatch, HealthRequest, HealthResponse, IngestResponse,
        RawEvent as ProtoRawEvent,
    },
    validator::EventValidator,
    IngestdResult, SinexError,
};
use ahash::AHashMap;
use async_nats::{jetstream, Client as NatsClient};
use sinex_core::db::models::{Provenance, RawEvent};
use sinex_core::db::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::telemetry::telemetry::{SystemTelemetryEmitter, TelemetryAccumulator};
use sinex_core::types::domain::{EventSource, EventType, HostName};
use sinex_core::types::{Id, Ulid};
use sqlx::{PgPool, Postgres, Transaction};
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
    sync::{mpsc, Mutex},
    time::{interval, Duration},
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};

// Shared ingestor version as a compile-time constant
const INGESTOR_VERSION: &str = "0.4.2";

// Cache for NATS subject strings to avoid repeated allocations
type SubjectCache = Mutex<AHashMap<(String, String), Arc<String>>>;

/// Main ingestion service
pub struct IngestService {
    config: IngestdConfig,
    db_pool: Option<PgPool>,
    nats_client: Option<NatsClient>,
    jetstream: Option<jetstream::Context>,
    validator: Arc<Mutex<EventValidator>>,
    stats: Arc<IngestStats>,
    shutdown_flag: Arc<AtomicBool>,
    event_buffer: Arc<Mutex<Vec<RawEvent>>>,
    last_flush: Arc<Mutex<SystemTime>>,
    telemetry: Option<Arc<TelemetryAccumulator>>,
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
                SinexError::network(format!("Failed to connect to NATS: {}", e))
                    .with_operation("service.connect_nats")
                    .with_context("nats_url", config.nats_url.clone())
            })?;
            let js = jetstream::new(client.clone());

            // Create or get the events stream
            let stream_config = jetstream::stream::Config {
                name: config.nats_stream_name.clone(),
                subjects: vec!["events.>".to_string()],
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
        let telemetry = None;

        let service = Self {
            config,
            db_pool,
            nats_client,
            jetstream,
            validator: Arc::new(Mutex::new(validator)),
            stats: Arc::new(IngestStats::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            event_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(Mutex::new(SystemTime::now())),
            telemetry,
            subject_cache: Arc::new(Mutex::new(AHashMap::new())),
        };

        info!("Ingestion service initialized");
        Ok(service)
    }

    /// Initialize telemetry system
    async fn initialize_telemetry(&mut self) {
        if self.config.dry_run || self.telemetry.is_some() {
            return;
        }

        // Create a channel for telemetry events
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Clone event buffer for telemetry injection
        let event_buffer = self.event_buffer.clone();

        // Spawn task to inject telemetry events into the main event stream
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let mut buffer = event_buffer.lock().await;
                buffer.push(event);
            }
        });

        let accumulator = TelemetryAccumulator::new("sinex-ingestd")
            .with_event_sender(tx.clone())
            .with_interval(Duration::from_secs(300)); // 5 minutes

        // Set global telemetry
        sinex_core::db::telemetry::telemetry::set_global_telemetry(accumulator.clone()).await;

        // Spawn telemetry emitter
        accumulator.clone().spawn_emitter();

        // Also spawn system telemetry emitter
        let system_emitter = SystemTelemetryEmitter::new(tx);
        system_emitter.spawn_emitter();

        self.telemetry = Some(Arc::new(accumulator));

        info!("Telemetry system initialized");
    }

    /// Run the ingestion service
    pub async fn run(&mut self) -> IngestdResult<()> {
        info!("Starting ingestion service");

        // Initialize telemetry
        self.initialize_telemetry().await;

        // Ensure socket directory exists
        if let Some(parent) = std::path::Path::new(&self.config.socket_path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Remove existing socket file if it exists
        if std::path::Path::new(&self.config.socket_path).exists() {
            tokio::fs::remove_file(&self.config.socket_path).await?;
        }

        // Create Unix Domain Socket listener
        let listener = UnixListener::bind(&self.config.socket_path)?;
        info!("Listening on Unix socket: {}", self.config.socket_path);

        // Set socket permissions (readable/writable by group)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o660);
            std::fs::set_permissions(&self.config.socket_path, perms)?;
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

        // Stats logging task
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        stats.log_stats();
                    }
                    _ = async {
                        loop {
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    } => {
                        break;
                    }
                }
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
                SinexError::service(format!("gRPC server error: {}", e))
                    .with_operation("service.start_grpc_server")
                    .with_context("socket_path", config.socket_path.clone())
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
        let jetstream = self.jetstream.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let stats = self.stats.clone();
        let subject_cache = self.subject_cache.clone();

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
                            Self::flush_events_static(
                                &event_buffer,
                                &last_flush,
                                &config,
                                db_pool.as_ref(),
                                jetstream.as_ref(),
                                &stats,
                                &subject_cache,
                            ).await;
                        }
                    }
                    _ = async {
                        loop {
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    } => {
                        // Final flush on shutdown
                        Self::flush_events_static(
                            &event_buffer,
                            &last_flush,
                            &config,
                            db_pool.as_ref(),
                            jetstream.as_ref(),
                            &stats,
                            &subject_cache,
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
                    _ = async {
                        loop {
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    } => {
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
    async fn process_outbox(pool: &PgPool, js: &jetstream::Context) -> IngestdResult<u32> {
        #[derive(sqlx::FromRow)]
        struct OutboxEntry {
            id: sqlx::types::Uuid,
            event_id: sqlx::types::Uuid,
            subject: String,
            payload: serde_json::Value,
            created_at: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
        }

        // Read pending outbox entries (limit 100 for batching)
        let pending = sqlx::query_as::<_, OutboxEntry>(
            "SELECT id, event_id, subject, payload, created_at FROM core.outbox ORDER BY created_at LIMIT 100"
        )
        .fetch_all(pool)
        .await?;

        if pending.is_empty() {
            return Ok(0);
        }

        let mut processed = 0;
        for entry in pending {
            // Publish to NATS JetStream
            let event_data = serde_json::to_vec(&entry.payload)?;
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(
                "Nats-Msg-Id",
                uuid_to_ulid(entry.event_id).to_string().as_str(),
            );

            match js
                .publish_with_headers(entry.subject, headers, event_data.into())
                .await
            {
                Ok(_) => {
                    // Delete from outbox after successful publish
                    match sqlx::query("DELETE FROM core.outbox WHERE id = $1")
                        .bind(entry.id)
                        .execute(pool)
                        .await
                    {
                        Ok(_) => processed += 1,
                        Err(e) => error!("Failed to delete outbox entry {}: {}", entry.id, e),
                    }
                }
                Err(e) => {
                    error!("Failed to publish outbox entry {} to NATS: {}", entry.id, e);
                    // Keep entry in outbox for retry
                }
            }
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
                        let mut validator_guard = validator.lock().await;
                        if let Err(e) = validator_guard.reload_schemas(&pool).await {
                            warn!("Failed to reload schemas: {}", e);
                        }
                    }
                    _ = async {
                        loop {
                            if shutdown_flag.load(Ordering::Relaxed) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    } => {
                        break;
                    }
                }
            }
        });
    }

    /// Flush events to database using transactional outbox pattern
    async fn flush_events_static(
        event_buffer: &Arc<Mutex<Vec<RawEvent>>>,
        last_flush: &Arc<Mutex<SystemTime>>,
        config: &IngestdConfig,
        db_pool: Option<&PgPool>,
        _jetstream: Option<&jetstream::Context>, // No longer used - outbox processor handles NATS
        stats: &IngestStats,
        _subject_cache: &Arc<SubjectCache>, // No longer used - subjects handled in outbox
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
            if let Err(e) = Self::batch_write_to_db(pool, &events).await {
                error!("Failed to write events to database: {}", e);
                stats.db_errors.fetch_add(1, Ordering::Relaxed);
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
    async fn batch_write_to_db(pool: &PgPool, events: &[RawEvent]) -> IngestdResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Begin transaction for atomicity
        let mut tx = pool.begin().await?;

        // Prepare arrays for UNNEST batch insert
        let mut event_ids = Vec::new();
        let mut sources = Vec::new();
        let mut event_types = Vec::new();
        let mut hosts = Vec::new();
        let mut payloads = Vec::new();
        let mut ts_origs = Vec::new();
        let mut ingestor_versions = Vec::new();
        let mut payload_schema_ids = Vec::new();
        let mut source_event_id_arrays = Vec::new();
        let mut source_material_ids = Vec::new();
        let mut source_material_offset_starts = Vec::new();
        let mut source_material_offset_ends = Vec::new();
        let mut anchor_bytes = Vec::new();
        let mut associated_blob_id_arrays = Vec::new();

        // Outbox entries for NATS publishing
        let mut outbox_entries = Vec::new();

        for event in events {
            // Generate ID if not present
            let event_id = event
                .id
                .as_ref()
                .map(|id| *id.as_ulid())
                .unwrap_or_else(|| Ulid::new());
            let event_uuid = ulid_to_uuid(event_id);

            event_ids.push(event_uuid);
            sources.push(event.source.as_str());
            event_types.push(event.event_type.as_str());
            hosts.push(event.host.as_str());
            payloads.push(&event.payload);
            ts_origs.push(event.ts_orig);
            ingestor_versions.push(event.ingestor_version.as_deref());

            payload_schema_ids.push(event.payload_schema_id.map(ulid_to_uuid));

            // Extract provenance into separate database fields
            let (source_event_ids_opt, source_material_id, offset_start, offset_end) =
                match &event.provenance {
                    Some(Provenance::Events(ids)) => {
                        let uuids: Vec<sqlx::types::Uuid> =
                            ids.iter().map(|id| ulid_to_uuid(*id.as_ulid())).collect();
                        (Some(uuids), None, None, None)
                    }
                    Some(Provenance::Material {
                        id,
                        offset_start,
                        offset_end,
                    }) => (
                        None,
                        Some(ulid_to_uuid(*id.as_ulid())),
                        *offset_start,
                        *offset_end,
                    ),
                    None => (None, None, None, None),
                };

            source_event_id_arrays.push(source_event_ids_opt);
            source_material_ids.push(source_material_id);
            source_material_offset_starts.push(offset_start);
            source_material_offset_ends.push(offset_end);
            anchor_bytes.push(event.anchor_byte);

            let blob_uuids = event
                .associated_blob_ids
                .as_ref()
                .map(|ids| ids.iter().map(|id| ulid_to_uuid(*id)).collect::<Vec<_>>());
            associated_blob_id_arrays.push(blob_uuids);

            // Prepare outbox entry for NATS publishing
            let subject = format!(
                "events.{}.{}",
                event.source.as_str().replace('.', "_"),
                event.event_type.as_str().replace('.', "_")
            );
            outbox_entries.push((event_id, subject, serde_json::to_value(event)?));
        }

        // Batch insert events using UNNEST - use raw query to avoid SQLX type issues
        sqlx::query(
            r#"
            INSERT INTO core.events (
                event_id, source, event_type, host, payload,
                ts_orig, ingestor_version, payload_schema_id, source_event_ids,
                source_material_id, source_material_offset_start, source_material_offset_end,
                anchor_byte, associated_blob_ids,
                payload_schema_name, payload_schema_version, processor_manifest_id
            )
            SELECT * FROM UNNEST(
                $1::ulid[], $2::text[], $3::text[], $4::text[], $5::jsonb[],
                $6::timestamptz[], $7::text[], $8::ulid[], $9::ulid[][],
                $10::ulid[], $11::bigint[], $12::bigint[],
                $13::bigint[], $14::ulid[][],
                $15::text[], $16::text[], $17::ulid[]
            )
            "#,
        )
        .bind(&event_ids)
        .bind(&sources)
        .bind(&event_types)
        .bind(&hosts)
        .bind(&payloads)
        .bind(&ts_origs)
        .bind(&ingestor_versions)
        .bind(&payload_schema_ids)
        .bind(serde_json::to_value(&source_event_id_arrays).unwrap())
        .bind(&source_material_ids)
        .bind(&source_material_offset_starts)
        .bind(&source_material_offset_ends)
        .bind(&anchor_bytes)
        .bind(serde_json::to_value(&associated_blob_id_arrays).unwrap())
        .bind(&vec![None::<&str>; events.len()]) // payload_schema_name
        .bind(&vec![None::<&str>; events.len()]) // payload_schema_version
        .bind(&vec![None::<i32>; events.len()]) // processor_manifest_id
        .execute(&mut *tx)
        .await?;

        // Insert outbox entries for NATS publishing
        for (event_id, subject, payload) in outbox_entries {
            sqlx::query!(
                "INSERT INTO core.outbox (event_id, subject, payload) VALUES ($1::ulid, $2, $3)",
                ulid_to_uuid(event_id) as sqlx::types::Uuid,
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

    /// Add event to buffer
    async fn add_event_to_buffer(&self, event: RawEvent) -> IngestdResult<()> {
        let event_type = event.event_type.clone();
        let start = std::time::Instant::now();

        let mut buffer = self.event_buffer.lock().await;
        buffer.push(event);

        // Record telemetry
        if let Some(ref telemetry) = self.telemetry {
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            telemetry.record_event_processed(&event_type, duration_ms);
        }

        // Check if we should flush immediately
        if buffer.len() >= self.config.batch_size {
            drop(buffer); // Release lock before flushing

            Self::flush_events_static(
                &self.event_buffer,
                &self.last_flush,
                &self.config,
                self.db_pool.as_ref(),
                self.jetstream.as_ref(),
                &self.stats,
                &self.subject_cache,
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
        Self::flush_events_static(
            &self.event_buffer,
            &self.last_flush,
            &self.config,
            self.db_pool.as_ref(),
            self.jetstream.as_ref(),
            &self.stats,
            &self.subject_cache,
        )
        .await;

        // Clean up socket file
        if std::path::Path::new(&self.config.socket_path).exists() {
            if let Err(e) = tokio::fs::remove_file(&self.config.socket_path).await {
                warn!("Failed to remove socket file: {}", e);
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
            telemetry: self.telemetry.clone(),
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
        request: Request<ProtoRawEvent>,
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
                    error: Some(format!("Event conversion failed: {}", e)),
                    event_id: None,
                }));
            }
        };

        // Validate event
        let validation_result = {
            let validator = self.service.validator.lock().await;
            validator.validate_event(&raw_event)?
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
                error: Some(format!("Internal error: {}", e)),
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

        let mut event_ids = Vec::new();
        let mut processed_count = 0;
        let mut failed_count = 0;

        for proto_event in batch.events {
            match self.proto_to_event(proto_event).await {
                Ok(raw_event) => {
                    // Validate event
                    let validation_result = {
                        let validator = self.service.validator.lock().await;
                        validator.validate_event(&raw_event)?
                    };

                    if validation_result.should_accept() {
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
                    } else {
                        failed_count += 1;
                        self.service
                            .stats
                            .validation_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
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

        self.service
            .stats
            .events_received
            .fetch_add(processed_count, Ordering::Relaxed);

        Ok(Response::new(BatchResponse {
            success: failed_count == 0,
            error: if failed_count > 0 {
                Some(format!("{} events failed validation", failed_count))
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
    async fn proto_to_event(&self, proto: ProtoRawEvent) -> IngestdResult<RawEvent> {
        // Validate and parse JSON payload
        let payload = sinex_core::types::validate_json(&proto.payload).map_err(|e| {
            SinexError::validation(format!("Invalid JSON payload: {}", e))
                .with_operation("service.parse_json_payload")
        })?;

        let _blob_id = proto
            .blob_id
            .map(|blob_id_str| {
                Ulid::from_str(&blob_id_str).map_err(|e| {
                    SinexError::validation(format!("Invalid blob ID: {}", e))
                        .with_operation("service.parse_blob_id")
                })
            })
            .transpose()?;

        // Look up schema ID from our in-memory cache
        let source = EventSource::new(proto.source);
        let event_type = EventType::new(proto.event_type);
        let schema_id = {
            let validator = self.service.validator.lock().await;
            validator
                .get_schema_id(&source, &event_type)
                .and_then(|id_arc| Ulid::from_str(&id_arc).ok())
        };

        let builder = RawEvent::builder()
            .source(source)
            .event_type(event_type)
            .host(HostName::new(proto.host))
            .payload(payload)
            .ingestor_version(INGESTOR_VERSION);

        Ok(if let Some(id) = schema_id {
            builder.payload_schema_id(id).build()
        } else {
            builder.build()
        })
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
