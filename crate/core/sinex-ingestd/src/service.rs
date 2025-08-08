//! Main ingestion service implementation

use crate::{
    config::IngestdConfig,
    proto::{
        ingest_service_server::{IngestService as IngestServiceTrait, IngestServiceServer},
        BatchResponse, EventBatch, HealthRequest, HealthResponse, IngestResponse,
        RawEvent as ProtoRawEvent,
    },
    validator::EventValidator,
    IngestdError, IngestdResult,
};
use ahash::AHashMap;
use async_nats::{jetstream, Client as NatsClient};
use once_cell::sync::Lazy;
use sinex_core::db::models::Event;
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::db::telemetry::telemetry::{SystemTelemetryEmitter, TelemetryAccumulator};
use sinex_core::types::domain::{EventSource, EventType, HostName};
use sinex_core::types::ulid::Ulid;
use sqlx::PgPool;
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

// Shared ingestor version to avoid repeated allocations
static INGESTOR_VERSION: Lazy<String> = Lazy::new(|| "0.4.2".to_string());

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
    event_buffer: Arc<Mutex<Vec<Event>>>,
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
                IngestdError::Connection(format!("Failed to connect to NATS: {}", e))
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
        sinex_db::telemetry::telemetry::set_global_telemetry(accumulator.clone()).await;

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
            .map_err(|e| IngestdError::Service(format!("gRPC server error: {}", e)))?;

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

    /// Flush events to database and NATS (static version for use in tasks)
    async fn flush_events_static(
        event_buffer: &Arc<Mutex<Vec<Event>>>,
        last_flush: &Arc<Mutex<SystemTime>>,
        config: &IngestdConfig,
        db_pool: Option<&PgPool>,
        jetstream: Option<&jetstream::Context>,
        stats: &IngestStats,
        subject_cache: &Arc<SubjectCache>,
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

        // Write to database
        if let Some(pool) = db_pool {
            if let Err(e) = Self::batch_write_to_db(pool, &events).await {
                error!("Failed to write events to database: {}", e);
                stats.db_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // Publish to NATS JetStream
        if let Some(js) = jetstream {
            if let Err(e) = Self::batch_publish_to_nats(js, config, &events, subject_cache).await {
                error!("Failed to publish events to NATS: {}", e);
                stats.nats_errors.fetch_add(1, Ordering::Relaxed);
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

    /// Batch write events to database with unified architecture
    ///
    /// This implements the unified events table architecture:
    /// - All events go to core.events
    /// - Raw events have source_event_ids = NULL, synthesis events have source_event_ids populated
    async fn batch_write_to_db(pool: &PgPool, events: &[Event]) -> IngestdResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Insert all events into core.events table

        for event in events {
            // Events are already in the correct format, just clone and insert
            pool.events().insert(event.clone()).await?;
        }

        debug!("Successfully wrote {} events to core.events", events.len());
        Ok(())
    }

    /// Batch publish events to NATS JetStream
    ///
    /// This implements the NATS JetStream pattern from ADR-009:
    /// All events go to subjects based on their source and event type.
    async fn batch_publish_to_nats(
        js: &jetstream::Context,
        _config: &IngestdConfig,
        events: &[Event],
        subject_cache: &Arc<SubjectCache>,
    ) -> IngestdResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Publish all events to JetStream with appropriate subjects
        for event in events {
            // Create subject based on source and event type
            // Format: events.<source>.<event_type>
            let cache_key = (event.source.to_string(), event.event_type.to_string());

            // Check cache first
            let subject = {
                let mut cache = subject_cache.lock().await;
                if let Some(cached) = cache.get(&cache_key) {
                    cached.clone()
                } else {
                    // Build new subject
                    let subject = Arc::new(format!(
                        "events.{}.{}",
                        cache_key.0.replace('.', "_"),
                        cache_key.1.replace('.', "_")
                    ));
                    cache.insert(cache_key, subject.clone());
                    subject
                }
            };

            // Serialize event data
            let event_data = serde_json::to_vec(event)?;

            // Publish to JetStream with deduplication ID
            let mut headers = async_nats::HeaderMap::new();
            if let Some(id) = &event.id {
                headers.insert("Nats-Msg-Id", id.to_string().as_str());
            }

            let subject_str = subject.to_string();
            js.publish_with_headers(subject_str, headers, event_data.into())
                .await
                .map_err(|e| {
                    IngestdError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to publish to NATS: {}", e),
                    ))
                })?;
        }

        debug!("Published {} events to NATS JetStream", events.len());
        Ok(())
    }

    /// Add event to buffer
    async fn add_event_to_buffer(&self, event: Event) -> IngestdResult<()> {
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

        // Convert proto event to Event
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
    /// Convert protobuf event to Event
    async fn proto_to_event(&self, proto: ProtoRawEvent) -> IngestdResult<Event> {
        // Validate and parse JSON payload
        let payload = sinex_types::validate_json(&proto.payload)
            .map_err(|e| IngestdError::Validation(format!("Invalid JSON payload: {}", e)))?;

        let _blob_id = proto
            .blob_id
            .map(|blob_id_str| {
                Ulid::from_str(&blob_id_str)
                    .map_err(|e| IngestdError::Validation(format!("Invalid blob ID: {}", e)))
            })
            .transpose()?;

        // Look up schema ID from our in-memory cache
        let schema_id = {
            let validator = self.service.validator.lock().await;
            validator
                .get_schema_id(&proto.source, &proto.event_type)
                .and_then(|id_arc| Ulid::from_str(&id_arc).ok())
        };

        let builder = Event::builder()
            .source(EventSource::new(proto.source))
            .event_type(EventType::new(proto.event_type))
            .host(HostName::new(proto.host))
            .payload(payload)
            .ingestor_version(INGESTOR_VERSION.clone());

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
