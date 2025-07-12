//! Main ingestion service implementation

use crate::{
    config::IngestdConfig,
    proto::{
        ingest_service_server::{IngestService as IngestServiceTrait, IngestServiceServer},
        BatchResponse, EventBatch, HealthRequest, HealthResponse, IngestResponse,
        RawEvent as ProtoRawEvent,
    },
    validator::{EventValidator, ValidationStats},
    IngestdError, IngestdResult,
};
use redis::{AsyncCommands, Client as RedisClient};
use sinex_core::RawEvent;
use sqlx::PgPool;
use sinex_ulid::Ulid;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tokio::{
    net::UnixListener,
    sync::Mutex,
    time::{interval, Duration},
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};

/// Main ingestion service
pub struct IngestService {
    config: IngestdConfig,
    db_pool: Option<PgPool>,
    redis_client: Option<RedisClient>,
    validator: Arc<Mutex<EventValidator>>,
    stats: Arc<IngestStats>,
    shutdown_flag: Arc<AtomicBool>,
    event_buffer: Arc<Mutex<Vec<RawEvent>>>,
    last_flush: Arc<Mutex<SystemTime>>,
}

impl IngestService {
    /// Create a new ingestion service
    pub async fn new(config: IngestdConfig) -> IngestdResult<Self> {
        info!("Initializing ingestion service");

        // Initialize database pool
        let db_pool = if config.dry_run {
            None
        } else {
            let pool = config.get_db_options().connect(&config.database_url).await?;
            Some(pool)
        };

        // Initialize Redis client
        let redis_client = if config.dry_run {
            None
        } else {
            let client = RedisClient::open(config.redis_url.as_str())?;
            Some(client)
        };

        // Initialize event validator
        let validator = if let Some(ref pool) = db_pool {
            EventValidator::load_schemas_from_db(pool, config.validate_schemas).await?
        } else {
            EventValidator::new(false)
        };

        let service = Self {
            config,
            db_pool,
            redis_client,
            validator: Arc::new(Mutex::new(validator)),
            stats: Arc::new(IngestStats::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            event_buffer: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(Mutex::new(SystemTime::now())),
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
        let redis_client = self.redis_client.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let stats = self.stats.clone();

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
                                redis_client.as_ref(),
                                &stats,
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
                            redis_client.as_ref(),
                            &stats,
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

    /// Flush events to database and Redis (static version for use in tasks)
    async fn flush_events_static(
        event_buffer: &Arc<Mutex<Vec<RawEvent>>>,
        last_flush: &Arc<Mutex<SystemTime>>,
        config: &IngestdConfig,
        db_pool: Option<&PgPool>,
        redis_client: Option<&RedisClient>,
        stats: &IngestStats,
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
            stats.events_processed.fetch_add(event_count as u64, Ordering::Relaxed);
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

        // Publish to Redis streams
        if let Some(client) = redis_client {
            if let Err(e) = Self::batch_publish_to_redis(client, config, &events).await {
                error!("Failed to publish events to Redis: {}", e);
                stats.redis_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        stats.events_processed.fetch_add(event_count as u64, Ordering::Relaxed);
        stats.batches_processed.fetch_add(1, Ordering::Relaxed);
        *last_flush.lock().await = SystemTime::now();

        debug!("Successfully flushed {} events", event_count);
    }

    /// Batch write events to database with dual-log routing
    /// 
    /// This implements the architectural decision for dual-log database:
    /// - Raw events (source_event_ids is None) go to raw.events
    /// - Synthesis events (source_event_ids is Some) go to synthesis.events
    async fn batch_write_to_db(pool: &PgPool, events: &[RawEvent]) -> IngestdResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = pool.begin().await?;

        // Separate events by type for routing
        let (raw_events, synthesis_events): (Vec<_>, Vec<_>) = events
            .iter()
            .partition(|event| event.is_raw_event());

        // Insert raw events into raw.events table
        for event in &raw_events {
            sqlx::query!(
                r#"
                INSERT INTO raw.events (
                    id, source, event_type, host, payload,
                    payload_schema_id, ts_orig, ingestor_version
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6::uuid, $7, $8
                )
                "#,
                event.id.to_uuid(),
                event.source,
                event.event_type,
                event.host,
                event.payload,
                event.payload_schema_id.map(|id| id.to_uuid()),
                event.ts_orig,
                event.ingestor_version
            )
            .execute(&mut *tx)
            .await?;
        }

        // Insert synthesis events into synthesis.events table
        for event in &synthesis_events {
            let source_raw_event_ids: Vec<sqlx::types::Uuid> = event
                .source_event_ids
                .as_ref()
                .map(|ids| ids.iter().map(|id| id.to_uuid()).collect())
                .unwrap_or_default();

            sqlx::query!(
                r#"
                INSERT INTO synthesis.events (
                    id, source, event_type, host, payload,
                    payload_schema_id, ts_orig, ingestor_version,
                    source_raw_event_ids, source_synthesis_event_ids
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6::uuid, $7, $8, $9::uuid[], $10::uuid[]
                )
                "#,
                event.id.to_uuid(),
                event.source,
                event.event_type,
                event.host,
                event.payload,
                event.payload_schema_id.map(|id| id.to_uuid()),
                event.ts_orig,
                event.ingestor_version,
                &source_raw_event_ids[..],
                &[] as &[sqlx::types::Uuid] // source_synthesis_event_ids - empty for now
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        debug!("Successfully wrote {} raw events and {} synthesis events", 
               raw_events.len(), synthesis_events.len());
        Ok(())
    }

    /// Batch publish events to unified Redis hotlog stream
    /// 
    /// This implements the unified hotlog pattern from the refactoring plan:
    /// All events go to a single "sinex:streams:hotlog" stream for maximum efficiency.
    /// Automata will filter events they care about based on source/event_type.
    async fn batch_publish_to_redis(
        client: &RedisClient,
        _config: &IngestdConfig,
        events: &[RawEvent],
    ) -> IngestdResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut conn = client.get_async_connection().await?;

        // Unified hotlog stream - single stream for all events
        const HOTLOG_STREAM: &str = "sinex:streams:hotlog";

        // Publish all events to the single hotlog stream
        for event in events {
            let event_data = serde_json::to_string(event)?;
            let fields = [
                ("event_id", event.id.to_string()),
                ("source", event.source.clone()),
                ("event_type", event.event_type.clone()),
                ("host", event.host.clone()),
                ("data", event_data),
                ("timestamp", event.ts_ingest.to_rfc3339()),
            ];

            let _: String = conn.xadd(HOTLOG_STREAM, "*", &fields).await?;
        }

        debug!("Published {} events to hotlog stream", events.len());
        Ok(())
    }

    /// Add event to buffer
    async fn add_event_to_buffer(&self, event: RawEvent) -> IngestdResult<()> {
        let mut buffer = self.event_buffer.lock().await;
        buffer.push(event);

        // Check if we should flush immediately
        if buffer.len() >= self.config.batch_size {
            drop(buffer); // Release lock before flushing
            
            Self::flush_events_static(
                &self.event_buffer,
                &self.last_flush,
                &self.config,
                self.db_pool.as_ref(),
                self.redis_client.as_ref(),
                &self.stats,
            ).await;
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
            self.redis_client.as_ref(),
            &self.stats,
        ).await;

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
            redis_client: self.redis_client.clone(),
            validator: self.validator.clone(),
            stats: self.stats.clone(),
            shutdown_flag: self.shutdown_flag.clone(),
            event_buffer: self.event_buffer.clone(),
            last_flush: self.last_flush.clone(),
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
        let raw_event = match self.proto_to_raw_event(proto_event).await {
            Ok(event) => event,
            Err(e) => {
                self.service.stats.validation_errors.fetch_add(1, Ordering::Relaxed);
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
            self.service.stats.validation_errors.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(IngestResponse {
                success: false,
                error: validation_result.error_message(),
                event_id: None,
            }));
        }

        let event_id = raw_event.id.to_string();

        // Add to buffer
        if let Err(e) = self.service.add_event_to_buffer(raw_event).await {
            error!("Failed to add event to buffer: {}", e);
            return Ok(Response::new(IngestResponse {
                success: false,
                error: Some(format!("Internal error: {}", e)),
                event_id: None,
            }));
        }

        self.service.stats.events_received.fetch_add(1, Ordering::Relaxed);

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
            match self.proto_to_raw_event(proto_event).await {
                Ok(raw_event) => {
                    // Validate event
                    let validation_result = {
                        let validator = self.service.validator.lock().await;
                        validator.validate_event(&raw_event)?
                    };

                    if validation_result.should_accept() {
                        let event_id = raw_event.id.to_string();
                        event_ids.push(event_id);

                        if let Err(e) = self.service.add_event_to_buffer(raw_event).await {
                            error!("Failed to add event to buffer: {}", e);
                            failed_count += 1;
                        } else {
                            processed_count += 1;
                        }
                    } else {
                        failed_count += 1;
                        self.service.stats.validation_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    warn!("Failed to convert proto event: {}", e);
                    failed_count += 1;
                    self.service.stats.validation_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        self.service.stats.events_received.fetch_add(processed_count, Ordering::Relaxed);

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
            status: if healthy { "healthy".to_string() } else { "shutting down".to_string() },
            message: None,
        }))
    }
}

impl IngestServiceImpl {
    /// Convert protobuf event to RawEvent
    async fn proto_to_raw_event(&self, proto: ProtoRawEvent) -> IngestdResult<RawEvent> {
        let payload: serde_json::Value = serde_json::from_str(&proto.payload)?;
        
        let blob_id = if let Some(blob_id_str) = proto.blob_id {
            Some(Ulid::from_str(&blob_id_str)
                .map_err(|e| IngestdError::Validation(format!("Invalid blob ID: {}", e)))?)
        } else {
            None
        };

        Ok(RawEvent {
            id: Ulid::new(),
            source: proto.source,
            event_type: proto.event_type,
            host: proto.host,
            payload,
            payload_schema_id: proto.schema_name.as_ref()
                .filter(|s| !s.is_empty())
                .and_then(|s| Ulid::from_str(s).ok()),
            ts_orig: None,
            ingestor_version: Some("0.4.2".to_string()),
            ts_ingest: chrono::Utc::now(),
            source_event_ids: None, // gRPC events are always raw events
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
    redis_errors: AtomicU64,
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
            redis_errors: AtomicU64::new(0),
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
        let redis_errors = self.redis_errors.load(Ordering::Relaxed);

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
            redis_errors = redis_errors,
            events_per_sec = format!("{:.2}", events_per_sec),
            "Ingestion service statistics"
        );
    }
}