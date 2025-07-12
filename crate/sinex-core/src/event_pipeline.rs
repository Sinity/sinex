/// Multi-stage event processing pipeline architecture
///
/// This module implements a formalized event processing pipeline with distinct stages:
/// 1. Collection - Raw event capture from sources
/// 2. Validation - Schema validation and integrity checks
/// 3. Enrichment - Metadata augmentation and normalization
/// 4. Storage - Persistence to database
/// 5. Distribution - Work queue and downstream processing
///
/// Each stage has clear input/output contracts and error handling.
use crate::{CoreError, EventReceiver, EventSender, JsonValue, RawEvent, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Pipeline configuration
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Buffer size between stages
    pub stage_buffer_size: usize,
    /// Enable detailed stage timing
    pub enable_timing: bool,
    /// Maximum events per batch for bulk operations
    pub batch_size: usize,
    /// Stage timeout configuration
    pub timeouts: StageTimeouts,
}

#[derive(Debug, Clone)]
pub struct StageTimeouts {
    pub validation_timeout_ms: u64,
    pub enrichment_timeout_ms: u64,
    pub storage_timeout_ms: u64,
    pub distribution_timeout_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            stage_buffer_size: 1000,
            enable_timing: true,
            batch_size: 100,
            timeouts: StageTimeouts {
                validation_timeout_ms: 1000,
                enrichment_timeout_ms: 2000,
                storage_timeout_ms: 5000,
                distribution_timeout_ms: 1000,
            },
        }
    }
}

/// Event processing stage metrics
#[derive(Debug, Clone, Default)]
pub struct StageMetrics {
    pub events_processed: u64,
    pub events_dropped: u64,
    pub processing_time_ms: u64,
    pub errors: u64,
}

/// Pipeline stage result
#[derive(Debug)]
pub enum StageResult<T> {
    /// Event processed successfully
    Success(T),
    /// Event dropped (filtered out)
    Dropped(String),
    /// Event failed processing with error
    Failed(CoreError),
}

impl<T> StageResult<T> {
    pub fn is_success(&self) -> bool {
        matches!(self, StageResult::Success(_))
    }

    pub fn is_dropped(&self) -> bool {
        matches!(self, StageResult::Dropped(_))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, StageResult::Failed(_))
    }
}

/// Event with stage-specific metadata
#[derive(Debug, Clone)]
pub struct StagedEvent {
    pub event: RawEvent,
    pub stage_metadata: HashMap<String, JsonValue>,
    pub timing: Option<EventTiming>,
}

#[derive(Debug, Clone)]
pub struct EventTiming {
    pub collection_timestamp: chrono::DateTime<chrono::Utc>,
    pub validation_duration_us: Option<u64>,
    pub enrichment_duration_us: Option<u64>,
    pub storage_duration_us: Option<u64>,
    pub distribution_duration_us: Option<u64>,
}

impl StagedEvent {
    pub fn new(event: RawEvent) -> Self {
        Self {
            event,
            stage_metadata: HashMap::new(),
            timing: Some(EventTiming {
                collection_timestamp: chrono::Utc::now(),
                validation_duration_us: None,
                enrichment_duration_us: None,
                storage_duration_us: None,
                distribution_duration_us: None,
            }),
        }
    }

    pub fn add_metadata(&mut self, key: impl Into<String>, value: JsonValue) {
        self.stage_metadata.insert(key.into(), value);
    }

    pub fn record_stage_duration(&mut self, stage: &str, duration_us: u64) {
        if let Some(timing) = &mut self.timing {
            match stage {
                "validation" => timing.validation_duration_us = Some(duration_us),
                "enrichment" => timing.enrichment_duration_us = Some(duration_us),
                "storage" => timing.storage_duration_us = Some(duration_us),
                "distribution" => timing.distribution_duration_us = Some(duration_us),
                _ => {}
            }
        }
    }
}

/// Base trait for pipeline stages
#[async_trait]
pub trait PipelineStage: Send + Sync {
    type Input: Send;
    type Output: Send;

    /// Stage name for logging and metrics
    fn stage_name(&self) -> &'static str;

    /// Process a single event through this stage
    async fn process(&self, input: Self::Input) -> StageResult<Self::Output>;

    /// Process a batch of events (default implementation processes individually)
    async fn process_batch(&self, inputs: Vec<Self::Input>) -> Vec<StageResult<Self::Output>> {
        let mut results = Vec::with_capacity(inputs.len());
        for input in inputs {
            results.push(self.process(input).await);
        }
        results
    }

    /// Get current stage metrics
    fn metrics(&self) -> StageMetrics;

    /// Reset stage metrics
    fn reset_metrics(&self);
}

/// Validation stage - ensures events meet schema requirements
pub struct ValidationStage {
    schema_registry: Arc<RwLock<HashMap<String, schemars::schema::RootSchema>>>,
    metrics: Arc<RwLock<StageMetrics>>,
}

impl Default for ValidationStage {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationStage {
    pub fn new() -> Self {
        Self {
            schema_registry: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(StageMetrics::default())),
        }
    }

    pub async fn register_schema(&self, event_type: String, schema: schemars::schema::RootSchema) {
        let mut registry = self.schema_registry.write().await;
        registry.insert(event_type, schema);
    }

    async fn validate_event_schema(&self, event: &RawEvent) -> Result<()> {
        let schemas = self.schema_registry.read().await;

        if let Some(_schema) = schemas.get(&event.event_type) {
            // TODO: Implement actual JSON schema validation
            // For now, just validate basic structure
            if event.payload.is_null() {
                return Err(CoreError::Validation(format!(
                    "Event {} has null payload",
                    event.event_type
                )));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl PipelineStage for ValidationStage {
    type Input = StagedEvent;
    type Output = StagedEvent;

    fn stage_name(&self) -> &'static str {
        "validation"
    }

    #[instrument(skip(self, input), fields(event_id = %input.event.id))]
    async fn process(&self, mut input: Self::Input) -> StageResult<Self::Output> {
        let start = std::time::Instant::now();

        match self.validate_event_schema(&input.event).await {
            Ok(()) => {
                let duration_us = start.elapsed().as_micros() as u64;
                input.record_stage_duration("validation", duration_us);

                let mut metrics = self.metrics.write().await;
                metrics.events_processed += 1;
                metrics.processing_time_ms += duration_us / 1000;

                debug!("Event {} validated successfully", input.event.id);
                StageResult::Success(input)
            }
            Err(e) => {
                let mut metrics = self.metrics.write().await;
                metrics.errors += 1;

                error!("Event {} failed validation: {}", input.event.id, e);
                StageResult::Failed(e)
            }
        }
    }

    fn metrics(&self) -> StageMetrics {
        self.metrics
            .try_read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    fn reset_metrics(&self) {
        if let Ok(mut metrics) = self.metrics.try_write() {
            *metrics = StageMetrics::default();
        }
    }
}

/// Enrichment stage - adds metadata and normalizes events
pub struct EnrichmentStage {
    hostname: String,
    version: String,
    metrics: Arc<RwLock<StageMetrics>>,
}

impl Default for EnrichmentStage {
    fn default() -> Self {
        Self::new()
    }
}

impl EnrichmentStage {
    pub fn new() -> Self {
        Self {
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            metrics: Arc::new(RwLock::new(StageMetrics::default())),
        }
    }

    fn enrich_event(&self, event: &mut RawEvent) {
        // Ensure host is set
        if event.host.is_empty() {
            event.host = self.hostname.clone();
        }

        // Ensure ingestor version is set
        if event.ingestor_version.is_none() {
            event.ingestor_version = Some(self.version.clone());
        }

        // Normalize timestamps if needed
        if event.ts_orig.is_none() {
            event.ts_orig = Some(event.ts_ingest);
        }
    }
}

#[async_trait]
impl PipelineStage for EnrichmentStage {
    type Input = StagedEvent;
    type Output = StagedEvent;

    fn stage_name(&self) -> &'static str {
        "enrichment"
    }

    #[instrument(skip(self, input), fields(event_id = %input.event.id))]
    async fn process(&self, mut input: Self::Input) -> StageResult<Self::Output> {
        let start = std::time::Instant::now();

        self.enrich_event(&mut input.event);

        // Add enrichment metadata
        input.add_metadata("enriched_at", chrono::Utc::now().to_rfc3339().into());
        input.add_metadata("enricher_version", self.version.clone().into());

        let duration_us = start.elapsed().as_micros() as u64;
        input.record_stage_duration("enrichment", duration_us);

        let mut metrics = self.metrics.write().await;
        metrics.events_processed += 1;
        metrics.processing_time_ms += duration_us / 1000;

        debug!("Event {} enriched successfully", input.event.id);
        StageResult::Success(input)
    }

    fn metrics(&self) -> StageMetrics {
        self.metrics
            .try_read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    fn reset_metrics(&self) {
        if let Ok(mut metrics) = self.metrics.try_write() {
            *metrics = StageMetrics::default();
        }
    }
}

/// Storage stage - persists events to database
pub struct StorageStage {
    db_pool: crate::DbPool,
    metrics: Arc<RwLock<StageMetrics>>,
}

impl StorageStage {
    pub fn new(db_pool: crate::DbPool) -> Self {
        Self {
            db_pool,
            metrics: Arc::new(RwLock::new(StageMetrics::default())),
        }
    }

    async fn store_event(&self, event: &RawEvent) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO raw.events (
                id, source, event_type, ts_orig, 
                host, ingestor_version, payload_schema_id, payload
            ) VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8
            )
            "#,
            event.id.to_uuid(),
            event.source,
            event.event_type,
            event.ts_orig,
            event.host,
            event.ingestor_version,
            event.payload_schema_id.map(|id| id.to_uuid()),
            event.payload
        )
        .execute(&self.db_pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl PipelineStage for StorageStage {
    type Input = StagedEvent;
    type Output = StagedEvent;

    fn stage_name(&self) -> &'static str {
        "storage"
    }

    #[instrument(skip(self, input), fields(event_id = %input.event.id))]
    async fn process(&self, mut input: Self::Input) -> StageResult<Self::Output> {
        let start = std::time::Instant::now();

        match self.store_event(&input.event).await {
            Ok(()) => {
                let duration_us = start.elapsed().as_micros() as u64;
                input.record_stage_duration("storage", duration_us);

                input.add_metadata("stored_at", chrono::Utc::now().to_rfc3339().into());

                let mut metrics = self.metrics.write().await;
                metrics.events_processed += 1;
                metrics.processing_time_ms += duration_us / 1000;

                debug!("Event {} stored successfully", input.event.id);
                StageResult::Success(input)
            }
            Err(e) => {
                let mut metrics = self.metrics.write().await;
                metrics.errors += 1;

                error!("Event {} failed storage: {}", input.event.id, e);
                StageResult::Failed(e)
            }
        }
    }

    fn metrics(&self) -> StageMetrics {
        self.metrics
            .try_read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    fn reset_metrics(&self) {
        if let Ok(mut metrics) = self.metrics.try_write() {
            *metrics = StageMetrics::default();
        }
    }
}

/// Distribution stage - sends events to work queue for processing
pub struct DistributionStage {
    metrics: Arc<RwLock<StageMetrics>>,
}

impl DistributionStage {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(StageMetrics::default())),
        }
    }

    async fn distribute_event(&self, _event: &RawEvent) -> Result<()> {
        // TODO: Replace with Redis Streams distribution in satellite architecture
        // For now, events are distributed directly via Redis rather than work_queue
        // This function will be removed as part of satellite migration

        Ok(())
    }
}

#[async_trait]
impl PipelineStage for DistributionStage {
    type Input = StagedEvent;
    type Output = StagedEvent;

    fn stage_name(&self) -> &'static str {
        "distribution"
    }

    #[instrument(skip(self, input), fields(event_id = %input.event.id))]
    async fn process(&self, mut input: Self::Input) -> StageResult<Self::Output> {
        let start = std::time::Instant::now();

        match self.distribute_event(&input.event).await {
            Ok(()) => {
                let duration_us = start.elapsed().as_micros() as u64;
                input.record_stage_duration("distribution", duration_us);

                input.add_metadata("distributed_at", chrono::Utc::now().to_rfc3339().into());

                let mut metrics = self.metrics.write().await;
                metrics.events_processed += 1;
                metrics.processing_time_ms += duration_us / 1000;

                debug!("Event {} distributed successfully", input.event.id);
                StageResult::Success(input)
            }
            Err(e) => {
                let mut metrics = self.metrics.write().await;
                metrics.errors += 1;

                error!("Event {} failed distribution: {}", input.event.id, e);
                StageResult::Failed(e)
            }
        }
    }

    fn metrics(&self) -> StageMetrics {
        self.metrics
            .try_read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    fn reset_metrics(&self) {
        if let Ok(mut metrics) = self.metrics.try_write() {
            *metrics = StageMetrics::default();
        }
    }
}

/// Complete event processing pipeline
pub struct EventPipeline {
    #[allow(dead_code)]
    config: PipelineConfig,
    validation_stage: ValidationStage,
    enrichment_stage: EnrichmentStage,
    storage_stage: StorageStage,
    distribution_stage: DistributionStage,
}

impl EventPipeline {
    pub fn new(config: PipelineConfig, db_pool: crate::DbPool) -> Self {
        Self {
            config,
            validation_stage: ValidationStage::new(),
            enrichment_stage: EnrichmentStage::new(),
            storage_stage: StorageStage::new(db_pool.clone()),
            distribution_stage: DistributionStage::new(),
        }
    }

    /// Process a single event through the complete pipeline
    #[instrument(skip(self, event), fields(event_id = %event.id))]
    pub async fn process_event(&self, event: RawEvent) -> Result<()> {
        let mut staged_event = StagedEvent::new(event);

        info!(
            "Processing event {} through pipeline",
            staged_event.event.id
        );

        // Stage 1: Validation
        staged_event = match self.validation_stage.process(staged_event).await {
            StageResult::Success(event) => event,
            StageResult::Dropped(reason) => {
                warn!("Event dropped during validation: {}", reason);
                return Ok(());
            }
            StageResult::Failed(e) => {
                error!("Event failed validation: {}", e);
                return Err(e);
            }
        };

        // Stage 2: Enrichment
        staged_event = match self.enrichment_stage.process(staged_event).await {
            StageResult::Success(event) => event,
            StageResult::Dropped(reason) => {
                warn!("Event dropped during enrichment: {}", reason);
                return Ok(());
            }
            StageResult::Failed(e) => {
                error!("Event failed enrichment: {}", e);
                return Err(e);
            }
        };

        // Stage 3: Storage
        staged_event = match self.storage_stage.process(staged_event).await {
            StageResult::Success(event) => event,
            StageResult::Dropped(reason) => {
                warn!("Event dropped during storage: {}", reason);
                return Ok(());
            }
            StageResult::Failed(e) => {
                error!("Event failed storage: {}", e);
                return Err(e);
            }
        };

        // Stage 4: Distribution
        match self.distribution_stage.process(staged_event).await {
            StageResult::Success(event) => {
                info!(
                    "Event {} processed successfully through all stages",
                    event.event.id
                );
                Ok(())
            }
            StageResult::Dropped(reason) => {
                warn!("Event dropped during distribution: {}", reason);
                Ok(())
            }
            StageResult::Failed(e) => {
                error!("Event failed distribution: {}", e);
                Err(e)
            }
        }
    }

    /// Start pipeline with input and output channels
    pub async fn start(
        &self,
        mut input: EventReceiver,
        _output: Option<EventSender>,
    ) -> Result<()> {
        info!("Starting event processing pipeline");

        while let Some(event) = input.recv().await {
            if let Err(e) = self.process_event(event).await {
                error!("Pipeline processing error: {}", e);
                // Continue processing other events
            }
        }

        info!("Event processing pipeline stopped");
        Ok(())
    }

    /// Get comprehensive pipeline metrics
    pub fn get_metrics(&self) -> PipelineMetrics {
        PipelineMetrics {
            validation: self.validation_stage.metrics(),
            enrichment: self.enrichment_stage.metrics(),
            storage: self.storage_stage.metrics(),
            distribution: self.distribution_stage.metrics(),
        }
    }

    /// Reset all pipeline metrics
    pub fn reset_metrics(&self) {
        self.validation_stage.reset_metrics();
        self.enrichment_stage.reset_metrics();
        self.storage_stage.reset_metrics();
        self.distribution_stage.reset_metrics();
    }
}

/// Complete pipeline metrics
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    pub validation: StageMetrics,
    pub enrichment: StageMetrics,
    pub storage: StageMetrics,
    pub distribution: StageMetrics,
}

impl PipelineMetrics {
    pub fn total_events_processed(&self) -> u64 {
        self.storage.events_processed // Use storage as authoritative count
    }

    pub fn total_events_dropped(&self) -> u64 {
        self.validation.events_dropped
            + self.enrichment.events_dropped
            + self.storage.events_dropped
            + self.distribution.events_dropped
    }

    pub fn total_errors(&self) -> u64 {
        self.validation.errors
            + self.enrichment.errors
            + self.storage.errors
            + self.distribution.errors
    }

    pub fn total_processing_time_ms(&self) -> u64 {
        self.validation.processing_time_ms
            + self.enrichment.processing_time_ms
            + self.storage.processing_time_ms
            + self.distribution.processing_time_ms
    }
}
