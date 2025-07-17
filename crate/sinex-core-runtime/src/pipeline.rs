//! Event pipeline processing utilities
//!
//! This module provides multi-stage event processing pipeline
//! with configurable stages and error handling.

use serde::{Deserialize, Serialize};
use sinex_core_types::{CoreError, RawEvent, Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error};

/// Pipeline stage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub max_concurrent_events: usize,
    pub stage_timeout: Duration,
    pub retry_attempts: u32,
    pub buffer_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_events: 100,
            stage_timeout: Duration::from_secs(30),
            retry_attempts: 3,
            buffer_size: 1000,
        }
    }
}

/// Event processing stage result
#[derive(Debug, Clone)]
pub struct StageResult {
    pub event: RawEvent,
    pub stage_name: String,
    pub duration: Duration,
    pub success: bool,
    pub error: Option<String>,
}

/// Event with pipeline metadata
#[derive(Debug, Clone)]
pub struct StagedEvent {
    pub event: RawEvent,
    pub stage_history: Vec<StageResult>,
    pub created_at: Instant,
}

impl StagedEvent {
    pub fn new(event: RawEvent) -> Self {
        Self {
            event,
            stage_history: Vec::new(),
            created_at: Instant::now(),
        }
    }

    pub fn add_stage_result(&mut self, result: StageResult) {
        self.stage_history.push(result);
    }

    pub fn total_processing_time(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Pipeline stage trait
#[async_trait::async_trait]
pub trait PipelineStage: Send + Sync {
    /// Process an event through this stage
    async fn process(&self, event: RawEvent) -> Result<RawEvent>;

    /// Get the name of this stage
    fn name(&self) -> &str;

    /// Get stage-specific metrics
    fn get_metrics(&self) -> HashMap<String, serde_json::Value> {
        HashMap::new()
    }
}

/// Event processing pipeline
pub struct EventPipeline {
    _config: PipelineConfig,
    stages: Vec<Box<dyn PipelineStage>>,
    metrics: PipelineMetrics,
}

impl EventPipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            _config: config,
            stages: Vec::new(),
            metrics: PipelineMetrics::new(),
        }
    }

    pub fn add_stage<S: PipelineStage + 'static>(mut self, stage: S) -> Self {
        self.stages.push(Box::new(stage));
        self
    }

    /// Process a single event through all stages
    pub async fn process_event(&self, event: RawEvent) -> Result<StagedEvent> {
        let mut staged_event = StagedEvent::new(event);

        for stage in &self.stages {
            let stage_start = Instant::now();
            let stage_name = stage.name().to_string();

            match stage.process(staged_event.event.clone()).await {
                Ok(processed_event) => {
                    let duration = stage_start.elapsed();
                    staged_event.event = processed_event;
                    staged_event.add_stage_result(StageResult {
                        event: staged_event.event.clone(),
                        stage_name: stage_name.clone(),
                        duration,
                        success: true,
                        error: None,
                    });

                    self.metrics.record_stage_success(&stage_name, duration);
                    debug!("Stage {} completed in {:?}", stage_name, duration);
                }
                Err(e) => {
                    let duration = stage_start.elapsed();
                    let error_msg = e.to_string();

                    staged_event.add_stage_result(StageResult {
                        event: staged_event.event.clone(),
                        stage_name: stage_name.clone(),
                        duration,
                        success: false,
                        error: Some(error_msg.clone()),
                    });

                    self.metrics.record_stage_failure(&stage_name, duration);
                    error!(
                        "Stage {} failed after {:?}: {}",
                        stage_name, duration, error_msg
                    );

                    return Err(e);
                }
            }
        }

        Ok(staged_event)
    }

    /// Get pipeline metrics
    pub fn get_metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }
}

/// Pipeline performance metrics
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    stage_metrics: HashMap<String, StageMetrics>,
    _total_processed: u64,
    _total_failed: u64,
}

impl PipelineMetrics {
    pub fn new() -> Self {
        Self {
            stage_metrics: HashMap::new(),
            _total_processed: 0,
            _total_failed: 0,
        }
    }

    fn record_stage_success(&self, _stage_name: &str, _duration: Duration) {
        // In a real implementation, this would use atomic operations
        // For now, this is a placeholder
    }

    fn record_stage_failure(&self, _stage_name: &str, _duration: Duration) {
        // In a real implementation, this would use atomic operations
        // For now, this is a placeholder
    }

    pub fn get_stage_metrics(&self, stage_name: &str) -> Option<&StageMetrics> {
        self.stage_metrics.get(stage_name)
    }
}

/// Metrics for a specific stage
#[derive(Debug, Clone)]
pub struct StageMetrics {
    pub successes: u64,
    pub failures: u64,
    pub total_duration: Duration,
    pub avg_duration: Duration,
}

/// Pipeline stage timeouts
#[derive(Debug, Clone)]
pub struct StageTimeouts {
    pub validation: Duration,
    pub enrichment: Duration,
    pub storage: Duration,
    pub distribution: Duration,
}

impl Default for StageTimeouts {
    fn default() -> Self {
        Self {
            validation: Duration::from_secs(5),
            enrichment: Duration::from_secs(10),
            storage: Duration::from_secs(15),
            distribution: Duration::from_secs(5),
        }
    }
}

/// Event timing information
#[derive(Debug, Clone)]
pub struct EventTiming {
    pub received_at: Instant,
    pub processing_started: Instant,
    pub processing_completed: Option<Instant>,
}

impl EventTiming {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            received_at: now,
            processing_started: now,
            processing_completed: None,
        }
    }

    pub fn mark_completed(&mut self) {
        self.processing_completed = Some(Instant::now());
    }

    pub fn total_time(&self) -> Option<Duration> {
        self.processing_completed
            .map(|completed| completed.duration_since(self.received_at))
    }
}

// Example pipeline stages
pub struct ValidationStage {
    name: String,
}

impl ValidationStage {
    pub fn new() -> Self {
        Self {
            name: "validation".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl PipelineStage for ValidationStage {
    async fn process(&self, event: RawEvent) -> Result<RawEvent> {
        // Validate the event structure
        if event.source.is_empty() {
            return Err(CoreError::Validation(
                "Event source cannot be empty".to_string(),
            ));
        }

        if event.event_type.is_empty() {
            return Err(CoreError::Validation(
                "Event type cannot be empty".to_string(),
            ));
        }

        Ok(event)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct EnrichmentStage {
    name: String,
}

impl EnrichmentStage {
    pub fn new() -> Self {
        Self {
            name: "enrichment".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl PipelineStage for EnrichmentStage {
    async fn process(&self, mut event: RawEvent) -> Result<RawEvent> {
        // Add enrichment data
        if let Some(payload) = event.payload.as_object_mut() {
            payload.insert(
                "enriched_at".to_string(),
                serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            );
            payload.insert(
                "enriched_by".to_string(),
                serde_json::Value::String("sinex-pipeline".to_string()),
            );
        }

        Ok(event)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct StorageStage {
    name: String,
}

impl StorageStage {
    pub fn new() -> Self {
        Self {
            name: "storage".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl PipelineStage for StorageStage {
    async fn process(&self, event: RawEvent) -> Result<RawEvent> {
        // In a real implementation, this would store the event
        debug!("Storing event: {} - {}", event.source, event.event_type);
        Ok(event)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct DistributionStage {
    name: String,
}

impl DistributionStage {
    pub fn new() -> Self {
        Self {
            name: "distribution".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl PipelineStage for DistributionStage {
    async fn process(&self, event: RawEvent) -> Result<RawEvent> {
        // In a real implementation, this would distribute the event
        debug!(
            "Distributing event: {} - {}",
            event.source, event.event_type
        );
        Ok(event)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_events::EventFactory;

    #[tokio::test]
    async fn test_pipeline_processing() {
        let config = PipelineConfig::default();
        let pipeline = EventPipeline::new(config)
            .add_stage(ValidationStage::new())
            .add_stage(EnrichmentStage::new())
            .add_stage(StorageStage::new())
            .add_stage(DistributionStage::new());

        let factory = EventFactory::new("test_source");
        let event = factory.create_event("test_event", serde_json::json!({"test": "data"}));

        let result = pipeline.process_event(event).await.unwrap();

        assert_eq!(result.stage_history.len(), 4);
        assert!(result.stage_history.iter().all(|r| r.success));

        // Check that enrichment was applied
        assert!(result.event.payload.get("enriched_at").is_some());
        assert!(result.event.payload.get("enriched_by").is_some());
    }

    #[tokio::test]
    async fn test_pipeline_validation_failure() {
        let config = PipelineConfig::default();
        let pipeline = EventPipeline::new(config).add_stage(ValidationStage::new());

        let factory = EventFactory::new("");
        let event = factory.create_event("test_event", serde_json::json!({}));

        let result = pipeline.process_event(event).await;
        assert!(result.is_err());
    }
}
