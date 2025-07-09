/// Integration module for event pipeline with unified collector
/// 
/// This module demonstrates how to integrate the new multi-stage event pipeline
/// with the existing unified collector architecture.

use crate::{
    EventPipeline, PipelineConfig, RawEvent, EventSender, EventReceiver,
    Result, CoreError, sources, DbPool
};
use tokio::sync::mpsc;
use tracing::{info, error, instrument};

/// Pipeline-aware event collector that processes events through stages
pub struct PipelineAwareCollector {
    pipeline: EventPipeline,
    config: PipelineConfig,
}

impl PipelineAwareCollector {
    pub fn new(db_pool: DbPool, config: PipelineConfig) -> Self {
        let pipeline = EventPipeline::new(config.clone(), db_pool);
        
        Self {
            pipeline,
            config,
        }
    }
    
    /// Start the collector with pipeline processing
    #[instrument(skip(self, event_receiver))]
    pub async fn start(self, event_receiver: EventReceiver) -> Result<()> {
        info!("Starting pipeline-aware collector");
        
        // Create channels between collection and pipeline
        let (pipeline_tx, pipeline_rx) = mpsc::channel(self.config.stage_buffer_size);
        
        // Move pipeline ownership into the task
        let pipeline = self.pipeline;
        let pipeline_task = tokio::spawn(async move {
            if let Err(e) = pipeline.start(pipeline_rx, None).await {
                error!("Pipeline processing failed: {}", e);
            }
        });
        
        // Process incoming events and send to pipeline
        Self::process_events_static(event_receiver, pipeline_tx).await?;
        
        // Wait for pipeline to finish
        if let Err(e) = pipeline_task.await {
            error!("Pipeline task failed: {}", e);
        }
        
        info!("Pipeline-aware collector stopped");
        Ok(())
    }
    
    /// Static method for processing events to avoid lifetime issues
    async fn process_events_static(mut receiver: EventReceiver, sender: EventSender) -> Result<()> {
        while let Some(event) = receiver.recv().await {
            if let Err(e) = sender.send(event).await {
                error!("Failed to send event to pipeline: {}", e);
                break;
            }
        }
        Ok(())
    }
    
    /// Create a metrics accessor that can be used after collector starts
    pub fn create_metrics_accessor(&self) -> PipelineMetricsAccessor {
        PipelineMetricsAccessor {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Metrics accessor for pipeline when collector has moved ownership
pub struct PipelineMetricsAccessor {
    _phantom: std::marker::PhantomData<()>,
}

impl PipelineMetricsAccessor {
    /// Note: This is a placeholder - in a real implementation, you'd need
    /// shared state or a different architecture to access metrics after start()
    pub fn get_metrics(&self) -> crate::PipelineMetrics {
        // Placeholder implementation
        crate::PipelineMetrics {
            validation: crate::StageMetrics::default(),
            enrichment: crate::StageMetrics::default(),
            storage: crate::StageMetrics::default(),
            distribution: crate::StageMetrics::default(),
        }
    }
}

/// Builder for creating pipeline-aware collectors with different configurations
pub struct PipelineCollectorBuilder {
    db_pool: Option<DbPool>,
    config: PipelineConfig,
}

impl PipelineCollectorBuilder {
    pub fn new() -> Self {
        Self {
            db_pool: None,
            config: PipelineConfig::default(),
        }
    }
    
    pub fn with_database(mut self, db_pool: DbPool) -> Self {
        self.db_pool = Some(db_pool);
        self
    }
    
    pub fn with_config(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }
    
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.config.stage_buffer_size = size;
        self
    }
    
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }
    
    pub fn with_timing_enabled(mut self, enabled: bool) -> Self {
        self.config.enable_timing = enabled;
        self
    }
    
    pub fn build(self) -> Result<PipelineAwareCollector> {
        let db_pool = self.db_pool.ok_or_else(|| {
            CoreError::Configuration("Database pool is required".to_string())
        })?;
        
        Ok(PipelineAwareCollector::new(db_pool, self.config))
    }
}

impl Default for PipelineCollectorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Example integration showing how to migrate from basic collector to pipeline-aware
pub mod migration_example {
    use super::*;
    
    /// Example showing basic pipeline integration
    pub async fn basic_pipeline_example(db_pool: DbPool) -> Result<()> {
        // Create pipeline-aware collector
        let collector = PipelineCollectorBuilder::new()
            .with_database(db_pool)
            .with_buffer_size(1000)
            .with_batch_size(50)
            .with_timing_enabled(true)
            .build()?;
        
        // Create metrics accessor before starting
        let metrics_accessor = collector.create_metrics_accessor();
        
        // Create event channel
        let (tx, rx) = mpsc::channel(1000);
        
        // Simulate some events
        let sample_events = vec![
            create_sample_filesystem_event(),
            create_sample_terminal_event(),
            create_sample_clipboard_event(),
        ];
        
        // Send sample events
        for event in sample_events {
            if let Err(e) = tx.send(event).await {
                error!("Failed to send sample event: {}", e);
            }
        }
        
        // Close sender to signal end
        drop(tx);
        
        // Start processing (consumes collector)
        collector.start(rx).await?;
        
        // Get final metrics
        let metrics = metrics_accessor.get_metrics();
        info!("Final pipeline metrics: {:?}", metrics);
        
        Ok(())
    }
    
    fn create_sample_filesystem_event() -> RawEvent {
        use crate::RawEventBuilder;
        
        RawEventBuilder::new(
            sources::FS,
            "file.created",
            serde_json::json!({
                "path": "/tmp/example.txt",
                "size": 1024,
                "permissions": 644
            })
        )
        .with_host("example-host")
        .build()
    }
    
    fn create_sample_terminal_event() -> RawEvent {
        use crate::RawEventBuilder;
        
        RawEventBuilder::new(
            sources::SHELL_KITTY,
            "command.executed",
            serde_json::json!({
                "command": "ls -la",
                "cwd": "/home/user",
                "exit_code": 0
            })
        )
        .with_host("example-host")
        .build()
    }
    
    fn create_sample_clipboard_event() -> RawEvent {
        use crate::RawEventBuilder;
        
        RawEventBuilder::new(
            sources::CLIPBOARD,
            "copied",
            serde_json::json!({
                "content_type": "text/plain",
                "length": 42
            })
        )
        .with_host("example-host")
        .build()
    }
}

/// Performance monitoring utilities for the pipeline
pub mod monitoring {
    use super::*;
    use std::time::Duration;
    use tokio::time;
    
    /// Metrics reporter that periodically logs pipeline performance
    /// Note: This is a simplified example. A real implementation would need
    /// shared state or a different architecture for monitoring after collector starts.
    pub struct PipelineMonitor {
        metrics_accessor: PipelineMetricsAccessor,
        report_interval: Duration,
    }
    
    impl PipelineMonitor {
        pub fn new(metrics_accessor: PipelineMetricsAccessor, report_interval: Duration) -> Self {
            Self {
                metrics_accessor,
                report_interval,
            }
        }
        
        /// Start periodic metrics reporting
        pub async fn start_monitoring(&self) {
            let mut interval = time::interval(self.report_interval);
            
            loop {
                interval.tick().await;
                let metrics = self.metrics_accessor.get_metrics();
                
                info!("Pipeline Performance Report:");
                info!("  Total Events Processed: {}", metrics.total_events_processed());
                info!("  Total Events Dropped: {}", metrics.total_events_dropped());
                info!("  Total Errors: {}", metrics.total_errors());
                info!("  Total Processing Time: {}ms", metrics.total_processing_time_ms());
                
                info!("  Stage Breakdown:");
                info!("    Validation: {} events, {} errors, {}ms", 
                      metrics.validation.events_processed,
                      metrics.validation.errors,
                      metrics.validation.processing_time_ms);
                info!("    Enrichment: {} events, {} errors, {}ms", 
                      metrics.enrichment.events_processed,
                      metrics.enrichment.errors,
                      metrics.enrichment.processing_time_ms);
                info!("    Storage: {} events, {} errors, {}ms", 
                      metrics.storage.events_processed,
                      metrics.storage.errors,
                      metrics.storage.processing_time_ms);
                info!("    Distribution: {} events, {} errors, {}ms", 
                      metrics.distribution.events_processed,
                      metrics.distribution.errors,
                      metrics.distribution.processing_time_ms);
            }
        }
    }
}