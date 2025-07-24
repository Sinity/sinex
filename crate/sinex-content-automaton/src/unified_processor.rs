//! Unified StatefulStreamProcessor implementation for Content Service Automaton
//!
//! This automaton provides content storage and retrieval capabilities.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Value};
use sinex_db::queries::EventQueries;
use sinex_events::RawEvent;
use sinex_satellite_sdk::{
    redis_stream_consumer::{
        BatchProcessingResult, EventBatchProcessor, RedisStreamConsumer,
        EventFilter as StreamEventFilter,
    },
    stream_processor::{
        Checkpoint, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon,
    },
    SatelliteError, SatelliteResult,
};
use sinex_services::ContentService;
use sinex_ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Content Service as a unified StatefulStreamProcessor
pub struct ContentServiceProcessor {
    context: Option<StreamProcessorContext>,
    service: Option<Arc<ContentService>>,
}

impl ContentServiceProcessor {
    pub fn new() -> Self {
        Self {
            context: None,
            service: None,
        }
    }

    /// Handle content RPC request
    async fn handle_content_request(&self, request: Value) -> SatelliteResult<Value> {
        let service = self.service.as_ref().unwrap();
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));

        match method {
            "content.store_blob" => self.handle_store_blob(service, params).await,
            "content.retrieve_blob" => self.handle_retrieve_blob(service, params).await,
            "content.get_metadata" => self.handle_get_metadata(service, params).await,
            _ => Err(SatelliteError::General(anyhow::anyhow!(
                "Unknown content method: {}",
                method
            ))),
        }
    }

    async fn handle_store_blob(
        &self,
        service: &ContentService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let content = params
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Missing content")))?;

        let filename = params
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("content.txt");

        let content_type = params
            .get("content_type")
            .and_then(|v| v.as_str())
            .unwrap_or("text/plain");

        let source = params
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("sinex-content-automaton");

        let annex_key = service
            .store_large_content(content.as_bytes(), filename, content_type, source)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("Content storage error: {}", e)))?;

        Ok(json!({ "annex_key": annex_key }))
    }

    async fn handle_retrieve_blob(
        &self,
        service: &ContentService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let blob_id = params
            .get("blob_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Invalid or missing blob_id")))?;

        let (_metadata, content) = service
            .retrieve_large_content(blob_id)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("Content retrieval error: {}", e)))?;

        // For text content, return as string
        let content_str = String::from_utf8(content.clone())
            .unwrap_or_else(|_| format!("[Binary content, {} bytes]", content.len()));

        Ok(json!({
            "blob_id": blob_id.to_string(),
            "content": content_str,
            "size": content.len()
        }))
    }

    async fn handle_get_metadata(
        &self,
        service: &ContentService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let blob_id = params
            .get("blob_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Invalid or missing blob_id")))?;

        let metadata = service
            .get_blob_metadata(blob_id)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("Metadata retrieval error: {}", e)))?;

        Ok(metadata)
    }

    /// Get event filters for this automaton
    fn event_filters() -> Vec<StreamEventFilter> {
        vec![
            StreamEventFilter::new(
                Some("rpc.content".to_string()),
                Some("request".to_string()),
            ),
        ]
    }
}

#[async_trait]
impl EventBatchProcessor for ContentServiceProcessor {
    async fn process_batch(&mut self, events: Vec<RawEvent>) -> SatelliteResult<BatchProcessingResult> {
        let mut successful_ids = Vec::new();
        let mut failed_ids = Vec::new();

        for event in events {
            let event_id = event.id.to_string();
            
            if event.source == "rpc.content" && event.event_type == "request" {
                match self.handle_content_request(event.payload.clone()).await {
                    Ok(response) => {
                        // Submit response as synthesis event
                        if let Some(ctx) = &self.context {
                            let response_event = serde_json::json!({
                                "request_id": event.payload.get("request_id"),
                                "response": response,
                                "service": "content"
                            });

                            // Create response event
                            let synthesis_event = sinex_events::EventFactory::new("rpc.content")
                                .create_event("response", response_event);

                            ctx.send_event(synthesis_event).await?;
                        }
                        successful_ids.push(event_id);
                    }
                    Err(e) => {
                        warn!("Content request failed: {}", e);
                        
                        // Submit error response
                        if let Some(ctx) = &self.context {
                            let error_response = serde_json::json!({
                                "request_id": event.payload.get("request_id"),
                                "error": {
                                    "code": -32603,
                                    "message": format!("Content service error: {}", e)
                                },
                                "service": "content"
                            });

                            let synthesis_event = sinex_events::EventFactory::new("rpc.content")
                                .create_event("response", error_response);

                            ctx.send_event(synthesis_event).await?;
                        }
                        failed_ids.push((event_id, e.to_string()));
                    }
                }
            } else {
                // Skip non-content events
                successful_ids.push(event_id);
            }
        }

        Ok(BatchProcessingResult {
            successful_ids,
            failed_ids,
            retry_ids: Vec::new(),
            checkpoint_data: None,
        })
    }

    async fn get_checkpoint_data(&self) -> Option<serde_json::Value> {
        None
    }
}

#[async_trait]
impl StatefulStreamProcessor for ContentServiceProcessor {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        info!("Initializing Content service automaton");
        
        // Initialize the Content service
        let service = Arc::new(ContentService::new(ctx.db_pool.clone()));
        
        self.service = Some(service);
        self.context = Some(ctx);
        
        info!("Content service automaton initialized successfully");
        Ok(())
    }

    async fn scan(
        &mut self,
        from: Checkpoint,
        until: TimeHorizon,
        args: ScanArgs,
    ) -> SatelliteResult<ScanReport> {
        let start_time = Utc::now();
        let mut events_processed = 0;

        match until {
            TimeHorizon::Continuous => {
                info!("Starting continuous content request processing from Redis Stream");
                
                let ctx = self.context.as_ref().unwrap();
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "content-service".to_string(),
                    Self::event_filters(),
                );

                let final_checkpoint = redis_consumer
                    .consume_continuous(from, self, args.shutdown_signal)
                    .await?;

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(final_checkpoint),
                    time_range: Some((start_time, Utc::now())),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["redis-stream".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new(),
                })
            }
            TimeHorizon::Historical { end_time } => {
                info!("Processing historical content requests up to {}", end_time);

                let ctx = self.context.as_ref().unwrap();
                
                // Determine start time from checkpoint
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    _ => end_time - chrono::Duration::days(7),
                };

                // Query RPC request events
                let events = EventQueries::get_events_by_type_and_time_range(
                    "rpc.content".to_string(),
                    "request".to_string(),
                    start_time,
                    end_time,
                    Some(1000),
                    None,
                )
                .fetch_all(&ctx.db_pool)
                .await?;

                // Process using batch processor
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "content-service".to_string(),
                    Self::event_filters(),
                );

                let final_checkpoint = redis_consumer
                    .consume_historical(events, self, 100)
                    .await?;

                events_processed = events.len();

                Ok(ScanReport {
                    events_processed,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(final_checkpoint),
                    time_range: Some((start_time, end_time)),
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["postgresql".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: Vec::new(),
                })
            }
            TimeHorizon::Snapshot => {
                // No snapshot mode for content service
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(Checkpoint::None),
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["snapshot".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: vec!["Content service does not support snapshot mode".to_string()],
                })
            }
        }
    }

    fn processor_name(&self) -> &str {
        "content-service"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

impl Default for ContentServiceProcessor {
    fn default() -> Self {
        Self::new()
    }
}