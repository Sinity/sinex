//! Unified StatefulStreamProcessor implementation for PKM Service Automaton
//!

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{json, Value};
use sinex_db::repositories::DbPoolExt;
use sinex_db::models::{Event, RawEvent, RpcPkmResponsePayload, RpcError};
use sinex_satellite_sdk::{
    redis_stream_consumer::{
        BatchProcessingResult, EventBatchProcessor, RedisStreamConsumer,
        EventFilter as StreamEventFilter},
    stream_processor::{
        Checkpoint, ProcessorType, ScanArgs, ScanReport, StatefulStreamProcessor,
        StreamProcessorContext, TimeHorizon},
    SatelliteError, SatelliteResult};
use sinex_services::PkmService;
use sinex_types::ulid::Ulid;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// PKM Service as a unified StatefulStreamProcessor
pub struct PkmServiceProcessor {
    context: Option<StreamProcessorContext>,
    service: Option<Arc<PkmService>>}

impl PkmServiceProcessor {
    pub fn new() -> Self {
        Self {
            context: None,
            service: None}
    }

    /// Handle PKM RPC request
    async fn handle_pkm_request(&self, event: &RawEvent, request: Value) -> SatelliteResult<Value> {
        let service = self.service.as_ref().unwrap();
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));

        match method {
            "pkm.create_note" => self.handle_create_note(service, event.id, params).await,
            "pkm.create_entities" => self.handle_create_entities(service, params).await,
            "pkm.link_entities" => self.handle_link_entities(service, params).await,
            "pkm.register_source_material" => self.handle_register_source_material(service, params).await,
            "pkm.get_recent_materials" => self.handle_get_recent_materials(service, params).await,
            _ => Err(SatelliteError::General(anyhow::anyhow!(
                "Unknown PKM method: {}",
                method
            )))}
    }

    async fn handle_create_note(
        &self,
        service: &PkmService,
        event_id: Ulid,
        params: Value,
    ) -> SatelliteResult<Value> {
        let content = params
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Missing content")))?;

        let tags = params
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let created_by = params
            .get("created_by")
            .and_then(|v| v.as_str())
            .unwrap_or("sinex-pkm-automaton");

        let source_material_id = params
            .get("source_material_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok());

        let annotation_id = service
            .create_note(event_id, content, tags, created_by, source_material_id)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("PKM error: {}", e)))?;

        Ok(json!({ "annotation_id": annotation_id.to_string() }))
    }

    async fn handle_create_entities(
        &self,
        service: &PkmService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let source_material_id = params
            .get("source_material_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Invalid or missing source_material_id")))?;

        let entities = params
            .get("entities")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        let name = v.get("name")?.as_str()?;
                        let entity_type = v.get("type")?.as_str()?;
                        Some((name.to_string(), entity_type.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let created_by = params
            .get("created_by")
            .and_then(|v| v.as_str())
            .unwrap_or("sinex-pkm-automaton");

        let entity_ids = service
            .create_entities_from_source_material(source_material_id, entities, created_by)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("PKM error: {}", e)))?;

        Ok(json!({ 
            "entity_ids": entity_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
            "source_material_id": source_material_id.to_string()
        }))
    }

    async fn handle_link_entities(
        &self,
        service: &PkmService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let from_entity_id = params
            .get("from_entity_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| {
                SatelliteError::General(anyhow::anyhow!("Invalid or missing from_entity_id"))
            })?;

        let to_entity_id = params
            .get("to_entity_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| {
                SatelliteError::General(anyhow::anyhow!("Invalid or missing to_entity_id"))
            })?;

        let relationship_type = params
            .get("relationship_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Missing relationship_type")))?;

        let properties = params
            .get("properties")
            .and_then(|v| v.as_object())
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let source_material_id = params
            .get("source_material_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok());

        let relation_id = service
            .link_entities(from_entity_id, to_entity_id, relationship_type, properties, source_material_id)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("PKM error: {}", e)))?;

        Ok(json!({ "relation_id": relation_id.to_string() }))
    }

    async fn handle_register_source_material(
        &self,
        service: &PkmService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let material_type = params
            .get("material_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Missing material_type")))?;

        let source_uri = params.get("source_uri").and_then(|v| v.as_str());

        let content = params
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::General(anyhow::anyhow!("Missing content")))?;

        let mime_type = params.get("mime_type").and_then(|v| v.as_str());

        let metadata = params
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let blob_id = service
            .register_source_material(
                material_type,
                source_uri,
                content.as_bytes(),
                mime_type,
                metadata,
            )
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("PKM error: {}", e)))?;

        Ok(json!({ "blob_id": blob_id.to_string() }))
    }

    async fn handle_get_recent_materials(
        &self,
        service: &PkmService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let material_type = params.get("material_type").and_then(|v| v.as_str());
        let limit = params.get("limit").and_then(|v| v.as_i64());

        let materials = service
            .get_recent_source_materials(material_type, limit)
            .await
            .map_err(|e| SatelliteError::General(anyhow::anyhow!("PKM error: {}", e)))?;

        Ok(json!({ "materials": materials }))
    }

    /// Get event filters for this automaton
    fn event_filters() -> Vec<StreamEventFilter> {
        vec![
            StreamEventFilter::new(
                Some("rpc.pkm".to_string()),
                Some("request".to_string()),
            ),
        ]
    }
}

#[async_trait]
impl EventBatchProcessor for PkmServiceProcessor {
    async fn process_batch(&mut self, events: Vec<RawEvent>) -> SatelliteResult<BatchProcessingResult> {
        let mut successful_ids = Vec::new();
        let mut failed_ids = Vec::new();

        for event in events {
            let event_id = event.id.to_string();
            
            if event.source == "rpc.pkm" && event.event_type == "request" {
                match self.handle_pkm_request(&event, event.payload.clone()).await {
                    Ok(response) => {
                        // Submit response as synthesis event
                        if let Some(ctx) = &self.context {
                            // Create response event
                            let synthesis_event = Event::from(RpcPkmResponsePayload {
                                request_id: event.payload.get("request_id").cloned(),
                                response: Some(response),
                                error: None,
                                service: "pkm".to_string(),
                            });

                            ctx.send_event(synthesis_event).await?;
                        }
                        successful_ids.push(event_id);
                    }
                    Err(e) => {
                        warn!("PKM request failed: {}", e);
                        
                        // Submit error response
                        if let Some(ctx) = &self.context {
                            let synthesis_event = Event::from(RpcPkmResponsePayload {
                                request_id: event.payload.get("request_id").cloned(),
                                response: None,
                                error: Some(RpcError {
                                    code: -32603,
                                    message: format!("PKM service error: {}", e),
                                }),
                                service: "pkm".to_string(),
                            });

                            ctx.send_event(synthesis_event).await?;
                        }
                        failed_ids.push((event_id, e.to_string()));
                    }
                }
            } else {
                // Skip non-PKM events
                successful_ids.push(event_id);
            }
        }

        Ok(BatchProcessingResult {
            successful_ids,
            failed_ids,
            retry_ids: Vec::new(),
            checkpoint_data: None})
    }

    async fn get_checkpoint_data(&self) -> Option<serde_json::Value> {
        None
    }
}

#[async_trait]
impl StatefulStreamProcessor for PkmServiceProcessor {
    async fn initialize(&mut self, ctx: StreamProcessorContext) -> SatelliteResult<()> {
        info!("Initializing PKM service automaton");
        
        // Initialize the PKM service
        let service = Arc::new(PkmService::new(ctx.db_pool.clone()));
        
        self.service = Some(service);
        self.context = Some(ctx);
        
        info!("PKM service automaton initialized successfully");
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
                // Real-time processing from Redis Stream
                info!("Starting continuous PKM request processing from Redis Stream");
                
                let ctx = self.context.as_ref().unwrap();
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "pkm-service".to_string(),
                    Self::event_filters(),
                );

                // This will run indefinitely for continuous mode
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
                    warnings: Vec::new()})
            }
            TimeHorizon::Historical { end_time } => {
                // Query historical events from PostgreSQL
                info!(
                    "Processing historical PKM requests up to {}",
                    end_time
                );

                let ctx = self.context.as_ref().unwrap();
                
                // Determine start time from checkpoint
                let start_time = match from {
                    Checkpoint::Timestamp { timestamp, .. } => timestamp,
                    _ => end_time - chrono::Duration::days(7), // Default to 7 days
                };

                // Query RPC request events
                let events = ctx.db_pool.events()
                    .get_events_by_type_and_time_range(
                        "rpc.pkm",
                        "request",
                        start_time,
                        end_time,
                        Some(1000),
                        None,
                    )
                    .await?;

                // Process using batch processor
                let mut redis_consumer = RedisStreamConsumer::from_context(
                    ctx.redis_client.clone(),
                    "pkm-service".to_string(),
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
                    warnings: Vec::new()})
            }
            TimeHorizon::Snapshot => {
                // No snapshot mode for PKM service
                Ok(ScanReport {
                    events_processed: 0,
                    duration: Utc::now() - start_time,
                    final_checkpoint: Some(Checkpoint::None),
                    time_range: None,
                    processor_stats: HashMap::new(),
                    successful_targets: vec!["snapshot".to_string()],
                    failed_targets: HashMap::new(),
                    warnings: vec!["PKM service does not support snapshot mode".to_string()]})
            }
        }
    }

    fn processor_name(&self) -> &str {
        "pkm-service"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Automaton
    }

    async fn current_checkpoint(&self) -> SatelliteResult<Checkpoint> {
        Ok(Checkpoint::None)
    }
}

impl Default for PkmServiceProcessor {
    fn default() -> Self {
        Self::new()
    }
}