//! PKM Service Automaton
//!
//! This automaton provides personal knowledge management capabilities,
//! extracting the PKM functionality from the gateway monolith.

use async_trait::async_trait;
use serde_json::{json, Value};
use sinex_satellite_sdk::{
    EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, ProcessingResult,
};
use sinex_satellite_sdk::{SatelliteError, SatelliteResult};
use sinex_services::PkmService;
use sinex_ulid::Ulid;
use std::sync::Arc;
use tracing::{info, warn};

/// PKM service automaton that responds to PKM requests
pub struct PkmServiceAutomaton {
    context: Option<HotlogAutomatonContext>,
    service: Option<Arc<PkmService>>,
}

impl PkmServiceAutomaton {
    pub fn new() -> Self {
        Self {
            context: None,
            service: None,
        }
    }

    /// Handle PKM RPC request
    async fn handle_pkm_request(&self, request: Value) -> SatelliteResult<Value> {
        let service = self.service.as_ref().unwrap();
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));

        match method {
            "pkm.create_note" => self.handle_create_note(service, params).await,
            "pkm.create_entities_from_list" => self.handle_create_entities(service, params).await,
            "pkm.link_entities" => self.handle_link_entities(service, params).await,
            _ => Err(SatelliteError::Automaton(format!(
                "Unknown PKM method: {}",
                method
            ))),
        }
    }

    async fn handle_create_note(
        &self,
        service: &PkmService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let event_id = params
            .get("event_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| SatelliteError::Automaton("Invalid or missing event_id".to_string()))?;

        let content = params
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::Automaton("Missing content".to_string()))?;

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

        let annotation_id = service
            .create_note(event_id, content, tags, created_by)
            .await
            .map_err(|e| SatelliteError::Automaton(format!("PKM error: {}", e)))?;

        Ok(json!({ "annotation_id": annotation_id.to_string() }))
    }

    async fn handle_create_entities(
        &self,
        service: &PkmService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let event_id = params
            .get("event_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| SatelliteError::Automaton("Invalid or missing event_id".to_string()))?;

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

        let entity_ids = service
            .create_entities_from_list(event_id, entities)
            .await
            .map_err(|e| SatelliteError::Automaton(format!("PKM error: {}", e)))?;

        Ok(json!({ "entity_ids": entity_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>() }))
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
                SatelliteError::Automaton("Invalid or missing from_entity_id".to_string())
            })?;

        let to_entity_id = params
            .get("to_entity_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Ulid>().ok())
            .ok_or_else(|| {
                SatelliteError::Automaton("Invalid or missing to_entity_id".to_string())
            })?;

        let relationship_type = params
            .get("relationship_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::Automaton("Missing relationship_type".to_string()))?;

        let properties = params
            .get("properties")
            .and_then(|v| v.as_object())
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let relation_id = service
            .link_entities(from_entity_id, to_entity_id, relationship_type, properties)
            .await
            .map_err(|e| SatelliteError::Automaton(format!("PKM error: {}", e)))?;

        Ok(json!({ "relation_id": relation_id.to_string() }))
    }
}

impl Default for PkmServiceAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HotlogAutomaton for PkmServiceAutomaton {
    fn automaton_name(&self) -> &str {
        "pkm-service"
    }

    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing PKM service automaton");

        // Initialize the PKM service
        let service = Arc::new(PkmService::new(ctx.db_pool.clone()));

        self.service = Some(service);
        self.context = Some(ctx);

        info!("PKM service automaton initialized successfully");
        Ok(())
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Listen for PKM RPC requests
            EventFilter::new(Some("rpc.pkm".to_string()), Some("request".to_string())),
        ]
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult> {
        let payload = event.event.payload.clone();

        // Handle PKM RPC requests
        if event.event.source == "rpc.pkm" && event.event.event_type == "request" {
            match self.handle_pkm_request(payload.clone()).await {
                Ok(response) => {
                    // Submit response as synthesis event
                    let _ctx = self.context.as_ref().unwrap();

                    let _response_event = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "response": response,
                        "service": "pkm"
                    });

                    // For now, just log - synthesis events need to be implemented in gRPC
                    info!("Service response logged");

                    Ok(ProcessingResult::Success {
                        checkpoint_data: None,
                    })
                }
                Err(e) => {
                    warn!("PKM request failed: {}", e);

                    // Submit error response
                    let _ctx = self.context.as_ref().unwrap();
                    let _error_response = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "error": {
                            "code": -32603,
                            "message": format!("PKM service error: {}", e)
                        },
                        "service": "pkm"
                    });

                    // For now, just log - synthesis events need to be implemented in gRPC
                    info!("Service response logged");

                    Ok(ProcessingResult::Success {
                        checkpoint_data: None,
                    })
                }
            }
        } else {
            Ok(ProcessingResult::Skip {
                reason: "Not a PKM request".to_string(),
            })
        }
    }
}
