//! Content Service Automaton
//!
//! This automaton provides content storage and retrieval capabilities,
//! extracting the content functionality from the gateway monolith.

use async_trait::async_trait;
use serde_json::{json, Value};
use sinex_satellite_sdk::{
    EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, ProcessingResult,
};
use sinex_satellite_sdk::{SatelliteError, SatelliteResult};
use sinex_services::ContentService;
use std::sync::Arc;
use tracing::{info, warn};

/// Content service automaton that responds to content requests
pub struct ContentServiceAutomaton {
    context: Option<HotlogAutomatonContext>,
    service: Option<Arc<ContentService>>,
}

impl ContentServiceAutomaton {
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
            _ => Err(SatelliteError::Automaton(format!(
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
            .ok_or_else(|| SatelliteError::Automaton("Missing content".to_string()))?;

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
            .map_err(|e| SatelliteError::Automaton(format!("Content storage error: {}", e)))?;

        Ok(json!({ "annex_key": annex_key }))
    }

    async fn handle_retrieve_blob(
        &self,
        service: &ContentService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let annex_key = params
            .get("annex_key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SatelliteError::Automaton("Missing annex_key".to_string()))?;

        let content = service
            .retrieve_content(annex_key)
            .await
            .map_err(|e| SatelliteError::Automaton(format!("Content retrieval error: {}", e)))?;

        let content_str =
            String::from_utf8(content).unwrap_or_else(|_| "<binary content>".to_string());

        Ok(json!({ "content": content_str }))
    }
}

impl Default for ContentServiceAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HotlogAutomaton for ContentServiceAutomaton {
    fn automaton_name(&self) -> &str {
        "content-service"
    }

    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing content service automaton");

        // Initialize the content service with blob manager
        let annex_path = std::path::PathBuf::from(
            std::env::var("SINEX_ANNEX_PATH").unwrap_or_else(|_| "/tmp/sinex-annex".to_string()),
        );

        // Ensure the annex directory exists
        std::fs::create_dir_all(&annex_path).map_err(|e| {
            SatelliteError::Automaton(format!("Failed to create annex directory: {}", e))
        })?;

        let annex_config = sinex_annex::AnnexConfig {
            repo_path: annex_path,
            num_copies: None,
            large_files: None,
        };

        let blob_manager = Arc::new(
            sinex_annex::BlobManager::new(annex_config, ctx.db_pool.clone()).map_err(|e| {
                SatelliteError::Automaton(format!("Failed to create blob manager: {}", e))
            })?,
        );

        let service = Arc::new(ContentService::new(ctx.db_pool.clone(), blob_manager));

        self.service = Some(service);
        self.context = Some(ctx);

        info!("Content service automaton initialized successfully");
        Ok(())
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Listen for content RPC requests
            EventFilter::new(Some("rpc.content".to_string()), Some("request".to_string())),
        ]
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult> {
        let payload = event.event.payload.clone();

        // Handle content RPC requests
        if event.event.source == "rpc.content" && event.event.event_type == "request" {
            match self.handle_content_request(payload.clone()).await {
                Ok(response) => {
                    // Submit response as synthesis event
                    let _ctx = self.context.as_ref().unwrap();

                    let _response_event = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "response": response,
                        "service": "content"
                    });

                    // For now, just log - synthesis events need to be implemented in gRPC
                    info!("Service response logged");

                    Ok(ProcessingResult::Success {
                        checkpoint_data: None,
                    })
                }
                Err(e) => {
                    warn!("Content request failed: {}", e);

                    // Submit error response
                    let _ctx = self.context.as_ref().unwrap();
                    let _error_response = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "error": {
                            "code": -32603,
                            "message": format!("Content service error: {}", e)
                        },
                        "service": "content"
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
                reason: "Not a content request".to_string(),
            })
        }
    }
}
